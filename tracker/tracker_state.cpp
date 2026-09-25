// This is the tracker state where all information regarding trackers is handled.
// currently using maps to handle all user, groups and metafiles. Maybe in future use file systems.
// Here all info a tracker can have is coded.

#include "tracker_state.hpp"
#include "../common/buffer.hpp"
#include "../common/sha1.hpp"

namespace p2p {

// for all states check if corresponding group, user exists or not
// for updating ones like join / leave group check if user, grp already exists or not
// always use mutex to handle state

namespace {
std::string hash_password(const std::string& pwd) {
    return SHA1::hash_buffer(pwd.data(), pwd.size());
}
} // namespace

Status TrackerState::create_user(const std::string& uid, const std::string& pwd) {
    if (uid.empty() || pwd.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    if (__Users.count(uid)) return ST_ALREADY_EXISTS;
    User u;
    u.id = uid;
    u.password = hash_password(pwd);
    __Users[uid] = u;
    return ST_OK;
}

Status TrackerState::login(const std::string& uid, const std::string& pwd,
                           const std::string& ip, uint16_t port) {
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Users.find(uid);
    if (it == __Users.end()) return ST_NOT_FOUND;
    if (it->second.password != hash_password(pwd)) return ST_BAD_CREDENTIALS;
    it->second.online = true;
    it->second.ip = ip;
    it->second.port = port;
    return ST_OK;
}

Status TrackerState::logout(const std::string& uid) {
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Users.find(uid);
    if (it == __Users.end()) return ST_NOT_FOUND;
    it->second.online = false;
    // A logged-out user stops seeding — drop it from every file's seeder
    // map now so a peer list handed out later doesn't include someone
    // nobody can reach. O(files), but logout/disconnect is rare next to
    // reads of that same list.
    for (auto& gkv : __Groups)
        for (auto& fkv : gkv.second.files)
            fkv.second.seeders.erase(uid);
    return ST_OK;
}

bool TrackerState::is_online(const std::string& uid) {
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Users.find(uid);
    return it != __Users.end() && it->second.online;
}

bool TrackerState::empty() {
    std::lock_guard<std::mutex> g(__mu_lock);
    return __Users.empty() && __Groups.empty();
}

std::vector<std::string> TrackerState::list_groups() {
    std::lock_guard<std::mutex> g(__mu_lock);
    std::vector<std::string> out;
    out.reserve(__Groups.size());
    for (const auto& kv : __Groups) out.push_back(kv.first);
    return out;
}

Status TrackerState::create_group(const std::string& gid, const std::string& owner) {
    if (gid.empty() || owner.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    if (!__Users.count(owner)) return ST_NOT_FOUND; // owner must exist
    if (__Groups.count(gid)) return ST_ALREADY_EXISTS; // no duplicate group ids
    Group grp;
    grp.id = gid;
    grp.owner = owner;
    grp.members.insert(owner); // the creator will be a member
    __Groups[gid] = grp;
    return ST_OK;
}

Status TrackerState::join_group(const std::string& gid, const std::string& uid) {
    if (gid.empty() || uid.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND; // no grp
    if (!__Users.count(uid)) return ST_NOT_FOUND; // no user
    Group& grp = it->second;
    if (grp.members.count(uid) || grp.pending.count(uid)) return ST_ALREADY_EXISTS;
    grp.pending.insert(uid);
    return ST_OK;
}

//If the owner leaves, ownership passes to the lexicographically smallest
//member (`*members.begin()` on set) — deterministic so both trackers land on the same
//new owner after independently replaying the same leave_group op. If no
//members remain, the group is dissolved. We can send the new owner string but no.
Status TrackerState::leave_group(const std::string& gid, const std::string& uid) {
    if (gid.empty() || uid.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND;
    Group& grp = it->second;
    if (!grp.members.count(uid)) return ST_NOT_MEMBER;
    grp.members.erase(uid);
    if (uid == grp.owner) {
        if (grp.members.empty()) __Groups.erase(it); // last member out: dissolve
        else grp.owner = *grp.members.begin(); // deterministic handoff
    }
    return ST_OK;
}

Status TrackerState::accept_request(const std::string& gid, const std::string& owner,
                                    const std::string& uid) {
    if (gid.empty() || owner.empty() || uid.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND;
    Group& grp = it->second;
    if (grp.owner != owner) return ST_NOT_OWNER;
    auto pit = grp.pending.find(uid);
    if (pit == grp.pending.end()) return ST_NOT_FOUND; // no such pending request
    grp.pending.erase(pit);
    grp.members.insert(uid);
    return ST_OK;
}

Status TrackerState::list_requests(const std::string& gid, const std::string& owner,
                                   std::vector<std::string>& out) {
    if (gid.empty() || owner.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND;
    const Group& grp = it->second;
    if (grp.owner != owner) return ST_NOT_OWNER;
    out.assign(grp.pending.begin(), grp.pending.end());
    return ST_OK;
}

Status TrackerState::add_file(const std::string& gid, const std::string& uid,
                              const std::string& ip, uint16_t port,
                              const FileMeta& meta) {
    if (gid.empty() || uid.empty() || meta.name.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND;
    Group& grp = it->second;
    if (!grp.members.count(uid)) return ST_NOT_MEMBER;

    // The uploader starts out holding every piece.
    PeerRef pr;
    pr.user_id = uid;
    pr.ip = ip;
    pr.port = port;
    pr.bitfield.assign((meta.piece_hashes.size() + 7) / 8, 0xFFu);

    auto fit = grp.files.find(meta.name);
    if (fit != grp.files.end()) {
        // Re-uploading identical content (e.g. a seeder that crashed and came
        // back, whose seeder entry logout() dropped) just re-registers it.
        if (fit->second.file_hash != meta.file_hash) return ST_ALREADY_EXISTS;
        fit->second.seeders[uid] = pr;
        return ST_OK;
    }

    FileMeta fm = meta;
    fm.seeders.clear();
    fm.seeders[uid] = pr;
    grp.files[meta.name] = std::move(fm);
    return ST_OK;
}

Status TrackerState::list_files(const std::string& gid, const std::string& uid,
                                std::vector<std::string>& out) {
    if (gid.empty() || uid.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND;
    Group& grp = it->second;
    if (!grp.members.count(uid)) return ST_NOT_MEMBER;
    out.clear();
    for (const auto& kv : grp.files) out.push_back(kv.first);
    return ST_OK;
}

Status TrackerState::get_file(const std::string& gid, const std::string& uid,
                              const std::string& fname, FileMeta& out) {
    if (gid.empty() || uid.empty() || fname.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND;
    Group& grp = it->second;
    if (!grp.members.count(uid)) return ST_NOT_MEMBER;
    auto fit = grp.files.find(fname);
    if (fit == grp.files.end()) return ST_NOT_FOUND;

    out = fit->second;
    // Deliberately NOT filtered by User::online here: MSG_LOGIN/MSG_LOGOUT
    // are per-connection session state and are not replicated between
    // trackers (see SyncManager), so a tracker other than the one a seeder
    // is actually connected to has no reliable view of that flag. Instead,
    // hand back every recorded seeder and let the downloader's own
    // peer_worker connection attempt be the liveness check — a dead peer
    // just fails to connect and its pieces get retried elsewhere, same as
    // any real BitTorrent-style tracker/client split of responsibility.
    return ST_OK;
}

Status TrackerState::stop_share(const std::string& gid, const std::string& uid,
                                const std::string& fname) {
    if (gid.empty() || uid.empty() || fname.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND;
    Group& grp = it->second;
    auto f_it = grp.files.find(fname);
    if (f_it == grp.files.end()) return ST_NOT_FOUND;
    auto sit = f_it->second.seeders.find(uid);
    if (sit == f_it->second.seeders.end()) return ST_NOT_FOUND; // wasn't seeding it
    f_it->second.seeders.erase(sit);
    // The file's metadata (hashes, size) stays even with zero seeders — the
    // record of what was once shared is still useful for peers rejoining it.
    return ST_OK;
}

Status TrackerState::update_bitfield(const std::string& gid, const std::string& uid,
                                     const std::string& ip, uint16_t port,
                                     const std::string& fname,
                                     const std::vector<uint8_t>& bits) {
    if (gid.empty() || uid.empty() || fname.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND;
    Group& grp = it->second;
    if (!grp.members.count(uid)) return ST_NOT_MEMBER;
    auto f_it = grp.files.find(fname);
    if (f_it == grp.files.end()) return ST_NOT_FOUND;

    // Upsert: a partial downloader becomes a seeder the moment it announces
    // its first piece, it need not have been present at add_file() time.
    PeerRef& pr = f_it->second.seeders[uid];
    pr.user_id = uid;
    pr.ip = ip;
    pr.port = port;
    pr.bitfield = bits;
    return ST_OK;
}

std::string TrackerState::snapshot() {
    std::lock_guard<std::mutex> g(__mu_lock);
    Buffer out;

    out.put_u32(static_cast<uint32_t>(__Users.size()));
    for (const auto& ukv : __Users) {
        const User& u = ukv.second;
        out.put_str(u.id);
        out.put_str(u.password);
        out.put_u8(u.online ? 1 : 0);
        out.put_str(u.ip);
        out.put_u16(u.port);
    }

    out.put_u32(static_cast<uint32_t>(__Groups.size()));
    for (const auto& gkv : __Groups) {
        const Group& grp = gkv.second;
        out.put_str(grp.id);
        out.put_str(grp.owner);

        out.put_u32(static_cast<uint32_t>(grp.members.size()));
        for (const auto& m : grp.members) out.put_str(m);

        out.put_u32(static_cast<uint32_t>(grp.pending.size()));
        for (const auto& p : grp.pending) out.put_str(p);

        out.put_u32(static_cast<uint32_t>(grp.files.size()));
        for (const auto& fkv : grp.files) {
            const FileMeta& fm = fkv.second;
            out.put_str(fm.name);
            out.put_u64(fm.size);
            out.put_str(fm.file_hash);

            out.put_u32(static_cast<uint32_t>(fm.piece_hashes.size()));
            for (const auto& h : fm.piece_hashes) out.put_str(h);

            out.put_u32(static_cast<uint32_t>(fm.seeders.size()));
            for (const auto& skv : fm.seeders) {
                const PeerRef& pr = skv.second;
                out.put_str(pr.user_id);
                out.put_str(pr.ip);
                out.put_u16(pr.port);
                out.put_u32(static_cast<uint32_t>(pr.bitfield.size()));
                out.put_raw(pr.bitfield.data(), pr.bitfield.size());
            }
        }
    }
    return out.str();
}

bool TrackerState::restore(const std::string& blob) {
    Buffer in(blob);

    std::map<std::string, User> users;
    uint32_t nusers = 0;
    if (!in.get_u32(nusers)) return false;
    for (uint32_t i = 0; i < nusers; i++) {
        User u;
        uint8_t online = 0;
        if (!in.get_str(u.id) || !in.get_str(u.password) || !in.get_u8(online) ||
            !in.get_str(u.ip) || !in.get_u16(u.port)) return false;
        u.online = online != 0;
        users[u.id] = std::move(u);
    }

    std::map<std::string, Group> groups;
    uint32_t ngroups = 0;
    if (!in.get_u32(ngroups)) return false;
    for (uint32_t i = 0; i < ngroups; i++) {
        Group grp;
        if (!in.get_str(grp.id) || !in.get_str(grp.owner)) return false;

        uint32_t nmembers = 0;
        if (!in.get_u32(nmembers)) return false;
        for (uint32_t j = 0; j < nmembers; j++) {
            std::string m;
            if (!in.get_str(m)) return false;
            grp.members.insert(m);
        }

        uint32_t npending = 0;
        if (!in.get_u32(npending)) return false;
        for (uint32_t j = 0; j < npending; j++) {
            std::string p;
            if (!in.get_str(p)) return false;
            grp.pending.insert(p);
        }

        uint32_t nfiles = 0;
        if (!in.get_u32(nfiles)) return false;
        for (uint32_t j = 0; j < nfiles; j++) {
            FileMeta fm;
            if (!in.get_str(fm.name) || !in.get_u64(fm.size) || !in.get_str(fm.file_hash)) return false;

            uint32_t npieces = 0;
            if (!in.get_u32(npieces)) return false;
            fm.piece_hashes.resize(npieces);
            for (uint32_t k = 0; k < npieces; k++)
                if (!in.get_str(fm.piece_hashes[k])) return false;

            uint32_t nseeders = 0;
            if (!in.get_u32(nseeders)) return false;
            for (uint32_t k = 0; k < nseeders; k++) {
                PeerRef pr;
                uint32_t nbits = 0;
                if (!in.get_str(pr.user_id) || !in.get_str(pr.ip) || !in.get_u16(pr.port) ||
                    !in.get_u32(nbits)) return false;
                pr.bitfield.resize(nbits);
                if (nbits > 0 && !in.get_raw(pr.bitfield.data(), nbits)) return false;
                fm.seeders[pr.user_id] = std::move(pr);
            }
            grp.files[fm.name] = std::move(fm);
        }
        groups[grp.id] = std::move(grp);
    }

    std::lock_guard<std::mutex> g(__mu_lock);
    __Users = std::move(users);
    __Groups = std::move(groups);
    return true;
}

} // namespace p2p
