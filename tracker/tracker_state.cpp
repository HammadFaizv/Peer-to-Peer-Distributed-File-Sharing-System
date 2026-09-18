// This is the tracker state where all information regarding trackers is handled.
// currently using maps to handle all user, groups and metafiles. Maybe in future use file systems.
// Here all info a tracker can have is coded.

#include "tracker_state.hpp"

namespace p2p {

// for all states check if corresponding group, user exists or not
// for updating ones like join / leave group check if user, grp already exists or not
// always use mutex to handle state

Status TrackerState::create_user(const std::string& uid, const std::string& pwd) {
    if (uid.empty() || pwd.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    if (__Users.count(uid)) return ST_ALREADY_EXISTS;
    User u;
    u.id = uid;
    u.password = pwd;
    __Users[uid] = u;
    return ST_OK;
}

Status TrackerState::login(const std::string& uid, const std::string& pwd,
                           const std::string& ip, uint16_t port) {
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Users.find(uid);
    if (it == __Users.end()) return ST_NOT_FOUND;
    if (it->second.password != pwd) return ST_BAD_CREDENTIALS;
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
    // TODO: a logged-out user stops seeding. Decide whether to remove it from
    // every FileMeta::seeders now (simple, but O(files)) or to filter offline
    // peers lazily when answering MSG_GET_FILE_META (cheaper, self-healing).
    return ST_OK;
}

bool TrackerState::is_online(const std::string& uid) {
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Users.find(uid);
    return it != __Users.end() && it->second.online;
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

// --- file metadata ---------------------------------------------------

Status TrackerState::add_file(const std::string& gid, const std::string& uid,
                              const std::string& ip, uint16_t port,
                              const FileMeta& meta) {
    if (gid.empty() || uid.empty() || meta.name.empty()) return ST_MALFORMED;
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = __Groups.find(gid);
    if (it == __Groups.end()) return ST_NOT_FOUND;
    Group& grp = it->second;
    if (!grp.members.count(uid)) return ST_NOT_MEMBER;
    if (grp.files.count(meta.name)) return ST_ALREADY_EXISTS;

    FileMeta fm = meta;
    fm.seeders.clear();
    // The uploader starts out holding every piece.
    PeerRef pr;
    pr.user_id = uid;
    pr.ip = ip;
    pr.port = port;
    pr.bitfield.assign((fm.piece_hashes.size() + 7) / 8, 0xFFu);
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

std::string TrackerState::snapshot() { return std::string(); }   // TODO
bool TrackerState::restore(const std::string&) { return false; } // TODO

} // namespace p2p
