// This is the tracker state where all information regarding trackers is handled.
// currently using maps to handle all user, groups and metafiles. Maybe in future use file systems.
// Here all info a tracker can have is coded.

#include "tracker_state.h"

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

// TODO: file metadata methods left ... FOOK!!!.

Status TrackerState::add_file(const std::string&, const std::string&, const FileMeta&) { return ST_NOT_IMPLEMENTED; }
Status TrackerState::list_files(const std::string&, const std::string&, std::vector<std::string>&) { return ST_NOT_IMPLEMENTED; }
Status TrackerState::get_file(const std::string&, const std::string&, const std::string&, FileMeta&) { return ST_NOT_IMPLEMENTED; }
Status TrackerState::stop_share(const std::string&, const std::string&, const std::string&) { return ST_NOT_IMPLEMENTED; }
Status TrackerState::update_bitfield(const std::string&, const std::string&, const std::string&, const std::vector<uint8_t>&) { return ST_NOT_IMPLEMENTED; }

std::string TrackerState::snapshot() { return std::string(); }   // TODO
bool TrackerState::restore(const std::string&) { return false; } // TODO

} // namespace p2p
