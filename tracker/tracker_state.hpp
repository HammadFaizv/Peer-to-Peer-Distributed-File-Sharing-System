#pragma once
// In-memory metadata for the tracker. Every public method takes
// the lock, so callers (one thread per client connection) need no extra
// synchronisation.
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "../common/protocol.hpp"

namespace p2p {

struct PeerRef {
    std::string user_id;
    std::string ip;
    uint16_t    port = 0;
    // Which pieces this peer currently holds. A partial downloader is still a
    // useful source, so do not wait for completion before adding it here.
    std::vector<uint8_t> bitfield;
};

struct FileMeta {
    std::string              name;
    uint64_t                 size = 0;
    std::string              file_hash;      // SHA1 of whole file
    std::vector<std::string> piece_hashes;   // SHA1 of each PIECE_SIZE chunk
    std::map<std::string, PeerRef> seeders;  // user_id -> peer
};

struct Group {
    std::string           id;
    std::string           owner;
    std::set<std::string> members;
    std::set<std::string> pending;           // join requests awaiting the owner
    std::map<std::string, FileMeta> files;   // file name -> metadata
};

struct User {
    std::string id;
    std::string password;                    // SHA1 hex digest, never plaintext
    bool        online = false;
    std::string ip;
    uint16_t    port = 0;                    // the client's own seeder port
};

class TrackerState {
public:
    Status create_user(const std::string& uid, const std::string& pwd);
    Status login(const std::string& uid, const std::string& pwd,
                 const std::string& ip, uint16_t port);
    Status logout(const std::string& uid);
    bool   is_online(const std::string& uid);
    bool   empty();

    Status create_group(const std::string& gid, const std::string& owner);
    Status join_group(const std::string& gid, const std::string& uid);
    Status leave_group(const std::string& gid, const std::string& uid);
    Status accept_request(const std::string& gid, const std::string& owner,
                          const std::string& uid);
    std::vector<std::string> list_groups();
    Status list_requests(const std::string& gid, const std::string& owner,
                         std::vector<std::string>& out);
                         
    Status add_file(const std::string& gid, const std::string& uid,
                    const std::string& ip, uint16_t port,
                    const FileMeta& meta);
    Status list_files(const std::string& gid, const std::string& uid,
                      std::vector<std::string>& out);
    Status get_file(const std::string& gid, const std::string& uid,
                    const std::string& fname, FileMeta& out);
    Status stop_share(const std::string& gid, const std::string& uid,
                      const std::string& fname);
    Status update_bitfield(const std::string& gid, const std::string& uid,
                           const std::string& ip, uint16_t port,
                           const std::string& fname,
                           const std::vector<uint8_t>& bits);

    // Serialize / restore whole state — used by a tracker rejoining the
    // cluster to bootstrap before replaying the operation log.
    std::string snapshot();
    bool        restore(const std::string& blob);

private:
    std::mutex __mu_lock;
    std::map<std::string, User>  __Users;
    std::map<std::string, Group> __Groups;
};

} // namespace p2p
