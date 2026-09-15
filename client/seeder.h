#pragma once
// The server half of the client: accepts peer connections and serves pieces.
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "piece_store.h"

namespace p2p {

// Key for a shared file: group id + file name.
struct ShareKey {
    std::string group;
    std::string file;
    bool operator<(const ShareKey& o) const {
        return group != o.group ? group < o.group : file < o.file;
    }
};

class Seeder {
public:
    bool start(const std::string& ip, uint16_t port);   // binds and spawns accept loop
    void stop();
    uint16_t port() const { return port_; }

    void add_share(const ShareKey& k, std::shared_ptr<PieceStore> store);
    void remove_share(const ShareKey& k);
    std::shared_ptr<PieceStore> find(const ShareKey& k);

private:
    void accept_loop();
    void serve_peer(int fd);   // handles MSG_PIECE_REQUEST / MSG_BITFIELD_REQUEST

    int         lfd_ = -1;
    uint16_t    port_ = 0;
    std::thread accept_thread_;
    bool        __running = false;

    std::mutex  __mu_lock;
    std::map<ShareKey, std::shared_ptr<PieceStore>> shares_;
};

} // namespace p2p
