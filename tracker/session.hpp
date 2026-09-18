#pragma once
// One instance per connected client, run on its own thread.
#include <string>
#include "tracker_state.hpp"
#include "sync.hpp"

namespace p2p {

class Session {
public:
    Session(int fd, std::string peer_ip, TrackerState& st, SyncManager& sync)
        : fd_(fd), __peer_ip(std::move(peer_ip)), __state(st), sync_(sync) {}

    void run();   // read/dispatch loop; closes fd_ on exit

    // Process one message the caller already read off fd_ (main.cpp has to
    // peek the first message to tell a client apart from a peer tracker
    // dialing in with MSG_SYNC_HELLO) before handing off to run() for the
    // rest of the connection's messages.
    void handle_one(uint16_t type, const std::string& payload) { dispatch(type, payload); }

private:
    void dispatch(uint16_t type, const std::string& payload);
    void reply(uint16_t status, const std::string& payload = std::string());

    int          fd_;
    std::string  __peer_ip;
    TrackerState& __state;
    SyncManager&  sync_;
    std::string  user_;      // empty until MSG_LOGIN succeeds
    uint16_t     seed_port_ = 0;
};

} // namespace p2p
