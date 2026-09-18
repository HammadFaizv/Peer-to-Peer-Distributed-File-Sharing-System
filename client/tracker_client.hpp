#pragma once
// Single connection to whichever tracker is reachable, with failover.
#include <mutex>
#include <string>
#include <vector>

#include "../common/protocol.hpp"

namespace p2p {

struct TrackerEndpoint { 
    std::string ip; 
    uint16_t port; 
};

class TrackerClient {
public:
    void configure(std::vector<TrackerEndpoint> eps) { eps_ = std::move(eps); }

    bool connect_any();   // tries each endpoint in turn
    void disconnect();

    // Sends a request and waits for the MSG_RESPONSE. On a dead connection it
    // reconnects to the other tracker and retries once
    // If a login is on file and the request has to
    // establish a fresh connection, it transparently replays MSG_LOGIN on
    // that connection first, so a fail-over doesn't surface ST_NOT_LOGGED_IN
    // for every command until the user manually logs in again.
    // the above issue has been checked out.
    bool request(uint16_t type, const std::string& payload,
                 uint16_t& status, std::string& resp);

    // Call after a successful MSG_LOGIN so a later fail-over can replay it;
    // clear_login() after MSG_LOGOUT so it stops doing that.
    void note_login(const std::string& uid, const std::string& pwd, uint16_t port);
    void clear_login();

private:
    std::mutex __mu_lock;
    std::vector<TrackerEndpoint> eps_;
    int fd_ = -1;
    size_t current_ = 0;

    bool        logged_in_ = false;
    std::string last_uid_, last_pwd_;
    uint16_t    last_port_ = 0;
};

} // namespace p2p
