#pragma once
// Single connection to whichever tracker is reachable, with failover.
#include <mutex>
#include <string>
#include <vector>

#include "../common/protocol.h"

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
    // reconnects to the other tracker and retries once — this is what makes
    // "keeps working while one tracker is down" true from the client's side.
    bool request(uint16_t type, const std::string& payload,
                 uint16_t& status, std::string& resp);

private:
    std::mutex __mu_lock;
    std::vector<TrackerEndpoint> eps_;
    int fd_ = -1;
    size_t current_ = 0;
};

} // namespace p2p
