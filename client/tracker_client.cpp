#include "tracker_client.h"
#include "../common/net.h"

#include <unistd.h>

namespace p2p {

bool TrackerClient::connect_any() {
    std::lock_guard<std::mutex> g(__mu_lock);
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    for (size_t i = 0; i < eps_.size(); i++) {
        size_t idx = (current_ + i) % eps_.size();
        int fd = tcp_connect(eps_[idx].ip, eps_[idx].port);
        if (fd >= 0) { fd_ = fd; current_ = idx; return true; }
    }
    return false;
}

void TrackerClient::disconnect() {
    std::lock_guard<std::mutex> g(__mu_lock);
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

bool TrackerClient::request(uint16_t type, const std::string& payload,
                            uint16_t& status, std::string& resp) {
    std::lock_guard<std::mutex> g(__mu_lock);
    for (int attempt = 0; attempt < 2; attempt++) {
        if (fd_ < 0) {
            bool ok = false;
            for (size_t i = 0; i < eps_.size(); i++) {
                size_t idx = (current_ + 1 + i) % eps_.size();
                int fd = tcp_connect(eps_[idx].ip, eps_[idx].port);
                if (fd >= 0) { 
                    fd_ = fd; 
                    current_ = idx; 
                    ok = true; 
                    break; 
                }
            }
            if (!ok) return false;
            // TODO: reconnect after failing over
            // Re-send MSG_LOGIN, or the user will
            // start getting ST_NOT_LOGGED_IN.
        }
        if (send_msg(fd_, type, 0, payload) != 1) { ::close(fd_); fd_ = -1; continue; }
        MsgHeader hdr;
        if (recv_msg(fd_, hdr, resp) != 1)        { ::close(fd_); fd_ = -1; continue; }
        status = hdr.status;
        return true;
    }
    return false;
}

} // namespace p2p
