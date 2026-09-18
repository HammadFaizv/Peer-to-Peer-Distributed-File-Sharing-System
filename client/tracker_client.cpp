#include "tracker_client.hpp"
#include "../common/net.hpp"
#include "../common/buffer.hpp"

#include <unistd.h>
#include <cstdio>

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
        bool fresh_connection = false;
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
            fresh_connection = true;
        }

        // A fresh connection after a fail-over has no session on it. Replay
        // the last successful login transparently so the caller's actual
        // request doesn't come back ST_NOT_LOGGED_IN just because it landed
        // on the other tracker.
        if (fresh_connection && logged_in_ && type != MSG_LOGIN) {
            Buffer lb;
            lb.put_str(last_uid_);
            lb.put_str(last_pwd_);
            lb.put_u16(last_port_);
            if (send_msg(fd_, MSG_LOGIN, 0, lb.str()) != 1) { ::close(fd_); fd_ = -1; continue; }
            MsgHeader lhdr;
            std::string lresp;
            if (recv_msg(fd_, lhdr, lresp) != 1) { ::close(fd_); fd_ = -1; continue; }
            if (lhdr.status != ST_OK) {
                std::fprintf(stderr, "[tracker_client] re-login after fail-over failed: %s\n",
                             status_str(lhdr.status));
            }
        }

        if (send_msg(fd_, type, 0, payload) != 1) { ::close(fd_); fd_ = -1; continue; }
        MsgHeader hdr;
        if (recv_msg(fd_, hdr, resp) != 1)        { ::close(fd_); fd_ = -1; continue; }
        status = hdr.status;
        return true;
    }
    return false;
}

void TrackerClient::note_login(const std::string& uid, const std::string& pwd, uint16_t port) {
    std::lock_guard<std::mutex> g(__mu_lock);
    last_uid_ = uid;
    last_pwd_ = pwd;
    last_port_ = port;
    logged_in_ = true;
}

void TrackerClient::clear_login() {
    std::lock_guard<std::mutex> g(__mu_lock);
    logged_in_ = false;
}

} // namespace p2p
