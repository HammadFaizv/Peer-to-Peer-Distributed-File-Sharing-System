#include "seeder.hpp"
#include "../common/net.hpp"
#include "../common/buffer.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstdio>

namespace p2p {

bool Seeder::start(const std::string& ip, uint16_t port) {
    lfd_ = tcp_listen(ip, port);
    if (lfd_ < 0) return false;
    port_ = port;
    __running = true;
    accept_thread_ = std::thread(&Seeder::accept_loop, this);
    return true;
}

void Seeder::stop() {
    __running = false;
    if (lfd_ >= 0) { ::shutdown(lfd_, SHUT_RDWR); ::close(lfd_); lfd_ = -1; }
    if (accept_thread_.joinable()) accept_thread_.join();
}

void Seeder::add_share(const ShareKey& k, std::shared_ptr<PieceStore> store) {
    std::lock_guard<std::mutex> g(__mu_lock);
    shares_[k] = std::move(store);
}

void Seeder::remove_share(const ShareKey& k) {
    std::lock_guard<std::mutex> g(__mu_lock);
    shares_.erase(k);
}

std::shared_ptr<PieceStore> Seeder::find(const ShareKey& k) {
    std::lock_guard<std::mutex> g(__mu_lock);
    auto it = shares_.find(k);
    return it == shares_.end() ? nullptr : it->second;
}

void Seeder::accept_loop() {
    while (__running) {
        int fd = ::accept(lfd_, nullptr, nullptr);
        if (fd < 0) { if (!__running) break; continue; }
        // TODO: a thread per peer connection is fine for 3 clients, but think
        // about an upper bound (a small pool + queue) and say so in the report.
        std::thread(&Seeder::serve_peer, this, fd).detach();
    }
}

void Seeder::serve_peer(int fd) {
    MsgHeader hdr;
    std::string payload;
    while (recv_msg(fd, hdr, payload) == 1) {
        Buffer in(payload);
        if (hdr.type == MSG_PIECE_REQUEST) {
            ShareKey k;
            uint32_t index = 0;
            if (!in.get_str(k.group) || !in.get_str(k.file) || !in.get_u32(index)) {
                send_msg(fd, MSG_PIECE_DATA, ST_MALFORMED, "");
                continue;
            }
            auto store = find(k);
            std::string data;
            if (!store || !store->read_piece(index, data)) {
                send_msg(fd, MSG_PIECE_DATA, ST_NOT_FOUND, "");
                continue;
            }
            Buffer out;
            out.put_u32(index);
            out.put_raw(data.data(), data.size());
            send_msg(fd, MSG_PIECE_DATA, ST_OK, out.str());
        } else if (hdr.type == MSG_BITFIELD_REQUEST) {
            ShareKey k;
            if (!in.get_str(k.group) || !in.get_str(k.file)) {
                send_msg(fd, MSG_BITFIELD_DATA, ST_MALFORMED, "");
                continue;
            }
            auto store = find(k);
            if (!store) {
                send_msg(fd, MSG_BITFIELD_DATA, ST_NOT_FOUND, "");
                continue;
            }
            auto bits = store->bitfield();
            std::string out(reinterpret_cast<const char*>(bits.data()), bits.size());
            send_msg(fd, MSG_BITFIELD_DATA, ST_OK, out);
        } else {
            send_msg(fd, MSG_RESPONSE, ST_NOT_IMPLEMENTED, "");
        }
    }
    ::close(fd);
}

} // namespace p2p
