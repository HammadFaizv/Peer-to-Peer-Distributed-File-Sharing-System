// ./tracker tracker_info.txt <tracker_no>
//
// tracker_info.txt holds one "ip port" line per tracker, in index order.
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "../common/net.hpp"
#include "tracker_state.hpp"
#include "sync.hpp"
#include "session.hpp"

using namespace p2p;

struct Endpoint {
    std::string ip;
    uint16_t port;
};

static bool load_tracker_info(const char* path, std::vector<Endpoint>& out) {
    std::ifstream f(path);
    if (!f) return false;
    std::string ip;
    int port;
    while (f >>ip>> port) out.push_back({ip, static_cast<uint16_t>(port) });
    return out.size() >= 2;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <tracker_info.txt> <tracker_no>\n", argv[0]);
        return 1;
    }
    std::vector<Endpoint> eps;
    if (!load_tracker_info(argv[1], eps)) {
        std::fprintf(stderr, "could not read two tracker entries from %s\n", argv[1]);
        return 1;
    }
    int idx = std::atoi(argv[2]);
    if (idx < 0 || idx >= static_cast<int>(eps.size())) {
        std::fprintf(stderr, "tracker_no out of range\n");
        return 1;
    }
    const Endpoint& self = eps[idx];
    const Endpoint& peer = eps[idx == 0 ? 1 : 0];

    TrackerState state;
    SyncManager sync(state, idx, peer.ip, peer.port);
    sync.start();

    int lfd = tcp_listen(self.ip, self.port); // start listening for connections
    if (lfd < 0) { 
        std::perror("listen"); 
        return 1; 
    }
    std::fprintf(stderr, "[tracker %d] listening on %s:%u\n", idx, self.ip.c_str(), self.port);
    
    // Console thread: the only command required is `quit`.
    std::thread console([&]() {
        std::string line;
        while (std::getline(std::cin, line)) {
            if (line == "quit") {
                sync.stop();
                ::shutdown(lfd, SHUT_RDWR);
                ::close(lfd);
                std::_Exit(0);   // TODO: drain live sessions first for a clean exit
            }
        }
    });
    console.detach();

    for (;;) {
        sockaddr_in ca{};
        socklen_t cl = sizeof(ca);
        int cfd = ::accept(lfd, reinterpret_cast<sockaddr*>(&ca), &cl); // for whatever listening accept
        if (cfd < 0) continue;
        char ipbuf[INET_ADDRSTRLEN] = {0};
        ::inet_ntop(AF_INET, &ca.sin_addr, ipbuf, sizeof(ipbuf));

        // Peek the first message on its own thread.
        // If it is MSG_SYNC_HELLO -> sync.handle_peer_connection()
        // otherwise replay the peeked message into a fresh Client Session
        std::thread([cfd, ip = std::string(ipbuf), &state, &sync]() {
            MsgHeader hdr;
            std::string payload;
            if (recv_msg(cfd, hdr, payload) != 1) { ::close(cfd); return; }
            if (hdr.type == MSG_SYNC_HELLO) {
                std::fprintf(stderr, "[tracker] inbound peer-tracker link\n");
                sync.handle_peer_connection(cfd); // owns cfd until the link drops
                return;
            }
            Session s(cfd, ip, state, sync); // save data for the connection
            s.handle_one(hdr.type, payload); // handle request
            s.run();
        }).detach();
    }
    return 0;
}
