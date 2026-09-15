#include "net.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <cstdlib>

namespace p2p {

int send_all(int fd, const void* buf, size_t n) {
    const char* p = static_cast<const char*>(buf);
    size_t sent = 0;
    while (sent < n) {
        ssize_t k = send(fd, p + sent, n - sent, MSG_NOSIGNAL);
        if (k < 0) { if (errno == EINTR) continue; return -1; }
        if (k == 0) return 0;
        sent += static_cast<size_t>(k);
    }
    return 1;
}

int recv_all(int fd, void* buf, size_t n) {
    char* p = static_cast<char*>(buf);
    size_t got = 0;
    while (got < n) {
        ssize_t k = recv(fd, p + got, n - got, 0);
        if (k < 0) { if (errno == EINTR) continue; return -1; }
        if (k == 0) return 0; // peer closed
        got += static_cast<size_t>(k);
    }
    return 1;
}

int send_msg(int fd, uint16_t type, uint16_t status, const std::string& payload) {
    MsgHeader h;
    h.magic  = htonl(PROTO_MAGIC);
    h.type   = htons(type);
    h.status = htons(status);
    h.length = htonl(static_cast<uint32_t>(payload.size()));
    int r = send_all(fd, &h, sizeof(h));
    if (r != 1) return r;
    if (payload.empty()) return 1;
    return send_all(fd, payload.data(), payload.size());
}

int recv_msg(int fd, MsgHeader& hdr, std::string& payload) {
    MsgHeader h;
    int r = recv_all(fd, &h, sizeof(h));
    if (r != 1) return r;
    hdr.magic  = ntohl(h.magic);
    hdr.type   = ntohs(h.type);
    hdr.status = ntohs(h.status);
    hdr.length = ntohl(h.length);
    if (hdr.magic != PROTO_MAGIC) return -1;
    if (hdr.length > MAX_PAYLOAD) return -1;  // guards against bogus allocations
    payload.assign(hdr.length, '\0');
    if (hdr.length == 0) return 1;
    return recv_all(fd, &payload[0], hdr.length);
}

int tcp_connect(const std::string& ip, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &a.sin_addr) != 1) { close(fd); return -1; }
    if (connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) < 0) { close(fd); return -1; }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

int tcp_listen(const std::string& ip, uint16_t port, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &a.sin_addr) != 1) { close(fd); return -1; }
    if (::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) < 0) { close(fd); return -1; }
    if (listen(fd, backlog) < 0) { close(fd); return -1; }
    return fd;
}

bool parse_endpoint(const std::string& s, std::string& ip, uint16_t& port) {
    //ip:port = 127.0.0.1:5000
    size_t c = s.find(':');
    if (c == std::string::npos) return false;
    ip = s.substr(0, c);
    long p = std::strtol(s.c_str() + c + 1, nullptr, 10);
    if (p <= 0 || p > 65535) return false;
    port = static_cast<uint16_t>(p);
    return true;
}

const char* status_str(uint16_t s) {
    switch (s) {
        case ST_OK:              return "OK";
        case ST_NOT_LOGGED_IN:   return "not logged in";
        case ST_BAD_CREDENTIALS: return "invalid credentials";
        case ST_ALREADY_EXISTS:  return "already exists";
        case ST_NOT_FOUND:       return "not found";
        case ST_NOT_OWNER:       return "not the group owner";
        case ST_NOT_MEMBER:      return "not a member of this group";
        case ST_NOT_IMPLEMENTED: return "not implemented";
        case ST_MALFORMED:       return "malformed request";
        default:                 return "error";
    }
}

} // namespace p2p
