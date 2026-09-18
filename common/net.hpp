#pragma once
#include <cstdint>
#include <string>
#include "protocol.hpp"

namespace p2p {

// 1 = success
// 0 = clean peer close
// -1 = error
int send_all(int fd, const void* buf, size_t n);
int recv_all(int fd, void* buf, size_t n);
int send_msg(int fd, uint16_t type, uint16_t status, const std::string& payload);
int recv_msg(int fd, MsgHeader& hdr, std::string& payload);

int tcp_connect(const std::string& ip, uint16_t port);              // -1 on failure
int tcp_listen(const std::string& ip, uint16_t port, int backlog = 64);
bool parse_endpoint(const std::string& s, std::string& ip, uint16_t& port); // "1.2.3.4:5000"

} // namespace p2p
