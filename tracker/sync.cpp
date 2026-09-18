#include "sync.hpp"
#include "../common/net.hpp"
#include "../common/buffer.hpp"

#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <vector>

namespace p2p {

namespace {
uint64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}
} // namespace

SyncManager::SyncManager(TrackerState& st, int self_index,
                         std::string peer_ip, uint16_t peer_port)
    : __state(st), __self_index(self_index),
      __peer_ip(std::move(peer_ip)), __peer_port(peer_port) {}

SyncManager::~SyncManager() { stop(); }

void SyncManager::start() {
    __running = true;
    __conn_thread = std::thread(&SyncManager::connect_loop, this);
}

void SyncManager::stop() {
    {
        std::lock_guard<std::mutex> g(__mu_lock);
        __running = false;
        if (__out_fd >= 0) { ::shutdown(__out_fd, SHUT_RDWR); }
    }
    if (__conn_thread.joinable()) __conn_thread.join();
}

void SyncManager::connect_loop() {
    if (__self_index != 0) return; // we are the acceptor side; nothing to dial

    while (true) {
        {
            std::lock_guard<std::mutex> g(__mu_lock);
            if (!__running) return;
        }
        int fd = tcp_connect(__peer_ip, __peer_port);
        if (fd < 0) {
            std::this_thread::sleep_for(std::chrono::seconds(2)); // fixed backoff
            continue;
        }
        {
            std::lock_guard<std::mutex> g(__mu_lock);
            __out_fd = fd;
        }
        std::fprintf(stderr, "[sync] linked to peer tracker\n");

        // 1. identify ourselves.
        {
            Buffer hello; 
            hello.put_u8(static_cast<uint8_t>(__self_index));
            std::lock_guard<std::mutex> g(__send_mu);
            send_msg(fd, MSG_SYNC_HELLO, 0, hello.str());
        }
        // 2. ask the peer to bring us up to date. A process that just
        // (re)started has empty state, so its own trimmed op log can't
        // reconstruct history — ask for a full snapshot instead. Otherwise
        // this is just a link drop and the catch-up replay suffices.
        if (__state.empty()) {
            std::lock_guard<std::mutex> g(__send_mu);
            send_msg(fd, MSG_SYNC_SNAPSHOT_REQUEST, 0, "");
        } else {
            uint64_t after;
            { std::lock_guard<std::mutex> g(__mu_lock); after = __applied_peer_seq; }
            Buffer cu;
            cu.put_u64(after);
            std::lock_guard<std::mutex> g(__send_mu);
            send_msg(fd, MSG_SYNC_CATCHUP, 0, cu.str());
        }

        // 3. full-duplex loop: apply whatever the peer sends until it drops.
        MsgHeader hdr;
        std::string payload;
        while (true) {
            int r = recv_msg(fd, hdr, payload);
            if (r != 1) break;
            process_peer_message(fd, hdr.type, payload);
        }

        // 4. link dropped — clean up and retry.
        {
            std::lock_guard<std::mutex> g(__mu_lock);
            if (__out_fd >= 0) { ::close(__out_fd); __out_fd = -1; }
            if (!__running) return;
        }
        std::fprintf(stderr, "[sync] peer link dropped, retrying\n");
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

void SyncManager::record_and_replicate(uint16_t type, const std::string& payload) {
    Op op;
    int fd;
    {
        std::lock_guard<std::mutex> g(__mu_lock);
        op.seq = __next_seq++;
        op.type = type;
        op.payload = payload;
        op.ts = now_ms();
        __log.push_back(op);
        fd = __out_fd;
    }
    if (fd < 0) return; // link is down; op stays in __log for catch-up later
    std::lock_guard<std::mutex> g(__send_mu);
    send_op_locked(fd, op);
    // next catchup delivers what is in logs so nothing is lost
}

void SyncManager::send_op_locked(int fd, const Op& op) {
    Buffer b;
    b.put_u64(op.seq);
    b.put_u16(op.type);
    b.put_u64(op.ts);
    b.put_str(op.payload);
    send_msg(fd, MSG_SYNC_OP, 0, b.str());
}

void SyncManager::handle_peer_connection(int fd) {
    {
        std::lock_guard<std::mutex> g(__mu_lock);
        if (__out_fd >= 0) ::close(__out_fd);
        __out_fd = fd;
    }
    std::fprintf(stderr, "[sync] peer tracker linked to us\n");

    // We only accept inbound links (self_index != 0); we never send our own
    // HELLO/CATCHUP on this side, but if WE are the one that just (re)started
    // — our own state is empty — we still need to ask the dialer for a full
    // snapshot rather than silently sitting on nothing.
    if (__state.empty()) {
        std::lock_guard<std::mutex> g(__send_mu);
        send_msg(fd, MSG_SYNC_SNAPSHOT_REQUEST, 0, "");
    }

    MsgHeader hdr;
    std::string payload;
    while (true) {
        int r = recv_msg(fd, hdr, payload);
        if (r != 1) break;
        process_peer_message(fd, hdr.type, payload);
    }

    std::fprintf(stderr, "[sync] peer link dropped\n");
    std::lock_guard<std::mutex> g(__mu_lock);
    if (__out_fd == fd) { ::close(fd); __out_fd = -1; }
    else ::close(fd); // superseded by a newer link already
}

void SyncManager::process_peer_message(int fd, uint16_t type, const std::string& payload) {
    switch (type) {
    case MSG_SYNC_HELLO: {
        // Purely informational; the fd itself is the link. Nothing to do.
        return;
    }
    case MSG_SYNC_CATCHUP: {
        Buffer in(payload);
        uint64_t after = 0;
        if (!in.get_u64(after)) return;
        send_catchup_from(fd, after);
        return;
    }
    case MSG_SYNC_OP: {
        Op op;
        Buffer in(payload);
        if (!in.get_u64(op.seq) || !in.get_u16(op.type) ||
            !in.get_u64(op.ts) || !in.get_str(op.payload)) {
            return; // malformed peer op; drop it rather than corrupt state
        }
        bool ok = apply_remote(op);
        if (ok) {
            std::lock_guard<std::mutex> g(__mu_lock);
            if (op.seq > __applied_peer_seq) __applied_peer_seq = op.seq;
        }
        // ACK regardless: a replay we've already applied (ST_ALREADY_EXISTS)
        // still needs to be ACKed so the sender can trim it from its log.
        Buffer ack; ack.put_u64(op.seq);
        std::lock_guard<std::mutex> g(__send_mu);
        send_msg(fd, MSG_SYNC_ACK, 0, ack.str());
        return;
    }
    case MSG_SYNC_ACK: {
        Buffer in(payload);
        uint64_t seq = 0;
        if (!in.get_u64(seq)) return;
        std::lock_guard<std::mutex> g(__mu_lock);
        while (!__log.empty() && __log.front().seq <= seq) __log.pop_front();
        return;
    }
    case MSG_SYNC_SNAPSHOT_REQUEST: {
        std::string blob = __state.snapshot();
        std::lock_guard<std::mutex> g(__send_mu);
        send_msg(fd, MSG_SYNC_SNAPSHOT_DATA, 0, blob);
        return;
    }
    case MSG_SYNC_SNAPSHOT_DATA: {
        if (__state.restore(payload)) {
            std::fprintf(stderr, "[sync] bootstrapped state from peer snapshot\n");
        } else {
            std::fprintf(stderr, "[sync] malformed snapshot from peer, ignoring\n");
        }
        return;
    }
    default:
        return;
    }
}

bool SyncManager::apply_remote(const Op& op) {
    Buffer in(op.payload);
    switch (op.type) {
    case MSG_CREATE_USER: {
        std::string uid, pwd;
        if (!in.get_str(uid) || !in.get_str(pwd)) return false;
        Status s = __state.create_user(uid, pwd);
        // ST_ALREADY_EXISTS means we already have this — not a failure to replicate.
        return s == ST_OK || s == ST_ALREADY_EXISTS;
    }
    case MSG_CREATE_GROUP: {
        std::string gid, owner;
        if (!in.get_str(gid) || !in.get_str(owner)) return false;
        Status s = __state.create_group(gid, owner);
        return s == ST_OK || s == ST_ALREADY_EXISTS;
    }
    case MSG_JOIN_GROUP: {
        std::string gid, uid;
        if (!in.get_str(gid) || !in.get_str(uid)) return false;
        Status s = __state.join_group(gid, uid);
        return s == ST_OK || s == ST_ALREADY_EXISTS;
    }
    case MSG_LEAVE_GROUP: {
        std::string gid, uid;
        if (!in.get_str(gid) || !in.get_str(uid)) return false;
        Status s = __state.leave_group(gid, uid);
        // ST_NOT_MEMBER on replay means we already applied this leave - not a failure.
        return s == ST_OK || s == ST_NOT_MEMBER;
    }
    case MSG_ACCEPT_REQUEST: {
        std::string gid, owner, uid;
        if (!in.get_str(gid) || !in.get_str(owner) || !in.get_str(uid)) return false;
        Status s = __state.accept_request(gid, owner, uid);
        // ST_NOT_FOUND on replay means the request is already gone
        return s == ST_OK || s == ST_NOT_FOUND;
    }

    case MSG_UPLOAD_FILE: {
        std::string gid;
        FileMeta meta;
        uint32_t pc = 0;
        if (!in.get_str(gid) || !in.get_str(meta.name) || !in.get_u64(meta.size) ||
            !in.get_str(meta.file_hash) || !in.get_u32(pc)) return false;
        meta.piece_hashes.resize(pc);
        for (uint32_t i = 0; i < pc; i++) if (!in.get_str(meta.piece_hashes[i])) return false;
        std::string uid, ip;
        uint16_t port = 0;
        if (!in.get_str(uid) || !in.get_str(ip) || !in.get_u16(port)) return false;
        Status s = __state.add_file(gid, uid, ip, port, meta);
        // ST_ALREADY_EXISTS on replay means we already have this file.
        return s == ST_OK || s == ST_ALREADY_EXISTS;
    }
    case MSG_STOP_SHARE: {
        std::string gid, fname, uid;
        if (!in.get_str(gid) || !in.get_str(fname) || !in.get_str(uid)) return false;
        Status s = __state.stop_share(gid, uid, fname);
        // ST_NOT_FOUND on replay means this seeder is already gone from the file.
        return s == ST_OK || s == ST_NOT_FOUND;
    }
    case MSG_HAVE_PIECES: {
        std::string gid, fname;
        uint32_t nbits = 0;
        if (!in.get_str(gid) || !in.get_str(fname) || !in.get_u32(nbits)) return false;
        std::vector<uint8_t> bits(nbits);
        if (nbits > 0 && !in.get_raw(bits.data(), nbits)) return false;
        std::string uid, ip;
        uint16_t port = 0;
        if (!in.get_str(uid) || !in.get_str(ip) || !in.get_u16(port)) return false;
        Status s = __state.update_bitfield(gid, uid, ip, port, fname, bits);
        return s == ST_OK;
    }

    // MSG_LOGIN/MSG_LOGOUT are per-connection session state, deliberately not replicated.
    default:
        return false;
    }
}

void SyncManager::send_catchup_from(int fd, uint64_t after_seq) {
    std::vector<Op> to_send;
    {
        std::lock_guard<std::mutex> g(__mu_lock);
        for (const auto& op : __log) {
            if (op.seq > after_seq) to_send.push_back(op);
        }
    }
    if (to_send.empty()) return;
    std::lock_guard<std::mutex> g(__send_mu);
    for (const auto& op : to_send) send_op_locked(fd, op);
}

} // namespace p2p
