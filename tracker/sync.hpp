#pragma once
// Tracker-to-tracker replication.
//
// DESIGN (decided):
//
//  * Topology: active/active over a SINGLE physical TCP link between the two
//    trackers. To avoid ending up with two redundant sockets (each side
//    dialing the other), only the lower-indexed tracker (index 0) dials out
//    in connect_loop(); the higher-indexed tracker (index 1) just waits for
//    that inbound connection in its accept loop and hands the fd to
//    handle_peer_connection(). The link is full-duplex: once established,
//    either side can push MSG_SYNC_OP on it whenever a local mutation
//    happens (record_and_replicate), and either side reads and applies
//    whatever the other side pushes.
//  * Every state-changing request produces an Op with a monotonically
//    increasing sequence number, scoped to the tracker that originated it
//    (__next_seq here numbers *our own* ops; the peer numbers its own
//    independently). The op log (__log) is the unit of replication —
//    replaying it from seq N is how a reconnecting peer catches up.
//  * Sync is fully async: record_and_replicate() applies locally first and
//    returns immediately; the network write is best-effort. If the link is
//    down the op simply stays buffered in __log until the peer reconnects and
//    asks for catch-up. This means a client talking to tracker B can briefly
//    NOT see a mutation (e.g. a new user) made via tracker A — that window
//    is the price of staying available while the link is down.
//  * Because most state here is add-only (users, group members, seeders),
//    replaying/merging is just "apply again" — TrackerState::create_user
//    returning ST_ALREADY_EXISTS on a replay is treated as success, not an
//    error. Removals are the hard part (need tombstones or last-writer-wins
//    on op.ts) and are left as a TODO in apply_remote().
//  * Handshake on (re)connect: dialer sends MSG_SYNC_HELLO (identifies
//    itself) then MSG_SYNC_CATCHUP{after_seq = __applied_peer_seq} so the
//    acceptor knows where to resume; the acceptor replays its log via
//    send_catchup_from(). Applied ops are ACKed (MSG_SYNC_ACK) so the sender
//    can trim its log instead of growing it forever.
//  * The link will drop. connect_loop() reconnects with a fixed backoff and
//    redoes the handshake every time.
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

#include "tracker_state.hpp"

namespace p2p {

struct Op {
    uint64_t    seq = 0;
    uint16_t    type = 0;      // reuse MSG_* opcodes
    std::string payload;       // same encoding as the client request
    uint64_t    ts = 0;        // for last-writer-wins on removals
};

class SyncManager {
public:
    SyncManager(TrackerState& st, int self_index,
                std::string peer_ip, uint16_t peer_port);
    ~SyncManager();

    void start();
    void stop();

    // Called by client-session threads after a mutation is applied locally.
    void record_and_replicate(uint16_t type, const std::string& payload);

    // Called by the listener thread when a peer tracker connects to us
    // (i.e. the first message it sent was MSG_SYNC_HELLO). Blocks, reading
    // and applying ops, until the link drops; the caller should run it on
    // its own thread.
    void handle_peer_connection(int fd);

private:
    void connect_loop();       // outbound: keep a link to the other tracker
    // Shared read-side logic for both the dialer's loop and
    // handle_peer_connection(): decode one already-received message and act
    // on it. `fd` is where replies (ACK, catch-up ops) get written.
    void process_peer_message(int fd, uint16_t type, const std::string& payload);
    bool apply_remote(const Op& op);
    void send_catchup_from(int fd, uint64_t after_seq);
    void send_op_locked(int fd, const Op& op); // caller holds __send_mu

    TrackerState&   __state;
    int             __self_index;
    std::string     __peer_ip;
    uint16_t        __peer_port;

    std::mutex      __mu_lock;            // guards __next_seq, __applied_peer_seq, __log, __out_fd
    uint64_t        __next_seq = 1;
    uint64_t        __applied_peer_seq = 0; // highest peer-originated seq we've applied
    std::deque<Op>  __log;           // our own ops, oldest first; trimmed on ACK

    std::mutex      __send_mu;       // serializes writes to __out_fd (framing is two send()s)
    int             __out_fd = -1;   // the one link to the peer, however it was established

    std::thread     __conn_thread;
    bool            __running = false;
};

} // namespace p2p
