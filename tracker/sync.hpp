#pragma once
// Tracker-to-tracker replication.
//
// DESIGN (decided):
//
// Topology: A single full-duplex TCP link. Tracker 0 dials; Tracker 1 listens. 
// Both push updates (MSG_SYNC_OP).

// Eventual Consistency: Local changes apply immediately for high availability, 
// generating an operation with a local sequence number that 
// replicates asynchronously.

// Idempotent Updates: Additive changes (like creating a user) can be 
// safely replayed.

// Catch-Up & Trimming: On reconnect, peers request missing operations 
// based on their last-seen sequence. Applied operations are ACKed to 
// free up log space.

// Fault Tolerance: Disconnects trigger automatic reconnects with a fixed backoff.

// Cold Starts: If a tracker fully restarts and loses its log, it 
// requests a complete state snapshot from its peer 
// instead of a standard catch-up.
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
