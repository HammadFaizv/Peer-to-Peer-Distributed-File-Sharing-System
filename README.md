# P2P Distributed File Sharing System

## 1. Overview

This project implements a **Peer-to-Peer (P2P) Distributed File Sharing System** in C++17 over raw TCP sockets — no external networking, database, or torrent libraries.

The system is designed to let users:

* Create and authenticate user accounts
* Create groups and manage join requests
* Share files within a group
* Discover peers that hold a file
* Download files from multiple peers concurrently, piece by piece
* Verify every downloaded piece with SHA1
* Keep two tracker servers' metadata in sync
* Keep working when one tracker is unreachable

Trackers **never store file contents** — only metadata (users, groups, file names/sizes/hashes, and which peers currently hold which pieces). File bytes move directly between clients.

See **Section 2.1** below for exactly which of these are implemented right now versus still TODO — the two tracker instances and the group workflow are live and tested; file transfer is not yet wired up.

---

## 2. System Architecture

The system consists of:

* **Tracker 0** and **Tracker 1** — two peer tracker processes, indices `0`/`1` into `tracker_info.txt`
* **Any number of clients** — each one both a downloader and a seeder

```text
                         ┌──────────────────┐
                         │    Tracker 0     │
                         │                  │
                         │ User Metadata    │
                         │ Group Metadata   │
                         │ File Metadata    │
                         └────────┬─────────┘
                                  │
                     single full-duplex TCP link
                    (tracker 0 dials, tracker 1 accepts)
                                  │
                         ┌────────▼─────────┐
                         │    Tracker 1     │
                         │                  │
                         │ User Metadata    │
                         │ Group Metadata   │
                         │ File Metadata    │
                         └──────────────────┘


          Metadata                         Metadata
             ▲                                ▲
             │                                │
      ┌──────┴──────┐                  ┌──────┴──────┐
      │   Client 1  │◄──── File ──────►│   Client 2  │
      │             │      Pieces      │             │
      │ Downloader  │                  │ Downloader  │
      │ + Seeder    │                  │ + Seeder    │
      └─────────────┘                  └─────────────┘
```

### 2.1 Implementation Status

| Area | Status |
|---|---|
| `create_user`, `login` | ✅ done |
| `logout` | ⚠️ partial — client sends `MSG_LOGOUT`, but `Session::dispatch()` has no case for it, so the tracker replies `ST_NOT_IMPLEMENTED` and the client prints "logged out" regardless of the reply. A user's session **is** still logged out implicitly when their TCP connection to the tracker closes (`Session::run()` calls `TrackerState::logout` on exit). |
| `create_group`, `join_group`, `leave_group`, `list_groups`, `list_requests`, `accept_request` | ✅ done, and replicated between trackers (see §19) |
| `upload_file` | ❌ not implemented — the client hashes the file locally and prints the result, but never sends `MSG_UPLOAD_FILE`. It also currently indexes `t[3]` for a command that only has 3 tokens (`upload_file <group> <path>` → valid indices are `0..2`), which reads past the end of the token vector — fix before using it. |
| `list_files`, `download_file`, `stop_share` | ❌ not implemented — no client-side command handler exists for `list_files`/`stop_share` at all (falls through to "unrecognised command"); `download_file` is a recognised command that just prints `TODO: download`. |
| `show_downloads` | ✅ wired to `DownloadManager::status_lines()`, but has nothing to show since downloads can't start yet |
| `TrackerState::add_file / list_files / get_file / stop_share / update_bitfield` | ❌ stubs returning `ST_NOT_IMPLEMENTED` |
| Piece transfer (`MSG_PIECE_REQUEST`/`MSG_PIECE_DATA`, `Seeder`, `DownloadManager`) | ⚠️ `Seeder` answers piece requests from disk; `DownloadManager::peer_worker` is an unfinished stub — no download loop yet |
| Tracker-to-tracker replication | ✅ done for every mutation above (see §19) |
| Tracker rejoining after a full **process restart** | ❌ `TrackerState::snapshot()`/`restore()` are stubs — a tracker that crashes and restarts comes back empty; only a *live* reconnect (link drop, not process death) replays missed ops via catch-up |
| Client transparently re-logging in after tracker failover | ❌ TODO in `TrackerClient::request()` — after failing over to the other tracker, the client is a fresh, unauthenticated connection and will start getting `ST_NOT_LOGGED_IN` until the user runs `login` again |

---

## Communication Channels

Three kinds of connection, all speaking the same 12-byte-header wire format (§5):

### Client ↔ Tracker
Registration, login/logout, group operations, (planned) file metadata and peer discovery.

### Client ↔ Client
(Planned) piece requests/responses for parallel downloads. `Seeder` already answers `MSG_PIECE_REQUEST`; nothing currently drives requests from the download side.

### Tracker ↔ Tracker
One persistent link carrying `MSG_SYNC_HELLO` / `MSG_SYNC_CATCHUP` / `MSG_SYNC_OP` / `MSG_SYNC_ACK` — see §19.

---

# 3. Project Structure

```text
p2p-file-sharing/
├── common/                    shared by both binaries
│   ├── protocol.hpp           opcodes, status codes, wire header, PIECE_SIZE
│   ├── buffer.hpp / .cpp      length-prefixed payload encode/decode
│   ├── net.hpp / .cpp         send_all / recv_all / send_msg / recv_msg / connect / listen
│   └── sha1.hpp / .cpp        self-contained SHA1 + streaming file/piece hashing
├── tracker/
│   ├── tracker_state.hpp/.cpp all metadata, mutex-guarded
│   ├── session.hpp / .cpp     one thread per connected client; request dispatch
│   ├── sync.hpp / .cpp        tracker-to-tracker replication (op log + catch-up)
│   ├── main.cpp                listener, per-connection threads, console `quit`
│   └── Makefile
├── client/
│   ├── tracker_client.hpp/.cpp one connection to a tracker, with failover
│   ├── seeder.hpp / .cpp      listening side; serves pieces to other peers
│   ├── piece_store.hpp/.cpp   pread/pwrite piece I/O + have-bitmap
│   ├── download_manager.hpp/.cpp concurrent multi-peer downloads (stubbed)
│   ├── main.cpp                CLI loop
│   └── Makefile
├── tracker_info.txt           "ip port" per line, one per tracker, in index order
├── Makefile                   top-level convenience: `make` builds both binaries
└── README.md
```

All headers use the `.hpp` extension; there are no bare `.h` files in this tree.

---

# 4. Common Networking Module

The networking layer (`common/net.hpp`/`.cpp`) is shared by both binaries and by every connection kind (client↔tracker, tracker↔tracker):

```cpp
int  send_all(int fd, const void* buf, size_t n);   // loops until n bytes are sent
int  recv_all(int fd, void* buf, size_t n);         // loops until n bytes are received
int  send_msg(int fd, uint16_t type, uint16_t status, const std::string& payload);
int  recv_msg(int fd, MsgHeader& hdr, std::string& payload);
int  tcp_connect(const std::string& ip, uint16_t port);
int  tcp_listen(const std::string& ip, uint16_t port, int backlog = 64);
bool parse_endpoint(const std::string& s, std::string& ip, uint16_t& port); // "1.2.3.4:5000"
```

## TCP Reliability

TCP is a byte stream, not a message stream — one `send()` is not guaranteed to arrive as one `recv()`. `send_all`/`recv_all` loop until the full requested byte count has moved (retrying on `EINTR`, stopping on a clean peer close or a real error), and `send_msg`/`recv_msg` build on them to frame a complete `MsgHeader` + payload.

---

# 5. Protocol Design

Every message is a **packed 12-byte header** (network byte order) followed by `length` payload bytes:

```text
+--------+------+--------+--------+
| magic  | type | status | length |
| 4B     | 2B   | 2B     | 4B     |
+--------+------+--------+--------+
|            payload (length B)   |
+----------------------------------+
```

```cpp
struct MsgHeader {
    uint32_t magic;   // 0x50325046 ("P2PF")
    uint16_t type;    // MsgType
    uint16_t status;  // Status; 0 on requests, meaningful on MSG_RESPONSE
    uint32_t length;  // payload byte count
};
```

`recv_msg` rejects any `length` above `MAX_PAYLOAD` (`PIECE_SIZE + 8192`), so a malformed peer can't force an unbounded allocation.

Payloads are encoded with `Buffer`: fixed-width integers (`put_u8/u16/u32/u64`) written as raw host-order bytes (**not** byte-swapped — see §33), and `u32`-length-prefixed strings (`put_str`/`get_str`).

### Message types (`common/protocol.hpp`)

```text
0x01xx  client  → tracker (requests)
  MSG_CREATE_USER    0x0101      MSG_LIST_GROUPS    0x0107
  MSG_LOGIN          0x0102      MSG_LIST_REQUESTS  0x0108
  MSG_LOGOUT         0x0103      MSG_ACCEPT_REQUEST 0x0109
  MSG_CREATE_GROUP   0x0104      MSG_UPLOAD_FILE    0x010A
  MSG_JOIN_GROUP     0x0105      MSG_LIST_FILES     0x010B
  MSG_LEAVE_GROUP    0x0106      MSG_GET_FILE_META  0x010C
                                 MSG_STOP_SHARE     0x010D
                                 MSG_HAVE_PIECES    0x010E

0x02xx  tracker → client (responses)
  MSG_RESPONSE       0x0201      generic reply; status lives in the header

0x03xx  client ↔ client (data plane)
  MSG_PIECE_REQUEST    0x0301    MSG_BITFIELD_REQUEST 0x0303
  MSG_PIECE_DATA       0x0302    MSG_BITFIELD_DATA    0x0304

0x04xx  tracker ↔ tracker (replication)
  MSG_SYNC_OP        0x0401      MSG_SYNC_CATCHUP   0x0403
  MSG_SYNC_ACK       0x0402      MSG_SYNC_HELLO     0x0404
```

### Status codes

```text
ST_OK 0   ST_ERR 1   ST_NOT_LOGGED_IN 2   ST_BAD_CREDENTIALS 3
ST_ALREADY_EXISTS 4   ST_NOT_FOUND 5   ST_NOT_OWNER 6
ST_NOT_MEMBER 7   ST_NOT_IMPLEMENTED 8   ST_MALFORMED 9
```

---

# 6. User and Group Management

Commands as actually parsed by `client/main.cpp` (all are `snake_case`, tokenized on whitespace):

| Command | Effect |
|---|---|
| `create_user <user_id> <password>` | Register a new account |
| `login <user_id> <password>` | Authenticate; starts a session on this connection |
| `logout` | See the caveat in §2.1 |
| `create_group <group_id>` | Create a group; caller becomes owner |
| `join_group <group_id>` | File a pending join request |
| `leave_group <group_id>` | Leave a group. If the owner leaves and members remain, ownership passes deterministically to the lexicographically smallest remaining member id (`*members.begin()` on the sorted `std::set`) — both trackers land on the same new owner independently when they replay the op. If no members remain, the group is dissolved. |
| `list_groups` | List every group in the system |
| `list_requests <group_id>` | Owner-only: list pending join requests |
| `accept_request <group_id> <user_id>` | Owner-only: move a user from pending to member |

Example:

```text
create_user hammad password123
login hammad password123
create_group programming
```

---

# 7. File Management (target design — not yet implemented)

The wire protocol, `TrackerState` methods, and `PieceStore`/`Seeder` scaffolding already assume this design; `TrackerState::add_file`/`list_files`/`get_file`/`stop_share` are stubs and the client's `upload_file`/`download_file`/`list_files`/`stop_share` are unfinished (§2.1). Documenting the intended design here since it's what the remaining `TODO`s in the code are working toward.

Files are divided into fixed **512 KB** pieces (`PIECE_SIZE` in `protocol.hpp`); the final piece may be shorter.

```text
File
│
├── Piece 0 → 512 KB
├── Piece 1 → 512 KB
└── Piece 2 → remaining bytes
```

Pieces are not separate files on disk — `PieceStore` accesses the one underlying file by offset by `pread`/`pwrite`.

---

# 8. SHA1 Integrity Verification

`common/sha1.hpp`/`.cpp` provides a self-contained SHA1 implementation (verified against the RFC 3174 test vectors) plus streaming file/piece hashing that never loads the whole file into memory. Two levels of hash are computed:

```text
Piece level:   SHA1(piece 0), SHA1(piece 1), ...
File level:    SHA1(complete file)
```

Both are stored as `FileMeta::piece_hashes` / `FileMeta::file_hash` once file metadata is implemented.

---

# 9. Upload (target design)

Intended flow, matching the `TODO` already sketched in `client/main.cpp`'s `upload_file` handler:

```text
Open file → determine size → hash every 512 KB piece → hash whole file
   → send MSG_UPLOAD_FILE {group, name, size, file_hash, piece_hashes}
   → on ST_OK, open a PieceStore for seeding and register with Seeder
```

The tracker would store this metadata only — never the file bytes.

---

# 10. File Metadata (target shape)

`FileMeta` (`tracker/tracker_state.hpp`) already has the shape this needs:

```cpp
struct FileMeta {
    std::string              name;
    uint64_t                 size;
    std::string              file_hash;      // SHA1 of whole file
    std::vector<std::string> piece_hashes;   // SHA1 of each piece
    std::map<std::string, PeerRef> seeders;  // user_id -> peer (ip, port, bitfield)
};
```

---

# 11. Listing Files (target design)

`list_files <group_id>` would return the files shared in a group. Not implemented client- or tracker-side yet (§2.1).

---

# 12. Peer Discovery (target design)

`MSG_GET_FILE_META` is meant to return a file's metadata plus the list of currently-online peers holding it (their `ip:port` and bitfield), so the downloader knows who to ask for which piece. Not implemented yet.

---

# 13. P2P Download (target design)

```text
download_file <group_id> <file_name> <destination_path>
```

would drive `DownloadManager`, which already has the shape for concurrent multi-peer piece fetching but `peer_worker` is an unfinished stub — no piece requests are actually issued yet.

---

# 14. Piece Verification (target design)

Every received piece would be SHA1-verified against its expected hash before being written to disk; a mismatch is discarded and re-requested from another peer. This is why verification must happen **before** the write, not after — see the report note in §32.

---

# 15. Piece Selection Strategy (to be decided)

Not yet implemented. The `download_manager.cpp` TODOs call for: a work queue, one worker thread per peer, timeout-and-requeue on a dead peer, and re-registering as a partial seeder after the first verified piece. Sequential/random/rarest-first are the standard strategies to compare in the report.

---

# 16. Concurrent Downloads (target design)

`DownloadManager` is designed to hold multiple `DownloadJob`s at once, each fanning out to several peer worker threads. Shared structures (download state, piece availability, peer lists) need the same mutex-per-object discipline `TrackerState` already uses.

---

# 17. Client as a Seeder

`client/seeder.hpp`/`.cpp` is implemented: it listens on the client's own `<IP>:<PORT>` and answers `MSG_PIECE_REQUEST` with `MSG_PIECE_DATA` read from `PieceStore`. What's missing is the other end — nothing currently drives `MSG_PIECE_REQUEST` from a download.

---

# 18. Stop Sharing (target design)

```text
stop_share <group_id> <file_name>
```

Would remove the current client as a seeder for that file without deleting the local copy. Not implemented client- or tracker-side yet.

---

# 19. Tracker Synchronization (implemented)

**Topology chosen:** active/active over a **single** full-duplex TCP link between the two trackers. To avoid two redundant sockets (each side dialing the other), only the lower-indexed tracker (`0`) dials out (`SyncManager::connect_loop`); the higher-indexed tracker (`1`) just waits for that inbound connection in its own accept loop and hands the fd to `SyncManager::handle_peer_connection`. Once up, either side can push an op whenever it applies a local mutation, and either side reads and applies whatever the other pushes — see `tracker/sync.hpp` for the full design write-up in comments.

```text
Tracker 0 (dials out)  ────────────────────►  Tracker 1 (accepts)
        │  MSG_SYNC_HELLO, MSG_SYNC_CATCHUP{after_seq}            │
        │◄───────────────────────────────────────────────────────┤
        │  replay of missed MSG_SYNC_OP, then live ops both ways  │
        │◄─────────────────────────────────────────────────────► │
```

**What replicates today:** `create_user`, `create_group`, `join_group`, `leave_group`, `accept_request`. Each is stamped with a per-origin monotonically increasing sequence number, buffered in an in-memory op log, and pushed over the link; on the receiving side `SyncManager::apply_remote` decodes the same payload and calls the matching `TrackerState` method directly (never through `record_and_replicate`, so the two trackers don't ping-pong the same op back and forth).

**Sync is fully async:** a mutation applies locally and replies to the client immediately; the network push is best-effort. If the link is down, the op just stays in the log until the peer reconnects and catches up — meaning a client talking to tracker B can briefly *not* see something created via tracker A. That window is the price of staying available while the link is down.

**Idempotent replay:** since state here is mostly add-only, "apply again" is the merge strategy — e.g. `create_user` returning `ST_ALREADY_EXISTS` on a replayed op counts as success, not failure. Removals (`leave_group`, `accept_request` consuming a pending request) are handled the same way by treating the "already gone" status as success too.

**What does not replicate:** `MSG_LOGIN`/`MSG_LOGOUT` are per-connection session state, deliberately not pushed as ops (see §2.1 for the failover consequence). File metadata ops aren't implemented yet, so there's nothing to replicate there either.

---

# 20. Tracker Failure

```text
Tracker 0      Tracker 1
    ✓              ✗
    │
    │
 Clients continue using Tracker 0
 (via TrackerClient's fail-over — see below)
```

`TrackerClient::request()` already fails over to the other configured tracker when a request fails, and retries once. **Caveat (§2.1):** the new connection has no session, so a user gets `ST_NOT_LOGGED_IN` until they `login` again — the client does not yet transparently replay the login on fail-over.

When the down tracker comes back, its `SyncManager::connect_loop` (if it's tracker 0) or the other side's inbound accept (if it's tracker 1) re-establishes the link, exchanges `MSG_SYNC_HELLO` + `MSG_SYNC_CATCHUP`, and the peer replays everything since the last applied sequence number — **as long as the tracker process itself stayed alive** (a link drop, not a crash). A tracker that actually restarts comes back with empty state, since `TrackerState::snapshot()`/`restore()` are still stubs (§2.1).

---

# 21. Handling Network Failures

Implemented:

* Partial TCP reads/writes (`send_all`/`recv_all` loop to completion)
* Malformed/oversized frames rejected (`recv_msg` checks `magic` and `length` against `MAX_PAYLOAD`)
* Tracker link drop → automatic reconnect with a fixed backoff + catch-up replay (§19)
* Client-side tracker fail-over (with the login caveat in §20)

Still open (see the file-transfer TODOs): peer disconnect mid-download, corrupted-piece re-request, and turning the connect backoff into something better than a fixed 2-second retry.

---

# 22. Download Progress

`show_downloads` is wired to `DownloadManager::status_lines()`. Since downloads can't start yet (§2.1), there's currently nothing to show. The `[C] <group> <file>` completed-download line format mentioned in some assignment specs is not yet implemented — decide and implement the format alongside the download loop.

---

# 23. Data Structures

### Tracker (`tracker/tracker_state.hpp`, one mutex over the whole thing)

```cpp
struct User  { std::string id, password; bool online; std::string ip; uint16_t port; };
struct Group {
    std::string id, owner;
    std::set<std::string> members;   // sorted — see the ownership-handoff rule in §6
    std::set<std::string> pending;   // join requests awaiting the owner
    std::map<std::string, FileMeta> files;
};
struct FileMeta { /* see §10 */ };

std::map<std::string, User>  users_;
std::map<std::string, Group> groups_;
```

### Client (`client/tracker_client.hpp`, `client/download_manager.hpp`, `client/piece_store.hpp`)

```text
one TrackerClient connection (with fail-over)
one Seeder (listens, answers MSG_PIECE_REQUEST from PieceStore)
DownloadManager: map of in-flight DownloadJobs, each with per-piece state
```

---

# 24. Compilation

From `p2p-file-sharing/`:

```bash
make            # builds both tracker/tracker and client/client
```

or individually:

```bash
cd tracker && make
cd client  && make
```

Clean:

```bash
make clean
```

---

# 25. Running the System

```bash
./tracker/tracker tracker_info.txt 0
./tracker/tracker tracker_info.txt 1
./client/client 127.0.0.1:6000 tracker_info.txt
```

* `tracker_info.txt` holds one `ip port` line per tracker, **in index order** — the second argument to `tracker` (`0` or `1`) indexes into it, zero-based.
* The client's `<IP>:<PORT>` is the address **it itself listens on** to seed pieces to other peers — give each client instance a distinct one.
* On a tracker's own stdin, typing `quit` shuts it down (`sync.stop()`, closes the listening socket, `std::_Exit(0)` — in-flight client threads are not drained first, a known TODO).

---

# 26. Example Usage (commands that work today)

```text
create_user A pw
login A pw
create_group G1

# from a second client:
create_user B pw
login B pw
join_group G1

# back on A's client:
list_requests G1
accept_request G1 B
list_groups
```

`upload_file` / `list_files` / `download_file` / `show_downloads` / `stop_share` are recognised or partially recognised commands but do not move any file data yet — see §2.1.

---

# 27. Error Handling

Checked today: socket creation/bind/listen/accept/connect failures, partial sends/receives, malformed or oversized frames, bad/duplicate user or group state (surfaced as `Status` codes back to the client), and tracker-link drops. File I/O and SHA1-mismatch error handling apply to the pieces of the file-transfer path that already exist (`PieceStore`, `SHA1`) but the paths that would trigger them (upload/download) aren't wired up yet.

---

# 28. Memory and File Descriptor Management

* Every accepted connection's fd is closed on every exit path of `Session::run()`.
* `SyncManager` closes and re-opens `out_fd_` around reconnects, and closes the listener socket on `quit`.
* Detached per-connection threads (`tracker/main.cpp`) mean `quit` does not wait for in-flight sessions to finish before exiting — documented as a known TODO rather than silently ignored.
* The op log (`SyncManager::log_`) is trimmed as `MSG_SYNC_ACK`s come in, so it doesn't grow unboundedly under normal operation.

---

# 29. Testing

Exercised so far, with two live tracker processes and multiple client instances against `127.0.0.1`:

* User registration/login round-tripping through either tracker
* Group create/join/accept/leave, including the owner-leaves-with-remaining-members ownership handoff, verified independently from **both** trackers (a client that only knows one tracker's address can see mutations made through the other)
* Tracker sync link coming up, replaying catch-up after a reconnect, and replicating in both directions

Not yet exercised, because the code paths don't exist yet: file upload/download of any size, multi-peer concurrent downloads, corrupted-piece re-fetch.

---

# 30. Failure Testing

### Tracker failure — tested
One tracker killed, the other continues serving client requests; a client using only the surviving tracker's address still sees all previously replicated state.

### Peer / download failure, corrupted piece — not yet testable
No download loop exists yet to exercise these against (§2.1, §15).

### Network partition between trackers — tested
Killing and restarting one tracker process while the other keeps running exercises the reconnect + catch-up path (§19), as long as the killed tracker's *state* wasn't relied upon (a full process restart currently loses it — see the `snapshot`/`restore` TODO in §2.1).

---

# 31. Performance Considerations

`SHA1::hash_file` already streams the file in `PIECE_SIZE` chunks rather than loading it whole, which is what makes large-file hashing memory-safe today even though upload isn't wired to the tracker yet. Parallel piece downloads across peers is the design `DownloadManager` is shaped for, once the worker loop is filled in.

---

# 32. Design Decisions

### Why active/active with a single link, lower index dials?
Two independent sockets between the same pair of trackers would double-deliver every op for no benefit; picking a deterministic dialer (lower index) avoids that without needing a negotiation step.

### Why treat a replayed "already applied" status as success?
Most tracker state here is add-only (users, group members). A reconnect can legitimately redeliver an op the peer already has (e.g. via a different path), so `apply_remote` treats `ST_ALREADY_EXISTS`/`ST_NOT_MEMBER`/`ST_NOT_FOUND` — depending on the op — as "nothing more to do," not an error, matching the note already in `tracker/sync.hpp`.

### Why deterministic ownership handoff on `leave_group`?
Because the op is replayed independently by both trackers rather than carrying an explicit "new owner" field, the rule has to be something both sides compute identically from the same `Group::members` set — hence "smallest id in the (already-sorted) `std::set`" rather than, say, "first to join."

### Why SHA1 per piece, not just per file?
Piece-level hashes (once wired up) let a corrupted piece be caught and re-requested immediately instead of only being discovered after the whole file finishes.

### Why 512 KB pieces?
Fixed by `PIECE_SIZE` in `protocol.hpp`, matching the assignment's spec.

---

# 33. Limitations and Assumptions

* File transfer (upload/download/list_files/stop_share) is not implemented — see §2.1 for the exact gap list.
* `logout` is not dispatched tracker-side; only implicit logout on connection close actually clears `User::online` (§2.1).
* A tracker that crashes and restarts loses all state — `snapshot`/`restore` are stubs. Only a live reconnect (link drop without process death) replays missed ops.
* `TrackerClient` does not transparently re-authenticate after failing over to the other tracker.
* `Buffer`'s integer fields (`put_u16/u32/u64`) are written as raw host-order bytes, not byte-swapped — the `MsgHeader` itself is (`send_msg`/`recv_msg` use `htonl`/`htons`/`ntohl`/`ntohs`), but payload integers are not. This assumes client and both trackers run on machines with the same endianness, which holds for the little-endian x86_64/ARM64 hosts this was built and tested on but would break across a big-endian peer.
* Passwords are stored in plaintext in `User::password` — flagged as a TODO in `tracker_state.hpp`, not addressed.
* One OS thread per connection, no cap — fine at the scale this was tested at, not meant to scale to hundreds of simultaneous connections without a rethink (thread pool / event loop).
* `client/main.cpp`'s `upload_file` handler currently indexes `t[3]` on a 3-token command — an out-of-bounds read; needs fixing before that command is used (§2.1).

---

# 34. Important Implementation Notes

No `system()`, `exec()`, `popen()`, or external torrent/database libraries are used anywhere in the codebase — networking is raw POSIX sockets (`<sys/socket.h>`, `<netinet/*>`, `<arpa/inet.h>`), threading is `std::thread`/`std::mutex`, and file I/O is `pread`/`pwrite` in `PieceStore`.

---

# 35. Technical Summary

```text
                    P2P FILE SHARING SYSTEM

                         ┌─────────────┐
                         │  Tracker 0  │
                         └──────┬──────┘
                                │
                    single link, active/active,
                      async op-log replication
                                │
                         ┌──────▼──────┐
                         │  Tracker 1  │
                         └─────────────┘
                                ▲
                                │
                         Metadata only
                                │
              ┌─────────────────┴─────────────────┐
              │                                    │
        ┌─────▼─────┐                        ┌─────▼─────┐
        │  Client A │◄──── Pieces (TODO) ───►│  Client B │
        │ Downloader│                         │ Downloader│
        │ + Seeder  │                         │ + Seeder  │
        └───────────┘                         └───────────┘
```

Design goals, in the order they were tackled:

1. Reliable message framing over TCP (`send_all`/`recv_all`, `send_msg`/`recv_msg`) — done
2. User + group management with mutex-protected shared state — done
3. Two-tracker redundancy and async op-log replication — done
4. Client-side tracker fail-over — done, minus transparent re-login
5. Piece-level and file-level SHA1 verification — hashing done, verification-on-receive not wired up
6. Parallel multi-peer piece transfer — not yet implemented
7. Concurrent downloads / piece selection strategy — not yet implemented

---

# 36. References

Document any external resources consulted during development (Linux TCP socket programming, POSIX threads, the SHA1 RFC, etc.) in the final technical report, as required by the assignment.
