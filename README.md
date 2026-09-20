# P2P Distributed File Sharing System

## 1. Overview

This project implements a **Peer-to-Peer (P2P) Distributed File Sharing System** in C++17 over raw TCP sockets — no external networking, database, or torrent libraries.

The system lets users:

* Create and authenticate user accounts
* Create groups and manage join requests
* Share files within a group
* Discover peers that hold a file
* Download files from multiple peers concurrently, piece by piece
* Verify every downloaded piece with SHA1
* Keep two tracker servers' metadata in sync
* Keep working when one tracker is unreachable

Trackers **never store file contents** — only metadata (users, groups, file names/sizes/hashes, and which peers currently hold which pieces). File bytes move directly between clients.

See **Section 2.1** below for the full implementation status — user/group management, file sharing end-to-end (upload/list/download/stop-share), tracker-to-tracker replication (including crash recovery via full-state snapshots), and client-side tracker fail-over with transparent re-login are all implemented and tested.

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
| `create_user`, `login` | ✅ done — passwords are SHA1-hashed before storage/comparison, never kept in plaintext |
| `logout` | ✅ done — dispatched tracker-side (`MSG_LOGOUT`) and implicitly on connection close; either path also evicts the user from every file's seeder list (see §17) |
| `create_group`, `join_group`, `leave_group`, `list_groups`, `list_requests`, `accept_request` | ✅ done, and replicated between trackers (see §19) |
| `upload_file`, `list_files`, `download_file`, `stop_share` | ✅ done, client and tracker side, replicated between trackers, tested same-tracker and cross-tracker (see §7, §9, §11–13, §18) |
| `show_downloads` | ✅ wired to `DownloadManager::status_lines()`; reports `[D]`/`[C]`/`[F]` per job (in-progress / completed / failed) |
| `TrackerState::add_file / list_files / get_file / stop_share / update_bitfield` | ✅ implemented (§10, §12) |
| Piece transfer (`MSG_PIECE_REQUEST`/`MSG_PIECE_DATA`, `MSG_BITFIELD_REQUEST`/`DATA`, `Seeder`, `DownloadManager`) | ✅ implemented: `Seeder` answers both piece and bitfield requests from disk; `DownloadManager` runs a real multi-peer download loop (§13, §15) |
| Tracker-to-tracker replication | ✅ done for every mutation above, including file operations (see §19) |
| Tracker rejoining after a full **process restart** | ✅ implemented — `TrackerState::snapshot()`/`restore()` plus a `SyncManager` bootstrap handshake (`MSG_SYNC_SNAPSHOT_REQUEST`/`DATA`) let a tracker that lost all in-memory state recover it wholesale from its peer (see §19–20) |
| Client transparently re-logging in after tracker failover | ✅ implemented — `TrackerClient` remembers the last successful login and silently replays `MSG_LOGIN` on any freshly (re)established connection before the caller's actual request (see §20) |
| Resuming an interrupted download | ✅ implemented — `PieceStore::resume_scan` re-verifies whatever is already on disk at the destination path against the expected piece hashes before deciding what still needs fetching (see §13) |

---

## Communication Channels

Three kinds of connection, all speaking the same 12-byte-header wire format (§5):

### Client ↔ Tracker
Registration, login/logout, group operations, file metadata, and peer discovery.

### Client ↔ Client
Piece and bitfield requests/responses for parallel downloads. `Seeder` answers `MSG_PIECE_REQUEST`/`MSG_BITFIELD_REQUEST`; `DownloadManager` drives the request side.

### Tracker ↔ Tracker
One persistent link carrying `MSG_SYNC_HELLO` / `MSG_SYNC_CATCHUP` / `MSG_SYNC_OP` / `MSG_SYNC_ACK` / `MSG_SYNC_SNAPSHOT_REQUEST` / `MSG_SYNC_SNAPSHOT_DATA` — see §19.

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
│   ├── sync.hpp / .cpp        tracker-to-tracker replication (op log + catch-up + snapshot bootstrap)
│   ├── main.cpp                listener, per-connection threads, console `quit`
│   └── Makefile
├── client/
│   ├── tracker_client.hpp/.cpp one connection to a tracker, with failover + auto re-login
│   ├── seeder.hpp / .cpp      listening side; serves pieces and bitfields to other peers
│   ├── piece_store.hpp/.cpp   pread/pwrite piece I/O + have-bitmap + resume scan
│   ├── download_manager.hpp/.cpp concurrent multi-peer downloads
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

Note: none of `tcp_connect`/`send_all`/`recv_all` set a socket timeout (`SO_RCVTIMEO`/`SO_SNDTIMEO`, or a bounded `connect()`). A cleanly closed connection (process exit, `kill`) is detected immediately — the OS sends a FIN/RST right away. An unresponsive-but-not-closed peer (e.g. the machine itself vanished from the network) can instead block a caller for the OS's default TCP timeout, which can be minutes. See §33.

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
  MSG_SYNC_OP               0x0401   MSG_SYNC_CATCHUP          0x0403
  MSG_SYNC_ACK              0x0402   MSG_SYNC_HELLO            0x0404
  MSG_SYNC_SNAPSHOT_REQUEST 0x0405   MSG_SYNC_SNAPSHOT_DATA    0x0406
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
| `create_user <user_id> <password>` | Register a new account; password is SHA1-hashed before storage |
| `login <user_id> <password>` | Authenticate; starts a session on this connection and is remembered for automatic replay after a tracker fail-over (§20) |
| `logout` | Ends the session; evicts this user from every file's seeder list (§17) |
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

# 7. File Management (implemented)

Files are divided into fixed **512 KB** pieces (`PIECE_SIZE` in `protocol.hpp`); the final piece may be shorter.

```text
File
│
├── Piece 0 → 512 KB
├── Piece 1 → 512 KB
└── Piece 2 → remaining bytes
```

Pieces are **not** separate files on disk — `PieceStore` accesses the one underlying file (the original at the uploader, the destination path at a downloader) by offset with `pread`/`pwrite`. There is no piece staging directory and no temp/`.part` file; a downloading file is `ftruncate`d to its final size up front (sparse) and each verified piece lands directly at `index * PIECE_SIZE` in that same file.

| Command | Effect |
|---|---|
| `upload_file <group_id> <file_path>` | Hash the file locally, publish its metadata (`MSG_UPLOAD_FILE`), then start seeding it (§9) |
| `list_files <group_id>` | List the files shared in a group (member-only) |
| `download_file <group_id> <file_name> <dest_path>` | Fetch a file's metadata and peer list (`MSG_GET_FILE_META`), then drive a multi-peer download (§13) |
| `stop_share <group_id> <file_name>` | Stop seeding a file without deleting the local copy (§18) |

---

# 8. SHA1 Integrity Verification

`common/sha1.hpp`/`.cpp` provides a self-contained SHA1 implementation (verified against the RFC 3174 test vectors) plus streaming file/piece hashing that never loads the whole file into memory. Three uses of it exist in this codebase:

```text
Piece level:   SHA1(piece 0), SHA1(piece 1), ...       — file transfer integrity
File level:    SHA1(complete file)                      — file transfer integrity
Password:      SHA1(password)                           — TrackerState::User::password
```

The first two are stored as `FileMeta::piece_hashes` / `FileMeta::file_hash`. The third means `User::password` never holds plaintext — `create_user`/`login` both hash the incoming password before storing/comparing.

---

# 9. Upload (implemented)

Actual flow, in `client/main.cpp`'s `upload_file` handler:

```text
Open file → determine size → hash every 512 KB piece → hash whole file
   → send MSG_UPLOAD_FILE {group, name, size, file_hash, piece_hashes}
   → on ST_OK, open a PieceStore with open_for_seed() and register it with Seeder::add_share()
```

The tracker stores this metadata only — never the file bytes. `TrackerState::add_file` also records the uploader as the file's first seeder, holding every piece (`PeerRef::bitfield` all-ones); this seeder entry's `ip`/`port` come from the session that made the request, not from a later lookup, so it stays correct once replicated to the other tracker (see §19, §32).

---

# 10. File Metadata

`FileMeta` (`tracker/tracker_state.hpp`) has the shape file operations need:

```cpp
struct FileMeta {
    std::string              name;
    uint64_t                 size;
    std::string              file_hash;      // SHA1 of whole file
    std::vector<std::string> piece_hashes;   // SHA1 of each piece
    std::map<std::string, PeerRef> seeders;  // user_id -> peer (ip, port, bitfield)
};
```

`FileMeta` itself outlives its seeders: `stop_share`/`logout` only remove a specific user's `PeerRef` from `seeders`, they never delete the `FileMeta` — so the file's name/size/hashes stay discoverable via `list_files`/`get_file` even with zero current seeders.

---

# 11. Listing Files (implemented)

`list_files <group_id>` returns every file name shared in that group (`TrackerState::list_files`, member-only — `ST_NOT_MEMBER` otherwise).

---

# 12. Peer Discovery (implemented)

`MSG_GET_FILE_META` returns a file's metadata (size, file hash, piece hashes) plus **every recorded seeder**, not just ones a tracker currently believes are "online". This is deliberate, not an oversight: `MSG_LOGIN`/`MSG_LOGOUT` are per-connection session state and are never replicated between trackers (§19), so a tracker other than the one a seeder is actually connected to has no reliable `User::online` flag for them. Filtering by it would silently return empty peer lists for anyone who uploaded/seeded via the *other* tracker. Instead, the tracker hands back every seeder it knows about and lets the downloader's own connection attempt be the liveness check — a dead peer just fails to connect and its pieces get retried elsewhere (§13, §15), the same division of responsibility a real BitTorrent tracker/client pair uses.

---

# 13. P2P Download (implemented)

```text
download_file <group_id> <file_name> <destination_path>
```

drives `DownloadManager::run_job`:

1. Open (or reopen) `dest_path` via `PieceStore::open_for_download` — creates it at full size (sparse) if new.
2. `PieceStore::resume_scan` re-hashes whatever bytes are already at `dest_path` against the expected `piece_hashes` and marks any that already match as already-held. On a brand new file nothing matches (a no-op); on a file left over from an earlier attempt (e.g. the client crashed mid-download), this lets the download pick up where it left off instead of redownloading everything.
3. Build a shared queue of the indices still needed, then spawn **one worker thread per peer** (not one per piece) — each worker keeps its single connection to its peer open and repeatedly pops the next needed index off the shared queue, so peers naturally do more work the faster they are.
4. If pieces are still missing after a round (every peer that had them died or gave up — see §15), re-query the tracker for a fresh peer list and try again, up to 3 rounds total.
5. Once every piece is present, re-hash the whole assembled file and compare against `file_hash`; only then register it with `Seeder::add_share` so this client becomes a source for others.

---

# 14. Piece Verification (implemented)

Every received piece is SHA1-verified against its expected hash **before** being written to disk (`PieceStore::write_piece` hashes the buffer first, and only calls `pwrite` on a match) — a corrupted piece is never written, so a later reader of this client's own seeded copy can never receive corruption that passed through it. A mismatch is simply not marked as "have" and gets requeued for another peer (§15). After the whole file is assembled, `run_job` does one more whole-file SHA1 pass and refuses to register as a seeder or report success if it doesn't match `file_hash`.

---

# 15. Piece Selection Strategy (decided)

**Work-stealing over a shared stack**, not a fixed sequential/rarest-first order: `DownloadJob::__queue` is a `std::vector<uint32_t>` of needed indices, popped from the back (`pop_back`) by whichever `peer_worker` thread gets there first. This is simple and gives every peer roughly equal opportunity to grab the next piece — the actual order pieces complete in depends on which peer's connection is fastest, not a predetermined sequence.

**Failure handling, per peer, in `peer_worker`:** a failed fetch (dead connection, or `ST_NOT_FOUND` because that specific peer doesn't have the piece) puts the index back on the queue and increments a `consecutive_failures` counter *for that worker*; a success resets it to 0. The worker only gives up on its peer once `consecutive_failures` reaches `kMaxConsecutiveFailures` (3). This means a peer that's simply missing one piece (but has everything else) isn't dropped after a single miss, while a peer whose connection is actually dead — every subsequent attempt fails too — still gives up quickly rather than looping forever. Because the queue is a LIFO stack, a piece that was just pushed back is often the very next one popped, so a worker's retries frequently land on the *same* piece a few times before it tries something else, rather than immediately rotating to a different one — a known simplification (see §33).

**Rarest-first is not implemented**, though the wire support for it exists: `Seeder` answers `MSG_BITFIELD_REQUEST` with the peer's actual bitfield (§17), but `DownloadManager`/`PeerAddr` never queries it or uses it to bias selection — every peer is treated as equally likely to have any given piece.

**Endgame mode** (requesting the last few pieces from every peer at once to avoid a long tail) is also not implemented.

---

# 16. Concurrent Downloads (implemented)

`DownloadManager` holds multiple `DownloadJob`s at once (keyed by `"group/file"` in `jobs_`, one thread in `threads_` per `start()`ed job), each fanning out internally to one `peer_worker` thread per peer as described in §13/§15. Shared structures — the per-job piece queue (`DownloadJob::__queue` + `__queue_mu`), the `PieceStore`'s own have-bitmap mutex, and `DownloadManager::__mu_lock` over `jobs_`/`threads_` — follow the same mutex-per-object discipline `TrackerState` uses.

---

# 17. Client as a Seeder (implemented)

`client/seeder.hpp`/`.cpp` listens on the client's own `<IP>:<PORT>` and answers both:
* `MSG_PIECE_REQUEST` → `MSG_PIECE_DATA`, read from `PieceStore` via `pread`
* `MSG_BITFIELD_REQUEST` → `MSG_BITFIELD_DATA`, the store's current have-bitmap

A share is added via `Seeder::add_share` (on successful `upload_file`, or on completing a download) and removed via `Seeder::remove_share` (on `stop_share`, or implicitly when the tracker evicts a logged-out user from its seeder list — see §6, §18).

---

# 18. Stop Sharing (implemented)

```text
stop_share <group_id> <file_name>
```

Sends `MSG_STOP_SHARE`; on `ST_OK`, `TrackerState::stop_share` removes just this user's `PeerRef` from that file's `seeders` (the file's own metadata survives — §10), replicates the removal to the other tracker, and the client calls `Seeder::remove_share` locally. The local copy on disk is never touched. If another client is mid-download from this peer when it stops sharing, its next piece request to this peer simply gets `ST_NOT_FOUND` — handled by the same retry/give-up logic as any other peer failure (§15), no special-casing needed.

---

# 19. Tracker Synchronization (implemented)

**Topology:** active/active over a **single** full-duplex TCP link between the two trackers. To avoid two redundant sockets (each side dialing the other), only the lower-indexed tracker (`0`) dials out (`SyncManager::connect_loop`); the higher-indexed tracker (`1`) just waits for that inbound connection in its own accept loop and hands the fd to `SyncManager::handle_peer_connection`. Once up, either side can push an op whenever it applies a local mutation, and either side reads and applies whatever the other pushes — see `tracker/sync.hpp` for the full design write-up in comments.

```text
Tracker 0 (dials out)  ────────────────────►  Tracker 1 (accepts)
        │  MSG_SYNC_HELLO, MSG_SYNC_CATCHUP{after_seq}            │
        │  (or MSG_SYNC_SNAPSHOT_REQUEST if this side is empty)   │
        │◄───────────────────────────────────────────────────────┤
        │  replay of missed MSG_SYNC_OP, then live ops both ways  │
        │◄─────────────────────────────────────────────────────► │
```

**What replicates:** `create_user`, `create_group`, `join_group`, `leave_group`, `accept_request`, `MSG_UPLOAD_FILE`, `MSG_STOP_SHARE`, `MSG_HAVE_PIECES`. Each is stamped with a per-origin monotonically increasing sequence number, buffered in an in-memory op log, and pushed over the link; on the receiving side `SyncManager::apply_remote` decodes the same payload and calls the matching `TrackerState` method directly (never through `record_and_replicate`, so the two trackers don't ping-pong the same op back and forth). The file-operation ops additionally carry the originating session's own `ip`/`port` explicitly in the payload (appended after the client-facing fields) — `TrackerState::add_file`/`update_bitfield` take these as parameters rather than looking them up locally, because on the *receiving* tracker the local `User` record for that peer has no `ip`/`port` (logins aren't replicated — see the rationale in §12/§32).

**Sync is fully async:** a mutation applies locally and replies to the client immediately; the network push is best-effort. If the link is down, the op just stays in the log until the peer reconnects and catches up — meaning a client talking to tracker B can briefly *not* see something created via tracker A. That window is the price of staying available while the link is down.

**Idempotent replay:** since state here is mostly add-only, "apply again" is the merge strategy — e.g. `create_user` returning `ST_ALREADY_EXISTS` on a replayed op counts as success, not failure. Removals (`leave_group`, `accept_request` consuming a pending request, `stop_share`) are handled the same way by treating the "already gone" status as success too.

**Recovering from a full process restart:** a live reconnect (link drop, process stayed up) replays missed ops via `MSG_SYNC_CATCHUP` as above, but `__log` is trimmed on ACK — it can't rebuild a tracker's history from scratch. So whenever the sync link (re)forms, each side checks its own state via `TrackerState::empty()`; a tracker that just (re)started has none, and sends `MSG_SYNC_SNAPSHOT_REQUEST` instead of a catch-up request. The peer answers with `MSG_SYNC_SNAPSHOT_DATA` carrying `TrackerState::snapshot()` — a full serialization of every user, group, and file's metadata (including per-seeder bitfields) — which the empty side loads wholesale via `TrackerState::restore()`. This runs symmetrically on both `connect_loop` (dialer) and `handle_peer_connection` (acceptor), since either tracker could be the one that crashed.

**What does not replicate:** `MSG_LOGIN`/`MSG_LOGOUT` are per-connection session state, deliberately not pushed as ops — see §12 for what this costs (`get_file` can't filter by "online") and §32 for the full rationale (replicating a flag that flips constantly would mean constantly reconciling contradictory writes, exactly the class of problem this op log's add-only, "apply-again-is-safe" design is built to avoid).

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

`TrackerClient::request()` fails over to the other configured tracker when a request fails, and retries once. If a login is on file (`TrackerClient::note_login`, set after any successful `MSG_LOGIN`), it transparently replays `MSG_LOGIN` on the freshly established connection *before* sending the caller's actual request — so a fail-over doesn't surface `ST_NOT_LOGGED_IN` for every subsequent command until the user manually logs in again.

When the down tracker comes back, its `SyncManager::connect_loop` (if it's tracker 0) or the other side's inbound accept (if it's tracker 1) re-establishes the link and exchanges `MSG_SYNC_HELLO` plus either `MSG_SYNC_CATCHUP` (link drop, state intact) or `MSG_SYNC_SNAPSHOT_REQUEST`/`DATA` (full restart, state lost — §19) as appropriate. Recovery in the snapshot case depends on the *surviving* tracker actually having current state to hand over; if both trackers crash at the same time, there is nothing to recover from, since neither `TrackerState` is ever persisted to disk (§33).

---

# 21. Handling Network Failures

Implemented:

* Partial TCP reads/writes (`send_all`/`recv_all` loop to completion)
* Malformed/oversized frames rejected (`recv_msg` checks `magic` and `length` against `MAX_PAYLOAD`)
* Tracker link drop → automatic reconnect with a fixed backoff + catch-up replay, or full snapshot bootstrap after a process restart (§19)
* Client-side tracker fail-over with transparent re-login (§20)
* Peer disconnect / missing piece mid-download → bounded per-peer retry then give-up-and-reassign (§15)
* Corrupted-piece re-request → hash mismatch before write means the piece is never accepted, and gets requeued for a different peer (§14)
* Interrupted download resuming from whatever is already correctly on disk (§13)

Still open: no socket-level timeouts (§4) — a peer whose *machine*, not just process, disappears can block a connect/recv rather than failing fast; and the connect backoff for the tracker-to-tracker link is still a fixed 2-second retry rather than exponential.

---

# 22. Download Progress

`show_downloads` is wired to `DownloadManager::status_lines()`, reporting one line per job: `[D] [group] file` (in progress), `[C] [group] file` (completed and verified), or `[F] [group] file` (failed — pieces remained missing after all retry rounds, or the final whole-file hash didn't match). This is status, not granular progress — there's no piece-count or percentage-complete indicator; a resumed download logs how many pieces it found already correct on disk to stderr (`[download] group/file: resuming, X/Y pieces already on disk`), but that's a log line, not part of `status_lines()`.

---

# 23. Data Structures

### Tracker (`tracker/tracker_state.hpp`, one mutex over the whole thing)

```cpp
struct User  { std::string id; std::string password; /* SHA1 hex digest */ bool online; std::string ip; uint16_t port; };
struct PeerRef { std::string user_id, ip; uint16_t port; std::vector<uint8_t> bitfield; };
struct FileMeta { /* see §10 */ };
struct Group {
    std::string id, owner;
    std::set<std::string> members;   // sorted — see the ownership-handoff rule in §6
    std::set<std::string> pending;   // join requests awaiting the owner
    std::map<std::string, FileMeta> files;
};

std::map<std::string, User>  __Users;
std::map<std::string, Group> __Groups;
```

`TrackerState::snapshot()`/`restore()` serialize/restore this entire structure (via `Buffer`) for the crash-recovery path in §19.

### Client (`client/tracker_client.hpp`, `client/download_manager.hpp`, `client/piece_store.hpp`)

```text
one TrackerClient connection (with fail-over + remembered login for auto re-login)
one Seeder (listens, answers MSG_PIECE_REQUEST/MSG_BITFIELD_REQUEST from PieceStore)
DownloadManager: map of in-flight DownloadJobs, each with:
  - a shared piece-index queue + mutex (work-stealing across its peer_worker threads)
  - a PieceStore (have-bitmap + resume_scan)
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

# 26. Example Usage

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

# A shares a file with the group:
upload_file G1 /path/to/some_file.bin

# from B's client:
list_files G1
download_file G1 some_file.bin /path/to/save_here.bin
show_downloads

# A stops sharing it (local copy is untouched):
stop_share G1 some_file.bin
```

This exact sequence (with two live tracker processes) was used to verify: same-tracker upload→download, cross-tracker upload→download (uploader on tracker 0, downloader talking only to tracker 1, exercising replication end-to-end), and `stop_share` correctly removing a seeder mid-availability.

---

# 27. Error Handling

Checked: socket creation/bind/listen/accept/connect failures, partial sends/receives, malformed or oversized frames, bad/duplicate user or group state (surfaced as `Status` codes back to the client), tracker-link drops, wrong passwords (`ST_BAD_CREDENTIALS` against the hashed password), file operations on a nonexistent group/file/non-member (`ST_NOT_FOUND`/`ST_NOT_MEMBER`), a corrupted or missing piece from a peer (requeued, peer retried a bounded number of times then dropped — §15), and a whole-file hash mismatch after assembly (job marked failed, never registered as a seed).

---

# 28. Memory and File Descriptor Management

* Every accepted connection's fd is closed on every exit path of `Session::run()`.
* `SyncManager` closes and re-opens `__out_fd` around reconnects, and closes the listener socket on `quit`.
* Detached per-connection threads (`tracker/main.cpp`) mean `quit` does not wait for in-flight sessions to finish before exiting — documented as a known TODO rather than silently ignored.
* The op log (`SyncManager::__log`) is trimmed as `MSG_SYNC_ACK`s come in, so it doesn't grow unboundedly under normal operation.
* `PieceStore`'s file descriptor lives as long as any `shared_ptr` to it does (a `Seeder::shares_` entry, or a `peer_worker`/`serve_peer` call in flight) — so removing a share (`stop_share`, logout eviction) never invalidates an in-flight read on that same object; the fd only closes once the last reference goes away.

---

# 29. Testing

Exercised, with two live tracker processes and multiple client instances against `127.0.0.1`:

* User registration/login round-tripping through either tracker, including rejecting a wrong password and accepting the correct one (SHA1-hashed comparison)
* Group create/join/accept/leave, including the owner-leaves-with-remaining-members ownership handoff, verified independently from **both** trackers
* Tracker sync link coming up, replaying catch-up after a reconnect, and replicating in both directions
* File upload → download, same-tracker and cross-tracker (uploader on tracker 0, downloader talking only to tracker 1) — downloaded file's SHA1 verified byte-identical to the source
* `stop_share` mid-availability, confirmed the seeder is dropped from later peer-discovery responses
* A user logging out, confirmed they're dropped from a file's seeder list and a subsequent download attempt correctly reports no peers
* Resuming a download into a destination file that already had some pieces correct on disk — confirmed only the missing pieces were re-fetched and the final hash still matched
* A tracker crashing (`kill -9`) and restarting fresh, confirmed it fully recovers users/groups/files/seeders via the snapshot bootstrap from its peer
* A client's tracker connection dying mid-session, confirmed the next command transparently fails over and re-logs in without user intervention

Not yet exercised: multi-peer (3+) concurrent piece fetching under real network latency/loss (only tested on localhost), and rarest-first piece selection (not implemented — §15).

---

# 30. Failure Testing

### Tracker failure — tested
One tracker killed, the other continues serving client requests; a client using only the surviving tracker's address still sees all previously replicated state. Separately, a killed-and-restarted tracker (empty state) was confirmed to fully recover via the snapshot bootstrap once reconnected, including file metadata and seeder entries with correct `ip`/`port`.

### Peer / download failure, corrupted piece — tested
A `peer_worker` that fails to fetch a piece (dead connection, or the peer legitimately not having it) requeues the piece and only gives up on that peer after several consecutive failures (§15); `stop_share` mid-download was confirmed to produce the same graceful "peer no longer has it" behavior as an actual peer failure.

### Network partition between trackers — tested
Killing and restarting one tracker process while the other keeps running exercises the reconnect + catch-up (or snapshot bootstrap, if the killed one lost its state) path (§19).

---

# 31. Performance Considerations

`SHA1::hash_file` streams the file in `PIECE_SIZE` chunks rather than loading it whole, which keeps hashing memory-safe for large files both at upload (piece + whole-file hashing) and at download completion (the final whole-file verification pass, which re-reads the entire assembled file once).

Piece transfer itself is **not pipelined**: `fetch_piece` is strict stop-and-wait — one request, wait for its response, then the next — per connection. With one seeder, total download time for a large file is roughly `piece_count × (round-trip latency + transfer time)`; over a low-latency LAN this is negligible, but round-trip cost dominates over higher-latency links. The mitigation actually implemented is peer-level parallelism, not per-connection pipelining: `DownloadManager` spawns one worker thread per available seeder, all pulling from a shared work queue (§13, §16), so wall-clock time scales down roughly with the number of peers holding the file — a partial seeder becomes usable to others immediately after its first verified piece (`MSG_HAVE_PIECES` announced per piece, not just at completion), not only once a full copy exists somewhere.

---

# 32. Design Decisions

### Why active/active with a single link, lower index dials?
Two independent sockets between the same pair of trackers would double-deliver every op for no benefit; picking a deterministic dialer (lower index) avoids that without needing a negotiation step.

### Why treat a replayed "already applied" status as success?
Most tracker state here is add-only (users, group members, file seeders). A reconnect can legitimately redeliver an op the peer already has (e.g. via a different path), so `apply_remote` treats `ST_ALREADY_EXISTS`/`ST_NOT_MEMBER`/`ST_NOT_FOUND` — depending on the op — as "nothing more to do," not an error, matching the note already in `tracker/sync.hpp`.

### Why deterministic ownership handoff on `leave_group`?
Because the op is replayed independently by both trackers rather than carrying an explicit "new owner" field, the rule has to be something both sides compute identically from the same `Group::members` set — hence "smallest id in the (already-sorted) `std::set`" rather than, say, "first to join."

### Why SHA1 per piece, not just per file?
Piece-level hashes let a corrupted piece be caught and re-requested immediately instead of only being discovered after the whole file finishes.

### Why 512 KB pieces?
Fixed by `PIECE_SIZE` in `protocol.hpp`, matching the assignment's spec.

### Why hash passwords with SHA1 specifically, rather than a purpose-built password hash (bcrypt/scrypt/Argon2)?
The assignment forbids external crypto/torrent/database libraries, and a self-contained, RFC-verified SHA1 implementation already existed for piece/file integrity (§8) — reusing it avoids hand-rolling a second hash primitive. It is **not** a substitute for a real password KDF (no salt, no iteration/work factor) — acceptable for this assignment's scope, not for a production system.

### Why does `get_file` return every recorded seeder instead of filtering by "online"?
Because `MSG_LOGIN`/`MSG_LOGOUT` are deliberately not replicated (§19), a tracker other than the one a seeder is connected to has no reliable `online` flag for them — filtering by it would silently drop legitimate cross-tracker peers. Handing back everything and letting the downloader's own connection attempt be the liveness check keeps the tracker's job (bookkeeping) and the client's job (reachability) cleanly separated.

### Why do file-op replication payloads carry `ip`/`port` explicitly instead of looking them up on the receiving tracker?
Same root cause as above: the receiving tracker's own `User` record for the originating peer has no `ip`/`port` if that peer never logged into *it* directly. Carrying the values through the replicated payload (set once, by the session that actually knows them) means a seeder's address is correct everywhere it's replicated to, regardless of which tracker a downloader later asks.

### Why detect "needs a snapshot" via `TrackerState::empty()` rather than an explicit flag?
A tracker that lost all its state via a crash is, by definition, indistinguishable in-memory from one that has genuinely never had any — both have empty `__Users`/`__Groups`. Using that as the trigger needs no extra persisted marker and is self-correcting: the moment either side has real state, the other stops requesting snapshots from it.

---

# 33. Limitations and Assumptions

* No in-flight request pipelining on a single peer connection — `fetch_piece` is strict stop-and-wait; parallelism comes from multiple peers, not multiple outstanding requests to one peer (§31).
* Piece selection is a shared LIFO stack, not rarest-first — a peer that just failed one piece often gets asked for the *same* piece again on its very next pop (since it was just pushed back) rather than a different one immediately; bounded by `kMaxConsecutiveFailures` so this doesn't loop forever (§15).
* `MSG_BITFIELD_REQUEST`/`DATA` are implemented on the `Seeder` side but never queried by `DownloadManager` — no rarest-first or availability-aware selection actually happens yet.
* No socket-level timeouts anywhere (`tcp_connect`, `send_all`/`recv_all`) — a cleanly crashed/killed peer fails fast (OS closes the socket immediately), but a peer whose machine becomes unreachable without a clean close can block a caller for the OS's default TCP timeout (§4, §21).
* Two trackers crashing at the same time loses all state permanently — neither `TrackerState` is ever persisted to disk; the snapshot-bootstrap recovery (§19) only works because the *other* tracker stayed up with current state to hand over.
* `Buffer`'s integer fields (`put_u16/u32/u64`) are written as raw host-order bytes, not byte-swapped — the `MsgHeader` itself is (`send_msg`/`recv_msg` use `htonl`/`htons`/`ntohl`/`ntohs`), but payload integers are not. This assumes client and both trackers run on machines with the same endianness, which holds for the little-endian x86_64/ARM64 hosts this was built and tested on but would break across a big-endian peer.
* Password hashing is a plain unsalted SHA1 digest, not a purpose-built password KDF — sufficient for this assignment's constraints (§32), not for production use.
* One OS thread per connection, no cap — fine at the scale this was tested at, not meant to scale to hundreds of simultaneous connections without a rethink (thread pool / event loop).
* A tracker's console `quit` doesn't drain in-flight client sessions before exiting (§25, §28) — a known TODO, not addressed here.
* The tracker-to-tracker reconnect backoff is a fixed 2 seconds, not exponential.

---

# 34. Important Implementation Notes

No `system()`, `exec()`, `popen()`, or external torrent/database/crypto libraries are used anywhere in the codebase — networking is raw POSIX sockets (`<sys/socket.h>`, `<netinet/*>`, `<arpa/inet.h>`), threading is `std::thread`/`std::mutex`, file I/O is `pread`/`pwrite` in `PieceStore`, and password/integrity hashing both go through the same self-contained SHA1 implementation.

---

# 35. Technical Summary

```text
                    P2P FILE SHARING SYSTEM

                         ┌─────────────┐
                         │  Tracker 0  │
                         └──────┬──────┘
                                │
                    single link, active/active,
              async op-log replication + snapshot bootstrap
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
        │  Client A │◄──── Pieces (live) ────►│  Client B │
        │ Downloader│                         │ Downloader│
        │ + Seeder  │                         │ + Seeder  │
        └───────────┘                         └───────────┘
```

Design goals, in the order they were tackled:

1. Reliable message framing over TCP (`send_all`/`recv_all`, `send_msg`/`recv_msg`) — done
2. User + group management with mutex-protected shared state — done
3. Two-tracker redundancy and async op-log replication, including full-state snapshot recovery after a crash — done
4. Client-side tracker fail-over with transparent re-login — done
5. Piece-level and file-level SHA1 verification, wired into the actual write/completion path — done
6. Parallel multi-peer piece transfer — done (peer-level parallelism; no per-connection pipelining, §31)
7. Concurrent downloads / piece selection strategy — done (work-stealing LIFO queue with bounded per-peer retry; rarest-first not implemented, §15)

---

# 36. References

Document any external resources consulted during development (Linux TCP socket programming, POSIX threads, the SHA1 RFC, etc.) in the final technical report, as required by the assignment.
