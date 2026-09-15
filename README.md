# Peer-to-Peer Distributed File Sharing System

Skeleton for AOS Assignment 3. It compiles and runs end-to-end for user
registration and login; everything else is deliberately left as `TODO` for you
to design and implement.

## Layout

```
.
├── common/          shared by both binaries
│   ├── protocol.h   opcodes, status codes, wire header, PIECE_SIZE
│   ├── buffer.*     length-prefixed payload encode/decode
│   ├── net.*        send_all / recv_all / send_msg / recv_msg / connect / listen
│   └── sha1.*       self-contained SHA1 + streaming file/piece hashing
├── tracker/
│   ├── tracker_state.*  all metadata, mutex-guarded
│   ├── session.*        one thread per connected client; request dispatch
│   ├── sync.*           tracker-to-tracker replication (op log + catch-up)
│   └── main.cpp         listener, console `quit`
├── client/
│   ├── tracker_client.* one connection to a tracker, with failover
│   ├── seeder.*         listening side; serves pieces to other peers
│   ├── piece_store.*    pread/pwrite piece I/O + have-bitmap
│   ├── download_manager.* concurrent multi-peer downloads
│   └── main.cpp         CLI loop
└── tracker_info.txt
```

## Build and run

```sh
make                                   # builds tracker/tracker and client/client
./tracker/tracker tracker_info.txt 0
./tracker/tracker tracker_info.txt 1
./client/client 127.0.0.1:6000 tracker_info.txt
```

The client's `<IP>:<PORT>` is the port it listens on to seed pieces. Give each
client a distinct one.

## Wire format

Every message is a packed 12-byte header followed by `length` payload bytes:

| field  | size | notes                              |
|--------|------|------------------------------------|
| magic  | 4    | `0x50325046` ("P2PF"), network order |
| type   | 2    | opcode from `MsgType`              |
| status | 2    | `Status`; 0 on requests            |
| length | 4    | payload byte count                 |

Payloads use `Buffer`: fixed-width integers and `u32`-length-prefixed strings.
`recv_msg` rejects any `length` above `MAX_PAYLOAD`, so a malformed peer cannot
make you allocate arbitrarily.

## What is already done

- Framing that survives partial reads and writes (`send_all` / `recv_all`)
- SHA1, verified against the RFC 3174 test vectors
- Streaming whole-file + per-piece hashing that never loads the file into memory
- Piece-level disk I/O with `pread`/`pwrite` and a have-bitmap
- Tracker accept loop, per-client session thread, `create user` / `login` /
  `list groups`, console `quit`
- Client seeder that answers `MSG_PIECE_REQUEST`
- Tracker failover scaffolding on the client side

## What is left for you (the graded parts)

1. **Remaining `TrackerState` methods** — groups, join requests, ownership,
   file metadata, seeder registration. Each follows the same
   lock → look up → check permission → mutate shape as `create_user`.
2. **Tracker synchronisation** (`tracker/sync.cpp`) — pick primary/backup or
   active/active, sync or async replication, and a catch-up protocol for a
   tracker that rejoins. The op log with sequence numbers is already sketched.
   Guard against re-replicating ops you received from the peer.
3. **Piece selection and the download loop** (`client/download_manager.cpp`) —
   work queue, one worker per peer, timeout-and-requeue on peer death,
   re-register as a partial seeder after the first verified piece.
4. **The rest of the CLI** in `client/main.cpp`.
5. **Cleanup and robustness** — close every fd on every path, bound the op log,
   handle a peer vanishing mid-transfer, run under `valgrind`.

## Things worth writing up in the report

- Why your chosen replication topology, and what a client observes during the
  window where the two trackers disagree
- Your piece selection strategy and how it compares to sequential/random/
  rarest-first
- Why verification happens before the piece is written to disk
- Thread budget: one thread per peer connection is fine at this scale, but say
  what you would do at 1000 peers
