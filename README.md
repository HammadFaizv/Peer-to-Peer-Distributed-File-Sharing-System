# P2P Distributed File Sharing System

## 1. Overview

This project implements a **Peer-to-Peer (P2P) Distributed File Sharing System** using C/C++ and TCP sockets.

The system allows users to:

* Create and authenticate user accounts
* Create and join groups
* Share files within groups
* Discover peers that have shared files
* Download files from multiple peers concurrently
* Verify downloaded file pieces using SHA1
* Re-download corrupted pieces from other peers
* Maintain synchronized metadata across two tracker servers
* Continue operating when one tracker becomes unavailable

The actual file contents are **not stored on the trackers**. Trackers maintain metadata such as users, groups, file information, piece hashes, and peer locations. File pieces are transferred directly between clients.

---

# 2. System Architecture

The system consists of:

* **Tracker 1**
* **Tracker 2**
* **Multiple Clients/Peers**

Each client can act as both:

* A downloader
* A seeder

### Architecture

```text
                         ┌──────────────────┐
                         │    Tracker 1     │
                         │                  │
                         │ User Metadata    │
                         │ Group Metadata   │
                         │ File Metadata    │
                         │ Peer Information │
                         └────────┬─────────┘
                                  │
                           Synchronization
                                  │
                         ┌────────▼─────────┐
                         │    Tracker 2     │
                         │                  │
                         │ User Metadata    │
                         │ Group Metadata   │
                         │ File Metadata    │
                         │ Peer Information │
                         └──────────────────┘


          Metadata                         Metadata
             ▲                                ▲
             │                                │
      ┌──────┴──────┐                  ┌──────┴──────┐
      │   Client 1  │◄──── File ─────►│   Client 2  │
      │              │     Pieces      │              │
      │ Downloader   │                 │ Downloader   │
      │ + Seeder     │                 │ + Seeder     │
      └──────────────┘                 └──────────────┘
```

## Communication Channels

The system uses TCP sockets for three types of communication:

### Client ↔ Tracker

Used for:

* User registration
* Login
* Group operations
* File metadata registration
* File discovery
* Peer discovery

### Client ↔ Client

Used for:

* Requesting file pieces
* Sending file pieces
* Parallel downloads

### Tracker ↔ Tracker

Used for:

* Synchronizing users
* Synchronizing groups
* Synchronizing file metadata
* Synchronizing peer information

---

# 3. Project Structure

```text
.
├── client/
│   ├── client.cpp
│   ├── client.h
│   ├── tracker_client.cpp
│   ├── peer_server.cpp
│   ├── downloader.cpp
│   ├── uploader.cpp
│   ├── ...
│   └── Makefile
│
├── tracker/
│   ├── tracker.cpp
│   ├── tracker.h
│   ├── tracker_server.cpp
│   ├── synchronization.cpp
│   ├── metadata.cpp
│   ├── ...
│   └── Makefile
│
├── common/
│   ├── tcp.cpp
│   ├── tcp.h
│   ├── protocol.cpp
│   ├── protocol.h
│   ├── sha1.cpp
│   ├── sha1.h
│   └── ...
│
└── README.md
```

The `common/` module contains functionality shared by both trackers and clients.

---

# 4. Common TCP Module

The networking layer is separated from the application logic.

The TCP module provides reusable functions such as:

```text
create_server_socket()
accept_connection()
connect_to_server()
send_all()
recv_all()
close_socket()
```

The same networking functions are used for:

```text
Client → Tracker
Client → Client
Tracker → Tracker
```

This avoids duplicating socket-handling code.

## TCP Reliability

TCP is stream-oriented and does not guarantee that one `send()` corresponds to one `recv()`.

Therefore, the implementation provides functions such as:

```text
send_all()
recv_all()
```

to ensure that the complete requested amount of data is transmitted or received.

---

# 5. Protocol Design

A custom application-level protocol is implemented over TCP.

Each message contains a header followed by a payload.

Conceptually:

```text
+----------------+----------------+----------------+
| Message Type   | Payload Length | Request ID     |
+----------------+----------------+----------------+
|                    Payload                       |
+--------------------------------------------------+
```

### Message Type

Identifies the operation being performed.

Example message types:

```text
CREATE_USER
LOGIN
CREATE_GROUP
JOIN_GROUP
LEAVE_GROUP
LIST_GROUPS
LIST_REQUESTS
ACCEPT_REQUEST
UPLOAD
LIST_FILES
DOWNLOAD
PIECE_REQUEST
PIECE_RESPONSE
STOP_SHARE
TRACKER_SYNC
ERROR
```

### Payload Length

Specifies the number of bytes in the payload.

This allows the receiver to determine exactly how much data must be read.

---

# 6. User and Group Management

The following operations are supported.

## Create User

```text
create user <user_id> <password>
```

Registers a new user.

Example:

```text
create user hammad password123
```

---

## Login

```text
login <user_id> <password>
```

Authenticates the user and starts a session.

---

## Create Group

```text
create group <group_id>
```

Creates a new group.

The user who creates the group becomes the owner.

---

## Join Group

```text
join group <group_id>
```

Sends a request to join the specified group.

The group owner must accept the request.

---

## Leave Group

```text
leave group <group_id>
```

Removes the current user from the group.

---

## List Groups

```text
list groups
```

Displays the groups available in the system.

---

## List Requests

```text
list requests <group_id>
```

Displays pending requests for a group.

Only the group owner can perform this operation.

---

## Accept Request

```text
accept request <group_id> <user_id>
```

Accepts a pending group membership request.

Only the group owner can perform this operation.

---

## Logout

```text
logout
```

Ends the current session and stops sharing files for that session.

---

# 7. File Management

Files are divided into pieces of:

```text
512 KB
```

The final piece may be smaller if the file size is not exactly divisible by 512 KB.

For example, a 1.3 MB file is logically divided as:

```text
File
│
├── Piece 0 → 512 KB
├── Piece 1 → 512 KB
└── Piece 2 → remaining bytes
```

The pieces do not need to be stored as separate files. The original file can be accessed using offsets.

---

# 8. SHA1 Integrity Verification

SHA1 hashes are calculated at two levels:

### Piece Level

Every piece has its own SHA1 hash.

```text
Piece 0 → SHA1(Piece 0)
Piece 1 → SHA1(Piece 1)
Piece 2 → SHA1(Piece 2)
```

### Complete File Level

The complete file also has a SHA1 hash.

```text
SHA1(complete file)
```

The piece hashes and complete-file hash are maintained as file metadata.

---

# 9. Upload

Command:

```text
upload file <group_id> <file_path>
```

Example:

```text
upload file programming /home/hammad/test.zip
```

The upload process is:

```text
Open file
   │
   ▼
Determine file size
   │
   ▼
Divide logically into 512 KB pieces
   │
   ▼
Calculate SHA1 for every piece
   │
   ▼
Calculate SHA1 for complete file
   │
   ▼
Send metadata to tracker
   │
   ▼
Register current client as a seeder
```

The tracker stores metadata, not the actual file contents.

---

# 10. File Metadata

For each shared file, the tracker maintains information such as:

```text
File name
Group ID
File size
Number of pieces
Piece SHA1 hashes
Complete file SHA1
Peer information
```

Example:

```text
File: test.zip
Size: 2 MB
Pieces: 4

Piece 0 → hash0
Piece 1 → hash1
Piece 2 → hash2
Piece 3 → hash3

Complete file → file_hash
```

---

# 11. Listing Files

Command:

```text
list files <group_id>
```

Displays files available within a group.

Only files shared with the specified group are returned.

---

# 12. Peer Discovery

When a client wants to download a file, it first contacts a tracker.

The tracker provides information about peers that are currently sharing the file.

For example:

```text
File: test.zip

Piece 0 → Client A
Piece 1 → Client A, Client B
Piece 2 → Client B
Piece 3 → Client A, Client B
```

The downloader uses this information to determine which peer should provide each piece.

---

# 13. P2P Download

Command:

```text
download file <group_id> <file_name> <destination_path>
```

Example:

```text
download file programming test.zip /home/hammad/downloads/
```

The download process is:

```text
                Tracker
                   │
            Peer information
                   │
                   ▼
              Downloader
             /     |      \
            /      |       \
         Peer A  Peer B  Peer C
           │       │        │
        Piece 0  Piece 2  Piece 3
           │       │        │
           └───────┼────────┘
                   │
                 Piece 1
                   │
                   ▼
             Reconstruct file
```

Different pieces can be downloaded simultaneously from different peers.

---

# 14. Piece Verification

Every received piece is verified immediately.

```text
Request Piece
      │
      ▼
Receive Piece
      │
      ▼
Calculate SHA1
      │
      ▼
Compare with expected hash
      │
   ┌──┴───┐
   │      │
  Match  Mismatch
   │      │
   ▼      ▼
 Save   Discard
          │
          ▼
   Request from another peer
```

A corrupted piece is never accepted into the final file.

---

# 15. Piece Selection Strategy

The downloader selects pieces based on the availability information received from trackers.

A piece-selection strategy can be used to avoid unnecessary duplicate downloads and improve peer utilization.

The implementation should ensure that:

* Already downloaded pieces are not unnecessarily requested again
* Multiple peers can be used simultaneously
* Failed peer connections can be retried
* Corrupted pieces can be requested from another peer

---

# 16. Concurrent Downloads

The client supports multiple downloads simultaneously.

For example:

```text
Client
│
├── Download A
│     ├── Peer 1
│     ├── Peer 2
│     └── Peer 3
│
└── Download B
      ├── Peer 4
      └── Peer 5
```

Individual pieces of the same file can also be downloaded concurrently.

Shared structures such as:

```text
Download state
Piece availability
Peer lists
File metadata
```

are protected using appropriate synchronization mechanisms.

---

# 17. Client as a Seeder

After downloading or uploading a file, a client can provide its available pieces to other clients.

For example:

```text
Client A
│
├── Piece 0 ✓
├── Piece 1 ✓
├── Piece 2 ✓
└── Piece 3 ✓
```

Another client can request:

```text
PIECE_REQUEST
file = test.zip
piece = 2
```

The peer responds with:

```text
PIECE_RESPONSE
file = test.zip
piece = 2
data = <piece bytes>
```

---

# 18. Stop Sharing

Command:

```text
stop share <group_id> <file_name>
```

Removes the current client as a seeder for that file/group.

The actual local file may still exist, but the client no longer advertises itself as a source for that shared file.

---

# 19. Tracker Synchronization

There are exactly two trackers:

```text
Tracker 1
   │
   │ synchronization
   ▼
Tracker 2
```

Both trackers maintain synchronized metadata.

Updates include:

* User creation
* Group creation
* Group membership changes
* File sharing information
* Peer information

When an update occurs on one tracker, it is propagated to the other tracker when it is available.

---

# 20. Tracker Failure

The system should continue operating when one tracker becomes unavailable.

Example:

```text
Tracker 1      Tracker 2
    ✓              ✗
    │
    │
 Clients continue
 using Tracker 1
```

When Tracker 2 becomes available again:

```text
Tracker 1
    │
    │ Missing updates
    ▼
Tracker 2
```

The recovered tracker synchronizes its state with the currently active tracker.

---

# 21. Handling Network Failures

The implementation handles failures such as:

* Tracker disconnection
* Peer disconnection
* Partial TCP messages
* Failed connections
* Corrupted file pieces
* Tracker synchronization failures
* Temporary network partitions

For example, if a peer disconnects during a download:

```text
Peer A
  │
  X connection lost
  │
  ▼
Downloader
  │
  ▼
Select another peer
  │
  ▼
Request missing piece
```

---

# 22. Download Progress

The system maintains the state of active downloads.

Completed downloads use the required format:

```text
[C] [group id] filename
```

Example:

```text
[C] programming test.zip
```

The following command displays current download progress:

```text
show downloads
```

---

# 23. Data Structures

The implementation uses in-memory data structures to maintain system state.

### Tracker

Conceptually:

```text
Users
 └── user_id → authentication information

Groups
 └── group_id
       ├── owner
       ├── members
       └── pending requests

Files
 └── file/group
       ├── size
       ├── piece count
       ├── piece hashes
       ├── complete file hash
       └── peers
```

### Client

The client maintains information such as:

```text
Logged-in user
Groups
Shared files
Downloaded files
Active downloads
Peer connections
Piece availability
```

Appropriate synchronization mechanisms are used when these structures are accessed by multiple threads.

---

# 24. Compilation

The project can be compiled using the provided Makefiles.

From the project root:

```bash
cd tracker
make
```

and:

```bash
cd client
make
```

This produces:

```text
tracker/tracker
client/client
```

If required, clean the generated files using:

```bash
make clean
```

---

# 25. Running the System

## Start Tracker 1

```bash
./tracker tracker_info.txt 1
```

## Start Tracker 2

```bash
./tracker tracker_info.txt 2
```

The tracker configuration file contains the addresses and ports required to communicate with the two trackers.

---

## Start a Client

```bash
./client <IP>:<PORT> tracker_info.txt
```

Example:

```bash
./client 127.0.0.1:6001 tracker_info.txt
```

The client connects to the specified tracker and uses the tracker information for metadata operations.

---

# 26. Example Usage

### Create user

```text
create user hammad password123
```

### Login

```text
login hammad password123
```

### Create group

```text
create group programming
```

### Upload file

```text
upload file programming /home/hammad/test.zip
```

### List files

```text
list files programming
```

### Another client joins

```text
create user ali password456
login ali password456
join group programming
```

The group owner accepts:

```text
list requests programming
accept request programming ali
```

### Download

```text
download file programming test.zip /home/ali/downloads/
```

### Monitor downloads

```text
show downloads
```

### Stop sharing

```text
stop share programming test.zip
```

### Logout

```text
logout
```

---

# 27. Error Handling

The implementation checks for errors during:

* Socket creation
* Binding
* Listening
* Accepting connections
* Connecting to peers
* Sending data
* Receiving data
* File opening
* File reading
* File writing
* Memory allocation
* SHA1 verification
* Tracker synchronization

Errors are communicated using appropriate status/error responses.

---

# 28. Memory and File Descriptor Management

The implementation ensures that:

* Dynamically allocated memory is released
* File descriptors are closed
* Socket descriptors are closed
* Failed operations do not leave resources allocated
* Threads terminate correctly
* Shared resources are synchronized

Special attention is given to avoiding memory leaks and segmentation faults.

---

# 29. Testing

Testing is performed using:

```text
2 Trackers
3 Clients
```

The system is tested with different file sizes:

```text
< 512 KB
= 512 KB
> 512 KB
Multiple MB
Large files
```

Different file types are tested, including:

```text
Text files
Binary files
Images
Compressed archives
```

---

# 30. Failure Testing

The following scenarios are tested.

### Tracker Failure

```text
Tracker 1 ✓
Tracker 2 ✗
```

Clients should continue operating through the available tracker.

### Peer Failure

During a download:

```text
Peer A
  │
  X
  │
  ▼
Downloader → Peer B
```

The downloader should obtain missing pieces from another available peer.

### Corrupted Piece

```text
Received SHA1 != Expected SHA1
```

The piece is discarded and requested again.

### Network Partition

Trackers may temporarily lose communication with each other.

When communication is restored, the tracker state is synchronized.

### Concurrent Downloads

Multiple files and multiple pieces are downloaded concurrently.

---

# 31. Performance Considerations

The implementation avoids loading entire large files into memory.

Files are processed piece-by-piece:

```text
512 KB
   ↓
process
   ↓
next 512 KB
   ↓
process
   ↓
...
```

This allows files approaching 1 GB to be handled without requiring the entire file to be stored in memory.

Parallel piece downloads are used to improve download performance.

---

# 32. Design Decisions

### Why 512 KB pieces?

The assignment specifies a fixed piece size of 512 KB. This allows large files to be transferred and verified incrementally.

### Why SHA1 per piece?

Piece-level hashes allow corrupted pieces to be detected immediately rather than waiting until the complete file has been downloaded.

### Why direct client-to-client transfer?

The P2P architecture avoids transferring the complete file through a centralized server and allows multiple peers to contribute pieces simultaneously.

### Why two trackers?

Two trackers provide redundancy and allow the system to remain operational when one tracker is unavailable.

### Why a common TCP module?

TCP communication is required for all three communication paths. Keeping socket operations in a common module avoids duplicated networking code and separates networking from application logic.

---

# 33. Limitations and Assumptions

The implementation makes the following assumptions:

* Clients communicate using IPv4 TCP sockets.
* The tracker configuration provides valid tracker addresses and ports.
* File paths supplied to upload/download operations are valid.
* SHA1 is used as required by the assignment.
* Tracker synchronization occurs whenever the peer tracker is available.
* The implementation focuses on correctness and reliability rather than implementing every possible optimization.

Any additional implementation-specific limitations should be documented here.

---

# 34. Important Implementation Notes

The system must not rely on:

```text
system()
exec()
popen()
```

or external torrent implementations/database libraries.

The implementation uses C/C++ and standard system calls/libraries for networking and file operations.

---

# 35. Technical Summary

```text
                    P2P FILE SHARING SYSTEM

                         ┌─────────────┐
                         │  Tracker 1  │
                         └──────┬──────┘
                                │
                           Synchronize
                                │
                         ┌──────▼──────┐
                         │  Tracker 2  │
                         └─────────────┘
                                ▲
                                │
                         Metadata only
                                │
              ┌─────────────────┴─────────────────┐
              │                                   │
        ┌─────▼─────┐                       ┌─────▼─────┐
        │  Client A │◄────── Pieces ──────►│  Client B │
        │            │                       │            │
        │ Downloader│                       │ Downloader │
        │ Seeder    │                       │ Seeder     │
        └───────────┘                       └────────────┘

File
 │
 ├── 512 KB Piece ── SHA1
 ├── 512 KB Piece ── SHA1
 ├── 512 KB Piece ── SHA1
 └── Final Piece ──── SHA1
 │
 └── Complete File ── SHA1
```

The main design goals are:

1. **P2P file transfer**
2. **Parallel multi-peer downloads**
3. **512 KB piece management**
4. **Piece-level and file-level SHA1 verification**
5. **Two-tracker redundancy**
6. **Tracker synchronization**
7. **TCP-based custom protocol**
8. **Concurrent operations**
9. **Failure handling**
10. **Efficient memory and resource management**

---

# 36. References

The implementation should document any external resources consulted during development.

Examples of topics that may be referenced:

* Linux TCP socket programming
* POSIX threads
* SHA1 implementation
* TCP stream behavior
* File descriptor operations
* Concurrent programming

All external resources used during implementation should be listed in the final technical report as required by the assignment.
