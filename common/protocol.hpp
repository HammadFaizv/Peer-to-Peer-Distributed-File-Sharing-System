// Wire protocol definitions shared by tracker and client.
#pragma once
#include <cstdint>

namespace p2p {

constexpr uint32_t PROTO_MAGIC   = 0x50325046u; // "P2PF"
constexpr uint32_t PIECE_SIZE    = 512u * 1024u; // size of one data block to seed
constexpr uint32_t MAX_PAYLOAD   = PIECE_SIZE + 8192u; // piece + framing slack
constexpr uint32_t SHA1_HEX_LEN  = 40u;

// Message opcodes.
//   0x01xx : client  -> tracker (requests)
//   0x02xx : tracker -> client  (responses)
//   0x03xx : client <-> client  (data plane)
//   0x04xx : tracker <-> tracker (replication)
enum MsgType : uint16_t {
    MSG_CREATE_USER      = 0x0101,
    MSG_LOGIN            = 0x0102,
    MSG_LOGOUT           = 0x0103,
    MSG_CREATE_GROUP     = 0x0104,
    MSG_JOIN_GROUP       = 0x0105,
    MSG_LEAVE_GROUP      = 0x0106,
    MSG_LIST_GROUPS      = 0x0107,
    MSG_LIST_REQUESTS    = 0x0108,
    MSG_ACCEPT_REQUEST   = 0x0109,
    MSG_UPLOAD_FILE      = 0x010A, // publish metadata (name, size, hashes)
    MSG_LIST_FILES       = 0x010B,
    MSG_GET_FILE_META    = 0x010C, // metadata + peer list for a download
    MSG_STOP_SHARE       = 0x010D,
    MSG_HAVE_PIECES      = 0x010E, // announce newly completed pieces

    MSG_RESPONSE         = 0x0201, // generic reply; status lives in the header

    MSG_PIECE_REQUEST    = 0x0301, // {group, file, piece_index}
    MSG_PIECE_DATA       = 0x0302, // raw piece bytes
    MSG_BITFIELD_REQUEST = 0x0303,
    MSG_BITFIELD_DATA    = 0x0304,

    MSG_SYNC_OP          = 0x0401, // one replicated mutation
    MSG_SYNC_ACK         = 0x0402,
    MSG_SYNC_CATCHUP     = 0x0403, // "send me everything after seq N"
    MSG_SYNC_HELLO       = 0x0404,
    MSG_SYNC_SNAPSHOT_REQUEST = 0x0405, // "I have nothing, send a full state dump"
    MSG_SYNC_SNAPSHOT_DATA    = 0x0406, // TrackerState::snapshot() blob
};

enum Status : uint16_t {
    ST_OK               = 0,
    ST_ERR              = 1,
    ST_NOT_LOGGED_IN    = 2,
    ST_BAD_CREDENTIALS  = 3,
    ST_ALREADY_EXISTS   = 4,
    ST_NOT_FOUND        = 5,
    ST_NOT_OWNER        = 6,
    ST_NOT_MEMBER       = 7,
    ST_NOT_IMPLEMENTED  = 8,
    ST_MALFORMED        = 9,
};

#pragma pack(push, 1)
struct MsgHeader {
    uint32_t magic;   // PROTO_MAGIC
    uint16_t type;    // MsgType
    uint16_t status;  // Status (0 on requests)
    uint32_t length;  // payload byte count
};
#pragma pack(pop)

static_assert(sizeof(MsgHeader) == 12, "header must pack to 12 bytes");

const char* status_str(uint16_t s);

} // namespace p2p
