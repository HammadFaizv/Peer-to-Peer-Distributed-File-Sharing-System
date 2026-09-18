// Session is a tracker and a single user moment
// here all the user sent commands are handled as an API through TCP
// pasha
#include "session.hpp"
#include "../common/net.hpp"
#include "../common/buffer.hpp"

#include <unistd.h>
#include <cstdio>

namespace p2p {

void Session::reply(uint16_t status, const std::string& payload) {
    send_msg(fd_, MSG_RESPONSE, status, payload);
}

void Session::run() {
    MsgHeader hdr;
    std::string payload;
    while (true) {
        int r = recv_msg(fd_, hdr, payload);
        if (r != 1) break;               // clean close or protocol error
        dispatch(hdr.type, payload);
    }
    if (!user_.empty()) __state.logout(user_);
    close(fd_);
}

void Session::dispatch(uint16_t type, const std::string& payload) {
    Buffer in(payload);

    switch (type) {
    case MSG_CREATE_USER: {
        std::string uid, pwd;
        if (!in.get_str(uid) || !in.get_str(pwd)) { reply(ST_MALFORMED); return; }
        Status s = __state.create_user(uid, pwd);
        if (s == ST_OK) sync_.record_and_replicate(type, payload);
        reply(s);
        return;
    }
    case MSG_LOGIN: {
        std::string uid, pwd;
        uint16_t port = 0;
        if (!in.get_str(uid) || !in.get_str(pwd) || !in.get_u16(port)) { reply(ST_MALFORMED); return; }
        Status s = __state.login(uid, pwd, __peer_ip, port);
        if (s == ST_OK) { user_ = uid; seed_port_ = port; }
        reply(s);
        return;
    }
    case MSG_LOGOUT: {
        if(user_.empty()) {reply(ST_NOT_LOGGED_IN); return;}
        Status s =__state.logout(user_);
        user_.clear();
        reply(s);
        return;
    }
    case MSG_LIST_GROUPS: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        Buffer out;
        auto gs = __state.list_groups();
        out.put_u32(static_cast<uint32_t>(gs.size()));
        for (const auto& g : gs) out.put_str(g);
        reply(ST_OK, out.str());
        return;
    }
    case MSG_CREATE_GROUP: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid;
        if (!in.get_str(gid)) { reply(ST_MALFORMED); return; }
        Status s = __state.create_group(gid, user_);
        if (s == ST_OK) {
            // remember to send group id with user because user not present in payload
            Buffer rep; rep.put_str(gid); rep.put_str(user_);
            sync_.record_and_replicate(type, rep.str());
        }
        reply(s);
        return;
    }
    case MSG_JOIN_GROUP: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid;
        if (!in.get_str(gid)) { reply(ST_MALFORMED); return; }
        Status s = __state.join_group(gid, user_);
        if (s == ST_OK) {
            Buffer rep; rep.put_str(gid); rep.put_str(user_);
            sync_.record_and_replicate(type, rep.str());
        }
        reply(s);
        return;
    }
    case MSG_LEAVE_GROUP: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid;
        if (!in.get_str(gid)) { reply(ST_MALFORMED); return; }
        Status s = __state.leave_group(gid, user_);
        if (s == ST_OK) {
            Buffer rep; rep.put_str(gid); rep.put_str(user_);
            sync_.record_and_replicate(type, rep.str());
        }
        reply(s);
        return;
    }
    case MSG_LIST_REQUESTS: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid;
        if (!in.get_str(gid)) { reply(ST_MALFORMED); return; }
        std::vector<std::string> pending;
        Status s = __state.list_requests(gid, user_, pending);
        if (s != ST_OK) { reply(s); return; }
        Buffer out;
        out.put_u32(static_cast<uint32_t>(pending.size()));
        for (const auto& u : pending) out.put_str(u);
        reply(ST_OK, out.str());
        return;
    }
    case MSG_ACCEPT_REQUEST: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid, uid;
        if (!in.get_str(gid) || !in.get_str(uid)) { reply(ST_MALFORMED); return; }
        Status s = __state.accept_request(gid, user_, uid);
        if (s == ST_OK) {
            Buffer rep; rep.put_str(gid); rep.put_str(user_); rep.put_str(uid);
            sync_.record_and_replicate(type, rep.str());
        }
        reply(s);
        return;
    }

    case MSG_UPLOAD_FILE: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid;
        FileMeta meta;
        uint32_t pc = 0;
        if (!in.get_str(gid) || !in.get_str(meta.name) || !in.get_u64(meta.size) ||
            !in.get_str(meta.file_hash) || !in.get_u32(pc)) { reply(ST_MALFORMED); return; }
        meta.piece_hashes.resize(pc);
        for (uint32_t i = 0; i < pc; i++) {
            if (!in.get_str(meta.piece_hashes[i])) { reply(ST_MALFORMED); return; }
        }
        Status s = __state.add_file(gid, user_, __peer_ip, seed_port_, meta);
        if (s == ST_OK) {
            // Carry our own known ip:port through so the peer tracker's
            // replayed add_file doesn't have to look it up locally
            Buffer rep(payload);
            rep.put_str(user_);
            rep.put_str(__peer_ip);
            rep.put_u16(seed_port_);
            sync_.record_and_replicate(type, rep.str());
        }
        reply(s);
        return;
    }
    case MSG_LIST_FILES: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid;
        if (!in.get_str(gid)) { reply(ST_MALFORMED); return; }
        std::vector<std::string> names;
        Status s = __state.list_files(gid, user_, names);
        if (s != ST_OK) { reply(s); return; }
        Buffer out;
        out.put_u32(static_cast<uint32_t>(names.size()));
        for (const auto& n : names) out.put_str(n);
        reply(ST_OK, out.str());
        return;
    }
    case MSG_GET_FILE_META: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid, fname;
        if (!in.get_str(gid) || !in.get_str(fname)) { reply(ST_MALFORMED); return; }
        FileMeta meta;
        Status s = __state.get_file(gid, user_, fname, meta);
        if (s != ST_OK) { reply(s); return; }
        Buffer out;
        out.put_u64(meta.size);
        out.put_str(meta.file_hash);
        out.put_u32(static_cast<uint32_t>(meta.piece_hashes.size()));
        for (const auto& h : meta.piece_hashes) out.put_str(h);
        out.put_u32(static_cast<uint32_t>(meta.seeders.size()));
        for (const auto& kv : meta.seeders) {
            const PeerRef& p = kv.second;
            out.put_str(p.user_id);
            out.put_str(p.ip);
            out.put_u16(p.port);
            out.put_u32(static_cast<uint32_t>(p.bitfield.size()));
            out.put_raw(p.bitfield.data(), p.bitfield.size());
        }
        reply(ST_OK, out.str());
        return;
    }
    case MSG_STOP_SHARE: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid, fname;
        if (!in.get_str(gid) || !in.get_str(fname)) { reply(ST_MALFORMED); return; }
        Status s = __state.stop_share(gid, user_, fname);
        if (s == ST_OK) {
            Buffer rep(payload); rep.put_str(user_);
            sync_.record_and_replicate(type, rep.str());
        }
        reply(s);
        return;
    }
    case MSG_HAVE_PIECES: {
        if (user_.empty()) { reply(ST_NOT_LOGGED_IN); return; }
        std::string gid, fname;
        uint32_t nbits = 0;
        if (!in.get_str(gid) || !in.get_str(fname) || !in.get_u32(nbits)) { reply(ST_MALFORMED); return; }
        std::vector<uint8_t> bits(nbits);
        if (nbits > 0 && !in.get_raw(bits.data(), nbits)) { reply(ST_MALFORMED); return; }
        Status s = __state.update_bitfield(gid, user_, __peer_ip, seed_port_, fname, bits);
        if (s == ST_OK) {
            Buffer rep(payload);
            rep.put_str(user_);
            rep.put_str(__peer_ip);
            rep.put_u16(seed_port_);
            sync_.record_and_replicate(type, rep.str());
        }
        reply(s);
        return;
    }

    default:
        reply(ST_NOT_IMPLEMENTED);
        return;
    }
}

} // namespace p2p
