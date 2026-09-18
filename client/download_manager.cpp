#include "download_manager.hpp"
#include "../common/net.hpp"
#include "../common/buffer.hpp"
#include "../common/sha1.hpp"

#include <unistd.h>
#include <cstdio>

namespace p2p {

namespace {
// Decodes an MSG_GET_FILE_META response body (see Session::dispatch's
// MSG_GET_FILE_META case for the wire format this mirrors) into a fresh peer
// list, skipping `self_uid` (no point asking ourselves for a piece).
bool decode_peer_list(const std::string& resp, const std::string& self_uid,
                      std::vector<PeerAddr>& out) {
    Buffer in(resp);
    uint64_t size = 0;
    std::string fhash;
    uint32_t pc = 0;
    if (!in.get_u64(size) || !in.get_str(fhash) || !in.get_u32(pc)) return false;
    for (uint32_t i = 0; i < pc; i++) { std::string h; if (!in.get_str(h)) return false; }
    uint32_t npeers = 0;
    if (!in.get_u32(npeers)) return false;
    out.clear();
    for (uint32_t i = 0; i < npeers; i++) {
        PeerAddr pa;
        uint32_t nbits = 0;
        if (!in.get_str(pa.user_id) || !in.get_str(pa.ip) || !in.get_u16(pa.port) ||
            !in.get_u32(nbits)) return false;
        std::vector<uint8_t> bits(nbits);
        if (nbits > 0 && !in.get_raw(bits.data(), nbits)) return false;
        if (pa.user_id != self_uid) out.push_back(pa);
    }
    return true;
}
} // namespace

DownloadManager::~DownloadManager() { join_all(); }

bool DownloadManager::start(std::shared_ptr<DownloadJob> job) {
    std::string key = job->group + "/" + job->file;
    {
        std::lock_guard<std::mutex> g(__mu_lock);
        if (jobs_.count(key)) return false;
        jobs_[key] = job;
        threads_.emplace_back(&DownloadManager::run_job, this, job);
    }
    return true;
}

void DownloadManager::run_job(std::shared_ptr<DownloadJob> job) {
    uint32_t pc = static_cast<uint32_t>(job->piece_hashes.size());
    job->store = std::make_shared<PieceStore>();
    if (!job->store->open_for_download(job->dest_path, job->size, pc)) {
        job->failed = true;
        return;
    }

    std::vector<PeerAddr> peers = job->peers;
    // Sequential piece selection: simple and good enough at the scale this
    // was tested at. A peer that dies or sends a bad piece drops its claim
    // back on the queue (peer_worker) so this loop only needs to notice when
    // pieces are still missing after everyone has had a turn.
    for (int round = 0; round < 3 && !job->store->complete() && !peers.empty(); ++round) {
        {
            std::lock_guard<std::mutex> g(job->__queue_mu);
            job->__queue.clear();
            for (uint32_t i = 0; i < pc; ++i)
                if (!job->store->have(i)) job->__queue.push_back(i);
        }

        std::vector<std::thread> workers;
        workers.reserve(peers.size());
        for (const auto& peer : peers) workers.emplace_back(&DownloadManager::peer_worker, this, job, peer);
        for (auto& t : workers) if (t.joinable()) t.join();

        if (job->store->complete()) break;

        // Some pieces are still missing because every peer that had them
        // died or misbehaved this round; ask the tracker for a fresh peer
        // list (it will reflect anyone who has come online meanwhile) and
        // try again, up to the round bound above.
        Buffer req; req.put_str(job->group); req.put_str(job->file);
        uint16_t status; std::string resp;
        peers.clear();
        if (tracker_.request(MSG_GET_FILE_META, req.str(), status, resp) && status == ST_OK) {
            decode_peer_list(resp, job->user_id, peers);
        }
    }

    if (!job->store->complete()) { job->failed = true; return; }

    std::string whole = SHA1::hash_file(job->dest_path, PIECE_SIZE, nullptr, nullptr);
    if (whole.empty() || whole != job->file_hash) { job->failed = true; return; }

    // As soon as the file is whole, this client becomes a full seeder for it.
    seeder_.add_share(ShareKey{job->group, job->file}, job->store);
    job->done = true;
}

void DownloadManager::peer_worker(std::shared_ptr<DownloadJob> job, PeerAddr peer) {
    int fd = tcp_connect(peer.ip, peer.port);
    if (fd < 0) return; // dead peer: just retire this worker, its pieces stay queued

    for (;;) {
        uint32_t index;
        {
            std::lock_guard<std::mutex> g(job->__queue_mu);
            if (job->__queue.empty()) break;
            index = job->__queue.back();
            job->__queue.pop_back();
        }

        std::string data;
        bool ok = fetch_piece(fd, *job, index, data) &&
                  job->store->write_piece(index, data, job->piece_hashes[index]);
        if (!ok) {
            // Dead connection or a corrupt/mismatched piece: give the piece
            // back for a different peer to try and retire this worker.
            std::lock_guard<std::mutex> g(job->__queue_mu);
            job->__queue.push_back(index);
            break;
        }

        // Announce the freshly completed piece so this client is usable as
        // a partial seeder immediately, not only once the whole file is done.
        Buffer hb;
        hb.put_str(job->group);
        hb.put_str(job->file);
        auto bits = job->store->bitfield();
        hb.put_u32(static_cast<uint32_t>(bits.size()));
        hb.put_raw(bits.data(), bits.size());
        uint16_t status;
        std::string resp;
        tracker_.request(MSG_HAVE_PIECES, hb.str(), status, resp);
    }
    ::close(fd);
}

bool DownloadManager::fetch_piece(int fd, const DownloadJob& job,
                                  uint32_t index, std::string& out) {
    Buffer req;
    req.put_str(job.group);
    req.put_str(job.file);
    req.put_u32(index);
    if (send_msg(fd, MSG_PIECE_REQUEST, 0, req.str()) != 1) return false;

    MsgHeader hdr;
    std::string payload;
    if (recv_msg(fd, hdr, payload) != 1) return false;
    if (hdr.type != MSG_PIECE_DATA || hdr.status != ST_OK) return false;

    Buffer in(payload);
    uint32_t got_index = 0;
    if (!in.get_u32(got_index) || got_index != index) return false;
    out.assign(payload, sizeof(uint32_t), payload.size() - sizeof(uint32_t));
    return true;
}

std::vector<std::string> DownloadManager::status_lines() {
    std::lock_guard<std::mutex> g(__mu_lock);
    std::vector<std::string> out;
    for (const auto& kv : jobs_) {
        const auto& j = kv.second;
        // The spec only mandates the completed form; add a progress form for
        // in-flight downloads and document it in your README.
        if (j->done) out.push_back("[C] [" + j->group + "] " + j->file);
        else if (j->failed) out.push_back("[F] [" + j->group + "] " + j->file);
        else out.push_back("[D] [" + j->group + "] " + j->file);
    }
    return out;
}

void DownloadManager::join_all() {
    std::vector<std::thread> ts;
    {
        std::lock_guard<std::mutex> g(__mu_lock);
        ts.swap(threads_);
    }
    for (auto& t : ts) if (t.joinable()) t.join();
}

} // namespace p2p
