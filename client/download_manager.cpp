#include "download_manager.hpp"
#include "../common/net.hpp"
#include "../common/buffer.hpp"
#include "../common/sha1.hpp"

#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <algorithm>
#include <cstdio>

namespace p2p {

namespace {
constexpr size_t kTargetConnections = 8;
constexpr uint32_t kAnnounceEveryPieces = 32;
constexpr auto kAnnounceInterval = std::chrono::seconds(1);
constexpr int kMaxRounds = 5;                         // ~20s of retrying in total
constexpr auto kRetryBackoff = std::chrono::seconds(2);
constexpr int kPeerIoTimeoutSec = 10;

// Decodes an MSG_GET_FILE_META response body into a fresh peer
// list skipping `self_uid`.
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
        // A finished or failed job can be restarted; resume_scan keeps its progress.
        auto it = jobs_.find(key);
        if (it != jobs_.end() && !it->second->done && !it->second->failed) return false;
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
    // dest_path contains bytes for pieces already downloaded. in case
    // client crashed mid-download - check what's already correct before fetching
    // dont redownload whole file.
    job->store->resume_scan(job->piece_hashes);
    if (job->store->have_count() > 0) {
        std::fprintf(stderr, "[download] %s/%s: resuming, %u/%u pieces already on disk\n",
                     job->group.c_str(), job->file.c_str(), job->store->have_count(), pc);
    }

    std::vector<PeerAddr> peers = job->peers;
    // Sequential piece selection: simple and good enough at this scale (upto 1gb)
    // A peer that dies or sends a bad piece - drops it
    // back on the queue (peer_worker) so this loop only needs to notice when
    // pieces are still missing after everyone has had a turn.
    for (int round = 0; round < kMaxRounds && !job->store->complete(); ++round) {
        if (round > 0) {
            // Back off so a seeder that crashed has time to come back, then
            // ask the tracker who currently holds the file.
            std::this_thread::sleep_for(kRetryBackoff * round);
            Buffer req; req.put_str(job->group); req.put_str(job->file);
            uint16_t status; std::string resp;
            peers.clear();
            if (tracker_.request(MSG_GET_FILE_META, req.str(), status, resp) && status == ST_OK)
                decode_peer_list(resp, job->user_id, peers);
            if (peers.empty()) continue;
        }
        {
            std::lock_guard<std::mutex> g(job->__queue_mu);
            job->__queue.clear();
            for (uint32_t i = 0; i < pc; ++i)
                if (!job->store->have(i)) job->__queue.push_back(i);
        }

        // Several connections per peer so a single seeder isn't stuck in
        // strict request/response lockstep; fewer each when there are many peers.
        size_t conns_per_peer = std::max<size_t>(1, kTargetConnections / peers.size());
        std::vector<std::thread> workers;
        workers.reserve(peers.size() * conns_per_peer);
        for (const auto& peer : peers)
            for (size_t c = 0; c < conns_per_peer; ++c)
                workers.emplace_back(&DownloadManager::peer_worker, this, job, peer);
        for (auto& t : workers) if (t.joinable()) t.join();

        // Pieces still missing here means every peer that had them died or
        // misbehaved this round; the next round retries with a fresh peer list.
        announce(*job);  // flush whatever the batching held back
    }

    // No whole-file re-hash: write_piece already verified every piece against
    // its SHA1, so re-reading the entire file from disk adds nothing.
    if (!job->store->complete()) { job->failed = true; return; }

    // As soon as the file is whole, this client becomes a full seeder for it.
    seeder_.add_share(ShareKey{job->group, job->file}, job->store);
    job->done = true;
}

namespace {
// A peer that's missing one piece (ST_NOT_FOUND) but otherwise fine
// shouldn't be dropped after a single miss; a genuinely dead connection
// will fail every attempt, so check for consecutive failure to get piece
// instead of the worker looping forever.
constexpr int maxConsecutiveFailures = 3;
} // namespace

void DownloadManager::peer_worker(std::shared_ptr<DownloadJob> job, PeerAddr peer) {
    int fd = tcp_connect(peer.ip, peer.port);
    if (fd < 0) return; // dead peer: just retire this worker, its pieces stay queued

    // Without a timeout, a peer whose machine vanished (no FIN/RST) leaves
    // recv blocked forever; the timeout turns that into a normal fetch failure.
    timeval tv{kPeerIoTimeoutSec, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    int consecutive_failures = 0;
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
            // Give the piece back for a different peer to try. Don't retire
            // this worker over one miss — so give up
            // on it once all failures pile up instead of retrying forever.
            std::lock_guard<std::mutex> g(job->__queue_mu);
            job->__queue.push_back(index);
            if (++consecutive_failures >= maxConsecutiveFailures) break;
            continue;
        }
        consecutive_failures = 0;
        maybe_announce(*job);
    }
    ::close(fd);
}

void DownloadManager::announce(DownloadJob& job) {
    {
        std::lock_guard<std::mutex> g(job.__announce_mu);
        job.pieces_since_announce = 0;
        job.last_announce = std::chrono::steady_clock::now();
    }
    // Lets other peers start pulling finished pieces from this client.
    Buffer hb;
    hb.put_str(job.group);
    hb.put_str(job.file);
    auto bits = job.store->bitfield();
    hb.put_u32(static_cast<uint32_t>(bits.size()));
    hb.put_raw(bits.data(), bits.size());
    uint16_t status;
    std::string resp;
    tracker_.request(MSG_HAVE_PIECES, hb.str(), status, resp);
}

void DownloadManager::maybe_announce(DownloadJob& job) {
    {
        std::lock_guard<std::mutex> g(job.__announce_mu);
        ++job.pieces_since_announce;
        bool due = job.pieces_since_announce >= kAnnounceEveryPieces ||
                   std::chrono::steady_clock::now() - job.last_announce >= kAnnounceInterval;
        if (!due) return;
    }
    announce(job);
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
        // only tells status not progress
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
