#include "download_manager.h"
#include "../common/net.h"
#include "../common/buffer.h"

#include <unistd.h>
#include <cstdio>

namespace p2p {

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

    // TODO:
    //  1. build the needed-piece queue (all indices, ordered by your chosen
    //     selection strategy)
    //  2. spawn one peer_worker per entry in job->peers
    //  3. join them; if pieces remain, re-ask the tracker for a fresh peer
    //     list and retry, up to some bound
    //  4. verify the assembled file's SHA1 against job->file_hash
    //  5. register the completed file with seeder_ so this client now seeds it

    job->done = true;
}

void DownloadManager::peer_worker(std::shared_ptr<DownloadJob> job, PeerAddr peer) {
    int fd = tcp_connect(peer.ip, peer.port);
    if (fd < 0) return;                 // dead peer: just retire this worker
    // TODO: loop { claim a piece; fetch_piece(); on success store->write_piece()
    // and announce MSG_HAVE_PIECES; on failure or hash mismatch return the
    // piece to the queue so a different peer retries it }
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
