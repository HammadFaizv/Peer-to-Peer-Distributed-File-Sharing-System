// download manager deal with all downloads using
// thread per peer for a download instead of per piece
// benefits - lower thread count no need to create new thread for each piece
// as pieces can be many
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "piece_store.hpp"
#include "seeder.hpp"
#include "tracker_client.hpp"

namespace p2p {

struct PeerAddr {
    std::string user_id, ip;
    uint16_t port;
};

struct DownloadJob {
    std::string group, file, dest_path;
    uint64_t    size = 0;
    std::string file_hash;
    std::vector<std::string> piece_hashes;
    std::vector<PeerAddr>    peers;
    std::string user_id;     // whoever is running this download (for MSG_HAVE_PIECES)

    std::shared_ptr<PieceStore> store;
    std::atomic<bool> done{false};
    std::atomic<bool> failed{false};

    // Piece indices still needed by some worker; shared across this job's
    // peer_worker threads so a piece a dead/bad peer dropped can be picked
    // up by another one.
    std::mutex __queue_mu;
    std::vector<uint32_t> __queue;
};

class DownloadManager {
public:
    DownloadManager(Seeder& s, TrackerClient& tc) : seeder_(s), tracker_(tc) {}
    ~DownloadManager();

    // Returns false if a job for (group,file) is already running.
    bool start(std::shared_ptr<DownloadJob> job);
    // Lines in the assignment's required format: "[C] [group] filename"
    std::vector<std::string> status_lines();
    void join_all();

private:
    void run_job(std::shared_ptr<DownloadJob> job);
    void peer_worker(std::shared_ptr<DownloadJob> job, PeerAddr peer);
    bool fetch_piece(int fd, const DownloadJob& job, uint32_t index, std::string& out);

    Seeder& seeder_;
    TrackerClient& tracker_;
    std::mutex __mu_lock;
    std::map<std::string, std::shared_ptr<DownloadJob>> jobs_;  // "group/file"
    std::vector<std::thread> threads_;
};

} // namespace p2p
