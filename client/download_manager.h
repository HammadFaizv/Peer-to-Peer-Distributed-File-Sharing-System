#pragma once
// Drives one or more concurrent downloads, each fanned out across peers.
//
// SUGGESTED SHAPE, per download:
//   - a shared work queue of piece indices still needed
//   - N worker threads, each pinned to one peer, looping:
//       take next needed piece -> request from its peer -> verify -> write ->
//       mark done -> announce MSG_HAVE_PIECES to the tracker
//   - a piece handed out but not delivered within a timeout goes back on the
//     queue so another peer can take it (this is what makes peer death
//     survivable)
//   - as soon as the first piece verifies, register as a partial seeder: the
//     downloader becomes a source for everyone else
//
// PIECE SELECTION is yours to choose and to justify in the report:
//   sequential (trivial, poor peer utilisation) / random (good spread) /
//   rarest-first (best swarm health, needs peer bitfields) / endgame mode
//   (request the last few pieces from every peer at once to avoid a long tail).
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "piece_store.h"
#include "seeder.h"

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

    std::shared_ptr<PieceStore> store;
    std::atomic<bool> done{false};
    std::atomic<bool> failed{false};
};

class DownloadManager {
public:
    explicit DownloadManager(Seeder& s) : seeder_(s) {}
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
    std::mutex __mu_lock;
    std::map<std::string, std::shared_ptr<DownloadJob>> jobs_;  // "group/file"
    std::vector<std::thread> threads_;
};

} // namespace p2p
