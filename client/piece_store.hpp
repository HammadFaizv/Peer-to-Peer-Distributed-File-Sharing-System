#pragma once
// Per-file piece bookkeeping and disk I/O.
//
// The file is never held in memory. A destination file is created at full size
// up front (ftruncate makes it sparse), and each verified piece is written
// straight to offset index*PIECE_SIZE with pwrite. Seeding reads with pread.
// Both are atomic with respect to the file offset, so many threads can share
// one fd without locking around lseek.
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "../common/protocol.hpp"

namespace p2p {

class PieceStore {
public:
    ~PieceStore();

    // Open an existing complete file for seeding.
    bool open_for_seed(const std::string& path, uint64_t size, uint32_t piece_count);
    // Create/reopen a destination file for downloading.
    bool open_for_download(const std::string& path, uint64_t size, uint32_t piece_count);

    bool read_piece(uint32_t index, std::string& out) const;
    // Verifies against expected_hash before writing. Returns false if corrupt.
    bool write_piece(uint32_t index, const std::string& data,
                     const std::string& expected_hash);

    bool     have(uint32_t index) const;
    uint32_t have_count() const;
    bool     complete() const;
    std::vector<uint8_t> bitfield() const;          // 1 bit per piece, MSB first
    void     set_bitfield(const std::vector<uint8_t>& bits);

    uint32_t piece_count() const { return piece_count_; }
    uint32_t piece_len(uint32_t index) const;       // last piece may be short
    uint64_t size() const { return size_; }

private:
    mutable std::mutex __mu_lock;
    int      fd_ = -1;
    uint64_t size_ = 0;
    uint32_t piece_count_ = 0;
    std::vector<uint8_t> have_; // bitmap
};

} // namespace p2p
