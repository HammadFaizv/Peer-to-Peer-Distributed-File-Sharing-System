#include "piece_store.hpp"
#include "../common/sha1.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>

namespace p2p {

PieceStore::~PieceStore() { if (fd_ >= 0) ::close(fd_); }

static inline size_t bitmap_bytes(uint32_t n) { 
    return (n + 7) / 8; 
}

bool PieceStore::open_for_seed(const std::string& path, uint64_t size, uint32_t pc) {
    std::lock_guard<std::mutex> g(__mu_lock);
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ < 0) return false;
    size_ = size;
    piece_count_ = pc;
    have_.assign(bitmap_bytes(pc), 0xFF);   // a seeder has everything
    return true;
}

bool PieceStore::open_for_download(const std::string& path, uint64_t size, uint32_t pc) {
    std::lock_guard<std::mutex> g(__mu_lock);
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) return false;
    if (::ftruncate(fd_, static_cast<off_t>(size)) < 0) { 
        ::close(fd_); fd_ = -1; 
        return false; 
    }
    size_ = size;
    piece_count_ = pc;
    have_.assign(bitmap_bytes(pc), 0x00);
    return true;
}

uint32_t PieceStore::piece_len(uint32_t index) const {
    if (index + 1 < piece_count_) return PIECE_SIZE;
    uint64_t rem = size_ - static_cast<uint64_t>(index) * PIECE_SIZE;
    return static_cast<uint32_t>(rem);
}

bool PieceStore::read_piece(uint32_t index, std::string& out) const {
    std::lock_guard<std::mutex> g(__mu_lock);
    if (fd_ < 0 || index >= piece_count_) return false;
    if (!(have_[index / 8] & (0x80u >> (index % 8)))) return false;
    uint32_t n = piece_len(index);
    out.assign(n, '\0');
    off_t off = static_cast<off_t>(index) * PIECE_SIZE;
    size_t got = 0;
    while (got < n) {
        ssize_t k = ::pread(fd_, &out[got], n - got, off + got);
        if (k <= 0) return false;
        got += static_cast<size_t>(k);
    }
    return true;
}

bool PieceStore::write_piece(uint32_t index, const std::string& data,
                             const std::string& expected_hash) {
    if (index >= piece_count_) return false;
    if (data.size() != piece_len(index)) return false;
    // Verify before touching the disk: a corrupt piece must never be written,
    // or a later reader would seed corruption onward.
    if (SHA1::hash_buffer(data.data(), data.size()) != expected_hash) return false;

    std::lock_guard<std::mutex> g(__mu_lock);
    if (fd_ < 0) return false;
    off_t off = static_cast<off_t>(index) * PIECE_SIZE;
    size_t put = 0;
    while (put < data.size()) {
        ssize_t k = ::pwrite(fd_, data.data() + put, data.size() - put, off + put);
        if (k <= 0) return false;
        put += static_cast<size_t>(k);
    }
    have_[index / 8] |= (0x80u >> (index % 8));
    return true;
}

bool PieceStore::have(uint32_t index) const {
    std::lock_guard<std::mutex> g(__mu_lock);
    if (index >= piece_count_) return false;
    return (have_[index / 8] & (0x80u >> (index % 8))) != 0;
}

uint32_t PieceStore::have_count() const {
    std::lock_guard<std::mutex> g(__mu_lock);
    uint32_t c = 0;
    for (uint32_t i = 0; i < piece_count_; ++i)
        if (have_[i / 8] & (0x80u >> (i % 8))) ++c;
    return c;
}

bool PieceStore::complete() const { return have_count() == piece_count_; }

std::vector<uint8_t> PieceStore::bitfield() const {
    std::lock_guard<std::mutex> g(__mu_lock);
    return have_;
}

void PieceStore::set_bitfield(const std::vector<uint8_t>& bits) {
    std::lock_guard<std::mutex> g(__mu_lock);
    have_ = bits;
}

} // namespace p2p
