#include "piece_store.hpp"
#include "../common/sha1.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <thread>

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

// Runs before any download worker starts, so hashing happens unlocked across
// all cores; only the final bitmap update takes the lock.
void PieceStore::resume_scan(const std::vector<std::string>& piece_hashes) {
    int fd;
    uint32_t n;
    {
        std::lock_guard<std::mutex> g(__mu_lock);
        if (fd_ < 0) return;
        fd = fd_;
        n = std::min(piece_count_, static_cast<uint32_t>(piece_hashes.size()));
    }
    std::vector<uint8_t> valid(n, 0);
    std::atomic<uint32_t> next{0};
    auto worker = [&] {
        std::string buf;
        for (uint32_t i; (i = next.fetch_add(1)) < n;) {
            uint32_t len = piece_len(i);
            off_t off = static_cast<off_t>(i) * PIECE_SIZE;
            // Never-written pieces are holes in the sparse file; skip hashing
            // them. lseek moves the shared offset, but all other I/O is pread/pwrite.
            off_t data = ::lseek(fd, off, SEEK_DATA);
            if (data < 0 || data >= off + static_cast<off_t>(len)) continue;
            buf.assign(len, '\0');
            size_t got = 0;
            while (got < len) {
                ssize_t k = ::pread(fd, &buf[got], len - got, off + got);
                if (k <= 0) break;
                got += static_cast<size_t>(k);
            }
            valid[i] = got == len && SHA1::hash_buffer(buf.data(), len) == piece_hashes[i];
        }
    };
    unsigned nt = std::min<unsigned>(std::max(1u, std::thread::hardware_concurrency()),
                                     std::max<uint32_t>(n, 1));
    std::vector<std::thread> pool;
    for (unsigned t = 0; t < nt; ++t) pool.emplace_back(worker);
    for (auto& t : pool) t.join();

    std::lock_guard<std::mutex> g(__mu_lock);
    for (uint32_t i = 0; i < n; ++i)
        if (valid[i]) have_[i / 8] |= (0x80u >> (i % 8));
}

uint32_t PieceStore::piece_len(uint32_t index) const {
    if (index + 1 < piece_count_) return PIECE_SIZE;
    uint64_t rem = size_ - static_cast<uint64_t>(index) * PIECE_SIZE;
    return static_cast<uint32_t>(rem);
}

// NEW: The lock only guards the bitmap; pread/pwrite at distinct offsets are safe
// concurrently, so disk I/O runs unlocked and parallel connections don't serialize.
// This ensure speed up in file transfer and file write for faster downloads.
bool PieceStore::read_piece(uint32_t index, std::string& out) const {
    int fd;
    {
        std::lock_guard<std::mutex> g(__mu_lock);
        if (fd_ < 0 || index >= piece_count_) return false;
        if (!(have_[index / 8] & (0x80u >> (index % 8)))) return false;
        fd = fd_;
    }
    uint32_t n = piece_len(index);
    out.assign(n, '\0');
    off_t off = static_cast<off_t>(index) * PIECE_SIZE;
    size_t got = 0;
    while (got < n) {
        ssize_t k = ::pread(fd, &out[got], n - got, off + got);
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

    int fd;
    {
        std::lock_guard<std::mutex> g(__mu_lock);
        if (fd_ < 0) return false;
        fd = fd_;
    }
    off_t off = static_cast<off_t>(index) * PIECE_SIZE;
    size_t put = 0;
    while (put < data.size()) {
        ssize_t k = ::pwrite(fd, data.data() + put, data.size() - put, off + put);
        if (k <= 0) return false;
        put += static_cast<size_t>(k);
    }
    // Mark only after the bytes are on disk so a reader never serves a half-written piece.
    std::lock_guard<std::mutex> g(__mu_lock);
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
