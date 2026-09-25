#include "sha1.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <cstdio>
#include <thread>
#include <vector>

namespace p2p {

static inline uint32_t rol(uint32_t v, int b) { return (v << b) | (v >> (32 - b)); }

void SHA1::reset() {
    h_[0] = 0x67452301u; h_[1] = 0xEFCDAB89u; h_[2] = 0x98BADCFEu;
    h_[3] = 0x10325476u; h_[4] = 0xC3D2E1F0u;
    buf_used_ = 0;
    total_bits_ = 0;
}

void SHA1::transform(const uint8_t* p) {
    uint32_t w[80];
    for (int i = 0; i < 16; ++i) {
        w[i] = (static_cast<uint32_t>(p[4*i])   << 24) |
               (static_cast<uint32_t>(p[4*i+1]) << 16) |
               (static_cast<uint32_t>(p[4*i+2]) <<  8) |
               (static_cast<uint32_t>(p[4*i+3]));
    }
    for (int i = 16; i < 80; ++i)
        w[i] = rol(w[i-3] ^ w[i-8] ^ w[i-14] ^ w[i-16], 1);

    uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3], e = h_[4];
    for (int i = 0; i < 80; ++i) {
        uint32_t f, k;
        if (i < 20)      { f = (b & c) | (~b & d);            k = 0x5A827999u; }
        else if (i < 40) { f = b ^ c ^ d;                     k = 0x6ED9EBA1u; }
        else if (i < 60) { f = (b & c) | (b & d) | (c & d);   k = 0x8F1BBCDCu; }
        else             { f = b ^ c ^ d;                     k = 0xCA62C1D6u; }
        uint32_t t = rol(a, 5) + f + e + k + w[i];
        e = d; d = c; c = rol(b, 30); b = a; a = t;
    }
    h_[0] += a; h_[1] += b; h_[2] += c; h_[3] += d; h_[4] += e;
}

void SHA1::update(const void* data, size_t n) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    total_bits_ += static_cast<uint64_t>(n) * 8;
    while (n > 0) {
        size_t take = 64 - buf_used_;
        if (take > n) take = n;
        std::memcpy(buf_ + buf_used_, p, take);
        buf_used_ += take;
        p += take;
        n -= take;
        if (buf_used_ == 64) { transform(buf_); buf_used_ = 0; }
    }
}

std::string SHA1::final_hex() {
    uint64_t bits = total_bits_;
    uint8_t pad = 0x80;
    update(&pad, 1);
    uint8_t zero = 0x00;
    while (buf_used_ != 56) update(&zero, 1);
    uint8_t len[8];
    for (int i = 0; i < 8; ++i) len[i] = static_cast<uint8_t>((bits >> (56 - 8*i)) & 0xFF);
    // update() would corrupt total_bits_, so feed the length block directly.
    std::memcpy(buf_ + buf_used_, len, 8);
    transform(buf_);
    buf_used_ = 0;

    char out[41];
    for (int i = 0; i < 5; ++i) std::snprintf(out + i*8, 9, "%08x", h_[i]);
    return std::string(out, 40);
}

std::string SHA1::hash_buffer(const void* data, size_t n) {
    SHA1 s;
    s.update(data, n);
    return s.final_hex();
}

std::string SHA1::hash_file(const std::string& path, uint32_t piece_size,
                            std::string* piece_hashes_out, uint64_t* size_out,
                            unsigned threads) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return std::string();
    struct stat st;
    if (::fstat(fd, &st) < 0) { ::close(fd); return std::string(); }

    const uint64_t size = static_cast<uint64_t>(st.st_size);
    const uint32_t pc = static_cast<uint32_t>((size + piece_size - 1) / piece_size);
    std::vector<std::string> hashes(pc);
    std::atomic<uint32_t> next{0};
    std::atomic<bool> failed{false};

    // Workers pull piece indices from a shared counter; each writes only its
    // own slot in `hashes`, so no lock is needed. pread keeps the fd shareable.
    auto worker = [&] {
        std::vector<char> buf(piece_size);
        for (uint32_t i; !failed && (i = next.fetch_add(1)) < pc;) {
            uint64_t off = static_cast<uint64_t>(i) * piece_size;
            size_t len = static_cast<size_t>(std::min<uint64_t>(piece_size, size - off));
            size_t got = 0;
            while (got < len) {
                ssize_t k = ::pread(fd, buf.data() + got, len - got, static_cast<off_t>(off + got));
                if (k <= 0) { failed = true; return; }
                got += static_cast<size_t>(k);
            }
            hashes[i] = hash_buffer(buf.data(), len);
        }
    };

    // NEW: optional thread argument for threads if non uses max possible for concurrency
    unsigned n = threads ? threads : std::max(1u, std::thread::hardware_concurrency());
    n = std::min<unsigned>(n, std::max<uint32_t>(pc, 1));
    std::vector<std::thread> pool;
    for (unsigned t = 0; t < n; ++t) pool.emplace_back(worker);
    for (auto& t : pool) t.join();
    ::close(fd);
    if (failed) return std::string();

    std::string blob;
    blob.reserve(static_cast<size_t>(pc) * 40);
    for (const auto& h : hashes) blob += h;
    if (piece_hashes_out) *piece_hashes_out = blob;
    if (size_out) *size_out = size;
    // NEW: Whole-file hash = SHA1 of the concatenated piece hashes (hex), since a
    // true streaming SHA1 over the file can't be parallelised. This is done for speed up.
    return hash_buffer(blob.data(), blob.size());
}

} // namespace p2p
