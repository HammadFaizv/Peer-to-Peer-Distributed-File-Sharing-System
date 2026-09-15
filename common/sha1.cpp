#include "sha1.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#include <cstdio>
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
                            std::string* piece_hashes_out, uint64_t* size_out) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return std::string();

    SHA1 whole;
    SHA1 piece;
    std::vector<char> chunk(64 * 1024);
    uint64_t total = 0, in_piece = 0;
    bool piece_open = false;

    for (;;) {
        ssize_t k = ::read(fd, chunk.data(), chunk.size());
        if (k < 0) { ::close(fd); return std::string(); }
        if (k == 0) break;
        size_t off = 0;
        while (off < static_cast<size_t>(k)) {
            if (!piece_open) { piece.reset(); in_piece = 0; piece_open = true; }
            size_t room = piece_size - in_piece;
            size_t take = static_cast<size_t>(k) - off;
            if (take > room) take = room;
            whole.update(chunk.data() + off, take);
            piece.update(chunk.data() + off, take);
            off += take;
            in_piece += take;
            total += take;
            if (in_piece == piece_size) {
                if (piece_hashes_out) piece_hashes_out->append(piece.final_hex());
                piece_open = false;
            }
        }
    }
    if (piece_open && piece_hashes_out) piece_hashes_out->append(piece.final_hex());
    ::close(fd);
    if (size_out) *size_out = total;
    return whole.final_hex();
}

} // namespace p2p
