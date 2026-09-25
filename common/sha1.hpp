#pragma once
// Self-contained SHA1 (RFC 3174) no external crypto library, per the
// assignment's constraint on third-party libraries
#include <cstdint>
#include <cstddef>
#include <string>

namespace p2p {

class SHA1 {
public:
    SHA1() { reset(); }
    void reset();
    void update(const void* data, size_t n);
    std::string final_hex(); // 40 lowercase hex chars

    static std::string hash_buffer(const void* data, size_t n);
    // Hashes each piece in parallel (one piece buffer per thread, never the
    // whole file). Returns SHA1 of the concatenated piece hashes, "" on error.
    // threads == 0 means use hardware_concurrency().
    static std::string hash_file(const std::string& path,
                                 uint32_t piece_size,
                                 std::string* piece_hashes_out = nullptr,
                                 uint64_t* size_out = nullptr,
                                 unsigned threads = 0);

private:
    void transform(const uint8_t* block);
    uint32_t h_[5];
    uint8_t  buf_[64];
    size_t   buf_used_;
    uint64_t total_bits_;
};

} // namespace p2p
