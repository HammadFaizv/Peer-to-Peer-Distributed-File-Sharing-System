#pragma once
// Minimal length-prefixed serialization for message payloads.
#include <cstdint>
#include <string>

namespace p2p {

class Buffer {
public:
    Buffer() = default;
    explicit Buffer(std::string data) : __data(std::move(data)) {}

    void put_u8(uint8_t v)   { __data.push_back(static_cast<char>(v)); }
    void put_u16(uint16_t v) { put_raw(&v, sizeof(v)); }
    void put_u32(uint32_t v) { put_raw(&v, sizeof(v)); }
    void put_u64(uint64_t v) { put_raw(&v, sizeof(v)); }
    void put_str(const std::string& s) { put_u32(static_cast<uint32_t>(s.size())); __data.append(s); }
    void put_raw(const void* p, size_t n) { __data.append(static_cast<const char*>(p), n); }

    bool get_u8(uint8_t& v)   { return get_raw(&v, sizeof(v)); }
    bool get_u16(uint16_t& v) { return get_raw(&v, sizeof(v)); }
    bool get_u32(uint32_t& v) { return get_raw(&v, sizeof(v)); }
    bool get_u64(uint64_t& v) { return get_raw(&v, sizeof(v)); }
    bool get_str(std::string& s);
    bool get_raw(void* p, size_t n);

    const std::string& str() const { return __data; }
    size_t remaining() const { return __data.size() - __rpos; }

private:
    std::string __data;
    size_t __rpos = 0;
};

} // namespace p2p
