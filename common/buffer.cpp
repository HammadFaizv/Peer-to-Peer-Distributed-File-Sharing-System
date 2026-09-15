#include "buffer.h"
#include <cstring>

namespace p2p {

bool Buffer::get_raw(void* p, size_t n) {
    if (remaining() < n) return false;
    std::memcpy(p, __data.data() + __rpos, n);
    __rpos += n;
    return true;
}

bool Buffer::get_str(std::string& s) {
    uint32_t n = 0;
    if (!get_u32(n)) return false;
    if (remaining() < n) return false;
    s.assign(__data, __rpos, n);
    __rpos += n;
    return true;
}

} // namespace p2p
