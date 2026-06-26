#pragma once

#include <cstddef>
#include <cstdint>

#include "snii/common/status.h"

namespace snii {

// LEB128 variable-length integer encoding + zigzag. out buffer must be >=10 bytes; returns number of bytes written.
size_t varint_len(uint64_t v);
size_t encode_varint32(uint32_t v, uint8_t* out);
size_t encode_varint64(uint64_t v, uint8_t* out);

// Decode a varint from the range [p, end); on success *next points to the next byte after the consumed input.
Status decode_varint32(const uint8_t* p, const uint8_t* end, uint32_t* v, const uint8_t** next);
Status decode_varint64(const uint8_t* p, const uint8_t* end, uint64_t* v, const uint8_t** next);

inline uint64_t zigzag_encode(int64_t v) {
    return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
}
inline int64_t zigzag_decode(uint64_t v) {
    return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1);
}

} // namespace snii
