#pragma once

#include <cstddef>
#include <cstdint>

#include "snii/common/slice.h"
#include "snii/common/status.h"

namespace snii {

// Slice read cursor: all section deserialization goes through this; any overrun returns Corruption.
class ByteSource {
public:
    explicit ByteSource(Slice s) : s_(s) {}

    Status get_u8(uint8_t* v);
    Status get_fixed16(uint16_t* v);
    Status get_fixed32(uint32_t* v);
    Status get_fixed64(uint64_t* v);
    Status get_varint32(uint32_t* v);
    Status get_varint64(uint64_t* v);
    Status get_zigzag(int64_t* v);
    Status get_bytes(size_t n, Slice* out);

    size_t remaining() const { return s_.size() - pos_; }
    size_t position() const { return pos_; }
    bool eof() const { return pos_ == s_.size(); }

    // Returns a sub-view starting at absolute offset start with length len (used by framer etc. to rewind over the CRC coverage region).
    Slice slice_from(size_t start, size_t len) const { return s_.subslice(start, len); }

private:
    Slice s_;
    size_t pos_ = 0;
};

} // namespace snii
