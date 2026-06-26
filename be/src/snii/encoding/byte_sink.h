#pragma once

#include <cstdint>
#include <vector>

#include "snii/common/slice.h"

namespace snii {

// append-only write cursor: all section serialization goes through this; manual byte assembly is forbidden.
// All multi-byte fixed-width fields are little-endian.
class ByteSink {
public:
    void put_u8(uint8_t v) { buf_.push_back(v); }
    void put_fixed16(uint16_t v);
    void put_fixed32(uint32_t v);
    void put_fixed64(uint64_t v);
    void put_varint32(uint32_t v);
    void put_varint64(uint64_t v);
    void put_zigzag(int64_t v);
    void put_bytes(Slice s);

    size_t size() const { return buf_.size(); }
    const std::vector<uint8_t>& buffer() const { return buf_; }
    Slice view() const { return Slice(buf_); }

    // Resets the cursor to empty while RETAINING the backing capacity, so a sink can
    // be reused across many small encodes (e.g. per-window region/prx scratch in the
    // windowed posting builder) without re-allocating each time -- this avoids the
    // cumulative small-allocation churn that fragments the heap arena and inflates
    // peak RSS during the merge of a high-df term split into thousands of windows.
    void clear() { buf_.clear(); }

    // Moves the backing buffer OUT to the caller (the sink is left empty), so an encoded
    // section can be handed off without the copy (+ copy-induced capacity slack) that
    // reading buffer() and copy-assigning would incur. Use only when the sink is not
    // reused afterward (a stack-local about to die, or one that is clear()'d next).
    std::vector<uint8_t> take() { return std::move(buf_); }

private:
    std::vector<uint8_t> buf_;
};

} // namespace snii
