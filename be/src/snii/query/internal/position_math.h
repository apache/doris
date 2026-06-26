#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace snii::query::internal {

inline bool build_position_offsets(size_t count, std::vector<uint32_t>* out) {
    if (count >= std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    out->clear();
    out->reserve(count);
    uint32_t offset = 0;
    while (out->size() < count) {
        out->push_back(offset);
        ++offset;
    }
    return true;
}

inline bool add_position_offset(uint32_t start, uint32_t offset, uint32_t* out) {
    if (start > std::numeric_limits<uint32_t>::max() - offset) return false;
    *out = start + offset;
    return true;
}

} // namespace snii::query::internal
