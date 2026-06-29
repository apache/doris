#pragma once

#include <crc32c/crc32c.h>

#include <cstdint>

#include "snii/common/slice.h"

namespace snii {

// CRC32C (Castagnoli, polynomial 0x1EDC6F41). Used to checksum the tail of each
// format block. Thin inline adapter over Doris's bundled Google crc32c thirdparty
// (crc32c::Extend / crc32c::Crc32c). That library computes the same canonical
// CRC32C (same reflected polynomial, same standard pre/post inversion), so every
// on-disk checksum stays byte-identical to the previous in-tree slice-by-8 /
// SSE4.2 implementation -- this is an implementation swap, not a format change.
// The leading :: keeps the crc32c namespace distinct from snii::crc32c() below.
inline uint32_t crc32c_extend(uint32_t crc, Slice data) {
    return ::crc32c::Extend(crc, data.data(), data.size());
}

inline uint32_t crc32c(Slice data) {
    return ::crc32c::Crc32c(data.data(), data.size());
}

} // namespace snii
