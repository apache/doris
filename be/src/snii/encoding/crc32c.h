#pragma once

#include <cstdint>

#include "snii/common/slice.h"

namespace snii {

// CRC32C (Castagnoli, polynomial 0x1EDC6F41). Used to checksum the tail of each format block.
uint32_t crc32c_extend(uint32_t crc, Slice data);

inline uint32_t crc32c(Slice data) {
    return crc32c_extend(0, data);
}

} // namespace snii
