#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "snii/common/slice.h"
#include "common/status.h"

namespace snii {

// Thin ZSTD wrapper. Used for compressing large payloads such as .prx windows. Decompression requires the caller to supply the original uncompressed length (from the block header).
doris::Status zstd_compress(Slice input, int level, std::vector<uint8_t>* out);
doris::Status zstd_decompress(Slice input, size_t expected_uncomp_len, std::vector<uint8_t>* out);

} // namespace snii
