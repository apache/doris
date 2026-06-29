#pragma once

#include <cstddef>
#include <cstdint>

#include "common/status.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/byte_source.h"

namespace snii {

// PFOR integer block encoder/decoder (unsigned uint32 array).
// Encoded layout: [u8 bit_width][varint n_exceptions][bit-packed low
// bits][exception table]. Selects the bit_width that minimizes total byte size;
// values exceeding it go into the exception table (index_delta, full_value).
// delta/zigzag is handled by the upper layer (.frq window); PFOR only processes
// unsigned integer arrays.
void pfor_encode(const uint32_t* values, size_t n, ByteSink* out);
doris::Status pfor_decode(ByteSource* src, size_t n, uint32_t* out);
doris::Status pfor_skip(ByteSource* src, size_t n);

} // namespace snii
