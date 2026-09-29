// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <crc32c/crc32c.h>

#include <cstddef>
#include <cstdint>

#include "storage/index/snii/common/slice.h"

namespace doris::snii {

// CRC32C (Castagnoli, polynomial 0x1EDC6F41). Used to checksum the tail of each
// format block. Thin inline adapter over Doris's bundled Google crc32c thirdparty
// (crc32c::Extend / crc32c::Crc32c). That library computes the same canonical
// CRC32C (same reflected polynomial, same standard pre/post inversion), so every
// on-disk checksum stays byte-identical to the previous in-tree slice-by-8 /
// SSE4.2 implementation -- this is an implementation swap, not a format change.
// The leading :: keeps the crc32c namespace distinct from crc32c() below.
inline uint32_t crc32c_extend(uint32_t crc, Slice data) {
    return ::crc32c::Extend(crc, data.data(), data.size());
}

inline uint32_t crc32c(Slice data) {
    return ::crc32c::Crc32c(data.data(), data.size());
}

#ifdef BE_TEST
// Test-only portable and SSE4.2 CRC32C reference functions. Hardware variants fall back to the portable path when SSE4.2 is unavailable.
namespace detail {
uint32_t crc32c_slice8_extend(uint32_t crc, Slice data);
uint32_t crc32c_hw_serial_extend(uint32_t crc, Slice data);
uint32_t crc32c_hw3_extend(uint32_t crc, Slice data);
size_t crc32c_interleave_threshold();
bool crc32c_has_hw();
} // namespace detail
#endif // BE_TEST

} // namespace doris::snii
