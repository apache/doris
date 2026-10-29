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

#include <cstdint>
#include <cstring>

#include "common/compiler_util.h"
#include "util/simd/bits.h"

namespace doris::simd {

#if defined(__x86_64__)
namespace detail {
struct alignas(16) SelectionLookupTable {
    uint16_t positions[256][8] {};
    uint8_t counts[256] {};

    constexpr SelectionLookupTable() {
        for (unsigned mask = 0; mask < 256; ++mask) {
            for (unsigned bit = 0; bit < 8; ++bit) {
                if (mask & (1U << bit)) {
                    positions[mask][counts[mask]++] = bit;
                }
            }
        }
    }
};

inline constexpr SelectionLookupTable SELECTION_LOOKUP_TABLE;
} // namespace detail
#endif

// Append base + bit positions in ascending order. The caller must provide room for
// bits_mask_length() entries, and base + bits_mask_length() - 1 must fit in uint16_t.
// Only the returned number of entries are valid; the lookup path can write extra entries.
ALWAYS_INLINE inline uint16_t append_selected_indices(
        decltype(bytes_mask_to_bits_mask(nullptr)) mask, uint16_t base, uint16_t* output) {
    uint16_t count = 0;
#if defined(__x86_64__)
    for (unsigned offset = 0; offset < 32; offset += 8) {
        const auto part = static_cast<uint8_t>(mask >> offset);
        uint16_t indices[8];
        for (unsigned lane = 0; lane < 8; ++lane) {
            indices[lane] = base + offset + detail::SELECTION_LOOKUP_TABLE.positions[part][lane];
        }
        // count <= offset, so the fixed-width store stays within 32 output entries.
        std::memcpy(output + count, indices, sizeof(indices));
        count += detail::SELECTION_LOOKUP_TABLE.counts[part];
    }
#else
    iterate_through_bits_mask(
            [&](const int bit_pos) { output[count++] = base + static_cast<uint16_t>(bit_pos); },
            mask);
#endif
    return count;
}

} // namespace doris::simd
