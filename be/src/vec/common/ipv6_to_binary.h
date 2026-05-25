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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/IPv6ToBinary.cpp
// and modified by Doris

#pragma once

#include <algorithm>

#include "vec/common/format_ip.h"

namespace doris::vectorized {

/// Result array could be indexed with all possible uint8 values without extra check.
/// For values greater than 128 we will store same value as for 128 (all bits set).
constexpr size_t IPV6_MASKS_COUNT = 256;
using RawMaskArrayV6 = std::array<uint8_t, IPV6_BINARY_LENGTH>;

static constexpr RawMaskArrayV6 generate_bit_mask(size_t prefix) {
    RawMaskArrayV6 arr {0};
    prefix = std::min(prefix, arr.size() * 8);
    int8_t i = IPV6_BINARY_LENGTH - 1;
    for (; prefix >= 8; --i, prefix -= 8) {
        arr[i] = 0xff;
    }
    if (prefix > 0) {
        arr[i--] = static_cast<uint8_t>(~(0xff >> prefix));
    }
    while (i >= 0) {
        arr[i--] = 0x00;
    }
    return arr;
}

static constexpr std::array<RawMaskArrayV6, IPV6_MASKS_COUNT> generate_bit_masks() {
    std::array<RawMaskArrayV6, IPV6_MASKS_COUNT> arr {};
    for (size_t i = 0; i < IPV6_MASKS_COUNT; ++i) {
        arr[i] = generate_bit_mask(i);
    }
    return arr;
}

/// Returns a reference to 16-byte array containing mask with first `prefix_len` bits set to `1` and `128 - prefix_len` to `0`.
/// Store in little-endian byte order
/// The reference is valid during all program execution time.
/// Values of prefix_len greater than 128 interpreted as 128 exactly.
inline const std::array<uint8_t, 16>& get_cidr_mask_ipv6(uint8_t prefix_len) {
    static constexpr auto IPV6_RAW_MASK_ARRAY = generate_bit_masks();
    return IPV6_RAW_MASK_ARRAY[prefix_len];
}

} // namespace doris::vectorized
