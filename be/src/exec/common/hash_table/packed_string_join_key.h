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

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace doris::packed_string_join {

inline constexpr size_t MAX_WIDTH = 16;

inline bool fits(const uint8_t* chars, size_t end, size_t length, size_t max_length) {
    // Zero padding would make strings that differ only by trailing zero bytes equal.
    return length <= max_length && (length == 0 || chars[end - 1] != 0);
}

template <typename Offset>
size_t max_build_key_length(const uint8_t* chars, const Offset* offsets, size_t rows) {
    size_t max_length = 1;
    for (size_t i = 1; i < rows; ++i) {
        const size_t length = offsets[i + 1] - offsets[i];
        if (!fits(chars, offsets[i + 1], length, MAX_WIDTH)) {
            return 0;
        }
        max_length = std::max(max_length, length);
    }
    return max_length;
}

template <typename Key>
inline constexpr auto MASKS = [] {
    std::array<Key, sizeof(Key) + 1> masks {};
    std::array<uint8_t, sizeof(Key)> bytes {};
    for (size_t i = 0; i < sizeof(Key); ++i) {
        bytes[i] = 0xff;
        masks[i + 1] = std::bit_cast<Key>(bytes);
    }
    return masks;
}();

// The string column's right padding permits a full-width load for a nonempty key.
template <typename Key>
inline Key pack(const uint8_t* chars, size_t begin, size_t length) {
    if (length == 0) {
        return Key {};
    }
    Key key;
    std::memcpy(&key, chars + begin, sizeof(Key));
    return key & MASKS<Key>[length];
}

template <bool Build, typename Key, typename Offset, typename Hash>
void prepare(const Offset* offsets, const uint8_t* chars, const uint8_t* null_map, uint32_t rows,
             uint32_t bucket_size, uint32_t no_match_bucket, size_t max_length, Key* keys,
             uint32_t* buckets, Hash&& hash) {
    if constexpr (Build) {
        // The build table does not insert its mock row at index zero.
        keys[0] = Key {};
        buckets[0] = 0;
    }
    for (uint32_t i = Build ? 1 : 0; i < rows; ++i) {
        if (null_map && null_map[i]) {
            keys[i] = Key {};
            buckets[i] = bucket_size;
            continue;
        }
        const size_t end = offsets[i + 1];
        const size_t begin = offsets[i];
        const size_t length = end - begin;
        if constexpr (!Build) {
            if (!fits(chars, end, length, max_length)) {
                keys[i] = Key {};
                buckets[i] = no_match_bucket;
                continue;
            }
        }
        const Key key = pack<Key>(chars, begin, length);
        keys[i] = key;
        buckets[i] = hash(key) & (bucket_size - 1);
    }
}

} // namespace doris::packed_string_join
