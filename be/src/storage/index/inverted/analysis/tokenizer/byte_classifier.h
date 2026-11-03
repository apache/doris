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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include "util/simd/bits.h"

namespace doris::segment_v2::inverted_index {

// The byte classes the english and basic tokenizers cut by, one bit per byte of a block of
// BYTE_CLASS_BLOCK bytes, bit i for byte i.
inline constexpr size_t BYTE_CLASS_BLOCK = 64;

struct ByteClasses {
    uint64_t alnum = 0; // ASCII letters and digits
    uint64_t lead = 0;  // 0xC0 and above: the bytes that can start a multi-byte character
};

inline bool is_ascii_upper(uint8_t c) {
    return static_cast<uint8_t>(c - 'A') < 26;
}

inline bool is_ascii_alnum(uint8_t c) {
    return static_cast<uint8_t>(c - '0') < 10 || static_cast<uint8_t>((c | 0x20) - 'a') < 26;
}

inline char ascii_lower(char c) {
    const auto byte = static_cast<uint8_t>(c);
    return static_cast<char>(byte + (static_cast<uint8_t>(is_ascii_upper(byte)) << 5));
}

// Folds A-Z in `text` to a-z and leaves every other byte as it is, as the analyzers that only
// lowercase ASCII do.
inline void ascii_lower(std::string& text) {
    for (char& c : text) {
        c = ascii_lower(c);
    }
}

namespace byte_classifier_detail {

inline uint64_t to_bits64(const uint8_t* bytes) {
    return static_cast<uint64_t>(simd::bytes32_mask_to_bits32_mask(bytes)) |
           (static_cast<uint64_t>(simd::bytes32_mask_to_bits32_mask(bytes + 32)) << 32);
}

} // namespace byte_classifier_detail

// Classifies the BYTE_CLASS_BLOCK readable bytes at `src`. With kLower, `lowered` receives them
// with A-Z folded; without, it is not touched. A plain loop the compiler vectorizes on every
// target, the byte masks turned into bits the way the Doris filters turn theirs.
template <bool kLower>
inline ByteClasses classify_block(const char* src, char* lowered) {
    uint8_t alnum[BYTE_CLASS_BLOCK];
    uint8_t lead[BYTE_CLASS_BLOCK];
    for (size_t i = 0; i < BYTE_CLASS_BLOCK; ++i) {
        const auto c = static_cast<uint8_t>(src[i]);
        const auto upper = static_cast<uint8_t>(static_cast<uint8_t>(c - 'A') < 26);
        const auto lower = static_cast<uint8_t>(static_cast<uint8_t>(c - 'a') < 26);
        const auto digit = static_cast<uint8_t>(static_cast<uint8_t>(c - '0') < 10);
        alnum[i] = upper | lower | digit;
        lead[i] = static_cast<uint8_t>(c >= 0xC0);
        if constexpr (kLower) {
            lowered[i] = static_cast<char>(c + (upper << 5));
        }
    }
    return {.alnum = byte_classifier_detail::to_bits64(alnum),
            .lead = byte_classifier_detail::to_bits64(lead)};
}

// classify_block() for the last `size` bytes of a value, 0 < `size` < BYTE_CLASS_BLOCK. The bytes
// go through a stack buffer, so nothing past the value is read, and the bits past `size` are 0.
template <bool kLower>
inline ByteClasses classify_tail(const char* src, size_t size, char* lowered) {
    char block[BYTE_CLASS_BLOCK] = {};
    std::memcpy(block, src, size);
    char block_lowered[BYTE_CLASS_BLOCK];
    const ByteClasses classes = classify_block<kLower>(block, block_lowered);
    if constexpr (kLower) {
        std::memcpy(lowered, block_lowered, size);
    }
    return classes;
}

} // namespace doris::segment_v2::inverted_index
