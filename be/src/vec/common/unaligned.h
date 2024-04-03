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
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/unaligned.h
// and modified by Doris

#pragma once

#include <bit>
#include <cstdint>
#include <cstring>
#include <type_traits>

template <typename T>
T unaligned_load(const void* address) {
    T res {};
    memcpy(&res, address, sizeof(res));
    return res;
}

/// We've had troubles before with wrong store size due to integral promotions
/// (e.g., unaligned_store(dest, uint16_t + uint16_t) stores an uint32_t).
/// To prevent this, make the caller specify the stored type explicitly.
/// To disable deduction of T, wrap the argument type with std::enable_if.
template <typename T>
void unaligned_store(void* address, const typename std::enable_if<true, T>::type& src) {
    static_assert(std::is_trivially_copyable_v<T>);
    memcpy(address, &src, sizeof(src));
}

inline void reverse_memcpy(void* dst, const void* src, size_t size) {
    uint8_t* uint_dst = reinterpret_cast<uint8_t*>(dst) + size; // Perform addition here
    const uint8_t* uint_src = reinterpret_cast<const uint8_t*>(src);

    while (size) {
        --uint_dst;
        *uint_dst = *uint_src;
        ++uint_src;
        --size;
    }
}

template <std::endian endian, typename T>
inline T unaligned_load_endian(const void* address) {
    T res {};
    if constexpr (std::endian::native == endian) {
        memcpy(&res, address, sizeof(res));
    } else {
        reverse_memcpy(&res, address, sizeof(res));
    }
    return res;
}

template <typename T>
inline T unaligned_load_little_endian(const void* address) {
    return unaligned_load_endian<std::endian::little, T>(address);
}