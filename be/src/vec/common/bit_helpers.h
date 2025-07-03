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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/BitHelpers.h.h
// and modified by Doris

#pragma once

namespace doris::vectorized {

template <typename T>
inline uint32_t get_leading_zero_bits_unsafe(T x) {
    assert(x != 0);

    if constexpr (sizeof(T) <= sizeof(unsigned int)) {
        return __builtin_clz(x);
    } else if constexpr (sizeof(T) <= sizeof(unsigned long int)) /// NOLINT
    {
        return __builtin_clzl(x);
    } else {
        return __builtin_clzll(x);
    }
}

template <typename T>
inline uint32_t bit_scan_reverse(T x) {
    return (std::max<size_t>(sizeof(T), sizeof(unsigned int))) * 8 - 1 -
           get_leading_zero_bits_unsafe(x);
}

} // namespace doris::vectorized