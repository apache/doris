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

#include <gtest/gtest.h>

#include <cstdint>
#include <random>

#include "core/column/columns_common.h"

namespace doris {

// H3: columns_common.cpp had `#if defined(__SSE2__) || defined(__aarch64__)
// && defined(__POPCNT__)` — && binds tighter than ||, and ARM toolchains
// never define __POPCNT__, so the SIMD block was silently compiled out on
// aarch64. These tests pin the semantics — the function deliberately
// compares SIGNED bytes (`> 0`, see the NOTE in the implementation), so
// bytes 128..255 (negative int8) must NOT be counted — and the scalar and
// SIMD paths must always agree.
namespace {
size_t ref_count(const IColumn::Filter& filt) {
    size_t n = 0;
    for (auto v : filt) {
        n += static_cast<int8_t>(v) > 0;
    }
    return n;
}
} // namespace

TEST(ColumnsCommonTest, CountBytesInFilterMatchesReference) {
    std::mt19937 rng(20260815);
    for (size_t size : {0, 1, 7, 63, 64, 65, 127, 128, 129, 1000, 4096, 65537}) {
        IColumn::Filter filt(size);
        for (auto& v : filt) {
            // values beyond {0,1} on purpose: covers positive and negative
            // int8 alike; only strictly-positive int8 may be counted
            v = static_cast<uint8_t>(rng() % 4 == 0 ? 0 : (rng() % 255 + 1));
        }
        ASSERT_EQ(ref_count(filt), count_bytes_in_filter(filt)) << "size=" << size;
    }
}

TEST(ColumnsCommonTest, CountBytesInFilterEdgePatterns) {
    IColumn::Filter all_zero(512, 0);
    EXPECT_EQ(0, count_bytes_in_filter(all_zero));
    IColumn::Filter all_one(512, 1);
    EXPECT_EQ(512, count_bytes_in_filter(all_one));
    // 127 is the largest positive int8: counted.
    IColumn::Filter all_max_positive(513, 127);
    EXPECT_EQ(513, count_bytes_in_filter(all_max_positive));
    // 128..255 are negative int8 and must NOT be counted (signed compare).
    IColumn::Filter all_negative(511, 255);
    EXPECT_EQ(0, count_bytes_in_filter(all_negative));
    IColumn::Filter all_msb_set(64, 128);
    EXPECT_EQ(0, count_bytes_in_filter(all_msb_set));
}

} // namespace doris
