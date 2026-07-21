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

// ScopedHeapHighWater 自检：确认峰值采样工具在本构建（ASAN /
// jemalloc / 皆无）下行为正确，供后续 spill/merge 峰值 RED 用例使用。

#include "heap_high_water.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

TEST(SpimiHeapHighWater, CapturesLargeAllocationSpike) {
    if (!ScopedHeapHighWater::Available()) {
        GTEST_SKIP() << "no ASAN allocator interface and no jemalloc — peak sampling unavailable";
    }
    constexpr size_t kSpike = 64ULL << 20; // 64MB，远高于采样/分配器噪声
    ScopedHeapHighWater hw;
    std::vector<uint8_t> big(kSpike);
    // 触页，防惰性提交被分配器统计绕过。
    for (size_t i = 0; i < big.size(); i += 4096) {
        big[i] = static_cast<uint8_t>(i);
    }
    // Stop() 同步补采一次（big 仍存活），不依赖采样线程在高负载机上的调度。
    hw.Stop();
    const size_t peak = hw.PeakDeltaBytes();
    EXPECT_GE(peak, kSpike / 2) << "64MB live allocation must show up in the high-water mark";
}

TEST(SpimiHeapHighWater, PeakIsIdempotentAfterStop) {
    if (!ScopedHeapHighWater::Available()) {
        GTEST_SKIP() << "no ASAN allocator interface and no jemalloc — peak sampling unavailable";
    }
    ScopedHeapHighWater hw;
    std::vector<uint8_t> a(8ULL << 20);
    for (size_t i = 0; i < a.size(); i += 4096) {
        a[i] = static_cast<uint8_t>(i);
    }
    const size_t p1 = hw.PeakDeltaBytes(); // 隐式 Stop()
    const size_t p2 = hw.PeakDeltaBytes(); // 幂等：停表后读数不再变
    EXPECT_EQ(p1, p2);
    EXPECT_GE(p1, (8ULL << 20) / 2);
}

} // namespace doris::segment_v2::inverted_index::spimi
