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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>

// ScopedHeapHighWater — 测试侧堆峰值采样工具（spill/merge 峰值断言用）。
//
// UT 构建（run-be-ut.sh）写死 USE_JEMALLOC=OFF 且默认 ASAN，benchmark 的
// jemalloc thread.peak 助手在 UT 二进制里不可用。本工具按可用性三级回退：
//   1. ASAN：__sanitizer_get_current_allocated_bytes()（进程级当前净分配，
//      精确非采样值）由 200µs 周期的采样线程取 max —— 高 df 归并在 ASAN 下
//      耗时秒级，采样分辨率足够。
//   2. 非 ASAN 且 USE_JEMALLOC：jemalloc 5.3 thread.peak.reset / read
//      （线程级净分配高水位，要求构造与 PeakDeltaBytes() 在同一线程）。
//   3. 两者皆无：Available()==false，调用方应 GTEST_SKIP()。
//
// 断言建议用比值式（峰值 ≤ α×Σ输入字节）为主、绝对值为辅，抗 ASAN redzone
// 膨胀与分配器留存噪声；峰值用例建议独立 --gtest_filter 运行，避免前序
// 测试的留存池抬高基线。

#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define SPIMI_TEST_HEAP_HIGH_WATER_ASAN 1
#endif
#endif
#if !defined(SPIMI_TEST_HEAP_HIGH_WATER_ASAN) && defined(__SANITIZE_ADDRESS__)
#define SPIMI_TEST_HEAP_HIGH_WATER_ASAN 1
#endif

#ifdef SPIMI_TEST_HEAP_HIGH_WATER_ASAN
#include <sanitizer/allocator_interface.h>
#elif defined(USE_JEMALLOC)
#include "runtime/memory/jemalloc_control.h"
#endif

namespace doris::segment_v2::inverted_index::spimi {

class ScopedHeapHighWater {
public:
    static bool Available() {
#if defined(SPIMI_TEST_HEAP_HIGH_WATER_ASAN) || defined(USE_JEMALLOC)
        return true;
#else
        return false;
#endif
    }

    ScopedHeapHighWater() {
#ifdef SPIMI_TEST_HEAP_HIGH_WATER_ASAN
        _baseline = __sanitizer_get_current_allocated_bytes();
        _peak.store(_baseline, std::memory_order_relaxed);
        // 采样线程：循环体内不分配（sleep_for + 原子 max），避免污染读数。
        _sampler = std::thread([this] {
            while (!_stop.load(std::memory_order_relaxed)) {
                SampleOnce();
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
        });
#elif defined(USE_JEMALLOC)
        JemallocControl::action_jemallctl("thread.peak.reset");
#endif
    }

    ScopedHeapHighWater(const ScopedHeapHighWater&) = delete;
    ScopedHeapHighWater& operator=(const ScopedHeapHighWater&) = delete;

    ~ScopedHeapHighWater() { Stop(); }

    // 停止采样（幂等）。jemalloc 回退路径要求与构造同线程调用。
    void Stop() {
#ifdef SPIMI_TEST_HEAP_HIGH_WATER_ASAN
        if (_sampler.joinable()) {
            SampleOnce(); // 收尾再采一次，兜住停表前的最后状态
            _stop.store(true, std::memory_order_relaxed);
            _sampler.join();
        }
#elif defined(USE_JEMALLOC)
        if (!_stopped) {
            _jemalloc_peak = static_cast<size_t>(
                    JemallocControl::get_jemallctl_value<uint64_t>("thread.peak.read"));
            _stopped = true;
        }
#endif
    }

    // 构造以来的净分配高水位（字节）。不可用时返回 0（先查 Available()）。
    size_t PeakDeltaBytes() {
        Stop();
#ifdef SPIMI_TEST_HEAP_HIGH_WATER_ASAN
        const size_t peak = _peak.load(std::memory_order_relaxed);
        return peak > _baseline ? peak - _baseline : 0;
#elif defined(USE_JEMALLOC)
        return _jemalloc_peak;
#else
        return 0;
#endif
    }

private:
#ifdef SPIMI_TEST_HEAP_HIGH_WATER_ASAN
    void SampleOnce() {
        const size_t cur = __sanitizer_get_current_allocated_bytes();
        size_t prev = _peak.load(std::memory_order_relaxed);
        while (cur > prev && !_peak.compare_exchange_weak(prev, cur, std::memory_order_relaxed)) {
        }
    }

    size_t _baseline = 0;
    std::atomic<size_t> _peak {0};
    std::atomic<bool> _stop {false};
    std::thread _sampler;
#elif defined(USE_JEMALLOC)
    size_t _jemalloc_peak = 0;
    bool _stopped = false;
#endif
};

} // namespace doris::segment_v2::inverted_index::spimi
