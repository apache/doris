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

#include <gen_cpp/Types_types.h> // TUniqueId, normally pulled in via pch.h
#include <gen_cpp/types.pb.h>  // PUniqueId, normally pulled in via pch.h
#include <iostream>

#include "common/signal_handler.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

namespace doris {

// T1: On aarch64, HAVE___SYNC_VAL_COMPARE_AND_SWAP is never defined and the
// x86 inline-asm branch is compiled out, so sync_val_compare_and_swap falls
// back to a non-atomic read-check-write implementation. FailureSignalHandler
// uses it to elect the single thread that dumps crash info. On weakly-ordered
// ARM with many cores, multiple crashing threads can all "win" the election
// and dump concurrently, corrupting the crash log or crashing again inside
// the handler. This test reproduces the race: exactly one thread must win.
TEST(SignalHandlerTest, CasRaceSingleWinner) {
    constexpr int kThreads = 128;
    for (int round = 0; round < 5; ++round) {
        pthread_t* entered = nullptr;
        std::vector<pthread_t> ids(kThreads);
        std::atomic<int> ready {0};
        std::atomic<bool> go {false};
        std::atomic<int> winners {0};
        std::vector<std::thread> threads;
        threads.reserve(kThreads);
        for (int i = 0; i < kThreads; ++i) {
            threads.emplace_back([&, i] {
                ids[i] = pthread_self();
                ready.fetch_add(1, std::memory_order_relaxed);
                while (!go.load(std::memory_order_acquire)) {
                }
                pthread_t* old = signal::sync_val_compare_and_swap(
                        &entered, static_cast<pthread_t*>(nullptr), &ids[i]);
                if (old == nullptr) {
                    winners.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }
        while (ready.load(std::memory_order_acquire) != kThreads) {
        }
        go.store(true, std::memory_order_release);
        for (auto& t : threads) {
            t.join();
        }
        EXPECT_EQ(1, winners.load())
                << "round " << round << ": " << winners.load()
                << " threads won the FailureSignalHandler election, expected exactly 1";
    }
}

// Basic single-threaded CAS semantics must hold on every platform.
TEST(SignalHandlerTest, CasBasicSemantics) {
    int value = 1;
    // oldval matches: swap happens, old value returned
    EXPECT_EQ(1, signal::sync_val_compare_and_swap(&value, 1, 2));
    EXPECT_EQ(2, value);
    // oldval does not match: no swap, current value returned
    EXPECT_EQ(2, signal::sync_val_compare_and_swap(&value, 1, 3));
    EXPECT_EQ(2, value);
}

} // namespace doris
