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

#include "util/jni_scan_heap_gate.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "common/config.h"

namespace doris {

namespace {
constexpr int64_t MB = 1024 * 1024;
} // namespace

// A 500 MB budget unless a test changes it.
class JniScanHeapGateTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_max_wait_ms = config::jni_scanner_heap_max_wait_ms;
        config::jni_scanner_heap_max_wait_ms = 60000;
    }

    void TearDown() override { config::jni_scanner_heap_max_wait_ms = _saved_max_wait_ms; }

    // For a reader whose scan never stops, so it always ends up admitted.
    void acquire(int64_t mb, JniScanHeapGate::Permit* permit, int64_t* wait_ns = nullptr) {
        int64_t ignored = 0;
        EXPECT_TRUE(_gate.acquire(
                mb * MB, []() { return false; }, permit, wait_ns ? wait_ns : &ignored));
    }

    // Acquires `permit` on a thread of its own and reports when it got it.
    std::thread acquire_async(int64_t mb, JniScanHeapGate::Permit* permit,
                              std::atomic<bool>* admitted) {
        return std::thread([this, mb, permit, admitted]() {
            acquire(mb, permit);
            admitted->store(true);
        });
    }

    static bool becomes_true(const std::atomic<bool>& flag) {
        for (int i = 0; i < 500 && !flag.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return flag.load();
    }

    bool waiters_become(int64_t n) {
        for (int i = 0; i < 500 && _gate.waiters() != n; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return _gate.waiters() == n;
    }

    std::atomic<int64_t> _budget_mb {500};
    JniScanHeapGate _gate {[this]() { return _budget_mb.load() * MB; }};

private:
    int64_t _saved_max_wait_ms = 0;
};

TEST_F(JniScanHeapGateTest, AdmitsWhileWhatTheReadersDeclaredFitsTheBudget) {
    JniScanHeapGate::Permit first;
    JniScanHeapGate::Permit second;
    int64_t wait_ns = -1;
    acquire(200, &first, &wait_ns);
    acquire(200, &second);
    EXPECT_LT(wait_ns, 100LL * 1000 * 1000);
    EXPECT_EQ(_gate.holders(), 2);
    EXPECT_EQ(_gate.admitted_bytes(), 400 * MB);

    // 400 + 200 > 500.
    JniScanHeapGate::Permit third;
    std::atomic<bool> admitted {false};
    auto waiter = acquire_async(200, &third, &admitted);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(admitted.load());

    // A reader closes and its share comes back: 200 + 200 fits.
    first.release();
    EXPECT_TRUE(becomes_true(admitted));
    waiter.join();
    EXPECT_EQ(_gate.holders(), 2);
    EXPECT_EQ(_gate.admitted_bytes(), 400 * MB);
}

TEST_F(JniScanHeapGateTest, ReadersAreAdmittedInTheOrderTheyCame) {
    JniScanHeapGate::Permit holder;
    acquire(400, &holder);

    // A large reader that does not fit waits, and a small one that would fit waits behind it rather
    // than passing it: otherwise a stream of small readers could keep the large one out for ever.
    JniScanHeapGate::Permit large;
    std::atomic<bool> large_admitted {false};
    auto large_waiter = acquire_async(300, &large, &large_admitted);
    ASSERT_TRUE(waiters_become(1));
    JniScanHeapGate::Permit small;
    std::atomic<bool> small_admitted {false};
    auto small_waiter = acquire_async(50, &small, &small_admitted);
    ASSERT_TRUE(waiters_become(2));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(large_admitted.load());
    EXPECT_FALSE(small_admitted.load());

    // 300 + 50 fits once the holder is gone, in that order.
    holder.release();
    EXPECT_TRUE(becomes_true(large_admitted));
    EXPECT_TRUE(becomes_true(small_admitted));
    large_waiter.join();
    small_waiter.join();
    EXPECT_EQ(_gate.admitted_bytes(), 350 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
}

TEST_F(JniScanHeapGateTest, AReaderLargerThanTheBudgetRunsAlone) {
    // Nobody holds a share, so nobody would give one back for it: waiting cannot help.
    JniScanHeapGate::Permit large;
    int64_t wait_ns = -1;
    acquire(800, &large, &wait_ns);
    EXPECT_LT(wait_ns, 100LL * 1000 * 1000);
    EXPECT_EQ(_gate.admitted_bytes(), 800 * MB);

    // While it runs, even the smallest reader waits.
    JniScanHeapGate::Permit small;
    std::atomic<bool> admitted {false};
    auto waiter = acquire_async(1, &small, &admitted);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(admitted.load());

    large.release();
    EXPECT_TRUE(becomes_true(admitted));
    waiter.join();
    EXPECT_EQ(_gate.holders(), 1);
}

TEST_F(JniScanHeapGateTest, ABiggerBudgetAppliesToTheReadersAlreadyWaiting) {
    JniScanHeapGate::Permit holder;
    acquire(400, &holder);

    JniScanHeapGate::Permit waiting;
    std::atomic<bool> admitted {false};
    auto waiter = acquire_async(200, &waiting, &admitted);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(admitted.load());

    // Nobody released anything; the waiter sees the new budget the next time it looks.
    _budget_mb = 600;
    EXPECT_TRUE(becomes_true(admitted));
    waiter.join();
    EXPECT_EQ(_gate.admitted_bytes(), 600 * MB);
}

TEST_F(JniScanHeapGateTest, AReaderWhoseScanStopsLeavesWithoutAShare) {
    JniScanHeapGate::Permit holder;
    acquire(500, &holder);

    std::atomic<bool> cancelled {false};
    std::thread canceller([&cancelled]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        cancelled.store(true);
    });
    JniScanHeapGate::Permit stopped;
    int64_t wait_ns = 0;
    // Its query is cancelled while it waits. Admitting it would open a Java scanner above the budget
    // for a query with nothing left to read - all of that query's waiting readers at once.
    EXPECT_FALSE(_gate.acquire(
            100 * MB, [&cancelled]() { return cancelled.load(); }, &stopped, &wait_ns));
    canceller.join();
    EXPECT_FALSE(stopped.held());
    EXPECT_EQ(_gate.holders(), 1);
    EXPECT_EQ(_gate.admitted_bytes(), 500 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
    EXPECT_GE(wait_ns, 200LL * 1000 * 1000);
}

TEST_F(JniScanHeapGateTest, AReaderWhoseScanHasStoppedTakesNothingEvenWhenItFits) {
    JniScanHeapGate::Permit permit;
    int64_t wait_ns = -1;
    EXPECT_FALSE(_gate.acquire(
            100 * MB, []() { return true; }, &permit, &wait_ns));
    EXPECT_FALSE(permit.held());
    EXPECT_EQ(_gate.holders(), 0);
    EXPECT_EQ(_gate.admitted_bytes(), 0);
    EXPECT_EQ(_gate.waiters(), 0);
    EXPECT_LT(wait_ns, 100LL * 1000 * 1000);
}

TEST_F(JniScanHeapGateTest, AStoppedReaderFirstInLineLetsTheNextOneIn) {
    JniScanHeapGate::Permit holder;
    acquire(300, &holder);

    // 300 + 400 > 500: the large reader waits first in line, and a small one behind it although
    // 300 + 100 would fit.
    std::atomic<bool> large_stopped {false};
    std::atomic<bool> large_returned {false};
    JniScanHeapGate::Permit large;
    std::thread large_waiter([this, &large_stopped, &large_returned, &large]() {
        int64_t ignored = 0;
        EXPECT_FALSE(_gate.acquire(
                400 * MB, [&large_stopped]() { return large_stopped.load(); }, &large, &ignored));
        large_returned.store(true);
    });
    ASSERT_TRUE(waiters_become(1));
    JniScanHeapGate::Permit small;
    std::atomic<bool> small_admitted {false};
    auto small_waiter = acquire_async(100, &small, &small_admitted);
    ASSERT_TRUE(waiters_become(2));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(small_admitted.load());

    // The large reader's scan stops: it leaves the line, and the small one, first now, fits while
    // the holder still holds its share.
    large_stopped.store(true);
    EXPECT_TRUE(becomes_true(large_returned));
    EXPECT_TRUE(becomes_true(small_admitted));
    large_waiter.join();
    small_waiter.join();
    EXPECT_FALSE(large.held());
    EXPECT_EQ(_gate.holders(), 2);
    EXPECT_EQ(_gate.admitted_bytes(), 400 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
}

TEST_F(JniScanHeapGateTest, GivesUpWaitingAfterTheLongestWait) {
    config::jni_scanner_heap_max_wait_ms = 200;
    JniScanHeapGate::Permit holder;
    acquire(500, &holder);

    JniScanHeapGate::Permit late;
    int64_t wait_ns = 0;
    acquire(100, &late, &wait_ns);
    EXPECT_TRUE(late.held());
    EXPECT_GE(wait_ns, 200LL * 1000 * 1000);
    EXPECT_EQ(_gate.admitted_bytes(), 600 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
}

TEST_F(JniScanHeapGateTest, PermitsReleaseOnceAndOnDestruction) {
    {
        JniScanHeapGate::Permit permit;
        acquire(100, &permit);
        permit.release();
        permit.release();
        EXPECT_FALSE(permit.held());
        EXPECT_EQ(_gate.holders(), 0);
        EXPECT_EQ(_gate.admitted_bytes(), 0);

        acquire(300, &permit);
        EXPECT_EQ(_gate.admitted_bytes(), 300 * MB);
    }
    // Destroyed while held: its share comes back.
    EXPECT_EQ(_gate.holders(), 0);
    EXPECT_EQ(_gate.admitted_bytes(), 0);

    // A permit that was never held releases nothing.
    JniScanHeapGate::Permit never_held;
    never_held.release();
    EXPECT_EQ(_gate.holders(), 0);
}

} // namespace doris
