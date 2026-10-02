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
#include "common/status.h"

namespace doris {

namespace {
constexpr int64_t MB = 1024 * 1024;
} // namespace

// A 1000 MB heap whose usage each test sets. With the ratio and reservation below, readers are
// admitted while used + 100 MB for every reader still opening (this one included) stays within
// 500 MB.
class JniScanHeapGateTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_enabled = config::enable_jni_scanner_heap_limiter;
        _saved_ratio = config::jni_scanner_max_heap_usage_ratio;
        _saved_reserved_mb = config::jni_scanner_heap_reserved_mb_per_open;
        _saved_max_wait_ms = config::jni_scanner_heap_max_wait_ms;
        config::enable_jni_scanner_heap_limiter = true;
        config::jni_scanner_max_heap_usage_ratio = 0.5;
        config::jni_scanner_heap_reserved_mb_per_open = 100;
        config::jni_scanner_heap_max_wait_ms = 60000;
    }

    void TearDown() override {
        config::enable_jni_scanner_heap_limiter = _saved_enabled;
        config::jni_scanner_max_heap_usage_ratio = _saved_ratio;
        config::jni_scanner_heap_reserved_mb_per_open = _saved_reserved_mb;
        config::jni_scanner_heap_max_wait_ms = _saved_max_wait_ms;
    }

    Status acquire(JniScanHeapGate::Permit* permit, int64_t* wait_ns = nullptr) {
        int64_t ignored = 0;
        return _gate.acquire([]() { return false; }, permit, wait_ns ? wait_ns : &ignored);
    }

    // Acquires `permit` on a thread of its own and reports when it got it.
    std::thread acquire_async(JniScanHeapGate::Permit* permit, std::atomic<bool>* admitted) {
        return std::thread([this, permit, admitted]() {
            ASSERT_TRUE(acquire(permit).ok());
            admitted->store(true);
        });
    }

    static bool becomes_true(const std::atomic<bool>& flag) {
        for (int i = 0; i < 500 && !flag.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return flag.load();
    }

    std::atomic<int64_t> _used_mb {0};
    std::atomic<int> _probes {0};
    Status _probe_status = Status::OK();
    JniScanHeapGate _gate {[this](JniScanHeapGate::HeapUsage* heap) {
        ++_probes;
        heap->used = _used_mb.load() * MB;
        heap->max = 1000 * MB;
        return _probe_status;
    }};

private:
    bool _saved_enabled = true;
    double _saved_ratio = 0;
    int64_t _saved_reserved_mb = 0;
    int64_t _saved_max_wait_ms = 0;
};

TEST_F(JniScanHeapGateTest, ReservesForReadersThatHaveNotProducedABatchYet) {
    _used_mb = 100;
    // 100 + 4 x 100 = 500: four readers opening at once fit.
    JniScanHeapGate::Permit permits[4];
    for (auto& permit : permits) {
        ASSERT_TRUE(acquire(&permit).ok());
    }
    EXPECT_EQ(_gate.active(), 4);
    EXPECT_EQ(_gate.opening(), 4);

    // A fifth would make 600: it waits although the heap measures only 100 MB, since the four
    // before it may each be about to take their share.
    JniScanHeapGate::Permit fifth;
    std::atomic<bool> admitted {false};
    auto waiter = acquire_async(&fifth, &admitted);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(admitted.load());

    // One of them produced its first batch. What it holds is in the measurement from now on, and
    // it is no longer reserved for: 100 + (3 + 1) x 100 = 500.
    permits[0].opened();
    EXPECT_TRUE(becomes_true(admitted));
    waiter.join();
    EXPECT_EQ(_gate.active(), 5);
    EXPECT_EQ(_gate.opening(), 4);
}

TEST_F(JniScanHeapGateTest, WaitsForTheHeapThatOpenReadersHoldToComeBack) {
    // Two readers past their first batch hold 420 MB of a heap that measured 20 MB before them.
    JniScanHeapGate::Permit first;
    JniScanHeapGate::Permit second;
    ASSERT_TRUE(acquire(&first).ok());
    ASSERT_TRUE(acquire(&second).ok());
    first.opened();
    second.opened();
    _used_mb = 420;

    // 420 + 100 > 500.
    JniScanHeapGate::Permit third;
    std::atomic<bool> admitted {false};
    auto waiter = acquire_async(&third, &admitted);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(admitted.load());

    // A reader closes and a GC returns what it held. Nobody tells the gate about the GC; the
    // waiter finds out by looking again.
    second.release();
    _used_mb = 220;
    EXPECT_TRUE(becomes_true(admitted));
    waiter.join();
}

TEST_F(JniScanHeapGateTest, AdmitsAReaderWhenNoOtherHoldsAPermit) {
    // Over the limit before anybody opens: nothing would ever free memory for this reader.
    _used_mb = 900;
    JniScanHeapGate::Permit first;
    ASSERT_TRUE(acquire(&first).ok());

    JniScanHeapGate::Permit second;
    std::atomic<bool> admitted {false};
    auto waiter = acquire_async(&second, &admitted);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(admitted.load());

    // The heap stays where it is, but the last other reader is gone: one at a time is the slowest
    // the gate gets.
    first.release();
    EXPECT_TRUE(becomes_true(admitted));
    waiter.join();
    EXPECT_EQ(_gate.active(), 1);
}

TEST_F(JniScanHeapGateTest, StopsWaitingWhenAskedTo) {
    _used_mb = 900;
    JniScanHeapGate::Permit first;
    ASSERT_TRUE(acquire(&first).ok());

    std::atomic<bool> cancelled {false};
    std::thread canceller([&cancelled]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        cancelled.store(true);
    });
    JniScanHeapGate::Permit second;
    int64_t wait_ns = 0;
    ASSERT_TRUE(_gate.acquire([&cancelled]() { return cancelled.load(); }, &second, &wait_ns).ok());
    canceller.join();
    // Admitted without the heap, so that the reader can see the stop itself; the permit is real.
    EXPECT_TRUE(second.held());
    EXPECT_EQ(_gate.active(), 2);
    EXPECT_GE(wait_ns, 200LL * 1000 * 1000);
}

TEST_F(JniScanHeapGateTest, GivesUpWaitingAfterTheLongestWait) {
    config::jni_scanner_heap_max_wait_ms = 200;
    _used_mb = 900;
    JniScanHeapGate::Permit first;
    ASSERT_TRUE(acquire(&first).ok());

    JniScanHeapGate::Permit second;
    int64_t wait_ns = 0;
    ASSERT_TRUE(acquire(&second, &wait_ns).ok());
    EXPECT_TRUE(second.held());
    EXPECT_GE(wait_ns, 200LL * 1000 * 1000);
}

TEST_F(JniScanHeapGateTest, LetsEveryReaderInWhenDisabled) {
    config::enable_jni_scanner_heap_limiter = false;
    _used_mb = 900;
    JniScanHeapGate::Permit permits[3];
    for (auto& permit : permits) {
        ASSERT_TRUE(acquire(&permit).ok());
    }
    EXPECT_EQ(_gate.active(), 3);
    // Disabled, the gate does not even measure the heap.
    EXPECT_EQ(_probes.load(), 0);
}

TEST_F(JniScanHeapGateTest, PermitsReleaseOnceAndOnDestruction) {
    {
        JniScanHeapGate::Permit permit;
        ASSERT_TRUE(acquire(&permit).ok());
        permit.opened();
        permit.opened();
        EXPECT_EQ(_gate.active(), 1);
        EXPECT_EQ(_gate.opening(), 0);
        permit.release();
        permit.release();
        EXPECT_FALSE(permit.held());
        EXPECT_EQ(_gate.active(), 0);

        ASSERT_TRUE(acquire(&permit).ok());
        EXPECT_EQ(_gate.active(), 1);
        EXPECT_EQ(_gate.opening(), 1);
    }
    // Destroyed while still opening: both counts come back.
    EXPECT_EQ(_gate.active(), 0);
    EXPECT_EQ(_gate.opening(), 0);

    // A permit that was never held releases nothing.
    JniScanHeapGate::Permit never_held;
    never_held.opened();
    never_held.release();
    EXPECT_EQ(_gate.active(), 0);
}

TEST_F(JniScanHeapGateTest, FailsWhenTheHeapCannotBeMeasured) {
    _probe_status = Status::InternalError("no JVM");
    JniScanHeapGate::Permit permit;
    auto status = acquire(&permit);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(permit.held());
    EXPECT_EQ(_gate.active(), 0);
}

} // namespace doris
