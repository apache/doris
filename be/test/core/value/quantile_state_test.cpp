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

#include "core/value/quantile_state.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstring>
#include <future>
#include <thread>

#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "util/tdigest.h"

namespace doris {

TEST(QuantileStateTest, SharedDigestWritesDoNotChangeSource) {
    QuantileState source;
    for (int i = 0; i < 4096; ++i) {
        source.add_value(10);
    }
    QuantileState copied = source;
    EXPECT_EQ(copied._tdigest_ptr, source._tdigest_ptr);
    copied.add_value(110);
    EXPECT_NE(copied._tdigest_ptr, source._tdigest_ptr);
    EXPECT_EQ(110, copied.get_value_by_percentile(1));
    EXPECT_EQ(10, source.get_value_by_percentile(1));

    QuantileState merged;
    merged.merge(source);
    merged.add_value(-10);
    EXPECT_EQ(-10, merged.get_value_by_percentile(0));
    EXPECT_EQ(10, source.get_value_by_percentile(0));
}

static QuantileState constant_state(double value, int count = 4096) {
    QuantileState state;
    for (int i = 0; i < count; ++i) {
        state.add_value(value);
    }
    return state;
}

TEST(QuantileStateTest, MergeDoesNotModifyDigestSourceForAnyTargetType) {
    for (int count : {0, 1, 2, 4096}) {
        auto source = constant_state(10);
        auto target = constant_state(-10, count);
        target.merge(source);
        EXPECT_EQ(count == 0 ? 10 : -10, target.get_value_by_percentile(0));
        EXPECT_EQ(10, target.get_value_by_percentile(1));
        target.add_value(110);
        EXPECT_EQ(110, target.get_value_by_percentile(1));
        EXPECT_EQ(10, source.get_value_by_percentile(0));
        EXPECT_EQ(10, source.get_value_by_percentile(1));
    }
}

TEST(QuantileStateTest, AssignmentAndExplicitMergeDetachOnlyOnce) {
    auto source = constant_state(10);
    QuantileState target;
    target = source;
    auto extra = constant_state(-10, 2);
    target.merge(extra);
    auto* detached = target._tdigest_ptr.get();
    EXPECT_NE(detached, source._tdigest_ptr.get());
    for (int i = 0; i < 100; ++i) {
        target.add_value(110);
        EXPECT_EQ(detached, target._tdigest_ptr.get());
    }
    EXPECT_EQ(-10, target.get_value_by_percentile(0));
    EXPECT_EQ(110, target.get_value_by_percentile(1));
    EXPECT_EQ(10, source.get_value_by_percentile(0));
    EXPECT_EQ(10, source.get_value_by_percentile(1));
}

TEST(QuantileStateTest, WindowReadsReuseDigestUntilResultIsCopied) {
    auto state = constant_state(10);
    auto* original = state._tdigest_ptr.get();
    for (int i = 11; i < 100; ++i) {
        state.add_value(i);
        EXPECT_EQ(i, state.get_value_by_percentile(1));
        EXPECT_EQ(original, state._tdigest_ptr.get());
    }
    auto saved_result = state;
    state.add_value(110);
    EXPECT_EQ(99, saved_result.get_value_by_percentile(1));
    EXPECT_EQ(110, state.get_value_by_percentile(1));
}

TEST(QuantileStateTest, WritesReuseDigestAfterLastCopyIsDestroyed) {
    auto state = constant_state(10);
    auto* original = state._tdigest_ptr.get();
    {
        auto copy = state;
        EXPECT_EQ(original, copy._tdigest_ptr.get());
    }
    state.add_value(110);
    EXPECT_EQ(original, state._tdigest_ptr.get());
    EXPECT_EQ(110, state.get_value_by_percentile(1));
}

TEST(QuantileStateTest, WritesReuseDigestAfterOtherCopyDetaches) {
    auto state = constant_state(10);
    auto* original = state._tdigest_ptr.get();
    auto copy = state;
    copy.add_value(110);
    state.add_value(-10);
    EXPECT_EQ(original, state._tdigest_ptr.get());
    EXPECT_EQ(10, state.get_value_by_percentile(1));
    EXPECT_EQ(110, copy.get_value_by_percentile(1));
    EXPECT_EQ(10, copy.get_value_by_percentile(0));
}

TEST(QuantileStateTest, SerializedSizeRemainsStableAcrossSharedQueries) {
    auto source = constant_state(10);
    auto copy = source;
    const auto size = source.get_serialized_size();
    EXPECT_EQ(10, copy.get_value_by_percentile(0.5));
    std::vector<uint8_t> bytes(size);
    ASSERT_EQ(size, source.serialize(bytes.data()));
    QuantileState restored(Slice(reinterpret_cast<char*>(bytes.data()), bytes.size()));
    EXPECT_EQ(10, restored.get_value_by_percentile(0.5));
}

static long serialized_digest_weight(const QuantileState& state) {
    std::vector<uint8_t> bytes(state.get_serialized_size());
    state.serialize(bytes.data());
    TDigest digest(0);
    digest.unserialize(bytes.data() + sizeof(float) + sizeof(uint8_t));
    return digest.total_weight();
}

TEST(QuantileStateTest, SelfMergeAndSharedSourceMerge) {
    for (int count : {2, 4096}) {
        auto state = constant_state(10, count);
        auto unchanged = state;
        state.merge(state);
        state.merge(unchanged);
        if (count == 2) {
            EXPECT_EQ(6, state._explicit_data.size());
        } else {
            EXPECT_EQ(3 * serialized_digest_weight(unchanged), serialized_digest_weight(state));
        }
        state.add_value(110);
        EXPECT_EQ(10, state.get_value_by_percentile(0));
        EXPECT_EQ(110, state.get_value_by_percentile(1));
        EXPECT_EQ(10, unchanged.get_value_by_percentile(1));
    }
}

static void expect_serialized_maximum(const QuantileState& state, double expected) {
    std::vector<uint8_t> bytes(state.get_serialized_size());
    ASSERT_EQ(bytes.size(), state.serialize(bytes.data()));
    QuantileState restored(Slice(reinterpret_cast<char*>(bytes.data()), bytes.size()));
    EXPECT_EQ(expected, restored.get_value_by_percentile(1));
}

TEST(QuantileStateTest, ConcurrentReadsSerializationAndIndependentWrites) {
    auto source = constant_state(10);
    std::vector<QuantileState> writers(4, source);
    std::barrier start(8);
    std::vector<std::thread> threads;
    for (int i = 0; i < 2; ++i) {
        threads.emplace_back([&] {
            start.arrive_and_wait();
            for (int j = 0; j < 100; ++j) {
                EXPECT_EQ(10, source.get_value_by_percentile(0.5));
            }
        });
    }
    threads.emplace_back([&] {
        start.arrive_and_wait();
        for (int j = 0; j < 100; ++j) {
            expect_serialized_maximum(source, 10);
        }
    });
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&, i] {
            start.arrive_and_wait();
            for (int j = 0; j < 100; ++j) {
                writers[i].add_value(110 + i);
                writers[i].merge(source);
                EXPECT_EQ(110 + i, writers[i].get_value_by_percentile(1));
            }
        });
    }
    start.arrive_and_wait();
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(10, source.get_value_by_percentile(1));
}

TEST(QuantileStateTest, SharedSourceDetachesAndReadsCanOverlap) {
    auto source = constant_state(10);
    // A processed query only needs a shared lock; compression still needs an exclusive lock.
    ASSERT_EQ(10, source.get_value_by_percentile(0.5));
    auto first = source;
    auto second = source;
    std::promise<void> first_entered;
    std::promise<void> release_first;
    auto released = release_first.get_future();
    std::atomic<int> arrivals {0};
    auto* sync = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync->set_call_back(
            "QuantileState::detach:source_locked",
            [&](auto&&) {
                if (arrivals.fetch_add(1) == 0) {
                    first_entered.set_value();
                    released.wait();
                }
            },
            &guard);
    sync->enable_processing();
    std::thread first_thread([&] { first.add_value(-10); });
    auto first_ready = first_entered.get_future().wait_for(std::chrono::seconds(10));
    std::promise<void> second_finished;
    std::thread second_thread([&] {
        second.add_value(110);
        second_finished.set_value();
    });
    std::promise<double> read_finished;
    auto read_result = read_finished.get_future();
    std::thread reader([&] { read_finished.set_value(source.get_value_by_percentile(0.5)); });
    auto second_ready = second_finished.get_future().wait_for(std::chrono::seconds(10));
    auto read_ready = read_result.wait_for(std::chrono::seconds(10));
    release_first.set_value();
    first_thread.join();
    second_thread.join();
    reader.join();
    sync->disable_processing();
    EXPECT_EQ(std::future_status::ready, first_ready);
    EXPECT_EQ(std::future_status::ready, second_ready);
    EXPECT_EQ(std::future_status::ready, read_ready);
    EXPECT_EQ(10, read_result.get());
    EXPECT_EQ(-10, first.get_value_by_percentile(0));
    EXPECT_EQ(10, first.get_value_by_percentile(1));
    EXPECT_EQ(10, second.get_value_by_percentile(0));
    EXPECT_EQ(110, second.get_value_by_percentile(1));
    EXPECT_EQ(10, source.get_value_by_percentile(0.5));
}

TEST(QuantileStateTest, SharedSourceMergesCanOverlap) {
    auto source = constant_state(10);
    auto first = constant_state(-10);
    auto second = constant_state(20);
    std::promise<void> first_entered;
    std::promise<void> second_entered;
    std::promise<void> release_first;
    auto released = release_first.get_future();
    std::atomic<int> arrivals {0};
    auto* sync = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync->set_call_back(
            "QuantileState::merge:source_locked",
            [&](auto&&) {
                if (arrivals.fetch_add(1) == 0) {
                    first_entered.set_value();
                    released.wait();
                } else {
                    second_entered.set_value();
                }
            },
            &guard);
    sync->enable_processing();
    std::thread first_thread([&] { first.merge(source); });
    auto first_ready = first_entered.get_future().wait_for(std::chrono::seconds(10));
    std::thread second_thread([&] { second.merge(source); });
    auto second_ready = second_entered.get_future().wait_for(std::chrono::seconds(10));
    release_first.set_value();
    first_thread.join();
    second_thread.join();
    sync->disable_processing();
    EXPECT_EQ(std::future_status::ready, first_ready);
    EXPECT_EQ(std::future_status::ready, second_ready);
    EXPECT_EQ(-10, first.get_value_by_percentile(0));
    EXPECT_EQ(20, second.get_value_by_percentile(1));
    EXPECT_EQ(10, source.get_value_by_percentile(0.5));
}

TEST(QuantileStateTest, ReadsLegacyUnprocessedDigestAndKeepsWireLayout) {
    TDigest legacy(2048);
    for (int i = 0; i < 4096; ++i) {
        legacy.add(10);
    }
    constexpr size_t header_size = sizeof(float) + sizeof(uint8_t);
    std::vector<uint8_t> bytes(header_size + legacy.serialized_size());
    const float compression = 2048;
    memcpy(bytes.data(), &compression, sizeof(compression));
    bytes[sizeof(float)] = TDIGEST;
    legacy.serialize(bytes.data() + header_size);
    QuantileState state(Slice(reinterpret_cast<char*>(bytes.data()), bytes.size()));
    EXPECT_EQ(10, state.get_value_by_percentile(0.5));
    bytes.resize(state.get_serialized_size());
    ASSERT_EQ(bytes.size(), state.serialize(bytes.data()));
    legacy.unserialize(bytes.data() + header_size);
    EXPECT_EQ(4096, legacy.total_weight());
    EXPECT_EQ(10, legacy.quantile(0.5));
}

TEST(QuantileStateTest, merge) {
    QuantileState empty;
    EXPECT_EQ(EMPTY, empty._type);
    empty.add_value(1);
    EXPECT_EQ(SINGLE, empty._type);
    empty.add_value(2);
    empty.add_value(3);
    empty.add_value(4);
    empty.add_value(5);
    EXPECT_EQ(1, empty.get_value_by_percentile(0));
    EXPECT_EQ(3, empty.get_value_by_percentile(0.5));
    EXPECT_EQ(5, empty.get_value_by_percentile(1));

    QuantileState another;
    another.add_value(6);
    another.add_value(7);
    another.add_value(8);
    another.add_value(9);
    another.add_value(10);
    EXPECT_EQ(6, another.get_value_by_percentile(0));
    EXPECT_EQ(8, another.get_value_by_percentile(0.5));
    EXPECT_EQ(10, another.get_value_by_percentile(1));

    another.merge(empty);
    EXPECT_EQ(1, another.get_value_by_percentile(0));
    EXPECT_EQ(5.5, another.get_value_by_percentile(0.5));
    EXPECT_EQ(10, another.get_value_by_percentile(1));
}

} // namespace doris
