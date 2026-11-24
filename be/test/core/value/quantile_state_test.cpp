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

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
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

static void expect_digest_samples(const QuantileState& state, int count, double minimum,
                                  double maximum) {
    ASSERT_EQ(TDIGEST, state._type);
    EXPECT_EQ(count, serialized_digest_weight(state));
    EXPECT_EQ(minimum, state.get_value_by_percentile(0));
    EXPECT_EQ(maximum, state.get_value_by_percentile(1));
}

TEST(QuantileStateTest, AddValueRetainsSampleAtExplicitLimit) {
    for (float compression : {QUANTILE_STATE_COMPRESSION_DEFAULT, QUANTILE_STATE_COMPRESSION_MAX}) {
        for (double value : {-100.0, 100.0}) {
            QuantileState state(compression);
            for (int i = 0; i < QUANTILE_STATE_EXPLICIT_NUM - 1; ++i) {
                state.add_value(0);
            }
            ASSERT_EQ(EXPLICIT, state._type);
            EXPECT_EQ(QUANTILE_STATE_EXPLICIT_NUM - 1, state._explicit_data.size());
            state.add_value(0);
            ASSERT_EQ(EXPLICIT, state._type);
            EXPECT_EQ(QUANTILE_STATE_EXPLICIT_NUM, state._explicit_data.size());

            state.add_value(value);
            EXPECT_TRUE(state._explicit_data.empty());
            expect_digest_samples(state, QUANTILE_STATE_EXPLICIT_NUM + 1, std::min(0.0, value),
                                  std::max(0.0, value));

            state.add_value(0);
            expect_digest_samples(state, QUANTILE_STATE_EXPLICIT_NUM + 2, std::min(0.0, value),
                                  std::max(0.0, value));
        }
    }
}

TEST(QuantileStateTest, LowCompressionBoundaryAndDefault) {
    EXPECT_EQ(QUANTILE_STATE_COMPRESSION_DEFAULT, QuantileState()._compression);
    for (float compression : {128.0F, 128.5F, 256.0F, 2047.0F, 2048.0F}) {
        QuantileState state(compression);
        const auto limit = std::min<size_t>(QUANTILE_STATE_EXPLICIT_NUM,
                                            static_cast<size_t>(std::ceil(compression)));
        for (size_t i = 0; i < limit; ++i) {
            state.add_value(0);
        }
        ASSERT_EQ(EXPLICIT, state._type) << compression;
        state.add_value(100);
        expect_digest_samples(state, limit + 1, 0, 100);
        EXPECT_EQ(0, state._explicit_data.capacity());
        EXPECT_EQ(compression, state._mutable_tdigest().compression());
        if (compression < QUANTILE_STATE_COMPRESSION_DEFAULT) {
            EXPECT_EQ(std::ceil(compression), state._mutable_tdigest().max_unprocessed());
        }
    }
}

TEST(QuantileStateTest, LowCompression1440SamplesRetainLessBuffer) {
    QuantileState low(128);
    QuantileState normal;
    for (int i = 0; i < 1440; ++i) {
        low.add_value(i);
        normal.add_value(i);
    }
    ASSERT_EQ(TDIGEST, low._type);
    ASSERT_EQ(EXPLICIT, normal._type);
    const auto& digest = low._mutable_tdigest();
    const size_t retained_bytes =
            (digest._processed.capacity() + digest._unprocessed.capacity()) * sizeof(Centroid) +
            digest._cumulative.capacity() * sizeof(Weight);
    EXPECT_LT(retained_bytes, normal._explicit_data.capacity() * sizeof(double));
    EXPECT_EQ(0, low._explicit_data.capacity());
    expect_digest_samples(low, 1440, 0, 1439);
}

static void expect_mixed_result_roundtrip(QuantileState result) {
    ASSERT_EQ(TDIGEST, result._type);
    EXPECT_EQ(2048, result._compression);
    EXPECT_EQ(2048, result._mutable_tdigest().compression());
    EXPECT_EQ(0, result._explicit_data.capacity());
    expect_digest_samples(result, 960, 0, 959);
    std::vector<uint8_t> bytes(result.get_serialized_size());
    ASSERT_EQ(bytes.size(), result.serialize(bytes.data()));
    QuantileState restored;
    ASSERT_TRUE(restored.deserialize(Slice(reinterpret_cast<char*>(bytes.data()), bytes.size())));
    EXPECT_EQ(2048, restored._compression);
    restored.add_value(1000);
    expect_digest_samples(restored, 961, 0, 1000);
}

TEST(QuantileStateTest, LowCompressionMixedMergeAndRoundTrip) {
    QuantileState low(128);
    QuantileState high(2048);
    for (int i = 0; i < 480; ++i) {
        low.add_value(i);
        high.add_value(i + 480);
    }
    auto low_copy = low;
    auto low_first = low;
    low_first.merge(high);
    auto high_first = high;
    high_first.merge(low);
    expect_mixed_result_roundtrip(low_first);
    expect_mixed_result_roundtrip(high_first);
    EXPECT_EQ(480, serialized_digest_weight(low_copy));
    EXPECT_EQ(2048, high._compression);
}

TEST(QuantileStateTest, LowCompressionMergePreservesWeightedSourceExtrema) {
    for (double sign : {1.0, -1.0}) {
        QuantileState source(256);
        source.add_value(0);
        for (int i = 0; i <= 256; ++i) {
            source.add_value(sign * 100);
        }
        auto& digest = source._mutable_tdigest();
        digest.add(static_cast<float>(sign * 1), 1);
        digest.add(static_cast<float>(sign * 100), 100000);
        const double source_min = std::min(0.0, sign * 100);
        const double source_max = std::max(0.0, sign * 100);
        EXPECT_EQ(source_min, source.get_value_by_percentile(0));
        EXPECT_EQ(source_max, source.get_value_by_percentile(1));
        if (sign > 0) {
            EXPECT_GT(digest.processed().front().mean(), source_min);
        }

        QuantileState lower(128);
        lower.add_value(sign * 100);
        source.merge(lower);
        EXPECT_EQ(256, source._compression);
        EXPECT_EQ(256, source._mutable_tdigest().compression());
        expect_digest_samples(source, 100260, source_min, source_max);
        std::vector<uint8_t> bytes(source.get_serialized_size());
        ASSERT_EQ(bytes.size(), source.serialize(bytes.data()));
        QuantileState restored;
        ASSERT_TRUE(
                restored.deserialize(Slice(reinterpret_cast<char*>(bytes.data()), bytes.size())));
        expect_digest_samples(restored, 100260, source_min, source_max);
    }
}

TEST(QuantileStateTest, HighCompressionMergePreservesLowDigestEndpoint) {
    QuantileState low(128);
    low.add_value(0);
    for (int i = 0; i <= 128; ++i) {
        low.add_value(100);
    }
    auto& digest = low._mutable_tdigest();
    digest.add(1, 1);
    digest.add(100, 100000);
    EXPECT_EQ(0, low.get_value_by_percentile(0));
    ASSERT_FALSE(digest.processed().empty());
    EXPECT_GT(digest.processed().front().mean(), 0);

    QuantileState high(2048);
    high.add_value(100);
    high.merge(low);
    EXPECT_EQ(2048, high._compression);
    EXPECT_EQ(2048, high._mutable_tdigest().compression());
    EXPECT_EQ(0, high.get_value_by_percentile(0));
    EXPECT_EQ(100, high.get_value_by_percentile(1));
}

TEST(QuantileStateTest, HighCompressionMergePreservesLowDigestMaximum) {
    QuantileState negative_low(128);
    negative_low.add_value(0);
    for (int i = 0; i <= 128; ++i) {
        negative_low.add_value(-100);
    }
    auto& negative_digest = negative_low._mutable_tdigest();
    negative_digest.add(-1, 1);
    negative_digest.add(-100, 100000);
    EXPECT_EQ(0, negative_low.get_value_by_percentile(1));
    ASSERT_FALSE(negative_digest.processed().empty());
    EXPECT_LT(negative_digest.processed().back().mean(), 0);

    QuantileState negative_high(2048);
    negative_high.add_value(-100);
    negative_high.merge(negative_low);
    EXPECT_EQ(-100, negative_high.get_value_by_percentile(0));
    EXPECT_EQ(0, negative_high.get_value_by_percentile(1));
}

static void expect_high_mixed_result(QuantileState& result, const char* label) {
    SCOPED_TRACE(label);
    auto& digest = result._mutable_tdigest();
    EXPECT_EQ(10000, result._compression);
    EXPECT_EQ(10000, digest.compression());
    EXPECT_EQ(0, result.get_value_by_percentile(0));
    EXPECT_EQ(60000, digest._max);
    expect_digest_samples(result, 60001, 0, 60000);
}

TEST(QuantileStateTest, LowCompressionMixedMergeSelectsHigherCompression) {
    QuantileState high(10000);
    for (int i = 0; i < 60000; ++i) {
        high.add_value(i);
    }
    ASSERT_EQ(TDIGEST, high._type);
    EXPECT_EQ(60000, high._mutable_tdigest().unprocessed().size());
    QuantileState low(128);
    low.add_value(60000);

    auto high_first = high;
    high_first.merge(low);
    expect_high_mixed_result(high_first, "pending high first");
    auto low_first = low;
    low_first.merge(high);
    expect_high_mixed_result(low_first, "pending low first");
    EXPECT_EQ(60000, high._mutable_tdigest().unprocessed().size());

    EXPECT_EQ(0, high.get_value_by_percentile(0));
    EXPECT_GT(high._mutable_tdigest().processed().size(), 1000);
    auto processed_high_first = high;
    processed_high_first.merge(low);
    expect_high_mixed_result(processed_high_first, "processed high first");
    auto processed_low_first = low;
    processed_low_first.merge(high);
    expect_high_mixed_result(processed_low_first, "processed low first");
}

TEST(QuantileStateTest, EmptyAndExplicitMergeUseNonemptyCompression) {
    QuantileState low(128);
    QuantileState high(2048);
    for (int i = 0; i < 64; ++i) {
        low.add_value(i);
        high.add_value(i + 64);
    }
    QuantileState empty;
    empty.merge(low);
    EXPECT_EQ(128, empty._compression);
    EXPECT_EQ(EXPLICIT, empty._type);
    empty.merge(high);
    EXPECT_EQ(2048, empty._compression);
    EXPECT_EQ(EXPLICIT, empty._type);
    EXPECT_EQ(128, empty._explicit_data.size());
    empty.add_value(128);
    EXPECT_EQ(EXPLICIT, empty._type);
    EXPECT_EQ(129, empty._explicit_data.size());

    auto reverse = high;
    reverse.merge(low);
    EXPECT_EQ(2048, reverse._compression);
    EXPECT_EQ(EXPLICIT, reverse._type);
    reverse.add_value(128);
    EXPECT_EQ(EXPLICIT, reverse._type);
    EXPECT_EQ(129, reverse._explicit_data.size());
}

TEST(QuantileStateTest, ReadOversizedLowExplicitThenAppend) {
    QuantileState legacy(2048);
    for (int i = 0; i < 1440; ++i) {
        legacy.add_value(i);
    }
    std::vector<uint8_t> bytes(legacy.get_serialized_size());
    ASSERT_EQ(bytes.size(), legacy.serialize(bytes.data()));
    const float low_compression = 128;
    std::memcpy(bytes.data(), &low_compression, sizeof(low_compression));
    QuantileState restored;
    ASSERT_TRUE(restored.deserialize(Slice(reinterpret_cast<char*>(bytes.data()), bytes.size())));
    ASSERT_EQ(EXPLICIT, restored._type);
    ASSERT_EQ(1440, restored._explicit_data.size());
    auto merged = restored;
    QuantileState singleton(2048);
    singleton.add_value(1440);
    merged.merge(singleton);
    EXPECT_EQ(2048, merged._compression);
    EXPECT_EQ(EXPLICIT, merged._type);
    EXPECT_EQ(1441, merged._explicit_data.size());
    restored.add_value(1440);
    EXPECT_EQ(0, restored._explicit_data.capacity());
    expect_digest_samples(restored, 1441, 0, 1440);
}

TEST(QuantileStateTest, LowCompressionThreePartialTreesAndSelfMerge) {
    QuantileState first(2048);
    QuantileState second(256);
    QuantileState third(128);
    for (int i = 0; i < 480; ++i) {
        first.add_value(10);
        second.add_value(20);
        third.add_value(30);
    }
    auto left_tree = first;
    left_tree.merge(second);
    left_tree.merge(third);
    auto right_tree = second;
    right_tree.merge(third);
    auto reverse = first;
    reverse.merge(right_tree);
    for (auto state : {left_tree, reverse}) {
        EXPECT_EQ(2048, state._compression);
        EXPECT_EQ(2048, state._mutable_tdigest().compression());
        expect_digest_samples(state, 1440, 10, 30);
        const auto before = state;
        state.merge(state);
        expect_digest_samples(state, 2880, 10, 30);
        expect_digest_samples(before, 1440, 10, 30);
    }
}

TEST(QuantileStateTest, MergeSingletonAndExplicitRetainsBoundarySample) {
    for (double value : {-100.0, 100.0}) {
        auto explicit_state = constant_state(0, QUANTILE_STATE_EXPLICIT_NUM);
        auto singleton = constant_state(value, 1);

        auto explicit_first = explicit_state;
        explicit_first.merge(singleton);
        auto singleton_first = singleton;
        singleton_first.merge(explicit_state);
        expect_digest_samples(explicit_first, QUANTILE_STATE_EXPLICIT_NUM + 1, std::min(0.0, value),
                              std::max(0.0, value));
        expect_digest_samples(singleton_first, QUANTILE_STATE_EXPLICIT_NUM + 1,
                              std::min(0.0, value), std::max(0.0, value));
        EXPECT_EQ(QUANTILE_STATE_EXPLICIT_NUM, explicit_state._explicit_data.size());
        EXPECT_EQ(0, explicit_state.get_value_by_percentile(1));
        EXPECT_EQ(value, singleton.get_value_by_percentile(1));
    }
}

TEST(QuantileStateTest, ExplicitMergeGroupingRetainsAllSamples) {
    auto state = constant_state(0, QUANTILE_STATE_EXPLICIT_NUM - 1);
    auto other = constant_state(0, 1);
    other.add_value(100);
    state.merge(other);
    expect_digest_samples(state, QUANTILE_STATE_EXPLICIT_NUM + 1, 0, 100);
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
