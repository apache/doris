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

#include "util/tdigest.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <future>
#include <latch>
#include <memory>
#include <numeric>
#include <random>
#include <thread>

#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"

namespace doris {

class TDigestTest : public ::testing::Test {
protected:
    // You can remove any or all of the following functions if its body
    // is empty.
    TDigestTest() {
        // You can do set-up work for each test here.
    }

    virtual ~TDigestTest() {
        // You can do clean-up work that doesn't throw exceptions here.
    }

    // If the constructor and destructor are not enough for setting up
    // and cleaning up each test, you can define the following methods:

    virtual void SetUp() {
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    virtual void TearDown() {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }
};

class TDigestSnapshotTest : public TDigestTest {
protected:
    static void expect_read_matches(const TDigest& reader, const TDigest& expected, int worker) {
        if (worker % 3 == 0) {
            EXPECT_FLOAT_EQ(expected.quantile(0.5), reader.quantile(0.5));
        } else if (worker % 3 == 1) {
            EXPECT_FLOAT_EQ(expected.cdf(40000), reader.cdf(40000));
        } else {
            const double levels[] = {0, 0.5, 1};
            const size_t permutation[] = {0, 1, 2};
            double values[3];
            double expected_values[3];
            reader.quantiles(levels, permutation, 3, values);
            expected.quantiles(levels, permutation, 3, expected_values);
            for (int i = 0; i < 3; ++i) {
                EXPECT_DOUBLE_EQ(expected_values[i], values[i]);
            }
        }
    }

    void SetUp() override {
        TDigestTest::SetUp();
        SyncPoint::get_instance()->enable_processing();
    }

    void TearDown() override {
        SyncPoint::get_instance()->disable_processing();
        SyncPoint::get_instance()->clear_all_call_backs();
        SyncPoint::get_instance()->clear_trace();
        TDigestTest::TearDown();
    }
};

static double quantile(const double q, const std::vector<double>& values) {
    double q1;
    if (values.size() == 0) {
        q1 = NAN;
    } else if (q == 1 || values.size() == 1) {
        q1 = values[values.size() - 1];
    } else {
        auto index = q * values.size();
        if (index < 0.5) {
            q1 = values[0];
        } else if (values.size() - index < 0.5) {
            q1 = values[values.size() - 1];
        } else {
            index -= 0.5;
            const int intIndex = static_cast<int>(index);
            q1 = values[intIndex + 1] * (index - intIndex) +
                 values[intIndex] * (intIndex + 1 - index);
        }
    }
    return q1;
}

static std::string serialize_digest(const TDigest& digest) {
    std::string bytes(digest.serialized_size(), '\0');
    digest.serialize(reinterpret_cast<uint8_t*>(bytes.data()));
    return bytes;
}

TEST_F(TDigestTest, CopiesShareUntilWritten) {
    TDigest source(100);
    source.add(10);
    source.add(20);
    auto copy = source;
    TDigest assigned;
    assigned = source;
    // Copying must not allocate another centroid buffer.
    EXPECT_EQ(source.unprocessed().data(), copy.unprocessed().data());
    EXPECT_EQ(source.unprocessed().data(), assigned.unprocessed().data());
    copy.add(30);
    EXPECT_NE(source.unprocessed().data(), copy.unprocessed().data());
    const auto* detached_data = copy._data.get();
    copy.add(40);
    EXPECT_EQ(detached_data, copy._data.get());
    EXPECT_EQ(2, source.total_weight());
    EXPECT_EQ(4, copy.total_weight());
    assigned.compress();
    EXPECT_TRUE(source.processed().empty());
    EXPECT_EQ(2, assigned.processed().size());
    EXPECT_FLOAT_EQ(20, source.quantile(1));
    EXPECT_FLOAT_EQ(40, copy.quantile(1));
}

TEST_F(TDigestTest, ConstReadsReuseSnapshotAndWritesInvalidateIt) {
    TDigest digest(100);
    digest.add(10);
    digest.add(20);
    const auto before = serialize_digest(digest);
    const auto* original_data = digest._data.get();
    const auto& readonly = digest;
    EXPECT_FLOAT_EQ(15, readonly.quantile(0.5));
    const auto* snapshot_address = digest._data->_processed_snapshot.get();
    const TDigest snapshot(*snapshot_address);
    EXPECT_FLOAT_EQ(20, readonly.quantile(1));
    EXPECT_EQ(snapshot_address, digest._data->_processed_snapshot.get());
    EXPECT_EQ(before, serialize_digest(readonly));
    digest.add(30);
    EXPECT_EQ(original_data, digest._data.get());
    EXPECT_FLOAT_EQ(30, readonly.quantile(1));
    EXPECT_FLOAT_EQ(20, readonly.quantile(0.5));
    EXPECT_FLOAT_EQ(20, snapshot.quantile(1));

    TDigest incoming(100);
    incoming.add(50);
    digest.merge(&incoming);
    EXPECT_FLOAT_EQ(50, readonly.quantile(1));
    EXPECT_FLOAT_EQ(25, readonly.quantile(0.5));
    const auto bytes = serialize_digest(incoming);
    digest.unserialize(reinterpret_cast<const uint8_t*>(bytes.data()));
    EXPECT_EQ(1, digest.total_weight());
    EXPECT_FLOAT_EQ(50, readonly.quantile(0.5));
}

TEST_F(TDigestTest, SelfAndAliasedBatchMergePreserveWeights) {
    for (bool processed : {false, true}) {
        TDigest digest(100);
        digest.add(10);
        digest.add(20);
        if (processed) {
            digest.compress();
        }
        auto copy = digest;
        digest.merge(&digest);
        EXPECT_EQ(4, digest.total_weight());
        EXPECT_EQ(2, copy.total_weight());
        EXPECT_FLOAT_EQ(15, digest.quantile(0.5));
        digest.add(std::vector<const TDigest*> {&digest, &copy, &copy});
        EXPECT_EQ(12, digest.total_weight());
        EXPECT_EQ(2, copy.total_weight());
        EXPECT_FLOAT_EQ(15, digest.quantile(0.5));
    }
}

TEST_F(TDigestTest, ConcurrentReadersAndIndependentWriters) {
    TDigest source(100);
    for (int i = 0; i < 500; ++i) {
        source.add(10);
    }
    const auto before = serialize_digest(source);
    std::latch start(8);
    std::vector<std::thread> threads;
    for (int worker = 0; worker < 8; ++worker) {
        threads.emplace_back([&, worker] {
            auto copy = source;
            start.arrive_and_wait();
            if (worker < 4) {
                for (int i = 0; i < 1000; ++i) {
                    copy.add(100 + worker);
                }
                EXPECT_EQ(1500, copy.total_weight());
                EXPECT_FLOAT_EQ(100 + worker, copy.quantile(1));
            } else {
                const double levels[] = {0, 0.5, 1};
                const size_t permutation[] = {0, 1, 2};
                for (int i = 0; i < 100; ++i) {
                    double values[3];
                    source.quantiles(levels, permutation, 3, values);
                    for (auto value : values) {
                        EXPECT_DOUBLE_EQ(10, value);
                    }
                    EXPECT_FLOAT_EQ(10, source.quantile(0.9));
                    EXPECT_FLOAT_EQ(0, source.cdf(0));
                    EXPECT_EQ(before, serialize_digest(source));
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(500, source.total_weight());
    EXPECT_EQ(before, serialize_digest(source));
}

TEST_F(TDigestSnapshotTest, ConcurrentColdReadsBuildOneSnapshot) {
    std::atomic<int> builds = 0;
    SyncPoint::get_instance()->set_call_back("TDigest::_processed_digest:build_snapshot",
                                             [&](auto&&) { ++builds; });
    TDigest source(10000);
    for (int i = 0; i < 79000; ++i) {
        source.add((i * 37) % 79000);
    }
    ASSERT_EQ(79000, source.unprocessed().size());
    const auto before = serialize_digest(source);
    auto expected = source;
    expected.compress();

    std::latch start(8);
    std::vector<const TDigest*> snapshots(8);
    std::vector<std::thread> threads;
    for (int worker = 0; worker < 8; ++worker) {
        threads.emplace_back([&, worker] {
            auto copy = source;
            const auto& reader = worker % 2 == 0 ? source : copy;
            start.arrive_and_wait();
            expect_read_matches(reader, expected, worker);
            snapshots[worker] = &reader._processed_digest();
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (const auto* snapshot : snapshots) {
        EXPECT_EQ(source._data->_processed_snapshot.get(), snapshot);
    }
    EXPECT_EQ(1, builds.load());
    EXPECT_EQ(before, serialize_digest(source));
}

TEST_F(TDigestTest, DetachPreservesWriteCapacity) {
    TDigest source(100);
    source.add(10);
    source.compress();
    source.add(20);
    auto copy = source;
    copy.add(30);
    EXPECT_GE(copy.processed().capacity(), source.processed().capacity());
    EXPECT_GE(copy.unprocessed().capacity(), source.unprocessed().capacity());
    const auto* buffer = copy.unprocessed().data();
    for (int i = 0; i < 100; ++i) {
        copy.add(i);
    }
    EXPECT_EQ(buffer, copy.unprocessed().data());
    EXPECT_EQ(2, source.total_weight());

    // Read copies should not inherit the spare capacity intended for writes.
    TDigest read_copy(*source._data);
    EXPECT_LT(read_copy.processed().capacity(), source.processed().capacity());
    EXPECT_LT(read_copy.unprocessed().capacity(), source.unprocessed().capacity());
}

TEST_F(TDigestSnapshotTest, FailedSnapshotBuildWakesReadersToRetry) {
    TDigest source(100);
    source.add(10);
    std::atomic<int> builds = 0;
    std::atomic<int> waiters = 0;
    std::atomic<int> failures = 0;
    std::promise<void> all_waiting;
    auto waiting = all_waiting.get_future();
    SyncPoint::get_instance()->set_call_back("TDigest::_processed_digest:wait_snapshot",
                                             [&](auto&&) {
                                                 if (++waiters == 7) {
                                                     all_waiting.set_value();
                                                 }
                                             });
    SyncPoint::get_instance()->set_call_back(
            "TDigest::_processed_digest:build_snapshot", [&](auto&& args) {
                if (++builds == 1) {
                    // Hold the builder outside the mutex until all other readers
                    // have registered to wait, then simulate an allocation failure.
                    EXPECT_EQ(std::future_status::ready,
                              waiting.wait_for(std::chrono::seconds(10)));
                    *std::any_cast<bool*>(args[0]) = true;
                }
            });
    std::latch start(8);
    std::vector<std::thread> threads;
    for (int worker = 0; worker < 8; ++worker) {
        threads.emplace_back([&] {
            start.arrive_and_wait();
            try {
                EXPECT_FLOAT_EQ(10, source.quantile(0.5));
            } catch (const std::bad_alloc&) {
                ++failures;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(1, failures.load());
    EXPECT_EQ(2, builds.load());
    EXPECT_FLOAT_EQ(10, source.quantile(0.5));
    EXPECT_EQ(2, builds.load());
}

TEST_F(TDigestTest, DeserializeDetachesSharedData) {
    TDigest source(100);
    source.add(10);
    auto copy = source;
    TDigest replacement(200);
    replacement.add(50, 3);
    const auto bytes = serialize_digest(replacement);
    copy.unserialize(reinterpret_cast<const uint8_t*>(bytes.data()));
    EXPECT_EQ(bytes, serialize_digest(copy));
    EXPECT_EQ(1, source.total_weight());
    EXPECT_EQ(3, copy.total_weight());
    EXPECT_FLOAT_EQ(10, source.quantile(0.5));
    EXPECT_FLOAT_EQ(50, copy.quantile(0.5));
    EXPECT_FLOAT_EQ(200, copy.compression());
}

TEST_F(TDigestTest, ConcurrentWritersWithOnlyTwoOwners) {
    for (int iteration = 0; iteration < 50; ++iteration) {
        TDigest left(100);
        for (int i = 0; i < 500; ++i) {
            left.add(10);
        }
        auto right = left;
        std::latch start(2);
        auto write = [&start](TDigest digest, Value value) {
            start.arrive_and_wait();
            digest.add(value);
            EXPECT_EQ(501, digest.total_weight());
            EXPECT_FLOAT_EQ(value, digest.quantile(1));
        };
        // No third handle keeps the original payload shared while writers detach.
        std::thread first(write, std::move(left), 100.0F);
        std::thread second(write, std::move(right), 200.0F);
        first.join();
        second.join();
    }
}

TEST_F(TDigestTest, CentroidRangeAddPreservesWeightsAndSupportsAliasing) {
    TDigest source(100);
    source.add(10, 2);
    source.add(20, 3);
    auto copy = source;
    copy.add(copy.unprocessed().cbegin(), copy.unprocessed().cend());
    EXPECT_EQ(5, source.total_weight());
    EXPECT_EQ(10, copy.total_weight());
    EXPECT_FLOAT_EQ(20, copy.quantile(1));
}

TEST_F(TDigestTest, LegacyUnprocessedWireFormat) {
    // Legacy layout: length, compression/min/max, two Index limits,
    // processed/unprocessed weights, then three size-prefixed arrays.
    std::string bytes;
    auto append = [&bytes](auto value) {
        bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
    };
    append(uint32_t(4 + 5 * 4 + 2 * sizeof(Index) + 3 * 4 + 4 * 4));
    append(100.0F);
    append(std::numeric_limits<Value>::max());
    append(std::numeric_limits<Value>::lowest());
    append(Index(200));
    append(Index(800));
    append(0.0F);
    append(2.0F);
    append(uint32_t(0));
    append(uint32_t(2));
    append(10.0F);
    append(1.0F);
    append(20.0F);
    append(1.0F);
    append(uint32_t(0));

    TDigest digest(100);
    digest.add(10);
    digest.add(20);
    EXPECT_EQ(bytes, serialize_digest(digest));
    TDigest restored(0);
    restored.unserialize(reinterpret_cast<const uint8_t*>(bytes.data()));
    EXPECT_FLOAT_EQ(15, restored.quantile(0.5));
    EXPECT_EQ(bytes, serialize_digest(restored));
}

TEST_F(TDigestTest, CrashAfterMerge) {
    TDigest digest(1000);
    std::uniform_real_distribution<> reals(0.0, 1.0);
    std::random_device gen;
    for (int i = 0; i < LOOP_LESS_OR_MORE(100, 100000); i++) {
        digest.add(reals(gen));
    }
    digest.compress();

    TDigest digest2(1000);
    digest2.merge(&digest);
    digest2.quantile(0.5);
}

TEST_F(TDigestTest, EmptyDigest) {
    TDigest digest(100);
    EXPECT_EQ(0, digest.processed().size());
}

TEST_F(TDigestTest, SingleValue) {
    TDigest digest(100);
    std::random_device gen;
    std::uniform_real_distribution<> dist(0, 1000);
    const auto value = dist(gen);
    digest.add(value);
    std::uniform_real_distribution<> dist2(0, 1.0);
    const double q = dist2(gen);
    EXPECT_NEAR(value, digest.quantile(0.0), 0.001f);
    EXPECT_NEAR(value, digest.quantile(q), 0.001f);
    EXPECT_NEAR(value, digest.quantile(1.0), 0.001f);
}

TEST_F(TDigestTest, FewValues) {
    // When there are few values in the tree, quantiles should be exact
    TDigest digest(1000);

    std::random_device gen;
    std::uniform_real_distribution<> reals(0.0, 100.0);
    std::uniform_int_distribution<> dist(0, 10);
    std::uniform_int_distribution<> bools(0, 1);
    std::uniform_real_distribution<> qvalue(0.0, 1.0);

    const auto length = 10; //dist(gen);

    std::vector<double> values;
    values.reserve(length);
    for (int i = 0; i < length; ++i) {
        auto const value = (i == 0 || bools(gen)) ? reals(gen) : values[i - 1];
        digest.add(value);
        values.push_back(value);
    }
    std::sort(values.begin(), values.end());
    digest.compress();

    EXPECT_EQ(digest.processed().size(), values.size());

    std::vector<double> testValues {0.0, 1.0e-10, qvalue(gen), 0.5, 1.0 - 1e-10, 1.0};
    for (auto q : testValues) {
        double q1 = quantile(q, values);
        auto q2 = digest.quantile(q);
        if (std::isnan(q1)) {
            EXPECT_TRUE(std::isnan(q2));
        } else {
            EXPECT_NEAR(q1, q2, 0.03) << "q = " << q;
        }
    }
}

TEST_F(TDigestTest, MoreThan2BValues) {
    TDigest digest(1000);

    std::random_device gen;
    std::uniform_real_distribution<> reals(0.0, 1.0);
    for (int i = 0; i < 1000; ++i) {
        const double next = reals(gen);
        digest.add(next);
    }
    for (int i = 0; i < 10; ++i) {
        const double next = reals(gen);
        const auto count = 1L << 28;
        digest.add(next, count);
    }
    EXPECT_EQ(static_cast<long>(1000 + float(10L * (1 << 28))), digest.total_weight());
    EXPECT_GT(digest.total_weight(), std::numeric_limits<int32_t>::max());
    std::vector<double> quantiles {0, 0.1, 0.5, 0.9, 1, reals(gen)};
    std::sort(quantiles.begin(), quantiles.end());
    auto prev = std::numeric_limits<double>::min();
    for (double q : quantiles) {
        const double v = digest.quantile(q);
        EXPECT_GE(v, prev) << "q = " << q;
        prev = v;
    }
}

TEST_F(TDigestTest, MergeTest) {
    TDigest digest1(1000);
    TDigest digest2(1000);

    digest2.add(std::vector<const TDigest*> {&digest1});
}

TEST_F(TDigestTest, TestSorted) {
    TDigest digest(1000);
    std::uniform_real_distribution<> reals(0.0, 1.0);
    std::uniform_int_distribution<> ints(0, 10);

    std::random_device gen;
    for (int i = 0; i < 10000; ++i) {
        digest.add(reals(gen), 1 + ints(gen));
    }
    digest.compress();
    Centroid previous(0, 0);
    for (auto centroid : digest.processed()) {
        if (previous.weight() != 0) {
            CHECK_LE(previous.mean(), centroid.mean());
        }
        previous = centroid;
    }
}

TEST_F(TDigestTest, ExtremeQuantiles) {
    TDigest digest(1000);
    // t-digest shouldn't merge extreme nodes, but let's still test how it would
    // answer to extreme quantiles in that case ('extreme' in the sense that the
    // quantile is either before the first node or after the last one)

    digest.add(10, 3);
    digest.add(20, 1);
    digest.add(40, 5);
    // this group tree is roughly equivalent to the following sorted array:
    // [ ?, 10, ?, 20, ?, ?, 50, ?, ? ]
    // and we expect it to compute approximate missing values:
    // [ 5, 10, 15, 20, 30, 40, 50, 60, 70]
    std::vector<double> values {5.0, 10.0, 15.0, 20.0, 30.0, 35.0, 40.0, 45.0, 50.0};
    std::vector<double> quantiles {1.5 / 9.0, 3.5 / 9.0, 6.5 / 9.0};
    for (auto q : quantiles) {
        EXPECT_NEAR(quantile(q, values), digest.quantile(q), 0.01) << "q = " << q;
    }
}

TEST_F(TDigestTest, BatchQuantilesMatchSingleQuantiles) {
    TDigest digest(1000);
    for (int i = 0; i < 10000; ++i) {
        digest.add(static_cast<double>((i * 37) % 1000));
    }

    std::vector<double> levels {0.9, 0.0, 0.5, 0.1, 1.0, 0.5, 0.99};
    std::vector<size_t> permutation(levels.size());
    std::iota(permutation.begin(), permutation.end(), 0);
    std::sort(permutation.begin(), permutation.end(),
              [&levels](size_t lhs, size_t rhs) { return levels[lhs] < levels[rhs]; });

    std::vector<double> results(levels.size());
    digest.quantiles(levels.data(), permutation.data(), levels.size(), results.data());

    for (size_t i = 0; i < levels.size(); ++i) {
        EXPECT_DOUBLE_EQ(results[i], static_cast<double>(digest.quantile(levels[i])));
    }
}

TEST_F(TDigestTest, BatchQuantilesMatchSingleQuantileInLeftTail) {
    TDigest digest(2);
    digest.add(0.0F);
    digest.compress();
    digest.add(1.5F, 2.0F);
    digest.add(3.0F, 3.0F);
    digest.compress();

    ASSERT_EQ(digest.processed().size(), 2);
    ASSERT_FLOAT_EQ(digest.processed()[0].mean(), 1.0F);
    ASSERT_FLOAT_EQ(digest.processed()[0].weight(), 3.0F);
    ASSERT_EQ(digest.total_weight(), 6);

    std::vector<double> levels {0.1};
    std::vector<size_t> permutation {0};
    std::vector<double> results(levels.size());
    digest.quantiles(levels.data(), permutation.data(), levels.size(), results.data());

    EXPECT_DOUBLE_EQ(results[0], static_cast<double>(digest.quantile(levels[0])));
}

TEST_F(TDigestTest, AllNegativeValuesHaveCorrectMaximum) {
    TDigest digest(1000);
    digest.add(-3.0);
    digest.add(-2.0);
    digest.add(-1.0);

    std::vector<double> levels {1.0};
    std::vector<size_t> permutation {0};
    std::vector<double> results(levels.size());
    digest.quantiles(levels.data(), permutation.data(), levels.size(), results.data());

    EXPECT_FLOAT_EQ(digest.quantile(levels[0]), -1.0F);
    EXPECT_DOUBLE_EQ(results[0], -1.0);
    EXPECT_DOUBLE_EQ(results[0], static_cast<double>(digest.quantile(levels[0])));
}

TEST_F(TDigestTest, BatchQuantilesHandleEmptyAndSingleValueDigests) {
    std::vector<double> levels {0.0, 0.5, 1.0};
    std::vector<size_t> permutation {0, 1, 2};
    std::vector<double> results(levels.size());

    TDigest empty_digest(1000);
    empty_digest.quantiles(levels.data(), permutation.data(), levels.size(), results.data());
    for (double result : results) {
        EXPECT_TRUE(std::isnan(result));
    }

    TDigest single_value_digest(1000);
    single_value_digest.add(42.0);
    single_value_digest.quantiles(levels.data(), permutation.data(), levels.size(), results.data());
    for (double result : results) {
        EXPECT_DOUBLE_EQ(result, 42.0);
    }
}

TEST_F(TDigestTest, Montonicity) {
    TDigest digest(1000);
    std::uniform_real_distribution<> reals(0.0, 1.0);
    std::random_device gen;
    for (int i = 0; i < LOOP_LESS_OR_MORE(10, 100000); i++) {
        digest.add(reals(gen));
    }

    double lastQuantile = -1;
    double lastX = -1;
    for (double z = 0; z <= 1; z += LOOP_LESS_OR_MORE(0.1, 1e-5)) {
        double x = digest.quantile(z);
        EXPECT_GE(x, lastX);
        lastX = x;

        double q = digest.cdf(z);
        EXPECT_GE(q, lastQuantile);
        lastQuantile = q;
    }
}

} // namespace doris
