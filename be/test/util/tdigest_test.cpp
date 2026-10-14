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

#include <array>
#include <memory>
#include <numeric>
#include <random>

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

TEST(WeightedTDigestTest, ScaleInvariantAfterCompressionSerializationAndMerge) {
    const std::array<float, 5> levels {0, 0.1, 0.5, 0.9, 1};
    WeightedTDigest reference(100);
    for (int i = 0; i < 10000; ++i) {
        reference.add(i % 100, 1);
    }
    std::array<float, 5> expected;
    for (size_t i = 0; i < levels.size(); ++i) {
        expected[i] = reference.quantile(levels[i]);
    }

    for (double weight : {1e-100, 1.0, 1e100, std::numeric_limits<double>::denorm_min(),
                          std::numeric_limits<double>::max()}) {
        SCOPED_TRACE(weight);
        WeightedTDigest source(100);
        for (int i = 0; i < 10000; ++i) {
            source.add(i % 100, weight);
        }
        source.quantile(0.5);
        source.add(50, weight);
        std::vector<uint8_t> bytes(source.serialized_size());
        ASSERT_EQ(bytes.size(), source.serialize(bytes.data()));
        WeightedTDigest restored(100);
        restored.unserialize(bytes.data());
        EXPECT_EQ(source.processed().size(), restored.processed().size());
        EXPECT_EQ(source.unprocessed().size(), restored.unprocessed().size());
        WeightedTDigest merged(100);
        merged.merge(&restored);
        for (size_t i = 0; i < levels.size(); ++i) {
            EXPECT_NEAR(expected[i], restored.quantile(levels[i]), 0.1);
            EXPECT_FLOAT_EQ(restored.quantile(levels[i]), merged.quantile(levels[i]));
        }
    }
}

TEST(WeightedTDigestTest, PreserveRatioWhenMergingDifferentWeightScales) {
    WeightedTDigest small(10000);
    small.add(0, 1e-100);
    small.quantile(0.5);
    WeightedTDigest large(10000);
    large.add(10, 1e100);
    large.merge(&small);
    EXPECT_FLOAT_EQ(0, large.quantile(0));
    EXPECT_FLOAT_EQ(10, large.quantile(0.5));
    EXPECT_FLOAT_EQ(10, large.quantile(1));
}

TEST_F(TDigestTest, PreserveLegacyFloatWireBytes) {
    // Construct the old wire layout independently: two unprocessed FLOAT centroids,
    // with values 0 and 10 and weights 1 and 3, must retain the exact byte format.
    std::vector<uint8_t> legacy;
    const auto append = [&](const auto& value) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(&value);
        legacy.insert(legacy.end(), bytes, bytes + sizeof(value));
    };
    append(uint32_t(52 + 16));
    append(float(100));
    append(std::numeric_limits<float>::max());
    append(std::numeric_limits<float>::lowest());
    append(size_t(200));
    append(size_t(800));
    append(float(0));
    append(float(4));
    append(uint32_t(0));
    append(uint32_t(2));
    append(float(0));
    append(float(1));
    append(float(10));
    append(float(3));
    append(uint32_t(0));

    TDigest digest(100);
    digest.add(0, 1);
    digest.add(10, 3);
    std::vector<uint8_t> serialized(digest.serialized_size());
    ASSERT_EQ(serialized.size(), digest.serialize(serialized.data()));
    EXPECT_EQ(legacy, serialized);
    TDigest restored(100);
    restored.unserialize(legacy.data());
    EXPECT_FLOAT_EQ(7.5, restored.quantile(0.5));
}

TEST_F(TDigestTest, CopyPreservesValuesWithoutSpareCapacity) {
    const auto allocated_bytes = [](const TDigest& digest) {
        return (digest._processed.capacity() + digest._unprocessed.capacity()) * sizeof(Centroid) +
               digest._cumulative.capacity() * sizeof(Weight);
    };
    TDigest source(10000);
    for (int i = 0; i < 300; ++i) {
        source.add(i);
    }
    source.compress();
    TDigest processed_copy(source);
    EXPECT_EQ(0, processed_copy.unprocessed().capacity());
    EXPECT_LT(allocated_bytes(processed_copy), 16 * 1024);
    source.add(1000);
    TDigest copy(source);
    EXPECT_EQ(301, copy.total_weight());
    EXPECT_LT(allocated_bytes(copy), 16 * 1024);
    // Continued writes may grow the copy's buffers, without changing the source.
    for (int i = 0; i < 1000; ++i) {
        copy.add(2000);
    }
    EXPECT_EQ(1301, copy.total_weight());
    EXPECT_EQ(0, copy.quantile(0));
    EXPECT_EQ(2000, copy.quantile(1));
    EXPECT_EQ(1000, source.quantile(1));
    EXPECT_EQ(299, processed_copy.quantile(1));
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
