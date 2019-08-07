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

#include <random>
#include <gtest/gtest.h>
#include "util/tdigest.h"

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

    static void SetUpTestCase() {
        static bool initialized = false;
        if (!initialized) {
            FLAGS_logtostderr = true;
            google::InstallFailureSignalHandler();
            google::InitGoogleLogging("testing::TDigestTest");
            initialized = true;
        }
    }

    // Objects declared here can be used by all tests in the test case for Foo.
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
            q1 = values[intIndex + 1] * (index - intIndex) + values[intIndex] * (intIndex + 1 - index);
        }
    }
    return q1;
}

TEST_F(TDigestTest, CrashAfterMerge) {
    TDigest digest(1000);
    std::uniform_real_distribution<> reals(0.0, 1.0);
    std::random_device gen;
    for (int i = 0; i < 100000; i++) {
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

    const auto length = 10;//dist(gen);

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

    std::vector<double> testValues{0.0, 1.0e-10, qvalue(gen), 0.5, 1.0 - 1e-10, 1.0};
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
    EXPECT_EQ(1000 + 10L * (1 << 28), digest.totalWeight());
    EXPECT_GT(digest.totalWeight(), std::numeric_limits<int32_t>::max());
    std::vector<double> quantiles{0, 0.1, 0.5, 0.9, 1, reals(gen)};
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

    digest2.add(std::vector<const TDigest *> {&digest1});
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
    std::vector<double> values{5.0, 10.0, 15.0, 20.0, 30.0, 35.0, 40.0, 45.0, 50.0};
    std::vector<double> quantiles{1.5 / 9.0, 3.5 / 9.0, 6.5 / 9.0};
    for (auto q : quantiles) {
        EXPECT_NEAR(quantile(q, values), digest.quantile(q), 0.01) << "q = " << q;
    }
}

TEST_F(TDigestTest, Montonicity) {
    TDigest digest(1000);
    std::uniform_real_distribution<> reals(0.0, 1.0);
    std::random_device gen;
    for (int i = 0; i < 100000; i++) {
        digest.add(reals(gen));
    }

    double lastQuantile = -1;
    double lastX = -1;
    for (double z = 0; z <= 1; z += 1e-5) {
        double x = digest.quantile(z);
        EXPECT_GE(x, lastX);
        lastX = x;

        double q = digest.cdf(z);
        EXPECT_GE(q, lastQuantile);
        lastQuantile = q;
    }
}

}  // namespace stesting

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
