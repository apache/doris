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

#include "util/radix_sort.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath> // IWYU pragma: keep
#include <cmath>
#include <cstdlib>
#include <limits>
#include <random>
#include <vector>

#include "gtest/gtest_pred_impl.h"

namespace doris {

class RadixSortTest : public ::testing::Test {
protected:
    // You can remove any or all of the following functions if its body
    // is empty.
    RadixSortTest() {
        // You can do set-up work for each test here.
    }

    virtual ~RadixSortTest() {
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

bool compare_float_with_epsilon(float a, float b, float E) {
    return std::abs(a - b) < E;
}

bool compare_double_with_epsilon(double a, double b, double E) {
    return std::abs(a - b) < E;
}

struct TestObject {
    float d1;
    float d2;
};

struct RadixSortTestTraits {
    using Element = TestObject;
    using Key = float;
    using CountType = uint32_t;
    using KeyBits = uint32_t;

    static constexpr size_t PART_SIZE_BITS = 8;

    using Transform = RadixSortFloatTransform<KeyBits>;
    using Allocator = RadixSortMallocAllocator;

    static Key& extractKey(Element& elem) { return elem.d1; }
};

TEST_F(RadixSortTest, TestObjectSort) {
    constexpr size_t num_values = 10000;
    std::vector<TestObject> data;
    data.resize(10004);
    // generating random data
    for (size_t i = 0; i < num_values; ++i) {
        data[i].d1 = 1.0 * num_values - i - 5000 + 0.1;
    }
    float nan = std::numeric_limits<float>::quiet_NaN();
    float max = std::numeric_limits<float>::max();
    float min = std::numeric_limits<float>::lowest();
    float infinity = std::numeric_limits<float>::infinity();
    data[num_values].d1 = nan;
    data[num_values + 1].d1 = max;
    data[num_values + 2].d1 = min;
    data[num_values + 3].d1 = infinity;
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data.begin(), data.end(), g);
    RadixSort<RadixSortTestTraits>::executeLSD(data.data(), data.size());
    for (size_t i = 0; i < num_values + 4; ++i) {
        if (i == 0) {
            EXPECT_TRUE(compare_float_with_epsilon(data[i].d1, min, 0.0000001));
        } else if (i == num_values + 1) {
            EXPECT_TRUE(compare_float_with_epsilon(data[i].d1, max, 0.0000001));
        } else if (i == num_values + 2) {
            EXPECT_TRUE(std::isinf(data[i].d1));
        } else if (i == num_values + 3) {
            EXPECT_TRUE(std::isnan(data[i].d1));
        } else {
            float tmp = 1.0 * i - 5000 + 0.1;
            EXPECT_TRUE(compare_float_with_epsilon(data[i].d1, tmp, 0.0000001));
        }
    }
}

} // namespace doris
