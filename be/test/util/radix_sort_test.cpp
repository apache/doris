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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <random>

#include "util/tdigest.h"

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

    static void SetUpTestCase() {
        static bool initialized = false;
        if (!initialized) {
            FLAGS_logtostderr = true;
            google::InstallFailureSignalHandler();
            google::InitGoogleLogging("testing::RadixSortTest");
            initialized = true;
        }
    }

    // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(RadixSortTest, TestUint32Sort) {
    constexpr size_t num_values = 10000;
    std::vector<uint32_t> data;
    // generating random data
    for (size_t i = 0; i < num_values; ++i) {
        data.push_back(num_values - i);
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data.begin(), data.end(), g);
    radixSortLSD(data.data(), data.size());
    for (size_t i = 0; i < num_values; ++i) {
        data[i] = i + 1;
    }
}

TEST_F(RadixSortTest, TestInt32Sort) {
    constexpr size_t num_values = 10000;
    std::vector<int32_t> data;
    // generating random data
    for (size_t i = 0; i < num_values; ++i) {
        data.push_back(num_values - i - 5000);
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data.begin(), data.end(), g);
    radixSortLSD(data.data(), data.size());
    for (size_t i = 0; i < num_values; ++i) {
        data[i] = i + 1 - 5000;
    }
}

bool compare_float_with_epsilon(float a, float b, float E) {
    return std::abs(a - b) < E;
}

TEST_F(RadixSortTest, TestFloatSort) {
    constexpr size_t num_values = 10000;
    std::vector<float> data;
    // generating random data
    for (size_t i = 0; i < num_values; ++i) {
        data.push_back(1.0 * num_values - i - 5000 + 0.1);
    }
    float nan = std::numeric_limits<float>::quiet_NaN();
    float max = std::numeric_limits<float>::max();
    float min = std::numeric_limits<float>::lowest();
    float infinity = std::numeric_limits<float>::infinity();
    data.push_back(nan);
    data.push_back(max);
    data.push_back(min);
    data.push_back(infinity);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data.begin(), data.end(), g);
    radixSortLSD(data.data(), data.size());
    for (size_t i = 0; i < num_values + 4; ++i) {
        if (i == 0) {
            ASSERT_TRUE(compare_float_with_epsilon(data[i], min, 0.0000001));
        } else if (i == num_values + 1) {
            ASSERT_TRUE(compare_float_with_epsilon(data[i], max, 0.0000001));
        } else if (i == num_values + 2) {
            ASSERT_TRUE(std::isinf(data[i]));
        } else if (i == num_values + 3) {
            ASSERT_TRUE(std::isnan(data[i]));
        } else {
            ASSERT_TRUE(compare_float_with_epsilon(data[i], 1.0 * i - 5000 + 0.1, 0.0000001));
        }
    }
}

bool compare_double_with_epsilon(double a, double b, double E) {
    return std::abs(a - b) < E;
}

TEST_F(RadixSortTest, TestDoubleSort) {
    constexpr size_t num_values = 10000;
    std::vector<double> data;
    // generating random data
    for (size_t i = 0; i < num_values; ++i) {
        data.push_back(num_values * 1.0 - i - 5000 + 0.1);
    }
    double nan = std::numeric_limits<double>::quiet_NaN();
    double max = std::numeric_limits<double>::max();
    double min = std::numeric_limits<double>::lowest();
    double infinity = std::numeric_limits<double>::infinity();
    data.push_back(nan);
    data.push_back(max);
    data.push_back(min);
    data.push_back(infinity);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data.begin(), data.end(), g);
    radixSortLSD(data.data(), data.size());
    for (size_t i = 0; i < num_values + 4; ++i) {
        if (i == 0) {
            ASSERT_TRUE(compare_double_with_epsilon(data[i], min, 0.0000001));
        } else if (i == num_values + 1) {
            ASSERT_TRUE(compare_double_with_epsilon(data[i], max, 0.0000001));
        } else if (i == num_values + 2) {
            ASSERT_TRUE(std::isinf(data[i]));
        } else if (i == num_values + 3) {
            ASSERT_TRUE(std::isnan(data[i]));
        } else {
            double tmp = 1.0 * i - 5000 + 0.1;
            ASSERT_TRUE(compare_double_with_epsilon(data[i], tmp, 0.0000001));
        }
    }
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
            ASSERT_TRUE(compare_float_with_epsilon(data[i].d1, min, 0.0000001));
        } else if (i == num_values + 1) {
            ASSERT_TRUE(compare_float_with_epsilon(data[i].d1, max, 0.0000001));
        } else if (i == num_values + 2) {
            ASSERT_TRUE(std::isinf(data[i].d1));
        } else if (i == num_values + 3) {
            ASSERT_TRUE(std::isnan(data[i].d1));
        } else {
            float tmp = 1.0 * i - 5000 + 0.1;
            ASSERT_TRUE(compare_float_with_epsilon(data[i].d1, tmp, 0.0000001));
        }
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
