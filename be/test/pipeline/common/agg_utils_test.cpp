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

#include "pipeline/common/agg_utils.h"

#include <gtest/gtest.h>

#include <cstdint>

#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::pipeline {

class AggregatedDataVariantsTest : public testing::Test {
protected:
    void SetUp() override { _variants = std::make_unique<AggregatedDataVariants>(); }

    std::unique_ptr<AggregatedDataVariants> _variants;
};

class AggregateDataContainerTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(AggregateDataContainerTest, BasicConstruction) {
    // Test basic construction with different sizes
    AggregateDataContainer container(sizeof(uint32_t), sizeof(double));
    EXPECT_EQ(0, container.total_count());
    EXPECT_EQ(0, container.memory_usage());
}

TEST_F(AggregateDataContainerTest, AppendDataBasic) {
    AggregateDataContainer container(sizeof(uint32_t), sizeof(double));

    // Test single append
    uint32_t key1 = 42;
    auto* data1 = container.append_data(key1);
    EXPECT_NE(nullptr, data1);
    EXPECT_EQ(1, container.total_count());

    // Test multiple appends
    uint32_t key2 = 43;
    auto* data2 = container.append_data(key2);
    EXPECT_NE(nullptr, data2);
    EXPECT_NE(data1, data2);
    EXPECT_EQ(2, container.total_count());
}

TEST_F(AggregateDataContainerTest, IteratorOperations) {
    AggregateDataContainer container(sizeof(uint32_t), sizeof(double));

    // Add some test data
    std::vector<uint32_t> test_keys = {1, 2, 3, 4, 5};
    std::vector<vectorized::AggregateDataPtr> test_data;

    for (auto key : test_keys) {
        test_data.push_back(container.append_data(key));
    }

    // Test iterator traversal
    auto it = container.begin();
    auto const_it = container.cbegin();

    for (size_t i = 0; i < test_keys.size(); ++i) {
        EXPECT_EQ(test_keys[i], it.get_key<uint32_t>());
        EXPECT_EQ(test_keys[i], const_it.get_key<uint32_t>());
        EXPECT_EQ(test_data[i], it.get_aggregate_data());
        ++it;
        ++const_it;
    }

    EXPECT_EQ(container.end(), it);
    EXPECT_EQ(container.cend(), const_it);
}

TEST_F(AggregateDataContainerTest, SubContainerExpansion) {
    AggregateDataContainer container(sizeof(uint32_t), sizeof(double));

    // Add enough elements to force container expansion
    const uint32_t SUB_CONTAINER_CAPACITY = 8192;
    const uint32_t TEST_SIZE = SUB_CONTAINER_CAPACITY + 100;

    for (uint32_t i = 0; i < TEST_SIZE; ++i) {
        auto* data = container.append_data(i);
        EXPECT_NE(nullptr, data);
    }

    EXPECT_EQ(TEST_SIZE, container.total_count());

    // Verify all elements are accessible
    auto it = container.begin();
    for (uint32_t i = 0; i < TEST_SIZE; ++i) {
        EXPECT_EQ(i, it.get_key<uint32_t>());
        ++it;
    }
}

TEST_F(AggregateDataContainerTest, EstimateMemory) {
    AggregateDataContainer container(sizeof(uint32_t), sizeof(double));

    // Test empty container
    EXPECT_GT(container.estimate_memory(100), 0);

    // Test partially filled container
    uint32_t key = 1;
    container.append_data(key);

    size_t estimate = container.estimate_memory(8192);
    EXPECT_GT(estimate, 0);

    // Test estimate with expansion needed
    estimate = container.estimate_memory(10000);
    EXPECT_GT(estimate, 0);
}

TEST_F(AggregateDataContainerTest, InitOnce) {
    AggregateDataContainer container(sizeof(uint32_t), sizeof(double));

    // Test initial state
    container.init_once();
    auto it1 = container.iterator;
    EXPECT_EQ(container.begin(), it1);

    // Test that subsequent calls don't change iterator
    container.init_once();
    auto it2 = container.iterator;
    EXPECT_EQ(it1, it2);
}

TEST_F(AggregateDataContainerTest, MemoryManagement) {
    AggregateDataContainer container(sizeof(uint32_t), sizeof(double));

    // Record initial memory usage
    int64_t initial_memory = container.memory_usage();

    // Add data and verify memory increases
    for (uint32_t i = 0; i < 1000; ++i) {
        container.append_data(i);
    }

    EXPECT_GT(container.memory_usage(), initial_memory);
}

TEST_F(AggregateDataContainerTest, EdgeCases) {
    AggregateDataContainer container(sizeof(uint32_t), sizeof(double));

    // Test empty container iterators
    EXPECT_EQ(container.begin(), container.end());
    EXPECT_EQ(container.cbegin(), container.cend());

    // Test single element
    uint32_t key = 42;
    container.append_data(key);

    auto it = container.begin();
    EXPECT_EQ(key, it.get_key<uint32_t>());
    ++it;
    EXPECT_EQ(container.end(), it);
}

TEST_F(AggregateDataContainerTest, LargeData) {
    AggregateDataContainer container(sizeof(uint32_t), sizeof(double));

    const auto count = AggregateDataContainer::SUB_CONTAINER_CAPACITY * 2 + 1;

    for (uint32_t i = 0; i < count; ++i) {
        auto* ptr = container.append_data(i);
        double value = i;
        memcpy(ptr, &value, sizeof(double));
    }

    auto it = container.begin();
    uint32_t expected_key = 0;
    while (it != container.end()) {
        EXPECT_EQ(expected_key, it.get_key<uint32_t>());
        auto value = *reinterpret_cast<double*>(it.get_aggregate_data());
        EXPECT_EQ(static_cast<double>(expected_key), value);
        ++it;
        ++expected_key;
    }
    EXPECT_EQ(container.end(), it);
}

TEST_F(AggregatedDataVariantsTest, TestWithoutKey) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeInt32>()};
    _variants->init(types, HashKeyType::without_key);
    ASSERT_TRUE(std::holds_alternative<std::monostate>(_variants->method_variant));
}

TEST_F(AggregatedDataVariantsTest, TestSerializedKey) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeString>()};
    _variants->init(types, HashKeyType::serialized);
    ASSERT_TRUE(std::holds_alternative<vectorized::MethodSerialized<AggregatedDataWithStringKey>>(
            _variants->method_variant));
}

TEST_F(AggregatedDataVariantsTest, TestStringKey) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeString>()};

    // Test string key
    _variants->init(types, HashKeyType::string_key);
    auto value = std::holds_alternative<
            vectorized::MethodStringNoCache<AggregatedDataWithShortStringKey>>(
            _variants->method_variant);
    ASSERT_TRUE(value);
}

TEST_F(AggregatedDataVariantsTest, TestNumericKeys) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeInt32>()};

    // Test int8 key
    _variants->init(types, HashKeyType::int8_key);
    auto value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt8, AggData<vectorized::UInt8>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int16 key
    _variants->init(types, HashKeyType::int16_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt16, AggData<vectorized::UInt16>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int32 key
    _variants->init(types, HashKeyType::int32_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt32, AggData<vectorized::UInt32>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int32 key phase2
    _variants->init(types, HashKeyType::int32_key_phase2);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt32, AggregatedDataWithUInt32KeyPhase2>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int64 key
    _variants->init(types, HashKeyType::int64_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt64, AggData<vectorized::UInt64>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int64 key phase2
    _variants->init(types, HashKeyType::int64_key_phase2);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt64, AggregatedDataWithUInt64KeyPhase2>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int128 key
    _variants->init(types, HashKeyType::int128_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt128, AggData<vectorized::UInt128>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int256 key
    _variants->init(types, HashKeyType::int256_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt256, AggData<vectorized::UInt256>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);
}

TEST_F(AggregatedDataVariantsTest, TestNullableKeys) {
    auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(
            std::make_shared<vectorized::DataTypeInt32>());
    std::vector<vectorized::DataTypePtr> types {nullable_type};

    // Test nullable int32
    _variants->init(types, HashKeyType::int32_key);
    auto value = std::holds_alternative<vectorized::MethodSingleNullableColumn<
            vectorized::MethodOneNumber<vectorized::UInt32, AggDataNullable<vectorized::UInt32>>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test nullable string
    _variants->init(types, HashKeyType::string_key);
    value = std::holds_alternative<vectorized::MethodSingleNullableColumn<
            vectorized::MethodStringNoCache<AggregatedDataWithNullableShortStringKey>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);
}

TEST_F(AggregatedDataVariantsTest, TestFixedKeys) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeInt32>(),
                                                std::make_shared<vectorized::DataTypeInt32>()};

    // Test fixed64
    _variants->init(types, HashKeyType::fixed64);
    ASSERT_TRUE(std::holds_alternative<vectorized::MethodKeysFixed<AggData<vectorized::UInt64>>>(
            _variants->method_variant));

    // Test fixed128
    _variants->init(types, HashKeyType::fixed128);
    ASSERT_TRUE(std::holds_alternative<vectorized::MethodKeysFixed<AggData<vectorized::UInt128>>>(
            _variants->method_variant));

    // Test fixed136
    _variants->init(types, HashKeyType::fixed136);
    ASSERT_TRUE(std::holds_alternative<vectorized::MethodKeysFixed<AggData<vectorized::UInt136>>>(
            _variants->method_variant));

    // Test fixed256
    _variants->init(types, HashKeyType::fixed256);
    ASSERT_TRUE(std::holds_alternative<vectorized::MethodKeysFixed<AggData<vectorized::UInt256>>>(
            _variants->method_variant));
}

TEST_F(AggregatedDataVariantsTest, TestInvalidKeyType) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeInt32>()};
    ASSERT_THROW(_variants->init(types, static_cast<HashKeyType>(-1)), Exception);
}
} // namespace doris::pipeline
