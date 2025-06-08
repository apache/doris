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

#include "pipeline/common/distinct_agg_utils.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "vec/common/uint128.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris {

class DistinctAggUtilsTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    template <typename T, typename TT>
    void test_function(const vectorized::DataTypePtr& data_type, HashKeyType key_type);
};

// Test DistinctHashSetType template specializations
TEST_F(DistinctAggUtilsTest, TestDistinctHashSetType) {
    // Test default template
    static_assert(std::is_same_v<typename DistinctHashSetType<vectorized::UInt32>::HashSet,
                                 PHHashSet<vectorized::UInt32, HashCRC32<vectorized::UInt32>>>);

    // Test UInt8 specialization
    static_assert(std::is_same_v<typename DistinctHashSetType<vectorized::UInt8>::HashSet,
                                 SmallFixedSizeHashSet<vectorized::UInt8>>);

    // Test Int8 specialization
    static_assert(std::is_same_v<typename DistinctHashSetType<vectorized::Int8>::HashSet,
                                 SmallFixedSizeHashSet<vectorized::Int8>>);
}

// Test DistinctPhase2HashSetType template specializations
TEST_F(DistinctAggUtilsTest, TestDistinctPhase2HashSetType) {
    // Test default template
    static_assert(
            std::is_same_v<typename DistinctPhase2HashSetType<vectorized::UInt32>::HashSet,
                           PHHashSet<vectorized::UInt32, HashMixWrapper<vectorized::UInt32>>>);

    // Test UInt8 specialization
    static_assert(std::is_same_v<typename DistinctPhase2HashSetType<vectorized::UInt8>::HashSet,
                                 SmallFixedSizeHashSet<vectorized::UInt8>>);

    // Test Int8 specialization
    static_assert(std::is_same_v<typename DistinctPhase2HashSetType<vectorized::Int8>::HashSet,
                                 SmallFixedSizeHashSet<vectorized::Int8>>);
}

TEST_F(DistinctAggUtilsTest, TestDistinctDataVariantsInitSerialized) {
    DistinctDataVariants variants;
    std::vector<vectorized::DataTypePtr> data_types = {
            std::make_shared<vectorized::DataTypeString>()};

    ASSERT_NO_THROW(variants.init(data_types, HashKeyType::serialized));
    ASSERT_TRUE(std::holds_alternative<vectorized::MethodSerialized<DistinctDataWithStringKey>>(
            variants.method_variant));
}

TEST_F(DistinctAggUtilsTest, TestDistinctDataVariantsInitString) {
    {
        DistinctDataVariants variants;
        auto data_type = std::make_shared<vectorized::DataTypeString>();

        ASSERT_NO_THROW(variants.init({data_type}, HashKeyType::string_key));
        ASSERT_TRUE(std::holds_alternative<
                    vectorized::MethodStringNoCache<DistinctDataWithShortStringKey>>(
                variants.method_variant));
    }

    {
        DistinctDataVariants variants;
        auto data_type = std::make_shared<vectorized::DataTypeString>();

        ASSERT_NO_THROW(variants.init({make_nullable(data_type)}, HashKeyType::string_key));
        ASSERT_TRUE(std::holds_alternative<
                    vectorized::MethodSingleNullableColumn<vectorized::MethodStringNoCache<
                            vectorized::DataWithNullKey<DistinctDataWithShortStringKey>>>>(
                variants.method_variant));
    }
}

template <typename T, typename TT>
void DistinctAggUtilsTest::test_function(const vectorized::DataTypePtr& data_type,
                                         HashKeyType key_type) {
    {
        DistinctDataVariants variants;
        variants.init({data_type}, key_type);
        // Check if the method_variant is of the expected type
        auto value =
                std::holds_alternative<vectorized::MethodOneNumber<T, TT>>(variants.method_variant);
        ASSERT_TRUE(value) << "key type: " << static_cast<int32_t>(key_type) << " test failed";
    }

    {
        DistinctDataVariants variants;
        variants.init({vectorized::make_nullable(data_type)}, key_type);
        // Check if the method_variant is of the expected type
        auto value = std::holds_alternative<vectorized::MethodSingleNullableColumn<
                vectorized::MethodOneNumber<T, vectorized::DataWithNullKey<TT>>>>(
                variants.method_variant);
        ASSERT_TRUE(value) << "key type: " << static_cast<int32_t>(key_type) << " test failed";
    }
}

// Test DistinctDataVariants::init() with different data types and hash key types
TEST_F(DistinctAggUtilsTest, TestDistinctDataVariantsInitNumerics) {
    test_function<vectorized::UInt8, DistinctData<vectorized::UInt8>>(
            std::make_shared<vectorized::DataTypeUInt8>(), HashKeyType::int8_key);
    test_function<vectorized::UInt16, DistinctData<vectorized::UInt16>>(
            std::make_shared<vectorized::DataTypeInt16>(), HashKeyType::int16_key);
    test_function<vectorized::UInt32, DistinctData<vectorized::UInt32>>(
            std::make_shared<vectorized::DataTypeInt32>(), HashKeyType::int32_key);
    test_function<vectorized::UInt32, DistinctDataPhase2<vectorized::UInt32>>(
            std::make_shared<vectorized::DataTypeInt32>(), HashKeyType::int32_key_phase2);
    test_function<vectorized::UInt64, DistinctData<vectorized::UInt64>>(
            std::make_shared<vectorized::DataTypeInt64>(), HashKeyType::int64_key);
    test_function<vectorized::UInt64, DistinctDataPhase2<vectorized::UInt64>>(
            std::make_shared<vectorized::DataTypeInt64>(), HashKeyType::int64_key_phase2);
    test_function<vectorized::UInt128, DistinctData<vectorized::UInt128>>(
            std::make_shared<vectorized::DataTypeInt128>(), HashKeyType::int128_key);
    test_function<vectorized::UInt256, DistinctData<vectorized::UInt256>>(
            std::make_shared<vectorized::DataTypeDecimal256>(), HashKeyType::int256_key);
}

TEST_F(DistinctAggUtilsTest, TestDistinctDataVariantsInitFixedKeys) {
    auto test_block = [](const std::vector<vectorized::DataTypePtr>& data_types,
                         HashKeyType key_type) {
        auto key_type_ = get_hash_key_type_fixed(data_types);
        ASSERT_EQ(key_type, key_type_) << "key type mismatch: " << static_cast<int32_t>(key_type)
                                       << " vs " << static_cast<int32_t>(key_type_);
        DistinctDataVariants variants;
        ASSERT_NO_THROW(variants.init(data_types, key_type_));

        switch (key_type) {
        case HashKeyType::fixed64:
            ASSERT_TRUE(std::holds_alternative<
                        vectorized::MethodKeysFixed<DistinctData<vectorized::UInt64>>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed128:
            ASSERT_TRUE(std::holds_alternative<
                        vectorized::MethodKeysFixed<DistinctData<vectorized::UInt128>>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed136:
            ASSERT_TRUE(std::holds_alternative<
                        vectorized::MethodKeysFixed<DistinctData<vectorized::UInt136>>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed256:
            ASSERT_TRUE(std::holds_alternative<
                        vectorized::MethodKeysFixed<DistinctData<vectorized::UInt256>>>(
                    variants.method_variant));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "AggregatedDataVariants meet invalid key type, type={}", key_type);
        }
    };

    test_block({std::make_shared<vectorized::DataTypeUInt8>(),
                std::make_shared<vectorized::DataTypeUInt8>()},
               HashKeyType::fixed64);

    test_block({std::make_shared<vectorized::DataTypeInt32>(),
                std::make_shared<vectorized::DataTypeInt32>()},
               HashKeyType::fixed64);

    test_block({std::make_shared<vectorized::DataTypeInt64>(),
                std::make_shared<vectorized::DataTypeUInt8>()},
               HashKeyType::fixed128);

    test_block({std::make_shared<vectorized::DataTypeInt64>(),
                std::make_shared<vectorized::DataTypeInt64>()},
               HashKeyType::fixed128);

    test_block({std::make_shared<vectorized::DataTypeInt128>(),
                std::make_shared<vectorized::DataTypeUInt8>()},
               HashKeyType::fixed136);

    test_block({std::make_shared<vectorized::DataTypeInt128>(),
                std::make_shared<vectorized::DataTypeInt128>()},
               HashKeyType::fixed256);
}

// Test error handling for invalid hash key type
TEST_F(DistinctAggUtilsTest, TestInvalidHashKeyType) {
    DistinctDataVariants variants;
    std::vector<vectorized::DataTypePtr> data_types = {
            std::make_shared<vectorized::DataTypeString>()};

    ASSERT_THROW({ variants.init(data_types, static_cast<HashKeyType>(-1)); }, Exception);
}

} // namespace doris