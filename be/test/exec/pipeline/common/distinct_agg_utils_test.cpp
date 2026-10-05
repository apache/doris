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

#include "exec/common/distinct_agg_utils.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/uint128.h"

namespace doris {

class DistinctAggUtilsTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    template <typename T, typename TT>
    void test_function(const DataTypePtr& data_type, HashKeyType key_type);
};

// Test DistinctHashSetType template specializations
TEST_F(DistinctAggUtilsTest, TestDistinctHashSetType) {
    // Test default template
    static_assert(std::is_same_v<typename DistinctHashSetType<UInt32>::HashSet,
                                 PHHashSet<UInt32, HashCRC32<UInt32>>>);

    // Test UInt8 specialization
    static_assert(std::is_same_v<typename DistinctHashSetType<UInt8>::HashSet,
                                 SmallFixedSizeHashSet<UInt8>>);

    // Test Int8 specialization
    static_assert(std::is_same_v<typename DistinctHashSetType<Int8>::HashSet,
                                 SmallFixedSizeHashSet<Int8>>);
}

// Test DistinctPhase2HashSetType template specializations
TEST_F(DistinctAggUtilsTest, TestDistinctPhase2HashSetType) {
    // Test default template
    static_assert(std::is_same_v<typename DistinctPhase2HashSetType<UInt32>::HashSet,
                                 PHHashSet<UInt32, HashMixWrapper<UInt32>>>);

    // Test UInt8 specialization
    static_assert(std::is_same_v<typename DistinctPhase2HashSetType<UInt8>::HashSet,
                                 SmallFixedSizeHashSet<UInt8>>);

    // Test Int8 specialization
    static_assert(std::is_same_v<typename DistinctPhase2HashSetType<Int8>::HashSet,
                                 SmallFixedSizeHashSet<Int8>>);
}

TEST_F(DistinctAggUtilsTest, TestDistinctDataVariantsInitSerialized) {
    DistinctDataVariants variants;
    std::vector<DataTypePtr> data_types = {std::make_shared<DataTypeString>()};

    ASSERT_NO_THROW(variants.init(data_types, HashKeyType::serialized));
    ASSERT_TRUE(std::holds_alternative<MethodSerialized<DistinctDataWithStringKey>>(
            variants.method_variant));
}

TEST_F(DistinctAggUtilsTest, TestDistinctDataVariantsInitString) {
    {
        DistinctDataVariants variants;
        auto data_type = std::make_shared<DataTypeString>();

        ASSERT_NO_THROW(variants.init({data_type}, HashKeyType::string_key));
        ASSERT_TRUE(std::holds_alternative<MethodStringNoCache<DistinctDataWithShortStringKey>>(
                variants.method_variant));
    }

    {
        DistinctDataVariants variants;
        auto data_type = std::make_shared<DataTypeString>();

        ASSERT_NO_THROW(variants.init({make_nullable(data_type)}, HashKeyType::string_key));
        ASSERT_TRUE(std::holds_alternative<MethodSingleNullableColumn<
                            MethodStringNoCache<DataWithNullKey<DistinctDataWithShortStringKey>>>>(
                variants.method_variant));
    }
}

template <typename T, typename TT>
void DistinctAggUtilsTest::test_function(const DataTypePtr& data_type, HashKeyType key_type) {
    {
        DistinctDataVariants variants;
        variants.init({data_type}, key_type);
        // Check if the method_variant is of the expected type
        auto value = std::holds_alternative<MethodOneNumber<T, TT>>(variants.method_variant);
        ASSERT_TRUE(value) << "key type: " << static_cast<int32_t>(key_type) << " test failed";
    }

    {
        DistinctDataVariants variants;
        variants.init({make_nullable(data_type)}, key_type);
        // Check if the method_variant is of the expected type
        auto value = std::holds_alternative<
                MethodSingleNullableColumn<MethodOneNumber<T, DataWithNullKey<TT>>>>(
                variants.method_variant);
        ASSERT_TRUE(value) << "key type: " << static_cast<int32_t>(key_type) << " test failed";
    }
}

// Test DistinctDataVariants::init() with different data types and hash key types
TEST_F(DistinctAggUtilsTest, TestDistinctDataVariantsInitNumerics) {
    test_function<UInt8, DistinctData<UInt8>>(std::make_shared<DataTypeUInt8>(),
                                              HashKeyType::int8_key);
    test_function<UInt16, DistinctData<UInt16>>(std::make_shared<DataTypeInt16>(),
                                                HashKeyType::int16_key);
    test_function<UInt32, DistinctData<UInt32>>(std::make_shared<DataTypeInt32>(),
                                                HashKeyType::int32_key);
    test_function<UInt32, DistinctDataPhase2<UInt32>>(std::make_shared<DataTypeInt32>(),
                                                      HashKeyType::int32_key_phase2);
    test_function<UInt64, DistinctData<UInt64>>(std::make_shared<DataTypeInt64>(),
                                                HashKeyType::int64_key);
    test_function<UInt64, DistinctDataPhase2<UInt64>>(std::make_shared<DataTypeInt64>(),
                                                      HashKeyType::int64_key_phase2);
    test_function<UInt128, DistinctData<UInt128>>(std::make_shared<DataTypeInt128>(),
                                                  HashKeyType::int128_key);
    test_function<UInt256, DistinctData<UInt256>>(std::make_shared<DataTypeDecimal256>(),
                                                  HashKeyType::int256_key);
}

TEST_F(DistinctAggUtilsTest, TestDistinctDataVariantsInitFixedKeys) {
    auto test_block = [](const std::vector<DataTypePtr>& data_types, HashKeyType key_type) {
        auto key_type_ = get_hash_key_type_fixed(data_types);
        ASSERT_EQ(key_type, key_type_) << "key type mismatch: " << static_cast<int32_t>(key_type)
                                       << " vs " << static_cast<int32_t>(key_type_);
        DistinctDataVariants variants;
        ASSERT_NO_THROW(variants.init(data_types, key_type_));

        switch (key_type) {
        case HashKeyType::fixed64:
            ASSERT_TRUE(std::holds_alternative<MethodKeysFixed<DistinctData<UInt64>>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed72:
            ASSERT_TRUE(std::holds_alternative<MethodKeysFixed<DistinctData<UInt72>>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed96:
            ASSERT_TRUE(std::holds_alternative<MethodKeysFixed<DistinctData<UInt96>>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed104:
            ASSERT_TRUE(std::holds_alternative<MethodKeysFixed<DistinctData<UInt104>>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed128:
            ASSERT_TRUE(std::holds_alternative<MethodKeysFixed<DistinctData<UInt128>>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed136:
            ASSERT_TRUE(std::holds_alternative<MethodKeysFixed<DistinctData<UInt136>>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed256:
            ASSERT_TRUE(std::holds_alternative<MethodKeysFixed<DistinctData<UInt256>>>(
                    variants.method_variant));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "AggregatedDataVariants meet invalid key type, type={}", key_type);
        }
    };

    test_block({std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeUInt8>()},
               HashKeyType::fixed64);

    test_block({std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()},
               HashKeyType::fixed64);

    test_block({std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeUInt8>()},
               HashKeyType::fixed72);

    test_block({std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()},
               HashKeyType::fixed128);

    test_block({std::make_shared<DataTypeInt128>(), std::make_shared<DataTypeUInt8>()},
               HashKeyType::fixed136);

    test_block({std::make_shared<DataTypeInt128>(), std::make_shared<DataTypeInt128>()},
               HashKeyType::fixed256);
}

// Test error handling for invalid hash key type
TEST_F(DistinctAggUtilsTest, TestInvalidHashKeyType) {
    DistinctDataVariants variants;
    std::vector<DataTypePtr> data_types = {std::make_shared<DataTypeString>()};

    ASSERT_THROW({ variants.init(data_types, static_cast<HashKeyType>(-1)); }, Exception);
}

} // namespace doris