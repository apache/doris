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

#include "pipeline/common/set_utils.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "vec/common/uint128.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris {

class SetUtilsTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    template <typename T, typename TT>
    void test_function(const vectorized::DataTypePtr& data_type, HashKeyType key_type);
};

TEST_F(SetUtilsTest, TestSetDataVariantsInitSerialized) {
    SetDataVariants variants;
    std::vector<vectorized::DataTypePtr> data_types = {
            std::make_shared<vectorized::DataTypeString>()};

    ASSERT_NO_THROW(variants.init(data_types, HashKeyType::serialized));
    ASSERT_TRUE(std::holds_alternative<SetSerializedHashTableContext>(variants.method_variant));
}

TEST_F(SetUtilsTest, TestSetDataVariantsInitString) {
    SetDataVariants variants;
    auto data_type = std::make_shared<vectorized::DataTypeString>();

    ASSERT_NO_THROW(variants.init({data_type}, HashKeyType::string_key));
    ASSERT_TRUE(std::holds_alternative<SetMethodOneString>(variants.method_variant));
}

template <typename T, typename TT>
void SetUtilsTest::test_function(const vectorized::DataTypePtr& data_type, HashKeyType key_type) {
    SetDataVariants variants;
    variants.init({data_type}, key_type);
    // Check if the method_variant is of the expected type
    auto value =
            std::holds_alternative<vectorized::MethodOneNumber<T, TT>>(variants.method_variant);
    ASSERT_TRUE(value) << "key type: " << static_cast<int32_t>(key_type) << " test failed";
}

// Test SetHashTableVariants::init() with different data types and hash key types
TEST_F(SetUtilsTest, TestSetDataVariantsInitNumerics) {
    test_function<vectorized::UInt8, SetData<vectorized::UInt8>>(
            std::make_shared<vectorized::DataTypeUInt8>(), HashKeyType::int8_key);
    test_function<vectorized::UInt16, SetData<vectorized::UInt16>>(
            std::make_shared<vectorized::DataTypeInt16>(), HashKeyType::int16_key);
    test_function<vectorized::UInt32, SetData<vectorized::UInt32>>(
            std::make_shared<vectorized::DataTypeInt32>(), HashKeyType::int32_key);
    test_function<vectorized::UInt64, SetData<vectorized::UInt64>>(
            std::make_shared<vectorized::DataTypeInt64>(), HashKeyType::int64_key);
    test_function<vectorized::UInt128, SetData<vectorized::UInt128>>(
            std::make_shared<vectorized::DataTypeInt128>(), HashKeyType::int128_key);
    test_function<vectorized::UInt256, SetData<vectorized::UInt256>>(
            std::make_shared<vectorized::DataTypeDecimal256>(), HashKeyType::int256_key);
}

TEST_F(SetUtilsTest, TestSetDataVariantsInitFixedKeys) {
    auto test_block = [](const std::vector<vectorized::DataTypePtr>& data_types,
                         HashKeyType key_type) {
        auto key_type_ = get_hash_key_type_fixed(data_types);
        ASSERT_EQ(key_type, key_type_) << "key type mismatch: " << static_cast<int32_t>(key_type)
                                       << " vs " << static_cast<int32_t>(key_type_);
        SetDataVariants variants;
        ASSERT_NO_THROW(variants.init(data_types, key_type_));

        switch (key_type) {
        case HashKeyType::fixed64:
            ASSERT_TRUE(std::holds_alternative<SetFixedKeyHashTableContext<vectorized::UInt64>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed128:
            ASSERT_TRUE(std::holds_alternative<SetFixedKeyHashTableContext<vectorized::UInt128>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed136:
            ASSERT_TRUE(std::holds_alternative<SetFixedKeyHashTableContext<vectorized::UInt136>>(
                    variants.method_variant));
            break;
        case HashKeyType::fixed256:
            ASSERT_TRUE(std::holds_alternative<SetFixedKeyHashTableContext<vectorized::UInt256>>(
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
TEST_F(SetUtilsTest, TestInvalidHashKeyType) {
    SetDataVariants variants;
    std::vector<vectorized::DataTypePtr> data_types = {
            std::make_shared<vectorized::DataTypeString>()};

    ASSERT_THROW({ variants.init(data_types, static_cast<HashKeyType>(-1)); }, Exception);
}

} // namespace doris