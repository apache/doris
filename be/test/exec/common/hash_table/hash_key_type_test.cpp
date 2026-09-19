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

#include "exec/common/hash_table/hash_key_type.h"

#include <gtest/gtest.h>

#include <memory>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"

namespace doris {

TEST(HashKeyTypeTest, BinaryKeysUsePayloadReferences) {
    auto type = std::make_shared<DataTypeVarbinary>();
    EXPECT_EQ(HashKeyType::string_key, get_hash_key_type({type}));
    EXPECT_EQ(HashKeyType::string_key, get_hash_key_type({make_nullable(type)}));
    EXPECT_EQ(HashKeyType::serialized,
              get_hash_key_type({type, std::make_shared<DataTypeInt32>()}));
    auto column = type->create_column();
    const std::vector<std::string> values = {"", std::string("\0", 1), std::string("a\0", 2),
                                             std::string(64, '\xff')};
    for (const auto& value : values) {
        column->insert_data(value.data(), value.size());
    }
    MethodStringNoCache<StringHashMap<int>> method;
    DorisVector<StringRef> keys;
    method.init_serialized_keys_impl({column.get()}, column->size(), keys);
    ASSERT_EQ(keys.size(), values.size());
    for (size_t row = 0; row < values.size(); ++row) {
        EXPECT_EQ(keys[row].to_string(), values[row]);
    }
    std::vector<StringRef> output_keys(keys.begin(), keys.end());
    MutableColumns output;
    output.push_back(type->create_column());
    method.insert_keys_into_columns(output_keys, output, output_keys.size());
    for (size_t row = 0; row < values.size(); ++row) {
        EXPECT_EQ(output[0]->get_data_at(row).to_string(), values[row]);
    }
}

TEST(HashKeyTypeTest, FixedWidthStructUsesSerializedKey) {
    const auto group_key = make_nullable(std::make_shared<DataTypeInt32>());

    for (const auto& field_type : DataTypes {make_nullable(std::make_shared<DataTypeInt8>()),
                                             make_nullable(std::make_shared<DataTypeInt32>())}) {
        SCOPED_TRACE(field_type->get_name());
        const auto struct_type =
                make_nullable(std::make_shared<DataTypeStruct>(DataTypes {field_type}));

        ASSERT_TRUE(struct_type->have_maximum_size_of_value());
        EXPECT_EQ(HashKeyType::serialized, get_hash_key_type({group_key, struct_type}));
        EXPECT_EQ(HashKeyType::serialized, get_hash_key_type_fixed({group_key, struct_type}));
    }
}

TEST(HashKeyTypeTest, SingleStructUsesSerializedKey) {
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt32>())}));

    EXPECT_EQ(HashKeyType::serialized, get_hash_key_type({struct_type}));
}

TEST(HashKeyTypeTest, NumericKeysUseFixedKey) {
    const DataTypes data_types {std::make_shared<DataTypeInt32>(),
                                std::make_shared<DataTypeInt32>()};

    EXPECT_EQ(HashKeyType::fixed64, get_hash_key_type(data_types));
    EXPECT_EQ(HashKeyType::fixed64, get_hash_key_type_fixed(data_types));
}

} // namespace doris
