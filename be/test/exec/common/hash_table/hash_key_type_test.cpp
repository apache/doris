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

namespace doris {

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
