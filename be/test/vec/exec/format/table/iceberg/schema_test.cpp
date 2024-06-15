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

#include "vec/exec/format/table/iceberg/schema.h"

#include <gtest/gtest.h>

namespace doris {
namespace iceberg {

class SchemaTest : public testing::Test {
public:
    SchemaTest() = default;
    virtual ~SchemaTest() = default;
};

TEST(SchemaTest, test_find_type) {
    std::vector<NestedField> nested_fields;
    nested_fields.reserve(2);
    NestedField field1(false, 1, "field1", std::make_unique<IntegerType>(), std::nullopt);
    NestedField field2(false, 2, "field2", std::make_unique<StringType>(), std::nullopt);
    nested_fields.emplace_back(std::move(field1));
    nested_fields.emplace_back(std::move(field2));

    Schema schema(1, std::move(nested_fields));

    Type* found_type1 = schema.find_type(1);
    Type* found_type2 = schema.find_type(2);

    EXPECT_NE(found_type1, nullptr);
    EXPECT_NE(found_type2, nullptr);
    EXPECT_EQ(found_type1->type_id(), TypeID::INTEGER);
    EXPECT_EQ(found_type2->type_id(), TypeID::STRING);
}

TEST(SchemaTest, test_find_field) {
    std::vector<NestedField> nested_fields;
    nested_fields.reserve(2);
    NestedField field1(false, 1, "field1", std::make_unique<IntegerType>(), std::nullopt);
    NestedField field2(false, 2, "field2", std::make_unique<StringType>(), std::nullopt);
    nested_fields.emplace_back(std::move(field1));
    nested_fields.emplace_back(std::move(field2));

    Schema schema(1, std::move(nested_fields));

    const NestedField* found_field1 = schema.find_field(1);
    const NestedField* found_field2 = schema.find_field(2);

    EXPECT_NE(found_field1, nullptr);
    EXPECT_NE(found_field2, nullptr);
    EXPECT_EQ(found_field1->field_id(), 1);
    EXPECT_EQ(found_field2->field_id(), 2);
}

} // namespace iceberg
} // namespace doris
