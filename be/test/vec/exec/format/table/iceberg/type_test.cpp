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

#include <gtest/gtest.h>

#include "vec/exec/format/table/iceberg/types.h"

namespace doris {
namespace iceberg {

class TypeTest : public testing::Test {
public:
    TypeTest() = default;
    virtual ~TypeTest() = default;
};

TEST(DecimalTypeTest, to_string) {
    DecimalType decimal_type(10, 2);
    EXPECT_EQ(decimal_type.to_string(), "decimal(10, 2)");
    EXPECT_EQ(decimal_type.type_id(), TypeID::DECIMAL);
}

TEST(BinaryTypeTest, to_string) {
    BinaryType binary_type;
    EXPECT_EQ(binary_type.to_string(), "binary");
    EXPECT_EQ(binary_type.type_id(), TypeID::BINARY);
}

TEST(FixedTypeTest, to_string) {
    FixedType fixed_type(16);
    EXPECT_EQ(fixed_type.to_string(), "fixed[16]");
    EXPECT_EQ(fixed_type.type_id(), TypeID::FIXED);
}

TEST(UUIDTypeTest, to_string) {
    UUIDType uuid_type;
    EXPECT_EQ(uuid_type.to_string(), "uuid");
    EXPECT_EQ(uuid_type.type_id(), TypeID::UUID);
}

TEST(StringTypeTest, to_string) {
    StringType string_type;
    EXPECT_EQ(string_type.to_string(), "string");
    EXPECT_EQ(string_type.type_id(), TypeID::STRING);
}

TEST(TimestampTypeTest, to_string) {
    TimestampType timestamp_type_with_utc(true);
    EXPECT_EQ(timestamp_type_with_utc.to_string(), "timestamptz");
    EXPECT_EQ(timestamp_type_with_utc.type_id(), TypeID::TIMESTAMP);

    TimestampType timestamp_type_without_utc(false);
    EXPECT_EQ(timestamp_type_without_utc.to_string(), "timestamp");
    EXPECT_EQ(timestamp_type_without_utc.type_id(), TypeID::TIMESTAMP);
}

TEST(TimeTypeTest, to_string) {
    TimeType time_type;
    EXPECT_EQ(time_type.to_string(), "time");
    EXPECT_EQ(time_type.type_id(), TypeID::TIME);
}

TEST(DateTypeTest, to_string) {
    DateType date_type;
    EXPECT_EQ(date_type.to_string(), "date");
    EXPECT_EQ(date_type.type_id(), TypeID::DATE);
}

TEST(DoubleTypeTest, to_string) {
    DoubleType double_type;
    EXPECT_EQ(double_type.to_string(), "double");
    EXPECT_EQ(double_type.type_id(), TypeID::DOUBLE);
}

TEST(FloatTypeTest, to_string) {
    FloatType float_type;
    EXPECT_EQ(float_type.to_string(), "float");
    EXPECT_EQ(float_type.type_id(), TypeID::FLOAT);
}

TEST(LongTypeTest, to_string) {
    LongType long_type;
    EXPECT_EQ(long_type.to_string(), "long");
    EXPECT_EQ(long_type.type_id(), TypeID::LONG);
}

TEST(IntegerTypeTest, to_string) {
    IntegerType integer_type;
    EXPECT_EQ(integer_type.to_string(), "int");
    EXPECT_EQ(integer_type.type_id(), TypeID::INTEGER);
}

TEST(BooleanTypeTest, to_string) {
    BooleanType boolean_type;
    EXPECT_EQ(boolean_type.to_string(), "boolean");
    EXPECT_EQ(boolean_type.type_id(), TypeID::BOOLEAN);
}

TEST(PrimitiveTypesTest, from_primitive_string) {
    EXPECT_EQ(Types::from_primitive_string("boolean")->type_id(), TypeID::BOOLEAN);
    EXPECT_EQ(Types::from_primitive_string("int")->type_id(), TypeID::INTEGER);
    EXPECT_EQ(Types::from_primitive_string("long")->type_id(), TypeID::LONG);
    EXPECT_EQ(Types::from_primitive_string("float")->type_id(), TypeID::FLOAT);
    EXPECT_EQ(Types::from_primitive_string("double")->type_id(), TypeID::DOUBLE);
    EXPECT_EQ(Types::from_primitive_string("date")->type_id(), TypeID::DATE);
    EXPECT_EQ(Types::from_primitive_string("time")->type_id(), TypeID::TIME);
    EXPECT_EQ(Types::from_primitive_string("timestamptz")->type_id(), TypeID::TIMESTAMP);
    EXPECT_EQ(Types::from_primitive_string("timestamp")->type_id(), TypeID::TIMESTAMP);
    EXPECT_EQ(Types::from_primitive_string("string")->type_id(), TypeID::STRING);
    EXPECT_EQ(Types::from_primitive_string("uuid")->type_id(), TypeID::UUID);
    EXPECT_EQ(Types::from_primitive_string("binary")->type_id(), TypeID::BINARY);
    EXPECT_EQ(Types::from_primitive_string("fixed[16]")->type_id(), TypeID::FIXED);
    EXPECT_EQ(Types::from_primitive_string("decimal(10, 2)")->type_id(), TypeID::DECIMAL);
}

TEST(MapTypeTest, test_basic_functions) {
    auto key_type = std::make_unique<StringType>();
    auto value_type = std::make_unique<IntegerType>();

    auto map_type = MapType::of_optional(1, 2, std::move(key_type), std::move(value_type));

    EXPECT_EQ(map_type->type_id(), TypeID::MAP);
    EXPECT_TRUE(map_type->is_map_type());
    EXPECT_EQ(map_type->key_id(), 1);
    EXPECT_EQ(map_type->value_id(), 2);
    EXPECT_TRUE(map_type->is_value_optional());
    EXPECT_EQ(map_type->to_string(), "map<string, int>");
}

TEST(ListTypeTest, test_basic_functions) {
    auto element_type = std::make_unique<StringType>();

    auto list_type = ListType::of_optional(1, std::move(element_type));

    EXPECT_EQ(list_type->type_id(), TypeID::LIST);
    EXPECT_TRUE(list_type->is_list_type());
    EXPECT_EQ(list_type->to_string(), "list<string>");
}

TEST(StructTypeTest, test_basic_functions) {
    NestedField field1(false, 1, "field1", std::make_unique<StringType>(), std::nullopt);
    NestedField field2(true, 2, "field2", std::make_unique<IntegerType>(), std::nullopt);

    std::vector<NestedField> fields;
    fields.reserve(2);
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));

    StructType struct_type(std::move(fields));

    EXPECT_EQ(struct_type.type_id(), TypeID::STRUCT);
    EXPECT_TRUE(struct_type.is_struct_type());
    EXPECT_EQ(struct_type.fields().size(), 2);
    EXPECT_EQ(struct_type.field(1)->field_name(), "field1");
    EXPECT_EQ(struct_type.field(2)->field_name(), "field2");
    EXPECT_EQ(struct_type.field_type("field1")->type_id(), TypeID::STRING);
    EXPECT_EQ(struct_type.field_type("field2")->type_id(), TypeID::INTEGER);
    EXPECT_EQ(struct_type.to_string(), "struct<string, int>");
}

} // namespace iceberg
} // namespace doris
