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

#include "exec/sink/writer/iceberg/iceberg_write_validator.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/table/iceberg/schema.h"
#include "format/table/iceberg/types.h"

namespace doris::iceberg {
namespace {

DataTypePtr nullable_int() {
    return make_nullable(std::make_shared<DataTypeInt32>());
}

ColumnPtr nullable_int_column(std::vector<int32_t> values, std::vector<uint8_t> nulls) {
    auto data = ColumnInt32::create();
    data->get_data().assign(values.begin(), values.end());
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign(nulls.begin(), nulls.end());
    return ColumnNullable::create(std::move(data), std::move(null_map));
}

void expect_data_quality_error(const Status& status, std::string_view path,
                               std::string_view location) {
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(path), std::string::npos);
    EXPECT_NE(status.to_string().find(location), std::string::npos);
}

} // namespace

TEST(IcebergWriteValidatorTest, RejectsTopLevelRequiredNullWithLocation) {
    std::vector<NestedField> fields;
    fields.emplace_back(false, 1, "payload", std::make_unique<StringType>(), std::nullopt);
    Schema schema(std::move(fields));
    auto type = make_nullable(std::make_shared<DataTypeString>());
    auto data = ColumnString::create();
    data->insert_data("A", 1);
    data->insert_default();
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign({0, 1});
    Block block({ColumnWithTypeAndName(ColumnNullable::create(std::move(data), std::move(null_map)),
                                       type, "payload")});

    expect_data_quality_error(validate_write_block(schema, block), "payload", "row 1");
}

TEST(IcebergWriteValidatorTest, SkipsRequiredChildWhenOptionalParentIsNull) {
    std::vector<NestedField> child_fields;
    child_fields.emplace_back(false, 2, "id", std::make_unique<IntegerType>(), std::nullopt);
    std::vector<NestedField> fields;
    fields.emplace_back(true, 1, "payload", std::make_unique<StructType>(std::move(child_fields)),
                        std::nullopt);
    Schema schema(std::move(fields));
    auto child_type = nullable_int();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {child_type}, Strings {"id"});
    auto parent_type = make_nullable(struct_type);
    Columns children {nullable_int_column({7}, {1})};
    auto parent_null_map = ColumnUInt8::create();
    parent_null_map->get_data().push_back(1);
    Block block(
            {ColumnWithTypeAndName(ColumnNullable::create(ColumnStruct::create(std::move(children)),
                                                          std::move(parent_null_map)),
                                   parent_type, "payload")});

    EXPECT_TRUE(validate_write_block(schema, block).ok());
}

TEST(IcebergWriteValidatorTest, RejectsRequiredStructChildNull) {
    std::vector<NestedField> child_fields;
    child_fields.emplace_back(false, 2, "id", std::make_unique<IntegerType>(), std::nullopt);
    std::vector<NestedField> fields;
    fields.emplace_back(false, 1, "payload", std::make_unique<StructType>(std::move(child_fields)),
                        std::nullopt);
    Schema schema(std::move(fields));
    auto child_type = nullable_int();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {child_type}, Strings {"id"});
    auto parent_type = make_nullable(struct_type);
    Columns children {nullable_int_column({7, 8}, {0, 1})};
    auto parent_null_map = ColumnUInt8::create();
    parent_null_map->get_data().assign({0, 0});
    Block block(
            {ColumnWithTypeAndName(ColumnNullable::create(ColumnStruct::create(std::move(children)),
                                                          std::move(parent_null_map)),
                                   parent_type, "payload")});

    expect_data_quality_error(validate_write_block(schema, block), "payload.id", "row 1");
}

TEST(IcebergWriteValidatorTest, RejectsRequiredListElementNull) {
    std::vector<NestedField> fields;
    fields.emplace_back(true, 1, "items", ListType::of_required(2, std::make_unique<IntegerType>()),
                        std::nullopt);
    Schema schema(std::move(fields));
    auto element_type = nullable_int();
    auto array_type = std::make_shared<DataTypeArray>(element_type);
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(3);
    auto array = ColumnArray::create(nullable_int_column({1, 2, 3}, {0, 1, 0}), std::move(offsets));
    auto null_map = ColumnUInt8::create();
    null_map->get_data().push_back(0);
    Block block(
            {ColumnWithTypeAndName(ColumnNullable::create(std::move(array), std::move(null_map)),
                                   make_nullable(array_type), "items")});

    expect_data_quality_error(validate_write_block(schema, block), "items.element", "element 1");
}

TEST(IcebergWriteValidatorTest, RejectsNullMapKey) {
    std::vector<NestedField> fields;
    fields.emplace_back(true, 1, "attrs",
                        MapType::of_required(2, 3, std::make_unique<IntegerType>(),
                                             std::make_unique<IntegerType>()),
                        std::nullopt);
    Schema schema(std::move(fields));
    auto key_type = std::make_shared<DataTypeInt32>();
    auto value_type = nullable_int();
    auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    auto offsets = ColumnMap::COffsets::create();
    offsets->get_data().push_back(2);
    auto map = ColumnMap::create(nullable_int_column({1, 2}, {0, 1}),
                                 nullable_int_column({10, 20}, {0, 0}), std::move(offsets));
    auto null_map = ColumnUInt8::create();
    null_map->get_data().push_back(0);
    Block block({ColumnWithTypeAndName(ColumnNullable::create(std::move(map), std::move(null_map)),
                                       make_nullable(map_type), "attrs")});

    expect_data_quality_error(validate_write_block(schema, block), "attrs.key", "map entry 1");
}

TEST(IcebergWriteValidatorTest, SkipsMapKeyWhenOptionalMapIsNull) {
    std::vector<NestedField> fields;
    fields.emplace_back(true, 1, "attrs",
                        MapType::of_required(2, 3, std::make_unique<IntegerType>(),
                                             std::make_unique<IntegerType>()),
                        std::nullopt);
    Schema schema(std::move(fields));
    auto key_type = std::make_shared<DataTypeInt32>();
    auto value_type = nullable_int();
    auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    auto offsets = ColumnMap::COffsets::create();
    offsets->get_data().push_back(1);
    // Keep a physically materialized NULL key under the NULL parent to prove that the parent mask,
    // rather than an empty map, is what suppresses the map-key check.
    auto map = ColumnMap::create(nullable_int_column({1}, {1}), nullable_int_column({10}, {0}),
                                 std::move(offsets));
    auto null_map = ColumnUInt8::create();
    null_map->get_data().push_back(1);
    Block block({ColumnWithTypeAndName(ColumnNullable::create(std::move(map), std::move(null_map)),
                                       make_nullable(map_type), "attrs")});

    EXPECT_TRUE(validate_write_block(schema, block).ok());
}

TEST(IcebergWriteValidatorTest, RejectsRequiredMapValueNull) {
    std::vector<NestedField> fields;
    fields.emplace_back(true, 1, "attrs",
                        MapType::of_required(2, 3, std::make_unique<IntegerType>(),
                                             std::make_unique<IntegerType>()),
                        std::nullopt);
    Schema schema(std::move(fields));
    auto key_type = std::make_shared<DataTypeInt32>();
    auto value_type = nullable_int();
    auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    auto offsets = ColumnMap::COffsets::create();
    offsets->get_data().push_back(2);
    auto map = ColumnMap::create(nullable_int_column({1, 2}, {0, 0}),
                                 nullable_int_column({10, 20}, {0, 1}), std::move(offsets));
    auto null_map = ColumnUInt8::create();
    null_map->get_data().push_back(0);
    Block block({ColumnWithTypeAndName(ColumnNullable::create(std::move(map), std::move(null_map)),
                                       make_nullable(map_type), "attrs")});

    expect_data_quality_error(validate_write_block(schema, block), "attrs.value", "map entry 1");
}

TEST(IcebergWriteValidatorTest, AcceptsValidRequiredNestedValues) {
    std::vector<NestedField> fields;
    fields.emplace_back(false, 1, "id", std::make_unique<IntegerType>(), std::nullopt);
    fields.emplace_back(true, 2, "items", ListType::of_required(3, std::make_unique<IntegerType>()),
                        std::nullopt);
    Schema schema(std::move(fields));
    auto id_type = nullable_int();
    auto id_column = nullable_int_column({1, 2}, {0, 0});
    auto element_type = nullable_int();
    auto array_type = make_nullable(std::make_shared<DataTypeArray>(element_type));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().assign({2, 3});
    auto array =
            ColumnArray::create(nullable_int_column({10, 20, 30}, {0, 0, 0}), std::move(offsets));
    auto array_nulls = ColumnUInt8::create();
    array_nulls->get_data().assign({0, 0});
    Block block({
            ColumnWithTypeAndName(std::move(id_column), id_type, "id"),
            ColumnWithTypeAndName(ColumnNullable::create(std::move(array), std::move(array_nulls)),
                                  array_type, "items"),
    });

    EXPECT_TRUE(validate_write_block(schema, block).ok());
}

} // namespace doris::iceberg
