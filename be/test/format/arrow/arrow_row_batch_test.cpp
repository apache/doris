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

#include "format/arrow/arrow_row_batch.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/arrow/arrow_block_convertor.h"

namespace doris {
namespace {

void expect_logical_type(const std::shared_ptr<arrow::Field>& field, const std::string& name) {
    ASSERT_NE(nullptr, field->metadata()) << field->ToString();
    auto value = field->metadata()->Get("doris_type");
    ASSERT_TRUE(value.ok()) << value.status();
    EXPECT_EQ(name, *value);
}

class ArrowLogicalTypeMetadataTest
        : public testing::TestWithParam<std::pair<PrimitiveType, const char*>> {};

TEST_P(ArrowLogicalTypeMetadataTest, PreservesTopLevelAndNestedFields) {
    const auto [primitive, name] = GetParam();
    auto type = make_nullable(DataTypeFactory::instance().create_data_type(primitive, false));
    auto string_type = std::make_shared<DataTypeString>();
    DataTypes types {type, std::make_shared<DataTypeArray>(type),
                     std::make_shared<DataTypeStruct>(DataTypes {type, string_type},
                                                      Strings {"typed", "text"}),
                     std::make_shared<DataTypeMap>(string_type, type)};
    Block block;
    for (size_t i = 0; i < types.size(); ++i) {
        block.insert({types[i]->create_column(), types[i], std::to_string(i)});
    }
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(get_arrow_schema_from_block(block, &schema, "UTC", true).ok());
    expect_logical_type(schema->field(0), name);
    expect_logical_type(schema->field(1)->type()->field(0), name);
    expect_logical_type(schema->field(2)->type()->field(0), name);
    EXPECT_EQ(nullptr, schema->field(2)->type()->field(1)->metadata());
    const auto& map = static_cast<const arrow::MapType&>(*schema->field(3)->type());
    expect_logical_type(map.item_field(), name);
    EXPECT_FALSE(map.key_field()->nullable());
    EXPECT_TRUE(map.item_field()->nullable());
    EXPECT_EQ(nullptr, map.key_field()->metadata());
}

INSTANTIATE_TEST_SUITE_P(LogicalTypes, ArrowLogicalTypeMetadataTest,
                         testing::Values(std::make_pair(TYPE_LARGEINT, "LARGEINT"),
                                         std::make_pair(TYPE_IPV4, "IPV4"),
                                         std::make_pair(TYPE_IPV6, "IPV6"),
                                         std::make_pair(TYPE_JSONB, "JSON"),
                                         std::make_pair(TYPE_VARIANT, "VARIANT")));

TEST(ArrowRowBatchMetadataTest, PreservesMapKeysAndDeepNestingThroughIpc) {
    auto integer = std::make_shared<DataTypeInt128>();
    auto structure = std::make_shared<DataTypeStruct>(DataTypes {integer}, Strings {"number"});
    auto array = std::make_shared<DataTypeArray>(structure);
    auto type = std::make_shared<DataTypeMap>(integer, array);
    Block block;
    block.insert({type->create_column(), type, "m"});
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(get_arrow_schema_from_block(block, &schema, "UTC").ok());
    std::string serialized;
    ASSERT_TRUE(serialize_arrow_schema(&schema, &serialized).ok());
    auto source = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(serialized));
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(source).ValueOrDie();
    EXPECT_TRUE(schema->Equals(*reader->schema(), true));
    const auto& map = static_cast<const arrow::MapType&>(*reader->schema()->field(0)->type());
    expect_logical_type(map.key_field(), "LARGEINT");
    EXPECT_EQ("key", map.key_field()->name());
    EXPECT_FALSE(map.key_field()->nullable());
    EXPECT_EQ("value", map.item_field()->name());
    auto item = map.item_type()->field(0);
    EXPECT_EQ("item", item->name());
    EXPECT_TRUE(item->nullable());
    expect_logical_type(item->type()->field(0), "LARGEINT");
    EXPECT_FALSE(item->type()->field(0)->nullable());
    auto physical =
            arrow::map(arrow::utf8(),
                       arrow::list(arrow::struct_({arrow::field("number", arrow::utf8(), false)})));
    EXPECT_TRUE(map.Equals(physical));
    EXPECT_FALSE(map.Equals(physical, true));
}

TEST(ArrowRowBatchMetadataTest, KeepsNativeTypesUnannotated) {
    auto type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()},
            Strings {"number", "text"});
    std::shared_ptr<arrow::DataType> arrow_type;
    ASSERT_TRUE(convert_to_arrow_type(type, &arrow_type, "UTC").ok());
    for (const auto& field : arrow_type->fields()) {
        EXPECT_EQ(nullptr, field->metadata());
        EXPECT_FALSE(field->nullable());
    }
}

TEST(ArrowRowBatchMetadataTest, PreservesLargeintExtremesAndNullsInRecordBatches) {
    auto integer = make_nullable(std::make_shared<DataTypeInt128>());
    auto array_type = std::make_shared<DataTypeArray>(integer);
    auto column = array_type->create_column();
    column->insert(Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_LARGEINT>(MAX_INT128),
                   Field::create_field<TYPE_LARGEINT>(MIN_INT128), Field()}));
    column->insert(Field::create_field<TYPE_ARRAY>(Array {}));
    Block block;
    block.insert({std::move(column), array_type, "numbers"});
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(get_arrow_schema_from_block(block, &schema, "UTC", true).ok());
    ArrowFlightArrowBlockConvertor converter(schema, cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(batch->ValidateFull().ok());
    std::string serialized;
    ASSERT_TRUE(serialize_record_batch(*batch, &serialized).ok());
    auto source = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(serialized));
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(source).ValueOrDie();
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    EXPECT_TRUE(batch->schema()->Equals(*schema, true));
    expect_logical_type(batch->schema()->field(0)->type()->field(0), "LARGEINT");
    const auto& lists = static_cast<const arrow::ListArray&>(*batch->column(0));
    EXPECT_EQ(0, lists.value_length(1));
    const auto& values = static_cast<const arrow::StringArray&>(*lists.values());
    EXPECT_EQ("170141183460469231731687303715884105727", values.GetString(0));
    EXPECT_EQ("-170141183460469231731687303715884105728", values.GetString(1));
    EXPECT_TRUE(values.IsNull(2));
}

TEST(ArrowRowBatchMetadataTest, PreservesNestedMapMetadataAndValuesInRecordBatches) {
    auto integer = make_nullable(std::make_shared<DataTypeInt128>());
    auto map_type = make_nullable(std::make_shared<DataTypeMap>(integer, integer));
    auto type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {map_type, std::make_shared<DataTypeString>()}, Strings {"mapping", "text"}));
    Map map;
    map.push_back(Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_LARGEINT>(MAX_INT128)}));
    map.push_back(Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_LARGEINT>(MIN_INT128)}));
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_STRUCT>(
            Struct {Field::create_field<TYPE_MAP>(map), Field::create_field<TYPE_STRING>("17")}));
    column->insert_default();
    Block block;
    block.insert({std::move(column), type, "nested"});
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(get_arrow_schema_from_block(block, &schema, "UTC", true).ok());
    ArrowFlightArrowBlockConvertor converter(schema, cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(batch->ValidateFull().ok());
    std::string serialized;
    ASSERT_TRUE(serialize_record_batch(*batch, &serialized).ok());
    auto source = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(serialized));
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(source).ValueOrDie();
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    const auto& structure = static_cast<const arrow::StructArray&>(*batch->column(0));
    EXPECT_TRUE(structure.IsNull(1));
    const auto& maps = static_cast<const arrow::MapArray&>(*structure.field(0));
    const auto& arrow_map_type = static_cast<const arrow::MapType&>(*maps.type());
    expect_logical_type(arrow_map_type.key_field(), "LARGEINT");
    expect_logical_type(arrow_map_type.item_field(), "LARGEINT");
    EXPECT_FALSE(arrow_map_type.key_field()->nullable());
    EXPECT_EQ("170141183460469231731687303715884105727",
              static_cast<const arrow::StringArray&>(*maps.keys()).GetString(0));
    EXPECT_EQ("-170141183460469231731687303715884105728",
              static_cast<const arrow::StringArray&>(*maps.items()).GetString(0));
    EXPECT_EQ("17", static_cast<const arrow::StringArray&>(*structure.field(1)).GetString(0));
    EXPECT_EQ(nullptr, structure.type()->field(1)->metadata());
}

} // namespace
} // namespace doris
