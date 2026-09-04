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

// ############################################################################
// What an Arrow schema says about the Doris types that Arrow cannot express.
//
// LARGEINT, IPV4, IPV6, JSON and VARIANT all travel as some other Arrow type,
// and once they arrive they are indistinguishable from a column that is
// natively of that type: a LARGEINT and a STRING are both utf8, an IPV4 and an
// INT are both int32. The field metadata is the only thing that tells them
// apart, so a field that loses it loses the type -- and an element of an
// ARRAY, MAP or STRUCT is a field like any other.
// ############################################################################

#include "format/arrow/arrow_row_batch.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant.h"
#include "core/field.h"
#include "format/arrow/arrow_block_convertor.h"

namespace doris {

namespace {

// "" when the field carries no Doris type, which is the answer for every type Arrow can express.
std::string doris_type_of(const std::shared_ptr<arrow::Field>& field) {
    if (field == nullptr || !field->HasMetadata()) {
        return "";
    }
    const auto found = field->metadata()->Get("doris_type");
    return found.ok() ? found.ValueUnsafe() : "";
}

DataTypePtr largeint() {
    return make_nullable(std::make_shared<DataTypeInt128>());
}

DataTypePtr string_type() {
    return make_nullable(std::make_shared<DataTypeString>());
}

DataTypePtr array_of(const DataTypePtr& item) {
    return make_nullable(std::make_shared<DataTypeArray>(item));
}

DataTypePtr map_of(const DataTypePtr& key, const DataTypePtr& value) {
    return make_nullable(std::make_shared<DataTypeMap>(key, value));
}

DataTypePtr struct_of(const DataTypes& elements, const Strings& names) {
    return make_nullable(std::make_shared<DataTypeStruct>(elements, names));
}

Block block_of(const std::vector<std::pair<std::string, DataTypePtr>>& columns) {
    Block block;
    for (const auto& [name, type] : columns) {
        block.insert(ColumnWithTypeAndName(type->create_column(), type, name));
    }
    return block;
}

std::shared_ptr<arrow::Schema> schema_of(
        const std::vector<std::pair<std::string, DataTypePtr>>& columns) {
    const Block block = block_of(columns);
    std::shared_ptr<arrow::Schema> schema;
    EXPECT_TRUE(get_arrow_schema_from_block(block, &schema, "UTC").ok());
    EXPECT_NE(schema, nullptr);
    return schema;
}

// The single child of a list, or the named child of a struct.
std::shared_ptr<arrow::Field> item_of(const std::shared_ptr<arrow::Field>& list_field) {
    return list_field->type()->field(0);
}

const arrow::MapType& map_of(const std::shared_ptr<arrow::Field>& map_field) {
    return dynamic_cast<const arrow::MapType&>(*map_field->type());
}

} // namespace

TEST(ArrowRowBatchSchemaTest, NestedLargeintKeepsItsDorisType) {
    auto schema = schema_of({
            {"scalar_value", largeint()},
            {"array_value", array_of(largeint())},
            {"struct_value", struct_of({largeint()}, {"count"})},
            {"map_value", map_of(string_type(), largeint())},
    });

    // The top level field is the one that already worked.
    EXPECT_EQ("LARGEINT", doris_type_of(schema->GetFieldByName("scalar_value")));

    EXPECT_EQ("LARGEINT", doris_type_of(item_of(schema->GetFieldByName("array_value"))));
    EXPECT_EQ("LARGEINT", doris_type_of(schema->GetFieldByName("struct_value")->type()->field(0)));
    EXPECT_EQ("LARGEINT", doris_type_of(map_of(schema->GetFieldByName("map_value")).item_field()));
}

TEST(ArrowRowBatchSchemaTest, LargeintMapKeyKeepsItsDorisType) {
    auto schema = schema_of({{"map_value", map_of(largeint(), string_type())}});

    const auto& map = map_of(schema->GetFieldByName("map_value"));
    EXPECT_EQ("LARGEINT", doris_type_of(map.key_field()));
    EXPECT_EQ("", doris_type_of(map.item_field()));
}

TEST(ArrowRowBatchSchemaTest, DorisTypeSurvivesEveryLevelOfNesting) {
    auto schema = schema_of({
            {"a", array_of(struct_of({largeint()}, {"n"}))},
            {"b", map_of(string_type(), array_of(array_of(largeint())))},
    });

    // array<struct<n:largeint>>
    EXPECT_EQ("LARGEINT", doris_type_of(item_of(schema->GetFieldByName("a"))->type()->field(0)));

    // map<string, array<array<largeint>>>
    const auto& map = map_of(schema->GetFieldByName("b"));
    EXPECT_EQ("LARGEINT", doris_type_of(map.item_field()->type()->field(0)->type()->field(0)));
}

TEST(ArrowRowBatchSchemaTest, NestedIpJsonAndVariantKeepTheirDorisType) {
    auto ipv4 = make_nullable(std::make_shared<DataTypeIPv4>());
    auto ipv6 = make_nullable(std::make_shared<DataTypeIPv6>());
    auto json = make_nullable(std::make_shared<DataTypeJsonb>());
    auto variant = make_nullable(std::make_shared<DataTypeVariant>());

    auto schema = schema_of({
            {"ip4", ipv4},
            {"json", json},
            {"variant", variant},
            {"nested", struct_of({ipv4, ipv6, json, variant}, {"ip4", "ip6", "json", "variant"})},
    });

    // IPV4 is the one whose value changes form -- it arrives as the address's 32 bits read as a
    // signed int32 -- so a nested IPV4 that lost its metadata could not be recovered at all.
    EXPECT_EQ("IPV4", doris_type_of(schema->GetFieldByName("ip4")));
    EXPECT_EQ("JSON", doris_type_of(schema->GetFieldByName("json")));
    EXPECT_EQ("VARIANT", doris_type_of(schema->GetFieldByName("variant")));

    const auto& nested = *schema->GetFieldByName("nested")->type();
    EXPECT_EQ("IPV4", doris_type_of(nested.field(0)));
    EXPECT_EQ("IPV6", doris_type_of(nested.field(1)));
    EXPECT_EQ("JSON", doris_type_of(nested.field(2)));
    EXPECT_EQ("VARIANT", doris_type_of(nested.field(3)));
}

TEST(ArrowRowBatchSchemaTest, TypesArrowCanExpressCarryNoDorisType) {
    // The negative half of the contract: if every utf8 field claimed a Doris type there would be
    // nothing to distinguish a LARGEINT from a column that really is a string.
    auto schema = schema_of({
            {"s", string_type()},
            {"i", make_nullable(std::make_shared<DataTypeInt32>())},
            {"nested", struct_of({string_type(), make_nullable(std::make_shared<DataTypeInt32>())},
                                 {"s", "i"})},
            {"arr", array_of(string_type())},
    });

    EXPECT_EQ("", doris_type_of(schema->GetFieldByName("s")));
    EXPECT_EQ("", doris_type_of(schema->GetFieldByName("i")));
    EXPECT_EQ("", doris_type_of(schema->GetFieldByName("nested")->type()->field(0)));
    EXPECT_EQ("", doris_type_of(schema->GetFieldByName("nested")->type()->field(1)));
    EXPECT_EQ("", doris_type_of(item_of(schema->GetFieldByName("arr"))));
}

TEST(ArrowRowBatchSchemaTest, OnlyTheMetadataIsNew) {
    // Naming a child or changing its nullability would be a different schema, and clients that
    // already read these columns type themselves from it. Compared against the types Arrow's own
    // constructors build: equal when metadata is ignored, different only once it is compared.
    auto schema = schema_of({
            {"array_value", array_of(largeint())},
            {"map_value", map_of(string_type(), largeint())},
    });

    const auto expected_list = arrow::list(arrow::utf8());
    const auto& list = *schema->GetFieldByName("array_value")->type();
    EXPECT_TRUE(list.Equals(*expected_list, /*check_metadata=*/false));
    EXPECT_FALSE(list.Equals(*expected_list, /*check_metadata=*/true));
    EXPECT_EQ("item", list.field(0)->name());
    EXPECT_TRUE(list.field(0)->nullable());

    const auto expected_map = arrow::map(arrow::utf8(), arrow::utf8());
    const auto& map = *schema->GetFieldByName("map_value")->type();
    EXPECT_TRUE(map.Equals(*expected_map, /*check_metadata=*/false));
    EXPECT_FALSE(map.Equals(*expected_map, /*check_metadata=*/true));
    const auto& as_map = dynamic_cast<const arrow::MapType&>(map);
    EXPECT_EQ("key", as_map.key_field()->name());
    EXPECT_FALSE(as_map.key_field()->nullable()) << "an Arrow map key is never nullable";
    EXPECT_EQ("value", as_map.item_field()->name());
    EXPECT_TRUE(as_map.item_field()->nullable());
}

// The schema is not only described to the client, it is also what the record batch builders are
// made from (FromBlockToRecordBatchConverter reads _schema->field(idx)->type()). A schema the data
// path cannot honour would turn a metadata fix into a broken result set.
TEST(ArrowRowBatchDataTest, NestedSchemaStillBuildsTheBatch) {
    const __int128_t value = 495;

    auto array_type = array_of(largeint());
    auto struct_type = struct_of({largeint()}, {"count"});
    auto map_type = map_of(string_type(), largeint());

    auto array_column = array_type->create_column();
    array_column->insert(
            Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_LARGEINT>(value)}));

    auto struct_column = struct_type->create_column();
    struct_column->insert(
            Field::create_field<TYPE_STRUCT>(Struct {Field::create_field<TYPE_LARGEINT>(value)}));

    auto map_column = map_type->create_column();
    map_column->insert(Field::create_field<TYPE_MAP>(Map {
            Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_STRING>(String("k"))}),
            Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_LARGEINT>(value)})}));

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(array_column), array_type, "array_value"));
    block.insert(ColumnWithTypeAndName(std::move(struct_column), struct_type, "struct_value"));
    block.insert(ColumnWithTypeAndName(std::move(map_column), map_type, "map_value"));

    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(get_arrow_schema_from_block(block, &schema, "UTC").ok());

    std::shared_ptr<arrow::RecordBatch> batch;
    cctz::time_zone utc;
    ASSERT_TRUE(
            convert_to_arrow_batch(block, schema, arrow::default_memory_pool(), &batch, utc).ok());
    ASSERT_NE(batch, nullptr);
    ASSERT_TRUE(batch->ValidateFull().ok()) << batch->ValidateFull().ToString();
    // Including the metadata: the batch the client reads describes its nested types the same way
    // the schema does.
    EXPECT_TRUE(batch->schema()->Equals(*schema, /*check_metadata=*/true));
    ASSERT_EQ(1, batch->num_rows());

    const auto& list = dynamic_cast<const arrow::ListArray&>(*batch->column(0));
    EXPECT_EQ("495", dynamic_cast<const arrow::StringArray&>(*list.values()).GetString(0));

    const auto& structure = dynamic_cast<const arrow::StructArray&>(*batch->column(1));
    EXPECT_EQ("495", dynamic_cast<const arrow::StringArray&>(*structure.field(0)).GetString(0));

    const auto& map = dynamic_cast<const arrow::MapArray&>(*batch->column(2));
    EXPECT_EQ("k", dynamic_cast<const arrow::StringArray&>(*map.keys()).GetString(0));
    EXPECT_EQ("495", dynamic_cast<const arrow::StringArray&>(*map.items()).GetString(0));
}

} // namespace doris
