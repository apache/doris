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

#include <arrow/api.h>
#include <arrow/extension/parquet_variant.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include <cmath>
#include <limits>

#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_serde.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "util/timezone_utils.h"

namespace doris {
namespace {

std::shared_ptr<arrow::DataType> native_variant() {
    return arrow::extension::variant(
            arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                            arrow::field("value", arrow::binary(), false)}));
}

MutableColumnPtr documents(const DataTypePtr& type) {
    auto column = type->create_column();
    auto serde = type->get_serde();
    DataTypeSerDe::FormatOptions options;
    for (std::string json : {R"({"a":[1,null,"x"]})", "null", "42", R"("text")"}) {
        Slice slice(json.data(), json.size());
        EXPECT_TRUE(serde->deserialize_one_cell_from_json(*column, slice, options).ok());
    }
    if (auto* legacy = check_and_get_column<ColumnVariant>(*column)) {
        legacy->finalize();
    }
    return column;
}

VariantRef value_at(const arrow::Array& array, int row) {
    const auto& storage = static_cast<const arrow::StructArray&>(
            *static_cast<const arrow::ExtensionArray&>(array).storage());
    auto metadata = static_cast<const arrow::BinaryArray&>(*storage.field(0)).GetView(row);
    auto value = static_cast<const arrow::BinaryArray&>(*storage.field(1)).GetView(row);
    return {{metadata.data(), metadata.size()}, {value.data(), value.size()}};
}

TEST(ArrowFlightVariantTest, NativeSchemaRejectsLegacyIncludingNestedAndEmptyResults) {
    auto legacy = std::make_shared<DataTypeVariant>();
    auto nullable = make_nullable(legacy);
    DataTypes types {legacy, nullable, std::make_shared<DataTypeArray>(nullable),
                     std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), nullable),
                     std::make_shared<DataTypeStruct>(
                             DataTypes {std::make_shared<DataTypeVariantV2>(), nullable},
                             Strings {"v2", "legacy"})};
    for (const auto& type : types) {
        SCOPED_TRACE(type->get_name());
        Block block {{type->create_column(), type, "v"}};
        std::shared_ptr<arrow::Schema> schema;
        auto status = ArrowFlightSchemaConvertor(block, "UTC").get_arrow_schema(&schema);
        EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
        EXPECT_NE(status.to_string().find("only supports Variant V2"), std::string::npos);
        EXPECT_NE(status.to_string().find("cast the result to STRING"), std::string::npos);
        EXPECT_TRUE(DorisArrowSchemaConvertor(block, "UTC").get_arrow_schema(&schema).ok());
    }
}

TEST(ArrowFlightVariantTest, LegacyNativeOutputRejectsValuesConstantsAndNulls) {
    auto type = std::make_shared<DataTypeVariant>();
    auto nulls = ColumnUInt8::create();
    nulls->get_data().resize_fill(4, 1);
    auto constant = type->create_column();
    DataTypeSerDe::FormatOptions options;
    std::string json = "42";
    Slice slice(json.data(), json.size());
    ASSERT_TRUE(type->get_serde()->deserialize_one_cell_from_json(*constant, slice, options).ok());
    assert_cast<ColumnVariant&>(*constant).finalize();
    ColumnsWithTypeAndName inputs {
            {documents(type), type, "v"},
            {ColumnConst::create(std::move(constant), 3), type, "v"},
            {ColumnNullable::create(documents(type), std::move(nulls)), make_nullable(type), "v"},
            {type->create_column(), type, "v"}};
    for (const auto& input : inputs) {
        Block block {input};
        ArrowFlightArrowBlockConvertor native(arrow::schema({arrow::field("v", native_variant())}),
                                              cctz::utc_time_zone());
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = native.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
        EXPECT_NE(status.to_string().find("only supports Variant V2"), std::string::npos);

        DorisArrowBlockConvertor utf8(block, "UTC", cctz::utc_time_zone());
        ASSERT_TRUE(utf8.init().ok());
        ASSERT_TRUE(utf8.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
        EXPECT_EQ(batch->num_rows(), block.rows());
        EXPECT_EQ(batch->column(0)->type_id(), arrow::Type::STRING);
        if (input.type->is_nullable()) {
            EXPECT_EQ(batch->column(0)->null_count(), 4);
        } else if (block.rows() == 4) {
            const auto& values = static_cast<const arrow::StringArray&>(*batch->column(0));
            EXPECT_EQ(values.GetString(0), R"({"a":[1, null, "x"]})");
            EXPECT_EQ(values.GetString(1), "{}");
            EXPECT_EQ(values.GetString(2), "42");
            EXPECT_EQ(values.GetString(3), R"("text")");
        }
    }
}

TEST(ArrowFlightVariantTest, LegacySerdeRejectsNativeStorageBeforeInspectingRows) {
    auto type = std::make_shared<DataTypeVariant>();
    auto column = documents(type);
    auto storage = std::static_pointer_cast<arrow::ExtensionType>(native_variant())->storage_type();
    auto builder = arrow::MakeBuilder(storage, arrow::default_memory_pool()).ValueOrDie();
    NullMap nulls(4, 1);
    for (int end : {0, 4}) {
        auto status = type->get_serde()->write_column_to_arrow(*column, &nulls, builder.get(), 0,
                                                               end, cctz::utc_time_zone());
        EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
        EXPECT_NE(status.to_string().find("only supports Variant V2"), std::string::npos);
        EXPECT_EQ(builder->length(), 0);
    }
}

TEST(ArrowFlightVariantTest, NativeResultPreservesValuesAndSqlNulls) {
    TimezoneUtils::load_timezones_to_cache();
    ASSERT_TRUE(register_arrow_variant_extension().ok());
    auto type = std::make_shared<DataTypeVariantV2>();
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 0, 0, 1});
    Block block;
    block.insert(
            {ColumnNullable::create(documents(type), std::move(nulls)), make_nullable(type), "v"});
    auto schema = arrow::schema({arrow::field("v", native_variant())});
    ArrowFlightArrowBlockConvertor converter(schema, cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_rows(), 4);
    EXPECT_FALSE(batch->column(0)->IsNull(1));
    EXPECT_TRUE(value_at(*batch->column(0), 1).is_null());
    EXPECT_EQ(value_at(*batch->column(0), 2).get_int(), 42);
    EXPECT_TRUE(batch->column(0)->IsNull(3));
    EXPECT_EQ(value_at(*batch->column(0), 0).basic_type(), VariantBasicType::OBJECT);

    // Extension metadata and storage must survive the same IPC boundary used by Flight.
    auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(output, batch->schema()).ValueOrDie();
    ASSERT_TRUE(writer->WriteRecordBatch(*batch).ok());
    ASSERT_TRUE(writer->Close().ok());
    auto input = std::make_shared<arrow::io::BufferReader>(output->Finish().ValueOrDie());
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(input).ValueOrDie();
    auto round_trip = reader->Next().ValueOrDie();
    EXPECT_TRUE(batch->Equals(*round_trip));
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 1, 3).ok());
    EXPECT_TRUE(value_at(*batch->column(0), 0).is_null());
    EXPECT_EQ(value_at(*batch->column(0), 1).get_int(), 42);
}

TEST(ArrowFlightVariantTest, SchemaMappingAndConstantScalar) {
    auto type = std::make_shared<DataTypeVariantV2>();
    std::shared_ptr<arrow::DataType> mapped;
    ASSERT_TRUE(DorisArrowSchemaConvertor("UTC").convert_to_arrow_type(type, &mapped).ok());
    EXPECT_TRUE(mapped->Equals(arrow::utf8()));
    ASSERT_TRUE(ArrowFlightSchemaConvertor("UTC").convert_to_arrow_type(type, &mapped).ok());
    EXPECT_TRUE(mapped->Equals(native_variant()));
    auto column = type->create_column();
    std::string json = R"("te\"xt\n\u4e2d")";
    Slice slice(json.data(), json.size());
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(type->get_serde()->deserialize_one_cell_from_json(*column, slice, options).ok());
    Block block;
    block.insert({ColumnConst::create(std::move(column), 3), type, "v"});
    ArrowFlightArrowBlockConvertor converter(arrow::schema({arrow::field("v", mapped, false)}),
                                             cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_rows(), 3);
    EXPECT_EQ(value_at(*batch->column(0), 2).get_string().to_string(), "te\"xt\n中");
}

TEST(ArrowFlightVariantTest, NativeSchemaPreservesNestedLogicalMetadata) {
    auto variant = std::make_shared<DataTypeVariantV2>();
    const auto nullable = make_nullable(variant);
    const auto string = std::make_shared<DataTypeString>();
    DataTypes types {nullable, std::make_shared<DataTypeArray>(nullable),
                     std::make_shared<DataTypeMap>(string, nullable),
                     std::make_shared<DataTypeStruct>(DataTypes {nullable}, Strings {"v"})};
    Block block;
    for (size_t i = 0; i < types.size(); ++i) {
        block.insert({types[i]->create_column(), types[i], std::to_string(i)});
    }
    {
        std::shared_ptr<arrow::Schema> schema;
        ASSERT_TRUE(ArrowFlightSchemaConvertor(block, "UTC").get_arrow_schema(&schema).ok());
        const auto& map = static_cast<const arrow::MapType&>(*schema->field(2)->type());
        for (const auto& field : {schema->field(0), schema->field(1)->type()->field(0),
                                  map.item_field(), schema->field(3)->type()->field(0)}) {
            EXPECT_TRUE(field->type()->Equals(native_variant()));
            EXPECT_TRUE(field->nullable());
            ASSERT_NE(nullptr, field->metadata());
            EXPECT_EQ("VARIANT", field->metadata()->Get("doris_type").ValueOrDie());
        }
        EXPECT_FALSE(map.key_field()->nullable());
    }
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(DorisArrowSchemaConvertor(block, "UTC").get_arrow_schema(&schema).ok());
    EXPECT_TRUE(schema->field(0)->type()->Equals(arrow::utf8()));
}

TEST(ArrowFlightVariantTest, TypedV2AndNestedStructPreserveNonJsonNumbers) {
    auto numbers = ColumnFloat64::create();
    numbers->insert_value(std::numeric_limits<double>::quiet_NaN());
    numbers->insert_value(std::numeric_limits<double>::infinity());
    auto values = ColumnVariantV2::create_typed(make_nullable(std::move(numbers)),
                                                std::make_shared<DataTypeFloat64>());
    auto variant = std::make_shared<DataTypeVariantV2>();
    auto type = std::make_shared<DataTypeStruct>(DataTypes {variant}, Strings {"v"});
    Block block;
    block.insert({ColumnStruct::create(Columns {std::move(values)}), type, "s"});
    std::shared_ptr<arrow::DataType> mapped;
    ASSERT_TRUE(ArrowFlightSchemaConvertor("UTC").convert_to_arrow_type(type, &mapped).ok());
    ArrowFlightArrowBlockConvertor converter(arrow::schema({arrow::field("s", mapped, false)}),
                                             cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(batch->ValidateFull().ok());
    const auto& child = *static_cast<const arrow::StructArray&>(*batch->column(0)).field(0);
    EXPECT_TRUE(std::isnan(value_at(child, 0).get_double()));
    EXPECT_TRUE(std::isinf(value_at(child, 1).get_double()));
}

TEST(ArrowFlightVariantTest, NestedTimezoneAliasesMatchPublishedSchema) {
    TimezoneUtils::load_timezones_to_cache();
    auto variant = std::make_shared<DataTypeVariantV2>();
    auto timestamp = DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, false, 0, 6);
    auto type =
            std::make_shared<DataTypeStruct>(DataTypes {variant, timestamp}, Strings {"v", "t"});
    auto times = timestamp->create_column();
    for (int i = 0; i < 4; ++i) {
        times->insert_default();
    }
    Block block {{ColumnStruct::create(Columns {documents(variant), std::move(times)}), type, "s"}};
    for (const std::string zone : {"+08:00", "+05:45", "-03:30"}) {
        cctz::time_zone timezone;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(zone, timezone));
        std::shared_ptr<arrow::DataType> mapped;
        ASSERT_TRUE(ArrowFlightSchemaConvertor(zone).convert_to_arrow_type(type, &mapped).ok());
        ArrowFlightArrowBlockConvertor converter(arrow::schema({arrow::field("s", mapped, false)}),
                                                 timezone);
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(batch->schema()->field(0)->type()->Equals(mapped));
    }
}

TEST(ArrowFlightVariantTest, NativeMetadataContainsOnlySelectedRowKeys) {
    for (int rows : {32, 64}) {
        SCOPED_TRACE(rows);
        auto type = std::make_shared<DataTypeVariantV2>();
        auto column = type->create_column();
        JsonStringToVariantEncoder encoder;
        for (int i = 0; i < rows; ++i) {
            std::string key = std::string(200, 'k') + std::to_string(i);
            std::string json = "{\"" + key + "\":" + std::to_string(i) + "}";
            encoder.add_json({json.data(), json.size()});
        }
        auto encoded = encoder.finish_batch();
        ASSERT_EQ(encoded.metadata_ref().dict_size(), rows);
        assert_cast<ColumnVariantV2&>(*column).insert_encoded_batch(encoded);
        auto nulls = ColumnUInt8::create();
        nulls->get_data().resize_fill(rows, 0);
        nulls->get_data()[rows / 2] = 1;
        Block block {{ColumnNullable::create(std::move(column), std::move(nulls)),
                      make_nullable(type), "v"}};
        ArrowFlightArrowBlockConvertor converter(
                arrow::schema({arrow::field("v", native_variant())}), cctz::utc_time_zone());
        for (int start : {0, 3}) {
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch,
                                                     start, rows);
            ASSERT_TRUE(status.ok()) << status;
            size_t metadata_bytes = 0;
            for (int i = start; i < rows; ++i) {
                if (i == rows / 2) {
                    EXPECT_TRUE(batch->column(0)->IsNull(i - start));
                    continue;
                }
                VariantRef value = value_at(*batch->column(0), i - start);
                // Internal dictionary sharing must not multiply every other row's keys on the wire.
                EXPECT_EQ(value.metadata.dict_size(), 1);
                metadata_bytes += value.metadata.size;
                VariantRef child;
                std::string key = std::string(200, 'k') + std::to_string(i);
                ASSERT_TRUE(value.object_find({key.data(), key.size()}, &child));
                EXPECT_EQ(child.get_int(), i);
            }
            EXPECT_LT(metadata_bytes, static_cast<size_t>(rows - start) * 220);
        }
    }
}

TEST(ArrowFlightVariantTest, NestedNativeCompactionPreservesPhysicalScalars) {
    VariantBatchBuilder builder;
    for (std::string key : {"first", "second"}) {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key({key.data(), key.size()});
        row.add_int(1);
        object.finish();
        row.finish();
    }
    for (int kind = 0; kind < 5; ++kind) {
        auto row = builder.begin_row();
        switch (kind) {
        case 0:
            row.add_decimal(4200, 2, 4);
            break;
        case 1:
            row.add_float(42.0F);
            break;
        case 2:
            row.add_date(1);
            break;
        case 3:
            row.add_binary({"\0\xff", 2});
            break;
        case 4:
            row.add_string({"42", 2});
            break;
        }
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_decimal(4200, 2, 4);
        row.add_float(42.0F);
        row.add_string({"42", 2});
        array.finish();
        row.finish();
    }
    auto encoded = builder.finish_batch();
    ASSERT_EQ(encoded.metadata_ref().dict_size(), 2);
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(encoded);
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (size_t i = 0; i < encoded.num_rows(); ++i) {
        offsets->get_data().push_back(i + 1);
    }
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeVariantV2>());
    Block block {
            {ColumnArray::create(make_nullable(std::move(values)), std::move(offsets)), type, "a"}};
    ArrowFlightArrowBlockConvertor converter(
            arrow::schema({arrow::field("a", arrow::list(native_variant()), false)}),
            cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    const auto& elements = *static_cast<const arrow::ListArray&>(*batch->column(0)).values();
    for (int i = 0; i < elements.length(); ++i) {
        auto actual = value_at(elements, i);
        EXPECT_EQ(actual.metadata.dict_size(), i < 2 ? 1 : 0);
        if (i >= 2) {
            // Integral decimals and floats must retain physical type/scale rather than normalize to integers.
            auto expected = encoded.value_at(i);
            EXPECT_EQ(actual.basic_type(), expected.basic_type());
            if (actual.basic_type() == VariantBasicType::PRIMITIVE) {
                EXPECT_EQ(actual.primitive_id(), expected.primitive_id());
            }
            EXPECT_EQ(actual.value, expected.value);
        }
    }
}

TEST(ArrowFlightVariantTest, NativeCompactionKeepsTerminalEmptyContainersAtDepthLimit) {
    for (std::string terminal : {"[]", "{}"}) {
        std::string json = terminal;
        for (size_t i = 0; i < VARIANT_MAX_NESTING_DEPTH; ++i) {
            json = "[" + json + "]";
        }
        JsonStringToVariantEncoder encoder;
        encoder.add_json({json.data(), json.size()});
        encoder.add_json({R"({"unused":0})", 12});
        auto encoded = encoder.finish_batch();
        auto values = ColumnVariantV2::create();
        values->insert_encoded_batch(encoded);
        Block block {{std::move(values), std::make_shared<DataTypeVariantV2>(), "v"}};
        ArrowFlightArrowBlockConvertor converter(
                arrow::schema({arrow::field("v", native_variant(), false)}), cctz::utc_time_zone());
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        ASSERT_TRUE(status.ok()) << status;
        auto value = value_at(*batch->column(0), 0);
        EXPECT_EQ(value.metadata.dict_size(), 0);
        for (size_t i = 0; i < VARIANT_MAX_NESTING_DEPTH; ++i) {
            value = value.array_at(0);
        }
        EXPECT_EQ(value.num_elements(), 0);
        EXPECT_EQ(value.basic_type(),
                  terminal == "[]" ? VariantBasicType::ARRAY : VariantBasicType::OBJECT);
    }
}

TEST(ArrowFlightVariantTest, SchemaRpcPreservesNestedVariantExtensions) {
    ASSERT_TRUE(register_arrow_variant_extension().ok());
    for (const auto& type : std::vector<std::shared_ptr<arrow::DataType>> {
                 arrow::utf8(), native_variant(), arrow::list(native_variant()),
                 arrow::map(arrow::utf8(), native_variant()),
                 arrow::struct_({arrow::field("v", native_variant())}),
                 arrow::list(arrow::struct_({arrow::field("v", native_variant())}))}) {
        SCOPED_TRACE(type->ToString());
        auto schema = arrow::schema({arrow::field("result", type)});
        std::string serialized;
        // Result schema discovery happens before batch conversion and must also support nested extensions.
        auto status = serialize_arrow_schema(&schema, &serialized);
        ASSERT_TRUE(status.ok()) << status;
        auto input = arrow::io::BufferReader::FromString(serialized);
        auto opened = arrow::ipc::RecordBatchStreamReader::Open(input.get());
        ASSERT_TRUE(opened.ok()) << opened.status();
        auto reader = opened.ValueOrDie();
        EXPECT_TRUE(reader->schema()->Equals(*schema, true));
        auto next = reader->Next();
        ASSERT_TRUE(next.ok()) << next.status();
        if (next.ValueOrDie() != nullptr) {
            EXPECT_EQ(next.ValueOrDie()->num_rows(), 0);
        }
    }
}

TEST(ArrowFlightVariantTest, EmptyResultHasNativeSchema) {
    auto type = std::make_shared<DataTypeVariantV2>();
    Block block;
    block.insert({type->create_column(), type, "v"});
    ArrowFlightArrowBlockConvertor converter(
            arrow::schema({arrow::field("v", native_variant(), false)}), cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_rows(), 0);
    EXPECT_TRUE(batch->column(0)->type()->Equals(native_variant()));
}

TEST(ArrowFlightVariantTest, NestedArrayAndDefaultJsonMode) {
    auto type = std::make_shared<DataTypeVariantV2>();
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().assign({2, 4});
    auto array_type = std::make_shared<DataTypeArray>(type);
    Block block;
    block.insert({ColumnArray::create(make_nullable(documents(type)), std::move(offsets)),
                  array_type, "a"});
    auto schema = arrow::schema(
            {arrow::field("a", arrow::list(arrow::field("item", native_variant(), true)), false)});
    ArrowFlightArrowBlockConvertor converter(schema, cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(batch->ValidateFull().ok());
    const auto& values = *static_cast<const arrow::ListArray&>(*batch->column(0)).values();
    EXPECT_EQ(value_at(values, 2).get_int(), 42);
    EXPECT_EQ(value_at(values, 3).get_string().to_string(), "text");

    DorisArrowBlockConvertor json(block, "UTC", cctz::utc_time_zone());
    ASSERT_TRUE(json.init().ok());
    ASSERT_TRUE(json.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
    EXPECT_EQ(static_cast<const arrow::ListArray&>(*batch->column(0)).values()->type_id(),
              arrow::Type::STRING);
    // Native Variant bindings belong to Flight, not the ordinary Arrow export path.
    EXPECT_FALSE(DorisArrowBlockConvertor(schema, cctz::utc_time_zone())
                         .convert_to_arrow(block, arrow::default_memory_pool(), &batch)
                         .ok());
}

} // namespace
} // namespace doris
