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
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
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

TEST(ArrowFlightVariantTest, NativeResultPreservesValuesAndSqlNulls) {
    TimezoneUtils::load_timezones_to_cache();
    ASSERT_TRUE(register_arrow_variant_extension().ok());
    for (DataTypePtr type : {DataTypePtr(std::make_shared<DataTypeVariant>()),
                             DataTypePtr(std::make_shared<DataTypeVariantV2>())}) {
        auto nulls = ColumnUInt8::create();
        nulls->get_data().assign({0, 0, 0, 1});
        Block block;
        block.insert({ColumnNullable::create(documents(type), std::move(nulls)),
                      make_nullable(type), "v"});
        auto schema = arrow::schema({arrow::field("v", native_variant())});
        ArrowFlightArrowBlockConvertor converter(schema, cctz::utc_time_zone());
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(batch->ValidateFull().ok());
        EXPECT_EQ(batch->num_rows(), 4);
        EXPECT_FALSE(batch->column(0)->IsNull(1));
        // Legacy Variant already renders a null root as {}; only V2 retains Variant null.
        EXPECT_EQ(value_at(*batch->column(0), 1).is_null(),
                  dynamic_cast<const DataTypeVariantV2*>(type.get()) != nullptr);
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
        ASSERT_TRUE(
                converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 1, 3).ok());
        EXPECT_EQ(value_at(*batch->column(0), 0).is_null(),
                  dynamic_cast<const DataTypeVariantV2*>(type.get()) != nullptr);
        EXPECT_EQ(value_at(*batch->column(0), 1).get_int(), 42);
    }
}

TEST(ArrowFlightVariantTest, SchemaMappingAndConstantScalar) {
    for (DataTypePtr type : {DataTypePtr(std::make_shared<DataTypeVariant>()),
                             DataTypePtr(std::make_shared<DataTypeVariantV2>())}) {
        std::shared_ptr<arrow::DataType> mapped;
        ASSERT_TRUE(ArrowFlightSchemaConvertor("UTC").convert_to_arrow_type(type, &mapped).ok());
        EXPECT_TRUE(mapped->Equals(arrow::utf8()));
        ASSERT_TRUE(
                ArrowFlightSchemaConvertor("UTC", true).convert_to_arrow_type(type, &mapped).ok());
        EXPECT_TRUE(mapped->Equals(native_variant()));
        auto column = type->create_column();
        std::string json = R"("te\"xt\n\u4e2d")";
        Slice slice(json.data(), json.size());
        DataTypeSerDe::FormatOptions options;
        ASSERT_TRUE(
                type->get_serde()->deserialize_one_cell_from_json(*column, slice, options).ok());
        if (auto* legacy = check_and_get_column<ColumnVariant>(*column)) {
            legacy->finalize();
        }
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
    ASSERT_TRUE(ArrowFlightSchemaConvertor("UTC", true).convert_to_arrow_type(type, &mapped).ok());
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

TEST(ArrowFlightVariantTest, LegacyTypedDecimalDoesNotRoundThroughDouble) {
    auto decimal_type = std::make_shared<DataTypeDecimal128>(20, 2);
    auto decimals = ColumnDecimal128V3::create(0, 2);
    const __int128 unscaled = 900719925474099301LL;
    decimals->insert_value(Decimal128V3(unscaled));
    auto values = ColumnVariant::create(0);
    values->create_root(decimal_type, std::move(decimals));
    values->finalize();
    auto type = std::make_shared<DataTypeVariant>();
    Block block;
    block.insert({std::move(values), type, "v"});
    ArrowFlightArrowBlockConvertor converter(
            arrow::schema({arrow::field("v", native_variant(), false)}), cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    auto decimal = value_at(*batch->column(0), 0).get_decimal();
    EXPECT_EQ(decimal.unscaled, unscaled);
    EXPECT_EQ(decimal.scale, 2);
}

TEST(ArrowFlightVariantTest, LegacyArrayPreservesExactDecimal) {
    auto decimal_type = std::make_shared<DataTypeDecimal128>(20, 2);
    auto decimals = ColumnDecimal128V3::create(0, 2);
    const __int128 unscaled = 900719925474099301LL;
    decimals->insert_value(Decimal128V3(unscaled));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(1);
    auto values = ColumnVariant::create(0);
    values->create_root(
            std::make_shared<DataTypeArray>(decimal_type),
            ColumnArray::create(make_nullable(std::move(decimals)), std::move(offsets)));
    values->finalize();
    Block block {{std::move(values), std::make_shared<DataTypeVariant>(), "v"}};
    ArrowFlightArrowBlockConvertor converter(
            arrow::schema({arrow::field("v", native_variant(), false)}), cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    auto element = value_at(*batch->column(0), 0).array_at(0);
    ASSERT_EQ(element.primitive_id(), VariantPrimitiveId::DECIMAL16);
    EXPECT_EQ(element.get_decimal().unscaled, unscaled);
    EXPECT_EQ(element.get_decimal().scale, 2);
}

TEST(ArrowFlightVariantTest, LegacyArrayPreservesNonFiniteNumbers) {
    auto numbers = ColumnFloat64::create();
    numbers->insert_value(std::numeric_limits<double>::quiet_NaN());
    numbers->insert_value(std::numeric_limits<double>::infinity());
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(2);
    auto values = ColumnVariant::create(0);
    values->create_root(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()),
                        ColumnArray::create(make_nullable(std::move(numbers)), std::move(offsets)));
    values->finalize();
    Block block {{std::move(values), std::make_shared<DataTypeVariant>(), "v"}};
    ArrowFlightArrowBlockConvertor converter(
            arrow::schema({arrow::field("v", native_variant(), false)}), cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    auto array = value_at(*batch->column(0), 0);
    EXPECT_TRUE(std::isnan(array.array_at(0).get_double()));
    EXPECT_TRUE(std::isinf(array.array_at(1).get_double()));
}

TEST(ArrowFlightVariantTest, MixedDateRootRetainsDateIdentityAndSlices) {
    auto date_type = make_nullable(std::make_shared<DataTypeDateV2>());
    auto dates = date_type->create_column();
    DateV2Value<DateV2ValueType> date;
    date.unchecked_set_time(2020, 1, 2, 0, 0, 0);
    dates->insert(Field::create_field<TYPE_DATEV2>(date));
    dates->insert_default();
    auto values = ColumnVariant::create(0);
    values->create_root(date_type, std::move(dates));
    auto object_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto object = object_type->create_column();
    object->insert_default();
    object->insert(Field::create_field<TYPE_INT>(7));
    ASSERT_TRUE(values->add_sub_column(PathInData("a"), std::move(object), object_type));
    values->finalize();
    ASSERT_FALSE(values->is_scalar_variant());
    Block block {{std::move(values), std::make_shared<DataTypeVariant>(), "v"}};
    ArrowFlightArrowBlockConvertor converter(
            arrow::schema({arrow::field("v", native_variant(), false)}), cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(value_at(*batch->column(0), 0).get_date(), 18263);
    EXPECT_EQ(value_at(*batch->column(0), 1).basic_type(), VariantBasicType::OBJECT);
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 1, 2).ok());
    EXPECT_EQ(value_at(*batch->column(0), 0).basic_type(), VariantBasicType::OBJECT);
}

TEST(ArrowFlightVariantTest, MixedStringRootsKeepStringIdentity) {
    for (auto primitive : {TYPE_CHAR, TYPE_VARCHAR, TYPE_STRING}) {
        auto type = std::make_shared<DataTypeString>(-1, primitive);
        auto root = type->create_column();
        for (const auto& text : {"hello", "true", "42", ""}) {
            root->insert_data(text, strlen(text));
        }
        auto root_nulls = ColumnUInt8::create();
        root_nulls->get_data().assign({0, 0, 0, 1});
        auto values = ColumnVariant::create(0);
        values->create_root(make_nullable(type),
                            ColumnNullable::create(std::move(root), std::move(root_nulls)));
        auto object_type = make_nullable(std::make_shared<DataTypeInt32>());
        auto object = object_type->create_column();
        object->insert_default();
        object->insert_default();
        object->insert_default();
        object->insert(Field::create_field<TYPE_INT>(7));
        ASSERT_TRUE(values->add_sub_column(PathInData("a"), std::move(object), object_type));
        values->finalize();
        ASSERT_FALSE(values->is_scalar_variant());
        Block block {{std::move(values), std::make_shared<DataTypeVariant>(), "v"}};
        ArrowFlightArrowBlockConvertor converter(
                arrow::schema({arrow::field("v", native_variant(), false)}), cctz::utc_time_zone());
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(value_at(*batch->column(0), 0).get_string().to_string(), "hello");
        EXPECT_EQ(value_at(*batch->column(0), 1).get_string().to_string(), "true");
        EXPECT_EQ(value_at(*batch->column(0), 2).get_string().to_string(), "42");
        EXPECT_EQ(value_at(*batch->column(0), 3).basic_type(), VariantBasicType::OBJECT);
    }
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
        ASSERT_TRUE(convert_to_arrow_type(type, &mapped, zone, true, true).ok());
        ArrowFlightArrowBlockConvertor converter(arrow::schema({arrow::field("s", mapped, false)}),
                                                 timezone);
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(batch->schema()->field(0)->type()->Equals(mapped));
    }
}

TEST(ArrowFlightVariantTest, LegacyDepthLimitExplainsNativeModeRestriction) {
    std::string json = "1";
    for (int i = 0; i < 129; ++i) {
        json = R"({"a":)" + json + "}";
    }
    auto type = std::make_shared<DataTypeVariant>();
    auto values = type->create_column();
    Slice slice(json.data(), json.size());
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(type->get_serde()->deserialize_one_cell_from_json(*values, slice, options).ok());
    assert_cast<ColumnVariant&>(*values).finalize();
    Block block {{std::move(values), type, "v"}};
    ArrowFlightArrowBlockConvertor utf8(block, "UTC", cctz::utc_time_zone());
    ASSERT_TRUE(utf8.init().ok());
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(utf8.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
    ArrowFlightArrowBlockConvertor native(
            arrow::schema({arrow::field("v", native_variant(), false)}), cctz::utc_time_zone());
    auto status = native.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("enable_arrow_flight_sql_native_variant=false"),
              std::string::npos);
}

TEST(ArrowFlightVariantTest, EmptyResultHasNativeSchema) {
    for (DataTypePtr type : {DataTypePtr(std::make_shared<DataTypeVariant>()),
                             DataTypePtr(std::make_shared<DataTypeVariantV2>())}) {
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

    ArrowFlightArrowBlockConvertor json(block, "UTC", cctz::utc_time_zone());
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
