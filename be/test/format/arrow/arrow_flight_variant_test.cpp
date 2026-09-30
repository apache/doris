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
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_varbinary.h"
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

Status convert_legacy_root(MutableColumnPtr column, DataTypePtr type, bool nested,
                           std::shared_ptr<arrow::RecordBatch>* batch) {
    if (nested) {
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->get_data().push_back(column->size());
        column = ColumnArray::create(make_nullable(std::move(column)), std::move(offsets));
        type = std::make_shared<DataTypeArray>(type);
    }
    if (type->get_primitive_type() != TYPE_VARIANT) {
        auto legacy = ColumnVariant::create(0);
        legacy->create_root(type, std::move(column));
        column = std::move(legacy);
    }
    assert_cast<ColumnVariant&>(*column).finalize();
    Block block {{std::move(column), std::make_shared<DataTypeVariant>(), "v"}};
    ArrowFlightArrowBlockConvertor converter(arrow::schema({arrow::field("v", native_variant())}),
                                             cctz::utc_time_zone());
    return converter.convert_to_arrow(block, arrow::default_memory_pool(), batch);
}

TEST(ArrowFlightVariantTest, LegacyTimeRejectsDurationsOutsideDay) {
    for (double micros : {-3600000000.0, -1.0, 0.0, 86399999999.0, 86400000000.0, 90000000000.0}) {
        for (bool nested : {false, true}) {
            auto times = ColumnTimeV2::create();
            times->insert_value(micros);
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = convert_legacy_root(std::move(times), std::make_shared<DataTypeTimeV2>(6),
                                              nested, &batch);
            if (micros < 0 || micros >= 86400000000.0) {
                EXPECT_FALSE(status.ok());
                EXPECT_NE(status.to_string().find("enable_arrow_flight_sql_native_variant=false"),
                          std::string::npos);
            } else {
                ASSERT_TRUE(status.ok()) << status;
                auto value = value_at(*batch->column(0), 0);
                if (nested) {
                    value = value.array_at(0);
                }
                EXPECT_EQ(value.get_time_ntz_micros(), static_cast<int64_t>(micros));
            }
        }
    }
}

TEST(ArrowFlightVariantTest, LegacyMapRejectsNullKeysWithoutCollidingWithStrings) {
    for (bool null_key : {false, true}) {
        for (bool nested : {false, true}) {
            auto keys = ColumnString::create();
            keys->insert_data("key", 3);
            keys->insert_data("null", 4);
            auto nulls = ColumnUInt8::create();
            nulls->get_data().assign({static_cast<UInt8>(null_key), 0});
            auto values = ColumnInt32::create();
            values->get_data().assign({1, 2});
            auto offsets = ColumnArray::ColumnOffsets::create();
            offsets->get_data().push_back(2);
            auto map = ColumnMap::create(ColumnNullable::create(std::move(keys), std::move(nulls)),
                                         make_nullable(std::move(values)), std::move(offsets));
            auto type =
                    std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                                  make_nullable(std::make_shared<DataTypeInt32>()));
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = convert_legacy_root(std::move(map), type, nested, &batch);
            if (null_key) {
                EXPECT_FALSE(status.ok());
                EXPECT_NE(status.to_string().find("MAP with NULL keys"), std::string::npos);
                EXPECT_NE(status.to_string().find("enable_arrow_flight_sql_native_variant=false"),
                          std::string::npos);
            } else {
                ASSERT_TRUE(status.ok()) << status;
                auto value = value_at(*batch->column(0), 0);
                if (nested) {
                    value = value.array_at(0);
                }
                VariantRef child;
                ASSERT_TRUE(value.object_find({"null", 4}, &child));
                EXPECT_EQ(child.get_int(), 2);
            }
        }
    }
}

TEST(ArrowFlightVariantTest, LegacyBinaryPreservesBytesInRootsAndArrays) {
    for (bool nested : {false, true}) {
        auto type = std::make_shared<DataTypeVarbinary>();
        auto values = type->create_column();
        const std::string bytes(
                "\0\xff\x80"
                "42",
                5);
        values->insert_data(bytes.data(), bytes.size());
        values->insert_data("", 0);
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = convert_legacy_root(std::move(values), type, nested, &batch);
        ASSERT_TRUE(status.ok()) << status;
        for (int i = 0; i < 2; ++i) {
            auto value = nested ? value_at(*batch->column(0), 0).array_at(i)
                                : value_at(*batch->column(0), i);
            EXPECT_EQ(value.get_binary().to_string(), i == 0 ? bytes : "");
        }
    }
}

TEST(ArrowFlightVariantTest, LegacyDocumentsPreserveTypedPaths) {
    // Dense paths, sparse paths and document snapshots must all retain typed values.
    for (int storage = 0; storage < 3; ++storage) {
        for (bool nested : {false, true}) {
            SCOPED_TRACE(::testing::Message() << "storage=" << storage << " nested=" << nested);
            auto legacy = ColumnVariant::create(0);
            auto root_type = make_nullable(std::make_shared<DataTypeString>());
            auto roots = root_type->create_column();
            roots->insert_default();
            legacy->create_root(root_type, std::move(roots));
            const Int128 exact = 900719925474099301LL;
            auto decimal_type = make_nullable(std::make_shared<DataTypeDecimal128>(20, 2));
            auto decimals = ColumnDecimal128V3::create(0, 2);
            decimals->insert_value(Decimal128V3(exact));
            auto decimal_column = make_nullable(std::move(decimals));
            auto date_type = make_nullable(std::make_shared<DataTypeDateV2>());
            auto dates = date_type->create_column();
            DateV2Value<DateV2ValueType> date;
            date.unchecked_set_time(2020, 1, 2, 0, 0, 0);
            dates->insert(Field::create_field<TYPE_DATEV2>(date));
            if (storage == 0) {
                ASSERT_TRUE(legacy->add_sub_column(PathInData("nested.amount"),
                                                   decimal_column->assert_mutable(), decimal_type));
                ASSERT_TRUE(legacy->add_sub_column(PathInData("nested.date"), std::move(dates),
                                                   date_type));
            } else {
                auto& map = assert_cast<ColumnMap&>(
                        storage == 1 ? legacy->get_sparse_column_mutable()
                                     : legacy->get_doc_value_column_mutable());
                auto& keys = assert_cast<ColumnString&>(map.get_keys());
                auto& values = assert_cast<ColumnString&>(map.get_values());
                ColumnVariant::Subcolumn decimal(decimal_column->assert_mutable(), decimal_type,
                                                 true);
                ColumnVariant::Subcolumn date_column(std::move(dates), date_type, true);
                decimal.serialize_to_binary_column(&keys, "nested.amount", &values, 0);
                date_column.serialize_to_binary_column(&keys, "nested.date", &values, 0);
                map.get_offsets()[0] = 2;
            }
            legacy->finalize();
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = convert_legacy_root(std::move(legacy),
                                              std::make_shared<DataTypeVariant>(), nested, &batch);
            ASSERT_TRUE(status.ok()) << status;
            auto value = value_at(*batch->column(0), 0);
            if (nested) {
                value = value.array_at(0);
            }
            VariantRef object;
            ASSERT_TRUE(value.object_find({"nested", 6}, &object));
            VariantRef amount;
            ASSERT_TRUE(object.object_find({"amount", 6}, &amount));
            ASSERT_EQ(amount.primitive_id(), VariantPrimitiveId::DECIMAL16);
            EXPECT_EQ(amount.get_decimal().unscaled, exact);
            EXPECT_EQ(amount.get_decimal().scale, 2);
            VariantRef day;
            ASSERT_TRUE(object.object_find({"date", 4}, &day));
            EXPECT_EQ(day.primitive_id(), VariantPrimitiveId::DATE);
            EXPECT_EQ(day.get_date(), 18263);
        }
    }
}

TEST(ArrowFlightVariantTest, LegacyPendingDocumentDefaultsPreserveValuesAndSlices) {
    auto legacy = ColumnVariant::create(0);
    auto root_type = make_nullable(std::make_shared<DataTypeString>());
    auto roots = root_type->create_column();
    roots->insert_many_defaults(3);
    legacy->create_root(root_type, std::move(roots));
    auto decimal_type = std::make_shared<DataTypeDecimal128>(20, 2);
    auto decimals = ColumnDecimal128V3::create(0, 2);
    const Int128 exact = 900719925474099301LL;
    decimals->insert_value(Decimal128V3(exact));
    ASSERT_TRUE(legacy->add_sub_column(PathInData("amount"), 3));
    auto* amount = legacy->get_subcolumn(PathInData("amount"));
    *amount = ColumnVariant::Subcolumn(1, true);
    amount->insert(decimal_type->get_field_with_data_type(*decimals, 0));
    amount->insert_default();
    // Scans can return lazy prefix/suffix defaults; output must not finalize the shared input.
    ASSERT_FALSE(amount->is_finalized());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 0, 1});
    Block block {{ColumnNullable::create(std::move(legacy), std::move(nulls)),
                  make_nullable(std::make_shared<DataTypeVariant>()), "v"}};
    ArrowFlightArrowBlockConvertor converter(arrow::schema({arrow::field("v", native_variant())}),
                                             cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(value_at(*batch->column(0), 0).num_elements(), 0);
    VariantRef value;
    ASSERT_TRUE(value_at(*batch->column(0), 1).object_find({"amount", 6}, &value));
    EXPECT_EQ(value.get_decimal().unscaled, exact);
    EXPECT_EQ(value.get_decimal().scale, 2);
    EXPECT_TRUE(batch->column(0)->IsNull(2));
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 1, 3).ok());
    ASSERT_TRUE(value_at(*batch->column(0), 0).object_find({"amount", 6}, &value));
    EXPECT_EQ(value.get_decimal().unscaled, exact);
    EXPECT_TRUE(batch->column(0)->IsNull(1));
    EXPECT_FALSE(amount->is_finalized());
}

TEST(ArrowFlightVariantTest, LegacyScanDocumentWithPendingArrayDefaults) {
    auto type = std::make_shared<DataTypeVariant>(9);
    auto column = type->create_column();
    DataTypeSerDe::FormatOptions options;
    for (std::string json : {"42", R"("text")", R"({"a":[1,null,"x"]})", "null"}) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(
                type->get_serde()->deserialize_one_cell_from_json(*column, slice, options).ok());
    }
    auto& legacy = assert_cast<ColumnVariant&>(*column);
    legacy.get_subcolumns().get_mutable_root()->data.finalize();
    auto* array = legacy.get_subcolumn(PathInData("a"));
    ASSERT_NE(array, nullptr);
    ASSERT_FALSE(array->is_finalized());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 0, 0, 1});
    Block block {{ColumnNullable::create(std::move(column), std::move(nulls)), make_nullable(type),
                  "v"}};
    ArrowFlightArrowBlockConvertor converter(arrow::schema({arrow::field("v", native_variant())}),
                                             cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(value_at(*batch->column(0), 0).get_int(), 42);
    EXPECT_EQ(value_at(*batch->column(0), 1).get_string().to_string(), "text");
    VariantRef value;
    ASSERT_TRUE(value_at(*batch->column(0), 2).object_find({"a", 1}, &value));
    ASSERT_EQ(value.num_elements(), 3);
    EXPECT_EQ(value.array_at(0).get_int(), 1);
    EXPECT_TRUE(value.array_at(1).is_null());
    EXPECT_EQ(value.array_at(2).get_string().to_string(), "x");
    EXPECT_TRUE(batch->column(0)->IsNull(3));
    EXPECT_FALSE(array->is_finalized());
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

TEST(ArrowFlightVariantTest, LegacyScalarNullKeepsEmptyObjectAndSqlNull) {
    auto type = std::make_shared<DataTypeVariant>();
    auto column = type->create_column();
    DataTypeSerDe::FormatOptions options;
    for (std::string json : {"42", "null", "null"}) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(
                type->get_serde()->deserialize_one_cell_from_json(*column, slice, options).ok());
    }
    auto& legacy = assert_cast<ColumnVariant&>(*column);
    legacy.finalize();
    ASSERT_TRUE(legacy.is_scalar_variant());
    std::string text;
    legacy.serialize_one_row_to_string(1, &text, options);
    ASSERT_EQ(text, "{}");
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 0, 1});
    Block block {{ColumnNullable::create(std::move(column), std::move(nulls)), make_nullable(type),
                  "v"}};
    ArrowFlightArrowBlockConvertor converter(arrow::schema({arrow::field("v", native_variant())}),
                                             cctz::utc_time_zone());
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
    EXPECT_EQ(value_at(*batch->column(0), 0).get_int(), 42);
    EXPECT_EQ(value_at(*batch->column(0), 1).basic_type(), VariantBasicType::OBJECT);
    EXPECT_TRUE(batch->column(0)->IsNull(2));
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 1, 3).ok());
    EXPECT_EQ(value_at(*batch->column(0), 0).basic_type(), VariantBasicType::OBJECT);
    EXPECT_TRUE(batch->column(0)->IsNull(1));
}

TEST(ArrowFlightVariantTest, LegacyCompositeRootsAndNestedLeaves) {
    const __int128 exact = 900719925474099301LL;
    for (int family = 0; family < 5; ++family) {
        for (int depth = family == 3 ? 1 : 0; depth < 3; ++depth) {
            SCOPED_TRACE(::testing::Message() << "family=" << family << " depth=" << depth);
            DataTypePtr type;
            MutableColumnPtr column;
            if (family <= 1 || family == 4) {
                auto decimal_type = std::make_shared<DataTypeDecimal128>(20, 2);
                auto decimals = ColumnDecimal128V3::create(0, 2);
                decimals->insert_value(Decimal128V3(exact));
                if (family != 1) {
                    DataTypePtr key_type = family == 0
                                                   ? DataTypePtr(std::make_shared<DataTypeString>())
                                                   : DataTypePtr(std::make_shared<DataTypeInt32>());
                    auto keys = key_type->create_column();
                    if (family == 0) {
                        keys->insert_data("d", 1);
                    } else {
                        keys->insert(Field::create_field<TYPE_INT>(-7));
                    }
                    auto offsets = ColumnArray::ColumnOffsets::create();
                    offsets->get_data().push_back(1);
                    type = std::make_shared<DataTypeMap>(make_nullable(key_type),
                                                         make_nullable(decimal_type));
                    column = ColumnMap::create(make_nullable(std::move(keys)),
                                               make_nullable(std::move(decimals)),
                                               std::move(offsets));
                } else {
                    type = std::make_shared<DataTypeStruct>(DataTypes {decimal_type},
                                                            Strings {"d"});
                    column = ColumnStruct::create(Columns {std::move(decimals)});
                }
            } else if (family == 2) {
                type = std::make_shared<DataTypeTimeV2>(6);
                auto times = ColumnTimeV2::create();
                times->insert_value(3'723'123'456.0);
                column = std::move(times);
            } else {
                type = std::make_shared<DataTypeVariant>();
                column = documents(type)->cut(0, 1)->assert_mutable();
            }
            for (int level = 0; level < depth; ++level) {
                auto offsets = ColumnArray::ColumnOffsets::create();
                offsets->get_data().push_back(1);
                column = ColumnArray::create(make_nullable(std::move(column)), std::move(offsets));
                type = std::make_shared<DataTypeArray>(type);
            }
            auto variant = ColumnVariant::create(0);
            variant->create_root(type, std::move(column));
            variant->finalize();
            Block block {{std::move(variant), std::make_shared<DataTypeVariant>(), "v"}};
            ArrowFlightArrowBlockConvertor converter(
                    arrow::schema({arrow::field("v", native_variant(), false)}),
                    cctz::utc_time_zone());
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
            EXPECT_TRUE(status.ok()) << status;
            if (!status.ok()) {
                continue;
            }
            auto value = value_at(*batch->column(0), 0);
            for (int level = 0; level < depth; ++level) {
                value = value.array_at(0);
            }
            if (family <= 1 || family == 4) {
                VariantRef decimal;
                const std::string key = family == 4 ? "-7" : "d";
                ASSERT_TRUE(value.object_find({key.data(), key.size()}, &decimal));
                EXPECT_EQ(decimal.get_decimal().unscaled, exact);
                EXPECT_EQ(decimal.get_decimal().scale, 2);
            } else if (family == 2) {
                EXPECT_EQ(value.get_time_ntz_micros(), 3'723'123'456LL);
            } else {
                VariantRef array;
                ASSERT_TRUE(value.object_find({"a", 1}, &array));
                EXPECT_EQ(array.array_at(0).get_int(), 1);
                EXPECT_TRUE(array.array_at(1).is_null());
                EXPECT_EQ(array.array_at(2).get_string().to_string(), "x");
            }
        }
    }
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
