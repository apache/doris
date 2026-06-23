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

#include "format_v2/parquet/parquet_type.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"

namespace doris::format::parquet {
namespace {

::parquet::SchemaDescriptor make_descriptor(const ::parquet::schema::NodePtr& node) {
    auto schema =
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, {node});
    ::parquet::SchemaDescriptor descriptor;
    descriptor.Init(schema);
    return descriptor;
}

ParquetTypeDescriptor resolve_node(const ::parquet::schema::NodePtr& node) {
    auto descriptor = make_descriptor(node);
    return resolve_parquet_type(descriptor.Column(0));
}

PrimitiveType primitive_type(const DataTypePtr& type) {
    return remove_nullable(type)->get_primitive_type();
}

int scale_of(const DataTypePtr& type) {
    return remove_nullable(type)->get_scale();
}

std::shared_ptr<arrow::Array> make_float16_array() {
    arrow::HalfFloatBuilder builder;
    EXPECT_TRUE(builder.Append(0x3E00).ok());
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

ParquetTypeDescriptor resolve_arrow_float16_type() {
    const auto schema = arrow::schema({arrow::field("f16", arrow::float16(), true)});
    const auto table = arrow::Table::Make(schema, {make_float16_array()});
    auto out_result = arrow::io::BufferOutputStream::Create();
    EXPECT_TRUE(out_result.ok());
    auto out = *out_result;
    EXPECT_TRUE(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1).ok());
    auto buffer_result = out->Finish();
    EXPECT_TRUE(buffer_result.ok());

    auto reader = ::parquet::ParquetFileReader::Open(
            std::make_shared<arrow::io::BufferReader>(*buffer_result));
    return resolve_parquet_type(reader->metadata()->schema()->Column(0));
}

} // namespace

TEST(ParquetTypeTest, ResolveLogicalIntegerMappings) {
    struct Case {
        int bit_width;
        bool is_signed;
        PrimitiveType expected_type;
        bool expected_unsigned;
    };
    const std::vector<Case> cases = {
            {8, true, TYPE_TINYINT, false},   {8, false, TYPE_SMALLINT, true},
            {16, true, TYPE_SMALLINT, false}, {16, false, TYPE_INT, true},
            {32, true, TYPE_INT, false},      {32, false, TYPE_BIGINT, true},
            {64, true, TYPE_BIGINT, false},   {64, false, TYPE_LARGEINT, true},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.bit_width);
        const auto node = ::parquet::schema::PrimitiveNode::Make(
                "c", ::parquet::Repetition::REQUIRED,
                ::parquet::LogicalType::Int(test_case.bit_width, test_case.is_signed),
                test_case.bit_width == 64 ? ::parquet::Type::INT64 : ::parquet::Type::INT32);
        const auto type = resolve_node(node);
        ASSERT_NE(type.doris_type, nullptr);
        EXPECT_EQ(primitive_type(type.doris_type), test_case.expected_type);
        EXPECT_EQ(type.integer_bit_width, test_case.bit_width);
        EXPECT_EQ(type.is_unsigned_integer, test_case.expected_unsigned);
        EXPECT_TRUE(type.supports_record_reader);
    }
}

TEST(ParquetTypeTest, ResolveLogicalTimeAndTimestampMappings) {
    const auto time_millis = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "time_ms", ::parquet::Repetition::REQUIRED,
            ::parquet::LogicalType::Time(false, ::parquet::LogicalType::TimeUnit::MILLIS),
            ::parquet::Type::INT32));
    ASSERT_NE(time_millis.doris_type, nullptr);
    EXPECT_EQ(primitive_type(time_millis.doris_type), TYPE_TIMEV2);
    EXPECT_EQ(time_millis.time_unit, ParquetTimeUnit::MILLIS);
    EXPECT_EQ(time_millis.extra_type_info, ParquetExtraTypeInfo::UNIT_MS);

    const auto time_micros = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "time_us", ::parquet::Repetition::REQUIRED,
            ::parquet::LogicalType::Time(false, ::parquet::LogicalType::TimeUnit::MICROS),
            ::parquet::Type::INT64));
    ASSERT_NE(time_micros.doris_type, nullptr);
    EXPECT_EQ(primitive_type(time_micros.doris_type), TYPE_TIMEV2);
    EXPECT_EQ(time_micros.time_unit, ParquetTimeUnit::MICROS);
    EXPECT_EQ(time_micros.extra_type_info, ParquetExtraTypeInfo::UNIT_MICROS);

    const auto adjusted_time = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "time_adjusted", ::parquet::Repetition::REQUIRED,
            ::parquet::LogicalType::Time(true, ::parquet::LogicalType::TimeUnit::MILLIS),
            ::parquet::Type::INT32));
    EXPECT_EQ(adjusted_time.doris_type, nullptr);
    EXPECT_FALSE(adjusted_time.supports_record_reader);
    EXPECT_FALSE(adjusted_time.unsupported_reason.empty());

    const auto timestamp_nanos = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "ts_ns", ::parquet::Repetition::OPTIONAL,
            ::parquet::LogicalType::Timestamp(true, ::parquet::LogicalType::TimeUnit::NANOS),
            ::parquet::Type::INT64));
    ASSERT_NE(timestamp_nanos.doris_type, nullptr);
    EXPECT_TRUE(timestamp_nanos.doris_type->is_nullable());
    EXPECT_EQ(primitive_type(timestamp_nanos.doris_type), TYPE_DATETIMEV2);
    EXPECT_TRUE(timestamp_nanos.is_timestamp);
    EXPECT_TRUE(timestamp_nanos.timestamp_is_adjusted_to_utc);
    EXPECT_EQ(timestamp_nanos.time_unit, ParquetTimeUnit::NANOS);
    EXPECT_EQ(timestamp_nanos.extra_type_info, ParquetExtraTypeInfo::UNIT_NS);
}

TEST(ParquetTypeTest, ResolveLogicalTimestampMatrix) {
    struct Case {
        ::parquet::LogicalType::TimeUnit::unit parquet_unit;
        bool adjusted_to_utc;
        ParquetTimeUnit expected_unit;
        ParquetExtraTypeInfo expected_extra;
        int expected_scale;
    };
    const std::vector<Case> cases = {
            {::parquet::LogicalType::TimeUnit::MILLIS, true, ParquetTimeUnit::MILLIS,
             ParquetExtraTypeInfo::UNIT_MS, 3},
            {::parquet::LogicalType::TimeUnit::MILLIS, false, ParquetTimeUnit::MILLIS,
             ParquetExtraTypeInfo::UNIT_MS, 3},
            {::parquet::LogicalType::TimeUnit::MICROS, true, ParquetTimeUnit::MICROS,
             ParquetExtraTypeInfo::UNIT_MICROS, 6},
            {::parquet::LogicalType::TimeUnit::MICROS, false, ParquetTimeUnit::MICROS,
             ParquetExtraTypeInfo::UNIT_MICROS, 6},
            {::parquet::LogicalType::TimeUnit::NANOS, true, ParquetTimeUnit::NANOS,
             ParquetExtraTypeInfo::UNIT_NS, 6},
            {::parquet::LogicalType::TimeUnit::NANOS, false, ParquetTimeUnit::NANOS,
             ParquetExtraTypeInfo::UNIT_NS, 6},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.expected_scale);
        const auto type = resolve_node(::parquet::schema::PrimitiveNode::Make(
                "ts", ::parquet::Repetition::OPTIONAL,
                ::parquet::LogicalType::Timestamp(test_case.adjusted_to_utc,
                                                  test_case.parquet_unit),
                ::parquet::Type::INT64));
        ASSERT_NE(type.doris_type, nullptr);
        EXPECT_TRUE(type.doris_type->is_nullable());
        EXPECT_EQ(primitive_type(type.doris_type), TYPE_DATETIMEV2);
        EXPECT_EQ(scale_of(type.doris_type), test_case.expected_scale);
        EXPECT_TRUE(type.is_timestamp);
        EXPECT_EQ(type.timestamp_is_adjusted_to_utc, test_case.adjusted_to_utc);
        EXPECT_EQ(type.time_unit, test_case.expected_unit);
        EXPECT_EQ(type.extra_type_info, test_case.expected_extra);
    }
}

TEST(ParquetTypeTest, ConvertedTimeIsRejectedButConvertedTimestampIsSupported) {
    const auto converted_time = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "time_ms", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
            ::parquet::ConvertedType::TIME_MILLIS));
    EXPECT_EQ(converted_time.doris_type, nullptr);
    EXPECT_FALSE(converted_time.supports_record_reader);
    EXPECT_FALSE(converted_time.unsupported_reason.empty());

    const auto converted_timestamp = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "ts_ms", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64,
            ::parquet::ConvertedType::TIMESTAMP_MILLIS));
    ASSERT_NE(converted_timestamp.doris_type, nullptr);
    EXPECT_EQ(primitive_type(converted_timestamp.doris_type), TYPE_DATETIMEV2);
    EXPECT_TRUE(converted_timestamp.is_timestamp);
    EXPECT_TRUE(converted_timestamp.timestamp_is_adjusted_to_utc);
    EXPECT_EQ(converted_timestamp.time_unit, ParquetTimeUnit::MILLIS);

    const auto converted_timestamp_micros = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "ts_us", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT64,
            ::parquet::ConvertedType::TIMESTAMP_MICROS));
    ASSERT_NE(converted_timestamp_micros.doris_type, nullptr);
    EXPECT_TRUE(converted_timestamp_micros.doris_type->is_nullable());
    EXPECT_EQ(primitive_type(converted_timestamp_micros.doris_type), TYPE_DATETIMEV2);
    EXPECT_EQ(scale_of(converted_timestamp_micros.doris_type), 6);
    EXPECT_TRUE(converted_timestamp_micros.is_timestamp);
    EXPECT_TRUE(converted_timestamp_micros.timestamp_is_adjusted_to_utc);
    EXPECT_EQ(converted_timestamp_micros.time_unit, ParquetTimeUnit::MICROS);
    EXPECT_EQ(converted_timestamp_micros.extra_type_info, ParquetExtraTypeInfo::UNIT_MICROS);
}

TEST(ParquetTypeTest, ResolveConvertedIntegerMappingsAndDecodedKinds) {
    struct Case {
        ::parquet::ConvertedType::type converted_type;
        ::parquet::Type::type physical_type;
        PrimitiveType expected_type;
        int bit_width;
        bool expected_unsigned;
        DecodedValueKind expected_value_kind;
    };
    const std::vector<Case> cases = {
            {::parquet::ConvertedType::INT_8, ::parquet::Type::INT32, TYPE_TINYINT, 8, false,
             DecodedValueKind::INT32},
            {::parquet::ConvertedType::UINT_8, ::parquet::Type::INT32, TYPE_SMALLINT, 8, true,
             DecodedValueKind::INT32},
            {::parquet::ConvertedType::INT_16, ::parquet::Type::INT32, TYPE_SMALLINT, 16, false,
             DecodedValueKind::INT32},
            {::parquet::ConvertedType::UINT_16, ::parquet::Type::INT32, TYPE_INT, 16, true,
             DecodedValueKind::INT32},
            {::parquet::ConvertedType::INT_32, ::parquet::Type::INT32, TYPE_INT, 32, false,
             DecodedValueKind::INT32},
            {::parquet::ConvertedType::UINT_32, ::parquet::Type::INT32, TYPE_BIGINT, 32, true,
             DecodedValueKind::UINT32},
            {::parquet::ConvertedType::INT_64, ::parquet::Type::INT64, TYPE_BIGINT, 64, false,
             DecodedValueKind::INT64},
            {::parquet::ConvertedType::UINT_64, ::parquet::Type::INT64, TYPE_LARGEINT, 64, true,
             DecodedValueKind::UINT64},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.converted_type);
        const auto type = resolve_node(::parquet::schema::PrimitiveNode::Make(
                "c", ::parquet::Repetition::REQUIRED, test_case.physical_type,
                test_case.converted_type));
        ASSERT_NE(type.doris_type, nullptr);
        EXPECT_EQ(primitive_type(type.doris_type), test_case.expected_type);
        EXPECT_EQ(type.integer_bit_width, test_case.bit_width);
        EXPECT_EQ(type.is_unsigned_integer, test_case.expected_unsigned);
        EXPECT_EQ(decoded_value_kind(type), test_case.expected_value_kind);
    }
}

TEST(ParquetTypeTest, ResolveConvertedDecimalCarriers) {
    struct Case {
        ::parquet::Type::type physical_type;
        int type_length;
        int precision;
        int scale;
        PrimitiveType expected_type;
        ParquetExtraTypeInfo expected_extra;
    };
    const std::vector<Case> cases = {
            {::parquet::Type::INT32, -1, 9, 2, TYPE_DECIMAL32, ParquetExtraTypeInfo::DECIMAL_INT32},
            {::parquet::Type::INT64, -1, 18, 6, TYPE_DECIMAL64,
             ParquetExtraTypeInfo::DECIMAL_INT64},
            {::parquet::Type::BYTE_ARRAY, -1, 20, 5, TYPE_DECIMAL128I,
             ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY},
            {::parquet::Type::FIXED_LEN_BYTE_ARRAY, 16, 38, 6, TYPE_DECIMAL128I,
             ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY},
            {::parquet::Type::FIXED_LEN_BYTE_ARRAY, 20, 39, 6, TYPE_DECIMAL256,
             ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.physical_type);
        const auto type = resolve_node(::parquet::schema::PrimitiveNode::Make(
                "d", ::parquet::Repetition::REQUIRED, test_case.physical_type,
                ::parquet::ConvertedType::DECIMAL, test_case.type_length, test_case.precision,
                test_case.scale));
        ASSERT_NE(type.doris_type, nullptr);
        EXPECT_EQ(primitive_type(type.doris_type), test_case.expected_type);
        EXPECT_TRUE(type.is_decimal);
        EXPECT_FALSE(type.is_string_like);
        EXPECT_EQ(type.decimal_precision, test_case.precision);
        EXPECT_EQ(type.decimal_scale, test_case.scale);
        EXPECT_EQ(type.extra_type_info, test_case.expected_extra);
    }
}

TEST(ParquetTypeTest, ResolveLogicalStringDateAndDecimalMappings) {
    const std::vector<std::shared_ptr<const ::parquet::LogicalType>> string_like_logical_types = {
            ::parquet::LogicalType::String(), ::parquet::LogicalType::Enum(),
            ::parquet::LogicalType::JSON(), ::parquet::LogicalType::BSON()};
    for (const auto& logical_type : string_like_logical_types) {
        const auto type = resolve_node(::parquet::schema::PrimitiveNode::Make(
                "s", ::parquet::Repetition::OPTIONAL, logical_type, ::parquet::Type::BYTE_ARRAY));
        ASSERT_NE(type.doris_type, nullptr);
        EXPECT_TRUE(type.doris_type->is_nullable());
        EXPECT_EQ(primitive_type(type.doris_type), TYPE_STRING);
        EXPECT_TRUE(type.is_string_like);
    }

    const auto uuid = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "uuid", ::parquet::Repetition::OPTIONAL, ::parquet::LogicalType::UUID(),
            ::parquet::Type::FIXED_LEN_BYTE_ARRAY, 16));
    ASSERT_NE(uuid.doris_type, nullptr);
    EXPECT_TRUE(uuid.doris_type->is_nullable());
    EXPECT_EQ(primitive_type(uuid.doris_type), TYPE_STRING);
    EXPECT_TRUE(uuid.is_string_like);

    const auto date = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "d", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::Date(),
            ::parquet::Type::INT32));
    ASSERT_NE(date.doris_type, nullptr);
    EXPECT_EQ(primitive_type(date.doris_type), TYPE_DATEV2);

    const auto decimal64 = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "d64", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::Decimal(18, 6),
            ::parquet::Type::INT64));
    ASSERT_NE(decimal64.doris_type, nullptr);
    EXPECT_EQ(primitive_type(decimal64.doris_type), TYPE_DECIMAL64);
    EXPECT_TRUE(decimal64.is_decimal);
    EXPECT_EQ(decimal64.decimal_precision, 18);
    EXPECT_EQ(decimal64.decimal_scale, 6);
    EXPECT_EQ(decimal64.extra_type_info, ParquetExtraTypeInfo::DECIMAL_INT64);

    const auto decimal128 = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "d128", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::Decimal(38, 6),
            ::parquet::Type::FIXED_LEN_BYTE_ARRAY, 16));
    ASSERT_NE(decimal128.doris_type, nullptr);
    EXPECT_EQ(primitive_type(decimal128.doris_type), TYPE_DECIMAL128I);
    EXPECT_TRUE(decimal128.is_decimal);
    EXPECT_EQ(decimal128.decimal_precision, 38);
    EXPECT_EQ(decimal128.decimal_scale, 6);
    EXPECT_EQ(decimal128.extra_type_info, ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY);

    const auto decimal256 = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "d256", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::Decimal(39, 6),
            ::parquet::Type::FIXED_LEN_BYTE_ARRAY, 20));
    ASSERT_NE(decimal256.doris_type, nullptr);
    EXPECT_EQ(primitive_type(decimal256.doris_type), TYPE_DECIMAL256);
    EXPECT_TRUE(decimal256.is_decimal);
    EXPECT_EQ(decimal256.decimal_precision, 39);
    EXPECT_EQ(decimal256.decimal_scale, 6);
    EXPECT_EQ(decimal256.extra_type_info, ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY);
    EXPECT_FALSE(decimal256.is_string_like);
}

TEST(ParquetTypeTest, LogicalConvertedAndPhysicalFallbackLevelsAreDistinct) {
    const auto logical_type = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "c", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::Int(8, true),
            ::parquet::Type::INT32));
    ASSERT_NE(logical_type.doris_type, nullptr);
    EXPECT_EQ(primitive_type(logical_type.doris_type), TYPE_TINYINT);
    EXPECT_EQ(logical_type.integer_bit_width, 8);

    const auto converted_type = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "c", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
            ::parquet::ConvertedType::INT_8));
    ASSERT_NE(converted_type.doris_type, nullptr);
    EXPECT_EQ(primitive_type(converted_type.doris_type), TYPE_TINYINT);
    EXPECT_EQ(converted_type.integer_bit_width, 8);

    const auto physical_type = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "c", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32));
    ASSERT_NE(physical_type.doris_type, nullptr);
    EXPECT_EQ(primitive_type(physical_type.doris_type), TYPE_INT);
    EXPECT_EQ(physical_type.integer_bit_width, -1);
}

TEST(ParquetTypeTest, ResolveDecimalStringLikeFloat16AndPhysicalFallback) {
    const auto decimal256 = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "d", ::parquet::Repetition::REQUIRED, ::parquet::Type::FIXED_LEN_BYTE_ARRAY,
            ::parquet::ConvertedType::DECIMAL, 20, 39, 6));
    ASSERT_NE(decimal256.doris_type, nullptr);
    EXPECT_EQ(primitive_type(decimal256.doris_type), TYPE_DECIMAL256);
    EXPECT_TRUE(decimal256.is_decimal);
    EXPECT_FALSE(decimal256.is_string_like);
    EXPECT_EQ(decimal256.decimal_precision, 39);
    EXPECT_EQ(decimal256.decimal_scale, 6);
    EXPECT_EQ(decimal256.extra_type_info, ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY);

    const auto plain_binary = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "s", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    ASSERT_NE(plain_binary.doris_type, nullptr);
    EXPECT_EQ(primitive_type(plain_binary.doris_type), TYPE_STRING);
    EXPECT_TRUE(plain_binary.is_string_like);

    const auto float16 = resolve_arrow_float16_type();
    ASSERT_NE(float16.doris_type, nullptr);
    EXPECT_TRUE(float16.doris_type->is_nullable());
    EXPECT_EQ(float16.physical_type, ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
    EXPECT_EQ(float16.fixed_length, 2);
    EXPECT_EQ(primitive_type(float16.doris_type), TYPE_FLOAT);
    EXPECT_EQ(float16.extra_type_info, ParquetExtraTypeInfo::FLOAT16);
    EXPECT_FALSE(float16.is_string_like);
    EXPECT_EQ(decoded_value_kind(float16), DecodedValueKind::FIXED_BINARY);
}

TEST(ParquetTypeTest, ResolveNullDescriptorAndPhysicalFallback) {
    const auto null_type = resolve_parquet_type(nullptr);
    EXPECT_EQ(null_type.doris_type, nullptr);
    EXPECT_EQ(null_type.physical_type, ::parquet::Type::UNDEFINED);
    EXPECT_TRUE(null_type.supports_record_reader);

    const auto int96 = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "ts", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT96));
    ASSERT_NE(int96.doris_type, nullptr);
    EXPECT_EQ(primitive_type(int96.doris_type), TYPE_DATETIMEV2);
    EXPECT_EQ(int96.extra_type_info, ParquetExtraTypeInfo::IMPALA_TIMESTAMP);
    EXPECT_EQ(decoded_value_kind(int96), DecodedValueKind::INT96);
}

TEST(ParquetTypeTest, ResolveEveryPhysicalFallback) {
    struct Case {
        ::parquet::schema::NodePtr node;
        PrimitiveType expected_type;
        DecodedValueKind expected_kind;
        bool expected_string_like = false;
    };
    const std::vector<Case> cases = {
            {::parquet::schema::PrimitiveNode::Make("b", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::BOOLEAN),
             TYPE_BOOLEAN, DecodedValueKind::BOOL},
            {::parquet::schema::PrimitiveNode::Make("i32", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::INT32),
             TYPE_INT, DecodedValueKind::INT32},
            {::parquet::schema::PrimitiveNode::Make("i64", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::INT64),
             TYPE_BIGINT, DecodedValueKind::INT64},
            {::parquet::schema::PrimitiveNode::Make("f", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::FLOAT),
             TYPE_FLOAT, DecodedValueKind::FLOAT},
            {::parquet::schema::PrimitiveNode::Make("d", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::DOUBLE),
             TYPE_DOUBLE, DecodedValueKind::DOUBLE},
            {::parquet::schema::PrimitiveNode::Make("s", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::BYTE_ARRAY),
             TYPE_STRING, DecodedValueKind::BINARY, true},
            {::parquet::schema::PrimitiveNode::Make("fs", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::FIXED_LEN_BYTE_ARRAY,
                                                    ::parquet::ConvertedType::NONE, 4),
             TYPE_STRING, DecodedValueKind::FIXED_BINARY, true},
            {::parquet::schema::PrimitiveNode::Make("ts", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::INT96),
             TYPE_DATETIMEV2, DecodedValueKind::INT96},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.expected_type);
        const auto type = resolve_node(test_case.node);
        ASSERT_NE(type.doris_type, nullptr);
        EXPECT_EQ(primitive_type(type.doris_type), test_case.expected_type);
        EXPECT_EQ(decoded_value_kind(type), test_case.expected_kind);
        EXPECT_EQ(type.is_string_like, test_case.expected_string_like);
        EXPECT_TRUE(type.supports_record_reader);
    }
}

TEST(ParquetTypeTest, InvalidLogicalAnnotationsFallBackOrRejectAsSpecified) {
    EXPECT_THROW(::parquet::LogicalType::Int(24, true), ::parquet::ParquetException);

    const auto nanos_time = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "time_ns", ::parquet::Repetition::REQUIRED,
            ::parquet::LogicalType::Time(false, ::parquet::LogicalType::TimeUnit::NANOS),
            ::parquet::Type::INT64));
    ASSERT_NE(nanos_time.doris_type, nullptr);
    EXPECT_EQ(primitive_type(nanos_time.doris_type), TYPE_BIGINT);
    EXPECT_TRUE(nanos_time.unsupported_reason.empty());

    const auto adjusted_nanos_time = resolve_node(::parquet::schema::PrimitiveNode::Make(
            "time_ns_utc", ::parquet::Repetition::REQUIRED,
            ::parquet::LogicalType::Time(true, ::parquet::LogicalType::TimeUnit::NANOS),
            ::parquet::Type::INT64));
    EXPECT_EQ(adjusted_nanos_time.doris_type, nullptr);
    EXPECT_FALSE(adjusted_nanos_time.supports_record_reader);
    EXPECT_FALSE(adjusted_nanos_time.unsupported_reason.empty());

    EXPECT_THROW(::parquet::schema::PrimitiveNode::Make("f16_bad", ::parquet::Repetition::REQUIRED,
                                                        ::parquet::LogicalType::Float16(),
                                                        ::parquet::Type::FIXED_LEN_BYTE_ARRAY, 4),
                 ::parquet::ParquetException);
}

} // namespace doris::format::parquet
