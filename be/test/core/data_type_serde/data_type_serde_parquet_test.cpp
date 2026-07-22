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

#include <cmath>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type_serde/parquet_decode_source.h"

namespace doris {
namespace {

#pragma pack(1)
struct TestParquetInt96Timestamp {
    int64_t nanos_of_day;
    int32_t julian_day;
};
#pragma pack()
static_assert(sizeof(TestParquetInt96Timestamp) == 12);

constexpr int32_t JULIAN_UNIX_EPOCH = 2440588;
constexpr int64_t NANOS_PER_DAY = 86400000000000LL;

class TestParquetDecodeSource final : public ParquetDecodeSource {
public:
    template <typename T>
    void set_fixed_values(const std::vector<T>& values) {
        _value_width = sizeof(T);
        _fixed_values.resize(values.size() * sizeof(T));
        memcpy(_fixed_values.data(), values.data(), _fixed_values.size());
    }

    void set_fixed_bytes(std::vector<uint8_t> values, size_t value_width) {
        _fixed_values = std::move(values);
        _value_width = value_width;
    }

    void set_dictionary(std::vector<uint8_t> values, size_t value_width,
                        std::vector<uint32_t> indices) {
        _dictionary = std::move(values);
        _dictionary_width = value_width;
        _indices = std::move(indices);
        _index_offset = 0;
        ++_dictionary_generation;
    }

    template <typename T>
    void set_fixed_dictionary(const std::vector<T>& values, std::vector<uint32_t> indices) {
        std::vector<uint8_t> encoded(values.size() * sizeof(T));
        memcpy(encoded.data(), values.data(), encoded.size());
        set_dictionary(std::move(encoded), sizeof(T), std::move(indices));
    }

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) override {
        DORIS_CHECK_LE((_fixed_offset + num_values) * _value_width, _fixed_values.size());
        const uint8_t* begin = _fixed_values.data() + _fixed_offset * _value_width;
        _fixed_offset += num_values;
        return consumer.consume(begin, num_values, _value_width);
    }

    Status decode_binary_values(size_t num_values, ParquetBinaryValueConsumer& consumer) override {
        DORIS_CHECK_LE(_binary_offset + num_values, _binary_refs.size());
        const StringRef* begin = _binary_refs.data() + _binary_offset;
        _binary_offset += num_values;
        return consumer.consume(begin, num_values);
    }

    Status skip_values(size_t num_values) override {
        _fixed_offset += num_values;
        _binary_offset += num_values;
        _index_offset += num_values;
        return Status::OK();
    }

    bool has_dictionary() const override { return !_dictionary.empty(); }
    uint64_t dictionary_generation() const override { return _dictionary_generation; }
    size_t dictionary_size() const override {
        return _dictionary_width == 0 ? 0 : _dictionary.size() / _dictionary_width;
    }

    Status decode_dictionary(ParquetFixedValueConsumer& fixed_consumer,
                             ParquetBinaryValueConsumer& binary_consumer) override {
        ++_dictionary_decode_calls;
        return fixed_consumer.consume(_dictionary.data(), dictionary_size(), _dictionary_width);
    }

    Status decode_dictionary_indices(size_t num_values, std::vector<uint32_t>* indices) override {
        DORIS_CHECK(indices != nullptr);
        DORIS_CHECK_LE(_index_offset + num_values, _indices.size());
        indices->assign(_indices.begin() + _index_offset,
                        _indices.begin() + _index_offset + num_values);
        _index_offset += num_values;
        return Status::OK();
    }

    size_t dictionary_decode_calls() const { return _dictionary_decode_calls; }

private:
    std::vector<uint8_t> _fixed_values;
    std::vector<StringRef> _binary_refs;
    std::vector<uint8_t> _dictionary;
    std::vector<uint32_t> _indices;
    size_t _value_width = 0;
    size_t _dictionary_width = 0;
    size_t _fixed_offset = 0;
    size_t _binary_offset = 0;
    size_t _index_offset = 0;
    uint64_t _dictionary_generation = 0;
    size_t _dictionary_decode_calls = 0;
};

TEST(DataTypeSerDeParquetTest, MaterializesLogicalUnsignedIntegersDirectly) {
    TestParquetDecodeSource source;
    source.set_fixed_values<int32_t>({-1, 0, 7});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                  .logical_type = ParquetLogicalType::INTEGER,
                                  .logical_integer_bit_width = 32,
                                  .logical_integer_is_signed = false};
    ParquetMaterializationState state;
    DataTypeInt64 type;
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 3, state).ok());
    const auto& data = assert_cast<const ColumnInt64&>(*column).get_data();
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 4294967295LL);
    EXPECT_EQ(data[1], 0);
    EXPECT_EQ(data[2], 7);
}

TEST(DataTypeSerDeParquetTest, LogicalIntegerRejectsOutOfRangePhysicalCarrier) {
    TestParquetDecodeSource source;
    source.set_fixed_values<int32_t>({1000});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                  .logical_type = ParquetLogicalType::INTEGER,
                                  .logical_integer_bit_width = 8,
                                  .logical_integer_is_signed = true};
    ParquetMaterializationState state;
    DataTypeInt8 type;
    auto column = type.create_column();

    EXPECT_TRUE(type.get_serde()
                        ->read_column_from_parquet(*column, source, context, 1, state)
                        .is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_EQ(column->size(), 0);
}

TEST(DataTypeSerDeParquetTest, LogicalIntegerDictionaryCarrierOverflowBecomesNull) {
    const int32_t overflow = 1000;
    std::vector<uint8_t> dictionary(sizeof(overflow));
    memcpy(dictionary.data(), &overflow, sizeof(overflow));
    TestParquetDecodeSource source;
    source.set_dictionary(std::move(dictionary), sizeof(overflow), {0, 0});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                  .encoding = ParquetValueEncoding::DICTIONARY,
                                  .logical_type = ParquetLogicalType::INTEGER,
                                  .logical_integer_bit_width = 8,
                                  .logical_integer_is_signed = true};
    IColumn::Filter null_map(2, 0);
    ParquetMaterializationState state;
    state.conversion_failure_null_map = &null_map;
    DataTypeInt8 type;
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 2, state).ok());
    EXPECT_EQ(column->size(), 2);
    EXPECT_EQ(null_map, IColumn::Filter({1, 1}));
}

TEST(DataTypeSerDeParquetTest, MaterializesFloat16Directly) {
    TestParquetDecodeSource source;
    source.set_fixed_values<uint16_t>({0x3C00, 0xC000, 0x7C00});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY,
                                  .logical_type = ParquetLogicalType::FLOAT16,
                                  .type_length = 2,
                                  .logical_float16 = true};
    ParquetMaterializationState state;
    DataTypeFloat32 type;
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 3, state).ok());
    const auto& data = assert_cast<const ColumnFloat32&>(*column).get_data();
    EXPECT_FLOAT_EQ(data[0], 1.0F);
    EXPECT_FLOAT_EQ(data[1], -2.0F);
    EXPECT_TRUE(std::isinf(data[2]));
}

TEST(DataTypeSerDeParquetTest, RescalesFixedBinaryDecimalDirectly) {
    TestParquetDecodeSource source;
    source.set_fixed_bytes({0x04, 0xD2, 0xFB, 0x2E}, 2); // 12.34 and -12.34 at scale 2.
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY,
                                  .logical_type = ParquetLogicalType::DECIMAL,
                                  .type_length = 2,
                                  .decimal_precision = 4,
                                  .decimal_scale = 2};
    ParquetMaterializationState state;
    DataTypeDecimal64 type(18, 3);
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 2, state).ok());
    const auto& data = assert_cast<const ColumnDecimal64&>(*column).get_data();
    EXPECT_EQ(data[0].value, 12340);
    EXPECT_EQ(data[1].value, -12340);
}

TEST(DataTypeSerDeParquetTest, RescalesExactIntegerDecimalsWithoutRowFailures) {
    TestParquetDecodeSource source;
    source.set_fixed_values<int32_t>({12300, -98700, 2147483600});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                  .logical_type = ParquetLogicalType::DECIMAL,
                                  .decimal_precision = 10,
                                  .decimal_scale = 4};
    ParquetMaterializationState state;
    DataTypeDecimal64 type(9, 2);
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 3, state).ok());
    const auto& data = assert_cast<const ColumnDecimal64&>(*column).get_data();
    // Exact scale reduction can stay on the fast integer path without per-row failure bookkeeping.
    EXPECT_EQ(data[0].value, 123);
    EXPECT_EQ(data[1].value, -987);
    EXPECT_EQ(data[2].value, 21474836);
}

TEST(DataTypeSerDeParquetTest, DecimalScaleDownRejectsLossyPlainAndDictionaryValues) {
    ParquetDecodeContext plain_context {.physical_type = ParquetPhysicalType::INT32,
                                        .logical_type = ParquetLogicalType::DECIMAL,
                                        .decimal_precision = 6,
                                        .decimal_scale = 2};
    DataTypeDecimal32 type(5, 1);
    {
        TestParquetDecodeSource source;
        source.set_fixed_values<int32_t>({1230, 1234});
        IColumn::Filter null_map(2, 0);
        ParquetMaterializationState state;
        state.conversion_failure_null_map = &null_map;
        auto column = type.create_column();

        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, source, plain_context, 2, state)
                            .ok());
        const auto& data = assert_cast<const ColumnDecimal32&>(*column).get_data();
        EXPECT_EQ(data[0].value, 123);
        EXPECT_EQ(data[1].value, 0);
        EXPECT_EQ(null_map, IColumn::Filter({0, 1}));
    }
    {
        TestParquetDecodeSource source;
        source.set_fixed_values<int32_t>({1234});
        ParquetMaterializationState state;
        state.enable_strict_mode = true;
        auto column = type.create_column();
        EXPECT_FALSE(type.get_serde()
                             ->read_column_from_parquet(*column, source, plain_context, 1, state)
                             .ok());
        EXPECT_EQ(column->size(), 0);
    }
    {
        TestParquetDecodeSource source;
        source.set_fixed_dictionary<int32_t>({1230, 1234}, {1, 0, 1});
        auto dictionary_context = plain_context;
        dictionary_context.encoding = ParquetValueEncoding::DICTIONARY;
        IColumn::Filter null_map(3, 0);
        ParquetMaterializationState state;
        state.conversion_failure_null_map = &null_map;
        auto column = type.create_column();

        ASSERT_TRUE(
                type.get_serde()
                        ->read_column_from_parquet(*column, source, dictionary_context, 3, state)
                        .ok());
        const auto& data = assert_cast<const ColumnDecimal32&>(*column).get_data();
        EXPECT_EQ(data[1].value, 123);
        EXPECT_EQ(null_map, IColumn::Filter({1, 0, 1}));
    }
}

TEST(DataTypeSerDeParquetTest, DecimalUsesWideIntermediateBeforeNarrowing) {
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                  .logical_type = ParquetLogicalType::DECIMAL,
                                  .decimal_precision = 19,
                                  .decimal_scale = 0};
    DataTypeDecimal32 type(9, 0);
    constexpr int64_t WRAPS_TO_ONE_IN_INT32 = 4294967297LL;
    for (bool dictionary : {false, true}) {
        TestParquetDecodeSource source;
        if (dictionary) {
            source.set_fixed_dictionary<int64_t>({WRAPS_TO_ONE_IN_INT32}, {0});
            context.encoding = ParquetValueEncoding::DICTIONARY;
        } else {
            source.set_fixed_values<int64_t>({WRAPS_TO_ONE_IN_INT32});
            context.encoding = ParquetValueEncoding::PLAIN;
        }
        IColumn::Filter null_map(1, 0);
        ParquetMaterializationState state;
        state.conversion_failure_null_map = &null_map;
        auto column = type.create_column();

        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, source, context, 1, state)
                            .ok());
        EXPECT_EQ(null_map, IColumn::Filter({1}));
        EXPECT_EQ(assert_cast<const ColumnDecimal32&>(*column).get_data()[0].value, 0);
    }
}

TEST(DataTypeSerDeParquetTest, DecimalAcceptsSignExtendedBinaryAfterWideExactScaling) {
    const std::vector<uint8_t> values {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xCE,
                                       0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFB, 0x32};
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY,
                                  .logical_type = ParquetLogicalType::DECIMAL,
                                  .type_length = 8,
                                  .decimal_precision = 19,
                                  .decimal_scale = 2};
    DataTypeDecimal32 type(5, 1);
    for (bool dictionary : {false, true}) {
        TestParquetDecodeSource source;
        if (dictionary) {
            source.set_dictionary(values, 8, {1, 0});
            context.encoding = ParquetValueEncoding::DICTIONARY;
        } else {
            source.set_fixed_bytes(values, 8);
            context.encoding = ParquetValueEncoding::PLAIN;
        }
        ParquetMaterializationState state;
        auto column = type.create_column();

        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, source, context, 2, state)
                            .ok());
        const auto& data = assert_cast<const ColumnDecimal32&>(*column).get_data();
        EXPECT_EQ(data[0].value, dictionary ? -123 : 123);
        EXPECT_EQ(data[1].value, dictionary ? 123 : -123);
    }
}

TEST(DataTypeSerDeParquetTest, ReusesTypedDictionary) {
    TestParquetDecodeSource source;
    std::vector<uint8_t> dictionary {'a', 'a', 'b', 'b'};
    source.set_dictionary(std::move(dictionary), 2, {1, 0, 1, 0});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY,
                                  .encoding = ParquetValueEncoding::DICTIONARY,
                                  .type_length = 2};
    ParquetMaterializationState state;
    DataTypeString type;
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 3, state).ok());
    ASSERT_EQ(state.typed_dictionary->size(), 2);
    EXPECT_EQ(column->get_data_at(0).to_string(), "bb");
    EXPECT_EQ(column->get_data_at(1).to_string(), "aa");
    EXPECT_EQ(column->get_data_at(2).to_string(), "bb");

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 1, state).ok());
    EXPECT_EQ(source.dictionary_decode_calls(), 1);
    EXPECT_EQ(column->get_data_at(3).to_string(), "aa");

    source.set_dictionary({'c', 'c'}, 2, {0});
    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 1, state).ok());
    EXPECT_EQ(source.dictionary_decode_calls(), 2);
    EXPECT_EQ(column->get_data_at(4).to_string(), "cc");
}

TEST(DataTypeSerDeParquetTest, MaterializesDictionaryIndicesWithoutValues) {
    TestParquetDecodeSource source;
    source.set_dictionary({'a', 'a', 'b', 'b'}, 2, {1, 0, 1});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY,
                                  .encoding = ParquetValueEncoding::DICTIONARY,
                                  .type_length = 2,
                                  .dictionary_index_only = true};
    ParquetMaterializationState state;
    DataTypeString type;
    auto column = ColumnInt32::create();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 3, state).ok());
    const auto& data = column->get_data();
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 1);
    EXPECT_EQ(data[1], 0);
    EXPECT_EQ(data[2], 1);
    EXPECT_FALSE(state.typed_dictionary);
}

TEST(DataTypeSerDeParquetTest, MaterializesDateDirectly) {
    TestParquetDecodeSource source;
    source.set_fixed_values<int32_t>({0, 1, -1});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                  .logical_type = ParquetLogicalType::DATE};
    ParquetMaterializationState state;
    DataTypeDateV2 type;
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 3, state).ok());
    EXPECT_EQ(type.to_string(*column, 0), "1970-01-01");
    EXPECT_EQ(type.to_string(*column, 1), "1970-01-02");
    EXPECT_EQ(type.to_string(*column, 2), "1969-12-31");
}

TEST(DataTypeSerDeParquetTest, MaterializesTimestampUnitsAndNegativeEpochDirectly) {
    TestParquetDecodeSource source;
    source.set_fixed_values<int64_t>({0, -1, 1000001});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                  .logical_type = ParquetLogicalType::TIMESTAMP,
                                  .time_unit = ParquetTimeUnit::MICROS,
                                  .timestamp_is_adjusted_to_utc = true};
    ParquetMaterializationState state;
    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 3, state).ok());
    EXPECT_EQ(type.to_string(*column, 0), "1970-01-01 00:00:00.000000");
    EXPECT_EQ(type.to_string(*column, 1), "1969-12-31 23:59:59.999999");
    EXPECT_EQ(type.to_string(*column, 2), "1970-01-01 00:00:01.000001");
}

TEST(DataTypeSerDeParquetTest, MaterializesTimeUnitsDirectly) {
    struct TimeUnitCase {
        ParquetTimeUnit unit;
        int64_t units_per_day;
        int64_t expected_last_micros;
    };
    const std::vector<TimeUnitCase> cases {
            {ParquetTimeUnit::MILLIS, 86400000, 86399999000},
            {ParquetTimeUnit::MICROS, 86400000000, 86399999999},
            {ParquetTimeUnit::NANOS, 86400000000000, 86399999999},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(static_cast<int>(test_case.unit));
        ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                      .logical_type = ParquetLogicalType::TIME,
                                      .time_unit = test_case.unit};
        DataTypeTimeV2 type(6);

        TestParquetDecodeSource source;
        source.set_fixed_values<int64_t>({0, test_case.units_per_day - 1});
        ParquetMaterializationState state;
        auto column = type.create_column();
        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, source, context, 2, state)
                            .ok());
        const auto& data = assert_cast<const ColumnTimeV2&>(*column).get_data();
        EXPECT_EQ(data[0], 0);
        EXPECT_EQ(data[1], test_case.expected_last_micros);

        TestParquetDecodeSource nullable_source;
        nullable_source.set_fixed_values<int64_t>({-1, test_case.units_per_day});
        IColumn::Filter null_map;
        null_map.resize_fill(2, 0);
        ParquetMaterializationState nullable_state;
        nullable_state.conversion_failure_null_map = &null_map;
        auto nullable_column = type.create_column();
        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*nullable_column, nullable_source, context,
                                                       2, nullable_state)
                            .ok());
        EXPECT_EQ(null_map, IColumn::Filter({1, 1}));

        TestParquetDecodeSource strict_source;
        strict_source.set_fixed_values<int64_t>({-1});
        ParquetMaterializationState strict_state;
        auto strict_column = type.create_column();
        EXPECT_FALSE(type.get_serde()
                             ->read_column_from_parquet(*strict_column, strict_source, context, 1,
                                                        strict_state)
                             .ok());
        EXPECT_EQ(strict_column->size(), 0);

        TestParquetDecodeSource unused_invalid_dictionary;
        unused_invalid_dictionary.set_fixed_dictionary<int64_t>(
                {-1, 0, test_case.units_per_day - 1, test_case.units_per_day}, {1, 2});
        auto dictionary_context = context;
        dictionary_context.encoding = ParquetValueEncoding::DICTIONARY;
        ParquetMaterializationState dictionary_state;
        auto dictionary_column = type.create_column();
        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*dictionary_column,
                                                       unused_invalid_dictionary,
                                                       dictionary_context, 2, dictionary_state)
                            .ok());
        EXPECT_EQ(dictionary_column->size(), 2);

        TestParquetDecodeSource referenced_invalid_dictionary;
        referenced_invalid_dictionary.set_fixed_dictionary<int64_t>(
                {-1, 0, test_case.units_per_day}, {0});
        ParquetMaterializationState invalid_dictionary_state;
        auto invalid_dictionary_column = type.create_column();
        EXPECT_FALSE(type.get_serde()
                             ->read_column_from_parquet(
                                     *invalid_dictionary_column, referenced_invalid_dictionary,
                                     dictionary_context, 1, invalid_dictionary_state)
                             .ok());
        EXPECT_EQ(invalid_dictionary_column->size(), 0);
    }
}

TEST(DataTypeSerDeParquetTest, MaterializesTimestampTzDirectly) {
    TestParquetDecodeSource source;
    source.set_fixed_values<int64_t>({-1, 1000001});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                  .logical_type = ParquetLogicalType::TIMESTAMP,
                                  .time_unit = ParquetTimeUnit::MICROS,
                                  .timestamp_is_adjusted_to_utc = true};
    ParquetMaterializationState state;
    DataTypeTimeStampTz type(6);
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 2, state).ok());
    const auto& data = assert_cast<const ColumnTimeStampTz&>(*column).get_data();
    EXPECT_EQ(data[0].year(), 1969);
    EXPECT_EQ(data[0].microsecond(), 999999);
    EXPECT_EQ(data[1].second(), 1);
    EXPECT_EQ(data[1].microsecond(), 1);
}

TEST(DataTypeSerDeParquetTest, ValidatesInt96BeforeDateTimeMaterialization) {
    {
        TestParquetDecodeSource source;
        source.set_fixed_values<TestParquetInt96Timestamp>(
                {{0, JULIAN_UNIX_EPOCH}, {NANOS_PER_DAY - 1, JULIAN_UNIX_EPOCH}});
        ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT96,
                                      .logical_type = ParquetLogicalType::TIMESTAMP};
        ParquetMaterializationState state;
        DataTypeDateTimeV2 type(6);
        auto column = type.create_column();

        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, source, context, 2, state)
                            .ok());
        EXPECT_EQ(type.to_string(*column, 0), "1970-01-01 00:00:00.000000");
        EXPECT_EQ(type.to_string(*column, 1), "1970-01-01 23:59:59.999999");
    }

    const std::vector<TestParquetInt96Timestamp> invalid_values {
            {NANOS_PER_DAY, JULIAN_UNIX_EPOCH},
            {-1, JULIAN_UNIX_EPOCH},
            {0, std::numeric_limits<int32_t>::max()}};
    {
        TestParquetDecodeSource source;
        source.set_fixed_values(invalid_values);
        ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT96,
                                      .logical_type = ParquetLogicalType::TIMESTAMP};
        ParquetMaterializationState state;
        DataTypeDateTimeV2 type(6);
        auto column = type.create_column();

        EXPECT_FALSE(type.get_serde()
                             ->read_column_from_parquet(*column, source, context, 3, state)
                             .ok());
        EXPECT_EQ(column->size(), 0);
    }
    {
        TestParquetDecodeSource source;
        source.set_fixed_values(invalid_values);
        ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT96,
                                      .logical_type = ParquetLogicalType::TIMESTAMP};
        IColumn::Filter null_map(3, 0);
        ParquetMaterializationState state;
        state.conversion_failure_null_map = &null_map;
        DataTypeDateTimeV2 type(6);
        auto column = type.create_column();

        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, source, context, 3, state)
                            .ok());
        EXPECT_EQ(column->size(), 3);
        EXPECT_EQ(null_map, IColumn::Filter({1, 1, 1}));
    }
}

TEST(DataTypeSerDeParquetTest, Int96DictionaryFailuresFollowDecodedIds) {
    const std::vector<TestParquetInt96Timestamp> dictionary_values {
            {0, JULIAN_UNIX_EPOCH}, {NANOS_PER_DAY, JULIAN_UNIX_EPOCH}};
    std::vector<uint8_t> dictionary(sizeof(TestParquetInt96Timestamp) * dictionary_values.size());
    memcpy(dictionary.data(), dictionary_values.data(), dictionary.size());
    TestParquetDecodeSource source;
    source.set_dictionary(std::move(dictionary), sizeof(TestParquetInt96Timestamp), {1, 0, 1});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT96,
                                  .encoding = ParquetValueEncoding::DICTIONARY,
                                  .logical_type = ParquetLogicalType::TIMESTAMP};
    IColumn::Filter null_map(3, 0);
    ParquetMaterializationState state;
    state.conversion_failure_null_map = &null_map;
    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 3, state).ok());
    EXPECT_EQ(column->size(), 3);
    EXPECT_EQ(null_map, IColumn::Filter({1, 0, 1}));
    EXPECT_EQ(type.to_string(*column, 1), "1970-01-01 00:00:00.000000");
}

TEST(DataTypeSerDeParquetTest, TimestampTzChecksUnitOverflowAndTargetRange) {
    constexpr int64_t MIN_TIMESTAMP_MICROS = -62135596800000000LL;
    constexpr int64_t MAX_TIMESTAMP_MICROS = 253402300799999999LL;
    constexpr int64_t YEAR_10000_MILLIS = 253402300800000LL;

    {
        TestParquetDecodeSource source;
        source.set_fixed_values<int64_t>({MIN_TIMESTAMP_MICROS, MAX_TIMESTAMP_MICROS});
        ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                      .logical_type = ParquetLogicalType::TIMESTAMP,
                                      .time_unit = ParquetTimeUnit::MICROS,
                                      .timestamp_is_adjusted_to_utc = true};
        ParquetMaterializationState state;
        DataTypeTimeStampTz type(6);
        auto column = type.create_column();

        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, source, context, 2, state)
                            .ok());
        const auto& data = assert_cast<const ColumnTimeStampTz&>(*column).get_data();
        EXPECT_EQ(data[0].year(), 1);
        EXPECT_EQ(data[1].year(), 9999);
        EXPECT_EQ(data[1].microsecond(), 999999);
    }
    {
        TestParquetDecodeSource source;
        source.set_fixed_values<int64_t>({std::numeric_limits<int64_t>::max()});
        ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                      .logical_type = ParquetLogicalType::TIMESTAMP,
                                      .time_unit = ParquetTimeUnit::MILLIS,
                                      .timestamp_is_adjusted_to_utc = true};
        ParquetMaterializationState state;
        DataTypeTimeStampTz type(6);
        auto column = type.create_column();

        EXPECT_FALSE(type.get_serde()
                             ->read_column_from_parquet(*column, source, context, 1, state)
                             .ok());
        EXPECT_EQ(column->size(), 0);
    }
    {
        TestParquetDecodeSource source;
        source.set_fixed_values<int64_t>({std::numeric_limits<int64_t>::max(), YEAR_10000_MILLIS});
        ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                      .logical_type = ParquetLogicalType::TIMESTAMP,
                                      .time_unit = ParquetTimeUnit::MILLIS,
                                      .timestamp_is_adjusted_to_utc = true};
        IColumn::Filter null_map(2, 0);
        ParquetMaterializationState state;
        state.conversion_failure_null_map = &null_map;
        DataTypeTimeStampTz type(6);
        auto column = type.create_column();

        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, source, context, 2, state)
                            .ok());
        EXPECT_EQ(column->size(), 2);
        EXPECT_EQ(null_map, IColumn::Filter({1, 1}));
    }
}

TEST(DataTypeSerDeParquetTest, TimestampTzDecodedValuesUseTheSameBounds) {
    constexpr int64_t YEAR_10000_MILLIS = 253402300800000LL;
    const std::vector<int64_t> values {std::numeric_limits<int64_t>::max(), YEAR_10000_MILLIS, 0};
    NullMap conversion_failures(3, 0);
    DecodedColumnView view {.value_kind = DecodedValueKind::INT64,
                            .time_unit = DecodedTimeUnit::MILLIS,
                            .row_count = 3,
                            .values = reinterpret_cast<const uint8_t*>(values.data()),
                            .enable_strict_mode = false,
                            .conversion_failure_null_map = &conversion_failures};
    DataTypeTimeStampTz type(6);
    auto column = type.create_column();

    ASSERT_TRUE(type.get_serde()->read_column_from_decoded_values(*column, view).ok());
    EXPECT_EQ(column->size(), 3);
    EXPECT_EQ(conversion_failures, NullMap({1, 1, 0}));
    EXPECT_EQ(assert_cast<const ColumnTimeStampTz&>(*column).get_data()[2].year(), 1970);
}

TEST(DataTypeSerDeParquetTest, TimestampTzDictionaryFailuresFollowDecodedIds) {
    constexpr int64_t YEAR_10000_MILLIS = 253402300800000LL;
    const std::vector<int64_t> dictionary_values {0, YEAR_10000_MILLIS};
    std::vector<uint8_t> dictionary(sizeof(int64_t) * dictionary_values.size());
    memcpy(dictionary.data(), dictionary_values.data(), dictionary.size());
    TestParquetDecodeSource source;
    source.set_dictionary(std::move(dictionary), sizeof(int64_t), {1, 0, 1});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                  .encoding = ParquetValueEncoding::DICTIONARY,
                                  .logical_type = ParquetLogicalType::TIMESTAMP,
                                  .time_unit = ParquetTimeUnit::MILLIS,
                                  .timestamp_is_adjusted_to_utc = true};
    IColumn::Filter null_map(3, 0);
    ParquetMaterializationState state;
    state.conversion_failure_null_map = &null_map;
    DataTypeTimeStampTz type(6);
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 3, state).ok());
    EXPECT_EQ(column->size(), 3);
    EXPECT_EQ(null_map, IColumn::Filter({1, 0, 1}));
    EXPECT_EQ(assert_cast<const ColumnTimeStampTz&>(*column).get_data()[1].year(), 1970);
}

TEST(DataTypeSerDeParquetTest, Int96DecodedValuesRejectInvalidNanos) {
    const std::vector<TestParquetInt96Timestamp> values {{NANOS_PER_DAY, JULIAN_UNIX_EPOCH}};
    NullMap conversion_failures(1, 0);
    DecodedColumnView view {.value_kind = DecodedValueKind::INT96,
                            .row_count = 1,
                            .values = reinterpret_cast<const uint8_t*>(values.data()),
                            .enable_strict_mode = false,
                            .conversion_failure_null_map = &conversion_failures};
    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();

    ASSERT_TRUE(type.get_serde()->read_column_from_decoded_values(*column, view).ok());
    EXPECT_EQ(column->size(), 1);
    EXPECT_EQ(conversion_failures, NullMap({1}));
}

TEST(DataTypeSerDeParquetTest, MaterializesFixedBinaryAsVarbinaryDirectly) {
    TestParquetDecodeSource source;
    source.set_fixed_bytes({0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
                            0x0C, 0x0D, 0x0E, 0x0F},
                           16);
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY,
                                  .logical_type = ParquetLogicalType::UUID,
                                  .type_length = 16,
                                  .logical_uuid = true};
    ParquetMaterializationState state;
    DataTypeVarbinary type;
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 1, state).ok());
    EXPECT_EQ(column->get_data_at(0).to_string(), std::string("\x00\x01\x02\x03\x04\x05\x06\x07"
                                                              "\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F",
                                                              16));
}

TEST(DataTypeSerDeParquetTest, NullableConversionFailuresBecomeNullOutsideStrictMode) {
    auto expect_null = [](DataTypeSerDeSPtr serde, MutableColumnPtr column,
                          TestParquetDecodeSource* source, const ParquetDecodeContext& context) {
        IColumn::Filter null_map;
        null_map.resize_fill(1, 0);
        ParquetMaterializationState state;
        state.conversion_failure_null_map = &null_map;
        EXPECT_TRUE(serde->read_column_from_parquet(*column, *source, context, 1, state).ok());
        EXPECT_EQ(column->size(), 1);
        EXPECT_EQ(null_map[0], 1);
    };

    {
        TestParquetDecodeSource source;
        source.set_fixed_values<int32_t>({std::numeric_limits<int32_t>::min()});
        DataTypeDateV2 type;
        expect_null(type.get_serde(), type.create_column(), &source,
                    {.physical_type = ParquetPhysicalType::INT32,
                     .logical_type = ParquetLogicalType::DATE});
    }
    {
        TestParquetDecodeSource source;
        source.set_fixed_values<int64_t>({std::numeric_limits<int64_t>::max()});
        DataTypeTimeV2 type(6);
        expect_null(type.get_serde(), type.create_column(), &source,
                    {.physical_type = ParquetPhysicalType::INT64,
                     .logical_type = ParquetLogicalType::TIME,
                     .time_unit = ParquetTimeUnit::MILLIS});
    }
    {
        TestParquetDecodeSource source;
        source.set_fixed_values<int64_t>({std::numeric_limits<int64_t>::max()});
        DataTypeDateTimeV2 type(6);
        expect_null(type.get_serde(), type.create_column(), &source,
                    {.physical_type = ParquetPhysicalType::INT64,
                     .logical_type = ParquetLogicalType::TIMESTAMP,
                     .time_unit = ParquetTimeUnit::MILLIS});
    }
    {
        TestParquetDecodeSource source;
        source.set_fixed_values<int32_t>({10});
        DataTypeDecimal32 type(1, 0);
        expect_null(type.get_serde(), type.create_column(), &source,
                    {.physical_type = ParquetPhysicalType::INT32,
                     .logical_type = ParquetLogicalType::DECIMAL,
                     .decimal_precision = 2,
                     .decimal_scale = 0});
    }
}

TEST(DataTypeSerDeParquetTest, DictionaryConversionFailuresFollowDecodedIds) {
    const int32_t overflow = 1000;
    std::vector<uint8_t> dictionary(sizeof(overflow));
    memcpy(dictionary.data(), &overflow, sizeof(overflow));
    TestParquetDecodeSource source;
    source.set_dictionary(std::move(dictionary), sizeof(overflow), {0, 0});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                  .encoding = ParquetValueEncoding::DICTIONARY};
    IColumn::Filter null_map;
    null_map.resize_fill(2, 0);
    ParquetMaterializationState state;
    state.conversion_failure_null_map = &null_map;
    DataTypeInt8 type;
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 2, state).ok());
    EXPECT_EQ(column->size(), 2);
    EXPECT_EQ(null_map[0], 1);
    EXPECT_EQ(null_map[1], 1);
    EXPECT_EQ(source.dictionary_decode_calls(), 1);
}

TEST(DataTypeSerDeParquetTest, NonNullableDictionaryConversionFailureRemainsAnError) {
    const int32_t overflow = 1000;
    std::vector<uint8_t> dictionary(sizeof(overflow));
    memcpy(dictionary.data(), &overflow, sizeof(overflow));
    TestParquetDecodeSource source;
    source.set_dictionary(std::move(dictionary), sizeof(overflow), {0});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                  .encoding = ParquetValueEncoding::DICTIONARY};
    ParquetMaterializationState state;
    DataTypeInt8 type;
    auto column = type.create_column();

    EXPECT_FALSE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 1, state).ok());
    EXPECT_EQ(column->size(), 0);
}

} // namespace
} // namespace doris
