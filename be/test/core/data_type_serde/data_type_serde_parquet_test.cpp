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
    TestParquetDecodeSource source;
    source.set_fixed_values<int64_t>({3723456789, -1000001});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                  .logical_type = ParquetLogicalType::TIME,
                                  .time_unit = ParquetTimeUnit::MICROS};
    ParquetMaterializationState state;
    DataTypeTimeV2 type(6);
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 2, state).ok());
    EXPECT_EQ(type.to_string(*column, 0), "01:02:03.456789");
    EXPECT_EQ(type.to_string(*column, 1), "-00:00:01.000001");
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

} // namespace
} // namespace doris
