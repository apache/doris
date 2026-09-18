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
#include <cctz/time_zone.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_array_serde.h"
#include "core/data_type_serde/data_type_datev2_serde.h"
#include "core/data_type_serde/data_type_map_serde.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_number_serde.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/data_type_serde/data_type_time_serde.h"
#include "core/data_type_serde/data_type_timestamptz_serde.h"
#include "core/data_type_serde/data_type_varbinary_serde.h"

namespace doris {
namespace {

class ScopedArrowInputValidation {
public:
    explicit ScopedArrowInputValidation(bool enabled)
            : _old_value(config::enable_arrow_input_validation) {
        config::enable_arrow_input_validation = enabled;
    }

    ~ScopedArrowInputValidation() { config::enable_arrow_input_validation = _old_value; }

private:
    bool _old_value;
};

template <typename Func>
void expect_invalid_arrow(Func&& func, std::string_view message) {
    bool thrown = false;
    try {
        std::forward<Func>(func)();
    } catch (const Exception& e) {
        thrown = true;
        EXPECT_EQ(e.code(), ErrorCode::INVALID_ARGUMENT) << e.to_string();
    }
    EXPECT_TRUE(thrown) << message;
}

template <typename OffsetType>
std::shared_ptr<arrow::Buffer> wrap_offsets(const std::vector<OffsetType>& offsets) {
    return arrow::Buffer::Wrap(offsets);
}

struct StringArrayHolder {
    StringArrayHolder(std::vector<int32_t> offsets_, std::string_view values_)
            : offsets(std::move(offsets_)), values(values_) {
        auto value_buffer = arrow::Buffer::Wrap(values.data(), values.size());
        array = std::make_shared<arrow::StringArray>(offsets.size() - 1, wrap_offsets(offsets),
                                                     value_buffer);
    }

    std::vector<int32_t> offsets;
    std::string values;
    std::shared_ptr<arrow::StringArray> array;
};

struct LargeListArrayHolder {
    LargeListArrayHolder(int64_t length, std::vector<int64_t> offsets_,
                         std::vector<int64_t> values_)
            : offsets(std::move(offsets_)), values(std::move(values_)) {
        auto values_array =
                std::make_shared<arrow::Int64Array>(values.size(), arrow::Buffer::Wrap(values));
        array = std::make_shared<arrow::LargeListArray>(arrow::large_list(arrow::int64()), length,
                                                        wrap_offsets(offsets), values_array);
    }

    std::vector<int64_t> offsets;
    std::vector<int64_t> values;
    std::shared_ptr<arrow::LargeListArray> array;
};

void expect_invalid_large_list_offsets(int64_t length, std::vector<int64_t> offsets,
                                       std::vector<int64_t> values, std::string_view message) {
    LargeListArrayHolder array(length, std::move(offsets), std::move(values));
    auto column = ColumnArray::create(
            ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create()),
            ColumnOffset64::create());
    auto nested_serde = std::make_shared<DataTypeNullableSerDe>(
            std::make_shared<DataTypeNumberSerDe<TYPE_BIGINT>>());
    DataTypeArraySerDe serde(nested_serde);

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.array.get(), 0,
                                                               length, cctz::utc_time_zone()));
            },
            message);
}

template <typename ArrowArrayType, PrimitiveType DorisType, typename DorisColumnType>
void expect_invalid_short_numeric_buffer(std::string_view message) {
    using ArrowValueType = typename ArrowArrayType::value_type;
    std::vector<ArrowValueType> values = {1};
    auto array = std::make_shared<ArrowArrayType>(2, arrow::Buffer::Wrap(values));
    auto column = DorisColumnType::create();
    DataTypeNumberSerDe<DorisType> serde;

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 2,
                                                               cctz::utc_time_zone()));
            },
            message);
}

template <typename ArrowArrayType, PrimitiveType DorisType, typename DorisColumnType>
void expect_invalid_missing_numeric_validity_bitmap(std::string_view message) {
    using ArrowValueType = typename ArrowArrayType::value_type;
    std::vector<ArrowValueType> values = {1, 2};
    auto array = std::make_shared<ArrowArrayType>(2, arrow::Buffer::Wrap(values),
                                                  std::shared_ptr<arrow::Buffer>(), 1);
    auto column = DorisColumnType::create();
    DataTypeNumberSerDe<DorisType> serde;

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 2,
                                                               cctz::utc_time_zone()));
            },
            message);
}

template <typename DorisColumnType, typename SerDeType>
void expect_invalid_temporal_arrow_buffers(const std::shared_ptr<arrow::DataType>& arrow_type,
                                           size_t value_width, SerDeType& serde,
                                           std::string_view type_name) {
    std::vector<uint8_t> short_values(value_width, 0);
    auto short_data = arrow::ArrayData::Make(
            arrow_type, 2, {std::shared_ptr<arrow::Buffer>(), arrow::Buffer::Wrap(short_values)},
            0);
    auto short_array = arrow::MakeArray(short_data);
    auto short_column = DorisColumnType::create();
    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*short_column, short_array.get(), 0,
                                                               2, cctz::utc_time_zone()));
            },
            std::string(type_name) + " short values buffer should be rejected");

    std::vector<uint8_t> values(value_width * 2, 0);
    auto missing_validity_data = arrow::ArrayData::Make(
            arrow_type, 2, {std::shared_ptr<arrow::Buffer>(), arrow::Buffer::Wrap(values)}, 1);
    auto missing_validity_array = arrow::MakeArray(missing_validity_data);
    auto missing_validity_column = DorisColumnType::create();
    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*missing_validity_column,
                                                               missing_validity_array.get(), 0, 2,
                                                               cctz::utc_time_zone()));
            },
            std::string(type_name) + " missing validity bitmap should be rejected");

    auto invalid_range_data = arrow::ArrayData::Make(
            arrow_type, 1, {std::shared_ptr<arrow::Buffer>(), arrow::Buffer::Wrap(values)}, 0);
    auto invalid_range_array = arrow::MakeArray(invalid_range_data);
    auto invalid_range_column = DorisColumnType::create();
    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*invalid_range_column,
                                                               invalid_range_array.get(), 0, 2,
                                                               cctz::utc_time_zone()));
            },
            std::string(type_name) + " invalid read range should be rejected");
}

void expect_invalid_varbinary_arrow(const arrow::Array& array, int64_t start, int64_t end,
                                    std::string_view message) {
    auto column = ColumnVarbinary::create();
    DataTypeVarbinarySerDe serde;
    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, &array, start, end,
                                                               cctz::utc_time_zone()));
            },
            message);
}

} // namespace

TEST(DataTypeSerDeArrowValidationTest, RejectsShortStringOffsetsBuffer) {
    ScopedArrowInputValidation validation(true);

    std::vector<int32_t> offsets = {0};
    std::string_view values = "abc";
    auto value_buffer = arrow::Buffer::Wrap(values.data(), values.size());
    auto array = std::make_shared<arrow::StringArray>(1, wrap_offsets(offsets), value_buffer);
    auto column = ColumnString::create();
    DataTypeStringSerDe serde(TYPE_STRING);

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 1,
                                                               cctz::utc_time_zone()));
            },
            "short string offsets buffer should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsStringValueRangeBeyondBuffer) {
    ScopedArrowInputValidation validation(true);

    StringArrayHolder array({0, 8}, "abc");
    auto column = ColumnString::create();
    DataTypeStringSerDe serde(TYPE_STRING);

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.array.get(), 0, 1,
                                                               cctz::utc_time_zone()));
            },
            "string value range beyond data buffer should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsNonMonotonicStringOffsets) {
    ScopedArrowInputValidation validation(true);

    StringArrayHolder array({3, 1}, "abcd");
    auto column = ColumnString::create();
    DataTypeStringSerDe serde(TYPE_STRING);

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.array.get(), 0, 1,
                                                               cctz::utc_time_zone()));
            },
            "non-monotonic string offsets should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsShortFixedWidthDataBuffer) {
    ScopedArrowInputValidation validation(true);

    std::vector<int64_t> values = {1};
    auto data_buffer = arrow::Buffer::Wrap(values);
    auto array = std::make_shared<arrow::Int64Array>(2, data_buffer);
    auto column = ColumnInt64::create();
    DataTypeNumberSerDe<TYPE_BIGINT> serde;

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 2,
                                                               cctz::utc_time_zone()));
            },
            "short int64 data buffer should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsShortEarlyReturnNumericDataBuffers) {
    ScopedArrowInputValidation validation(true);

    expect_invalid_short_numeric_buffer<arrow::HalfFloatArray, TYPE_FLOAT, ColumnFloat32>(
            "short half-float data buffer should be rejected");
    expect_invalid_short_numeric_buffer<arrow::UInt8Array, TYPE_SMALLINT, ColumnInt16>(
            "short uint8 data buffer should be rejected");
    expect_invalid_short_numeric_buffer<arrow::UInt16Array, TYPE_INT, ColumnInt32>(
            "short uint16 data buffer should be rejected");
    expect_invalid_short_numeric_buffer<arrow::UInt32Array, TYPE_BIGINT, ColumnInt64>(
            "short uint32 data buffer should be rejected");
    expect_invalid_short_numeric_buffer<arrow::UInt64Array, TYPE_LARGEINT, ColumnInt128>(
            "short uint64 data buffer should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsMissingEarlyReturnNumericValidityBitmaps) {
    ScopedArrowInputValidation validation(true);

    expect_invalid_missing_numeric_validity_bitmap<arrow::HalfFloatArray, TYPE_FLOAT,
                                                   ColumnFloat32>(
            "missing half-float validity bitmap should be rejected");
    expect_invalid_missing_numeric_validity_bitmap<arrow::UInt8Array, TYPE_SMALLINT, ColumnInt16>(
            "missing unsigned widening validity bitmap should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsMalformedNewTemporalArrays) {
    ScopedArrowInputValidation validation(true);

    DataTypeTimeV2SerDe time_serde;
    expect_invalid_temporal_arrow_buffers<ColumnTimeV2>(arrow::time32(arrow::TimeUnit::SECOND),
                                                        sizeof(arrow::Time32Array::value_type),
                                                        time_serde, "time32");
    expect_invalid_temporal_arrow_buffers<ColumnTimeV2>(arrow::time64(arrow::TimeUnit::MICRO),
                                                        sizeof(arrow::Time64Array::value_type),
                                                        time_serde, "time64");

    DataTypeTimeStampTzSerDe timestamptz_serde(6);
    expect_invalid_temporal_arrow_buffers<ColumnTimeStampTz>(
            arrow::timestamp(arrow::TimeUnit::MICRO), sizeof(arrow::TimestampArray::value_type),
            timestamptz_serde, "timestamp");

    DataTypeDateV2SerDe date_serde;
    expect_invalid_temporal_arrow_buffers<ColumnDateV2>(
            arrow::date64(), sizeof(arrow::Date64Array::value_type), date_serde, "date64");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsMalformedVarbinaryArrays) {
    ScopedArrowInputValidation validation(true);

    std::string values = "abc";
    const auto value_buffer = arrow::Buffer::Wrap(values.data(), values.size());

    std::vector<int32_t> short_offsets = {0};
    arrow::BinaryArray short_binary_offsets(1, wrap_offsets(short_offsets), value_buffer);
    expect_invalid_varbinary_arrow(short_binary_offsets, 0, 1,
                                   "short binary offsets buffer should be rejected");

    std::vector<int32_t> negative_offsets = {-1, 0};
    arrow::BinaryArray negative_binary_offsets(1, wrap_offsets(negative_offsets), value_buffer);
    expect_invalid_varbinary_arrow(negative_binary_offsets, 0, 1,
                                   "negative binary offset should be rejected");

    std::vector<int32_t> non_monotonic_offsets = {2, 1};
    arrow::BinaryArray non_monotonic_binary_offsets(1, wrap_offsets(non_monotonic_offsets),
                                                    value_buffer);
    expect_invalid_varbinary_arrow(non_monotonic_binary_offsets, 0, 1,
                                   "non-monotonic binary offsets should be rejected");

    std::vector<int32_t> oversized_offsets = {0, 4};
    arrow::BinaryArray oversized_binary_value(1, wrap_offsets(oversized_offsets), value_buffer);
    expect_invalid_varbinary_arrow(oversized_binary_value, 0, 1,
                                   "binary value range beyond buffer should be rejected");

    std::vector<int64_t> short_large_offsets = {0};
    arrow::LargeBinaryArray short_large_binary_offsets(1, wrap_offsets(short_large_offsets),
                                                       value_buffer);
    expect_invalid_varbinary_arrow(short_large_binary_offsets, 0, 1,
                                   "short large-binary offsets buffer should be rejected");

    std::vector<int64_t> oversized_large_offsets = {0, 4};
    arrow::LargeBinaryArray oversized_large_binary_value(1, wrap_offsets(oversized_large_offsets),
                                                         value_buffer);
    expect_invalid_varbinary_arrow(oversized_large_binary_value, 0, 1,
                                   "large-binary value range beyond buffer should be rejected");

    std::vector<uint8_t> short_fixed_values(3, 0);
    arrow::FixedSizeBinaryArray short_fixed_binary(arrow::fixed_size_binary(3), 2,
                                                   arrow::Buffer::Wrap(short_fixed_values));
    expect_invalid_varbinary_arrow(short_fixed_binary, 0, 2,
                                   "short fixed-size binary buffer should be rejected");

    std::vector<int32_t> valid_offsets = {0, 1};
    arrow::BinaryArray missing_validity(1, wrap_offsets(valid_offsets), value_buffer,
                                        std::shared_ptr<arrow::Buffer>(), 1);
    expect_invalid_varbinary_arrow(missing_validity, 0, 1,
                                   "missing varbinary validity bitmap should be rejected");

    arrow::BinaryArray valid_binary(1, wrap_offsets(valid_offsets), value_buffer);
    expect_invalid_varbinary_arrow(valid_binary, 0, 2,
                                   "invalid varbinary read range should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsSlicedArrowArray) {
    ScopedArrowInputValidation validation(true);

    std::vector<int64_t> values = {1, 2, 3};
    auto original = std::make_shared<arrow::Int64Array>(3, arrow::Buffer::Wrap(values));
    auto sliced = original->Slice(1, 2);
    auto column = ColumnInt64::create();
    DataTypeNumberSerDe<TYPE_BIGINT> serde;

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, sliced.get(), 0, 2,
                                                               cctz::utc_time_zone()));
            },
            "sliced Arrow array should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsShortBooleanDataBitmap) {
    ScopedArrowInputValidation validation(true);

    std::vector<uint8_t> bits = {0xFF};
    auto data_buffer = arrow::Buffer::Wrap(bits);
    auto array = std::make_shared<arrow::BooleanArray>(9, data_buffer);
    auto column = ColumnUInt8::create();
    DataTypeNumberSerDe<TYPE_BOOLEAN> serde;

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 9,
                                                               cctz::utc_time_zone()));
            },
            "short boolean data bitmap should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsShortValidityBitmap) {
    ScopedArrowInputValidation validation(true);

    std::vector<uint8_t> validity = {0xFF};
    std::vector<int64_t> values(9, 1);
    auto validity_buffer = arrow::Buffer::Wrap(validity);
    auto data_buffer = arrow::Buffer::Wrap(values);
    auto array = std::make_shared<arrow::Int64Array>(9, data_buffer, validity_buffer);
    auto column = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto nested_serde = std::make_shared<DataTypeNumberSerDe<TYPE_BIGINT>>();
    DataTypeNullableSerDe serde(nested_serde);

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 9,
                                                               cctz::utc_time_zone()));
            },
            "short validity bitmap should be rejected before IsNull");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsMissingValidityBitmapWithNullCount) {
    ScopedArrowInputValidation validation(true);

    std::vector<int64_t> values = {1, 2};
    auto data_buffer = arrow::Buffer::Wrap(values);
    auto array = std::make_shared<arrow::Int64Array>(2, data_buffer,
                                                     std::shared_ptr<arrow::Buffer>(), 1);
    auto column = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto nested_serde = std::make_shared<DataTypeNumberSerDe<TYPE_BIGINT>>();
    DataTypeNullableSerDe serde(nested_serde);

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 2,
                                                               cctz::utc_time_zone()));
            },
            "missing validity bitmap with positive null_count should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsListOffsetsBeyondValuesLength) {
    ScopedArrowInputValidation validation(true);

    StringArrayHolder values({0, 1}, "a");
    std::vector<int32_t> offsets = {0, 2};
    auto offsets_buffer = wrap_offsets(offsets);
    auto array = std::make_shared<arrow::ListArray>(arrow::list(arrow::utf8()), 1, offsets_buffer,
                                                    values.array);
    auto column = ColumnArray::create(
            ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()),
            ColumnOffset64::create());
    auto nested_serde = std::make_shared<DataTypeNullableSerDe>(
            std::make_shared<DataTypeStringSerDe>(TYPE_STRING));
    DataTypeArraySerDe serde(nested_serde);

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 1,
                                                               cctz::utc_time_zone()));
            },
            "list offsets beyond values length should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsMalformedLargeListOffsets) {
    ScopedArrowInputValidation validation(true);

    expect_invalid_large_list_offsets(1, {0}, {1},
                                      "short large-list offsets buffer should be rejected");
    expect_invalid_large_list_offsets(1, {-1, 0}, {1},
                                      "negative large-list offset should be rejected");
    expect_invalid_large_list_offsets(2, {0, 2, 1}, {1, 2},
                                      "non-monotonic large-list offsets should be rejected");
    expect_invalid_large_list_offsets(1, {0, 2}, {1},
                                      "large-list offsets beyond values length should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsMapOffsetsBeyondKeysLength) {
    ScopedArrowInputValidation validation(true);

    StringArrayHolder keys({0, 1}, "k");
    std::vector<int64_t> item_values = {1};
    auto items = std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(item_values));
    std::vector<int32_t> offsets = {0, 2};
    auto offsets_buffer = wrap_offsets(offsets);
    auto array = std::make_shared<arrow::MapArray>(arrow::map(arrow::utf8(), arrow::int64()), 1,
                                                   offsets_buffer, keys.array, items);
    auto column = ColumnMap::create(ColumnString::create(), ColumnInt64::create(),
                                    ColumnOffset64::create());
    DataTypeMapSerDe serde(std::make_shared<DataTypeStringSerDe>(TYPE_STRING),
                           std::make_shared<DataTypeNumberSerDe<TYPE_BIGINT>>());

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 1,
                                                               cctz::utc_time_zone()));
            },
            "map offsets beyond keys length should be rejected");
}

} // namespace doris
