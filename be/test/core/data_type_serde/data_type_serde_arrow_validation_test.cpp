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
#include <cstring>
#include <limits>
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
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_array_serde.h"
#include "core/data_type_serde/data_type_map_serde.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_number_serde.h"
#include "core/data_type_serde/data_type_string_serde.h"
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

std::shared_ptr<arrow::Buffer> wrap_offsets(const std::vector<int32_t>& offsets) {
    return arrow::Buffer::Wrap(offsets);
}

std::shared_ptr<arrow::Buffer> wrap_large_offsets(const std::vector<int64_t>& offsets) {
    return arrow::Buffer::Wrap(offsets);
}

auto make_array_column() {
    return ColumnArray::create(
            ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()),
            ColumnOffset64::create());
}

DataTypeArraySerDe make_string_array_serde() {
    return DataTypeArraySerDe(std::make_shared<DataTypeNullableSerDe>(
            std::make_shared<DataTypeStringSerDe>(TYPE_STRING)));
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
    StringArrayHolder values({0, 1}, "a");
    DataTypeArraySerDe serde = make_string_array_serde();

    const auto expect_invalid_offsets = [&](const std::vector<int64_t>& offsets,
                                            std::string_view message) {
        auto array = std::make_shared<arrow::LargeListArray>(
                arrow::large_list(arrow::utf8()), 1, wrap_large_offsets(offsets), values.array);
        auto column = make_array_column();
        expect_invalid_arrow(
                [&] {
                    static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 1,
                                                                   cctz::utc_time_zone()));
                },
                message);
    };

    expect_invalid_offsets({0}, "short LargeList offsets buffer should be rejected");
    expect_invalid_offsets({0, -1}, "negative LargeList offsets should be rejected");
    expect_invalid_offsets({1, 0}, "non-monotonic LargeList offsets should be rejected");
    expect_invalid_offsets({0, 2}, "LargeList offsets beyond values length should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, ReadsLargeListWithUnalignedOffsets) {
    ScopedArrowInputValidation validation(true);

    std::vector<uint8_t> storage(2 * sizeof(int64_t) + alignof(int64_t));
    auto* offsets_data = storage.data();
    while (reinterpret_cast<uintptr_t>(offsets_data) % alignof(int64_t) == 0) {
        ++offsets_data;
    }
    const int64_t offsets[] = {0, 1};
    memcpy(offsets_data, offsets, sizeof(offsets));

    StringArrayHolder values({0, 1}, "a");
    auto array = std::make_shared<arrow::LargeListArray>(
            arrow::large_list(arrow::utf8()), 1, arrow::Buffer::Wrap(offsets_data, sizeof(offsets)),
            values.array);
    auto column = make_array_column();
    DataTypeArraySerDe serde = make_string_array_serde();

    ASSERT_TRUE(
            serde.read_column_from_arrow(*column, array.get(), 0, 1, cctz::utc_time_zone()).ok());
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(column->get_offsets()[0], 1);
}

TEST(DataTypeSerDeArrowValidationTest, RejectsFixedSizeListOffsetOverflow) {
    ScopedArrowInputValidation validation(true);

    auto values = std::make_shared<arrow::Int32Array>(0, std::shared_ptr<arrow::Buffer>());
    constexpr int64_t list_size = std::numeric_limits<int32_t>::max();
    const int64_t overflowing_index = std::numeric_limits<int64_t>::max() / list_size + 1;
    auto array = std::make_shared<arrow::FixedSizeListArray>(
            arrow::fixed_size_list(arrow::int32(), list_size), overflowing_index, values);
    auto column = make_array_column();
    DataTypeArraySerDe serde = make_string_array_serde();

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(),
                                                               overflowing_index, overflowing_index,
                                                               cctz::utc_time_zone()));
            },
            "FixedSizeList offset multiplication overflow should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, RejectsVarbinaryValueRangeBeyondBuffer) {
    ScopedArrowInputValidation validation(true);

    std::vector<int32_t> offsets = {0, 8};
    const std::string values = "a";
    auto array = std::make_shared<arrow::BinaryArray>(
            1, wrap_offsets(offsets), arrow::Buffer::Wrap(values.data(), values.size()));
    auto column = ColumnVarbinary::create();
    DataTypeVarbinarySerDe serde;

    expect_invalid_arrow(
            [&] {
                static_cast<void>(serde.read_column_from_arrow(*column, array.get(), 0, 1,
                                                               cctz::utc_time_zone()));
            },
            "VARBINARY value range beyond data buffer should be rejected");
}

TEST(DataTypeSerDeArrowValidationTest, ReadsEmptyVarbinaryWithoutValuesBuffer) {
    ScopedArrowInputValidation validation(true);

    std::vector<int32_t> offsets = {0, 0};
    auto array = std::make_shared<arrow::BinaryArray>(1, wrap_offsets(offsets),
                                                      std::shared_ptr<arrow::Buffer>());
    auto column = ColumnVarbinary::create();
    DataTypeVarbinarySerDe serde;

    ASSERT_TRUE(
            serde.read_column_from_arrow(*column, array.get(), 0, 1, cctz::utc_time_zone()).ok());
    ASSERT_EQ(column->size(), 1);
    EXPECT_TRUE(column->get_data_at(0).empty());
}

TEST(DataTypeSerDeArrowValidationTest, ReadsLargeVarbinaryWithUnalignedOffsets) {
    ScopedArrowInputValidation validation(true);

    std::vector<uint8_t> storage(2 * sizeof(int64_t) + alignof(int64_t));
    auto* offsets_data = storage.data();
    while (reinterpret_cast<uintptr_t>(offsets_data) % alignof(int64_t) == 0) {
        ++offsets_data;
    }
    const int64_t offsets[] = {0, 1};
    memcpy(offsets_data, offsets, sizeof(offsets));
    const std::string values = "a";
    auto array = std::make_shared<arrow::LargeBinaryArray>(
            1, arrow::Buffer::Wrap(offsets_data, sizeof(offsets)),
            arrow::Buffer::Wrap(values.data(), values.size()));
    auto column = ColumnVarbinary::create();
    DataTypeVarbinarySerDe serde;

    ASSERT_TRUE(
            serde.read_column_from_arrow(*column, array.get(), 0, 1, cctz::utc_time_zone()).ok());
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(column->get_data_at(0).to_string(), values);
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
