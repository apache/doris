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
#include <streamvbyte.h>

#include <cstddef>
#include <iostream>
#include <type_traits>

#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/common_data_type_serder_test.h"
#include "core/data_type/common_data_type_test.h"
#include "core/data_type/data_type.h"
#include "core/data_type_serde/data_type_timestamptz_serde.h"
#include "core/field.h"
#include "testutil/test_util.h"
#include "util/slice.h"
#include "util/string_util.h"
namespace doris {
static std::string test_data_dir;

static auto serde_tz_0 = std::make_shared<DataTypeTimeStampTzSerDe>(0);
static auto serde_tz_3 = std::make_shared<DataTypeTimeStampTzSerDe>(3);
static auto serde_tz_6 = std::make_shared<DataTypeTimeStampTzSerDe>(6);

static ColumnTimeStampTz::MutablePtr column_tz_0;
static ColumnTimeStampTz::MutablePtr column_tz_3;
static ColumnTimeStampTz::MutablePtr column_tz_6;

class DataTypeTimeStampTzSerDeTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";

        column_tz_0 = ColumnTimeStampTz::create();
        column_tz_3 = ColumnTimeStampTz::create();
        column_tz_6 = ColumnTimeStampTz::create();

        load_columns_data();
    }
    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        TimezoneUtils::load_timezones_to_cache();
        cctz::time_zone tz;
        TimezoneUtils::find_cctz_time_zone("+08:00", tz);
        auto test_func = [&](const MutableColumnPtr& column, const auto& serde,
                             const std::string& data_file_name) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serdes = {serde};
            load_columns_data_from_file(columns, serdes, ';', {0},
                                        test_data_dir + "/" + data_file_name);
            EXPECT_TRUE(!column->empty());
        };
        test_func(column_tz_0->get_ptr(), serde_tz_0, "TIMESTAMPTZ(0).csv");
        test_func(column_tz_3->get_ptr(), serde_tz_3, "TIMESTAMPTZ(3).csv");
        test_func(column_tz_6->get_ptr(), serde_tz_6, "TIMESTAMPTZ(6).csv");

        std::cout << "loading test dataset done" << std::endl;
    }
};
TEST_F(DataTypeTimeStampTzSerDeTest, serdes) {
    TimezoneUtils::load_timezones_to_cache();
    auto option = DataTypeSerDe::FormatOptions();
    char field_delim = ';';
    option.field_delim = std::string(1, field_delim);
    cctz::time_zone tz;
    TimezoneUtils::find_cctz_time_zone("+08:00", tz);
    option.timezone = &tz;
    auto test_func = [&](const auto& serde, const auto& source_column) {
        using SerdeType = decltype(serde);
        using ColumnType = typename std::remove_reference<SerdeType>::type::ColumnType;

        auto row_count = source_column->size();

        {
            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);
            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());

            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j != row_count; ++j) {
                auto st =
                        serde.serialize_one_cell_to_json(*source_column, j, buffer_writer, option);
                EXPECT_TRUE(st.ok()) << "Failed to serialize column at row " << j << ": " << st;

                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                Slice slice {actual_str_value.data(), actual_str_value.size()};
                st = serde.deserialize_one_cell_from_json(*deser_column, slice, option);
                EXPECT_TRUE(st.ok()) << "Failed to deserialize column at row " << j << ": " << st;
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
            }
        }

        // test serialize_column_to_json
        {
            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);

            VectorBufferWriter buffer_writer(*ser_col.get());
            auto st = serde.serialize_column_to_json(*source_column, 0, source_column->size(),
                                                     buffer_writer, option);
            EXPECT_TRUE(st.ok()) << "Failed to serialize column to json: " << st;
            buffer_writer.commit();

            std::string json_data((char*)ser_col->get_chars().data(), ser_col->get_chars().size());
            std::vector<std::string> strs = doris::split(json_data, std::string(1, field_delim));
            std::vector<Slice> slices;
            for (const auto& s : strs) {
                Slice tmp_slice(s.data(), s.size());
                tmp_slice.trim_prefix();
                slices.emplace_back(tmp_slice);
            }

            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            uint64_t num_deserialized = 0;
            st = serde.deserialize_column_from_json_vector(*deser_column, slices, &num_deserialized,
                                                           option);
            EXPECT_TRUE(st.ok()) << "Failed to deserialize column from json: " << st;
            EXPECT_EQ(num_deserialized, row_count);
            for (size_t j = 0; j != row_count; ++j) {
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
            }
        }

        {
            // test write_column_to_pb/read_column_from_pb
            PValues pv = PValues();
            Status st = serde.write_column_to_pb(*source_column, pv, 0, row_count);
            EXPECT_TRUE(st.ok()) << "Failed to write column to pb: " << st;

            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            st = serde.read_column_from_pb(*deser_column, pv);
            EXPECT_TRUE(st.ok()) << "Failed to read column from pb: " << st;
            for (size_t j = 0; j != row_count; ++j) {
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
            }
        }
        {
            // test write_one_cell_to_jsonb/read_one_cell_from_jsonb
            JsonbWriterT<JsonbOutStream> jsonb_writer;
            jsonb_writer.writeStartObject();
            Arena pool;
            DataTypeSerDe::FormatOptions options;
            auto tz = cctz::utc_time_zone();
            options.timezone = &tz;

            for (size_t j = 0; j != row_count; ++j) {
                serde.write_one_cell_to_jsonb(*source_column, jsonb_writer, pool, 0, j, options);
            }
            jsonb_writer.writeEndObject();

            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);
            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            const JsonbDocument* pdoc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(jsonb_writer.getOutput()->getBuffer(),
                                                            jsonb_writer.getOutput()->getSize(),
                                                            &pdoc);
            EXPECT_TRUE(st.ok()) << "Failed to create JsonbDocument: " << st;
            const JsonbDocument& doc = *pdoc;
            for (auto it = doc->begin(); it != doc->end(); ++it) {
                serde.read_one_cell_from_jsonb(*deser_column, it->value());
            }
            for (size_t j = 0; j != row_count; ++j) {
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
            }
        }
    };
    test_func(*serde_tz_0, column_tz_0);
    test_func(*serde_tz_3, column_tz_3);
    test_func(*serde_tz_6, column_tz_6);
}

// Roundtrip via write_one_cell_to_binary + DataTypeSerDe::deserialize_binary_to_*
// (the variant sparse-column path). Regression for DORIS-25915: the writer
// previously inherited DataTypeNumberSerDe's default, which omitted the scale
// byte, while the reader expected a scale byte — a 1-byte layout mismatch that
// silently corrupted timestamptz values stored in variant sparse paths.
TEST_F(DataTypeTimeStampTzSerDeTest, binary_roundtrip) {
    auto test_func = [&](const DataTypeTimeStampTzSerDe& serde,
                         const ColumnTimeStampTz::MutablePtr& source_column, uint32_t scale) {
        const size_t row_count = source_column->size();
        ASSERT_GT(row_count, 0);

        // Per-row layout: [type:1][scale:1][value:8] = 10 bytes.
        constexpr size_t kBytesPerRow =
                sizeof(uint8_t) + sizeof(uint8_t) + sizeof(TimestampTzValue::underlying_value);

        auto chars_col = ColumnString::create();
        ColumnString::Chars& chars = chars_col->get_chars();
        std::vector<size_t> row_offsets;
        row_offsets.reserve(row_count + 1);
        row_offsets.push_back(0);
        for (size_t i = 0; i < row_count; ++i) {
            serde.write_one_cell_to_binary(*source_column, chars, i);
            row_offsets.push_back(chars.size());
        }

        // Layout: each row contributes exactly 10 bytes; first byte is the
        // TIMESTAMPTZ FieldType; second is the scale.
        ASSERT_EQ(chars.size(), row_count * kBytesPerRow);
        for (size_t i = 0; i < row_count; ++i) {
            ASSERT_EQ(row_offsets[i + 1] - row_offsets[i], kBytesPerRow);
            EXPECT_EQ(chars[row_offsets[i]],
                      static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ));
            EXPECT_EQ(chars[row_offsets[i] + 1], static_cast<uint8_t>(scale));
        }

        // Roundtrip via deserialize_binary_to_column (column path).
        {
            auto inner_col = ColumnTimeStampTz::create();
            auto null_map = ColumnUInt8::create();
            auto nullable_col = ColumnNullable::create(std::move(inner_col), std::move(null_map));
            for (size_t i = 0; i < row_count; ++i) {
                const uint8_t* start = chars.data() + row_offsets[i];
                const uint8_t* end =
                        DataTypeSerDe::deserialize_binary_to_column(start, *nullable_col);
                EXPECT_EQ(end - start, kBytesPerRow);
            }
            const auto& read_col =
                    assert_cast<const ColumnTimeStampTz&>(nullable_col->get_nested_column());
            for (size_t i = 0; i < row_count; ++i) {
                EXPECT_EQ(read_col.get_element(i), source_column->get_element(i))
                        << "row " << i << " mismatched after binary roundtrip";
            }
        }

        // Roundtrip via deserialize_binary_to_field (field path); scale
        // should propagate into FieldInfo.
        {
            for (size_t i = 0; i < row_count; ++i) {
                const uint8_t* start = chars.data() + row_offsets[i];
                Field f;
                FieldInfo info;
                const uint8_t* end = DataTypeSerDe::deserialize_binary_to_field(start, f, info);
                EXPECT_EQ(end - start, kBytesPerRow);
                EXPECT_EQ(info.scale, static_cast<int>(scale));
                EXPECT_EQ(f.get<TYPE_TIMESTAMPTZ>(), source_column->get_element(i))
                        << "row " << i << " mismatched after field roundtrip";
            }
        }
    };

    test_func(*serde_tz_0, column_tz_0, 0);
    test_func(*serde_tz_3, column_tz_3, 3);
    test_func(*serde_tz_6, column_tz_6, 6);
}
} // namespace doris
