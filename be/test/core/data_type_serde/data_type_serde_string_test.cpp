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
#include <lz4/lz4.h>
#include <streamvbyte.h>

#include <cstddef>
#include <iomanip>
#include <iostream>
#include <limits>
#include <type_traits>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/data_type/common_data_type_serder_test.h"
#include "core/data_type/common_data_type_test.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/types.h"
#include "storage/olap_common.h"
#include "testutil/test_util.h"

namespace doris {
static std::string test_data_dir;

static auto serde_str = std::make_shared<DataTypeStringSerDe>(TYPE_STRING);

static ColumnString::MutablePtr column_str32;
static ColumnString64::MutablePtr column_str64;
class DataTypeStringSerDeTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";

        column_str32 = ColumnString::create();
        column_str64 = ColumnString64::create();

        load_columns_data();
    }

    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        {
            MutableColumns columns;
            columns.push_back(column_str32->get_ptr());
            DataTypeSerDeSPtrs serde = {serde_str};
            std::string data_file = test_data_dir + "/STRING.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
            EXPECT_TRUE(!column_str32->empty());
            column_str32->insert_default();

            column_str64->insert_range_from(*column_str32, 0, column_str32->size());
        }
        std::cout << "column str size: " << column_str32->size() << std::endl;
    }
};
TEST_F(DataTypeStringSerDeTest, serdes) {
    auto test_func = [](const auto& serde, const auto& source_column) {
        using SerdeType = decltype(serde);
        using ColumnType = typename std::remove_reference<SerdeType>::type::ColumnStrType;

        auto row_count = source_column->size();
        auto option = DataTypeSerDe::FormatOptions();
        char field_delim = ';';
        option.field_delim = std::string(1, field_delim);

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
                EXPECT_EQ(deser_col_with_type->get_data_at(j), source_column->get_data_at(j));
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
                EXPECT_EQ(deser_col_with_type->get_data_at(j), source_column->get_data_at(j));
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
                EXPECT_EQ(deser_col_with_type->get_data_at(j), source_column->get_data_at(j));
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
            ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
            const JsonbDocument& doc = *pdoc;
            for (auto it = doc->begin(); it != doc->end(); ++it) {
                serde.read_one_cell_from_jsonb(*deser_column, it->value());
            }
            for (size_t j = 0; j != row_count; ++j) {
                EXPECT_EQ(deser_col_with_type->get_data_at(j), source_column->get_data_at(j));
            }
        }
    };
    test_func(*serde_str, column_str32);
}

// Run with UBSan enabled to catch misalignment errors.
TEST_F(DataTypeStringSerDeTest, ArrowMemNotAligned) {
    // 1.Prepare the data.
    std::vector<std::string> strings = {"hello", "world!", "test", "unaligned", "memory"};

    int32_t total_length = 0;
    std::vector<int32_t> offsets = {0};
    for (const auto& str : strings) {
        total_length += static_cast<int32_t>(str.length());
        offsets.push_back(total_length);
    }

    // 2.Create an unaligned memory buffer.
    std::vector<uint8_t> value_storage(total_length + 10);
    std::vector<uint8_t> offset_storage((strings.size() + 1) * sizeof(int32_t) + 10);

    uint8_t* unaligned_value_data = value_storage.data() + 1;
    uint8_t* unaligned_offset_data = offset_storage.data() + 1;

    // 3. Copy data to unaligned memory
    int32_t current_pos = 0;
    for (size_t i = 0; i < strings.size(); ++i) {
        memcpy(unaligned_value_data + current_pos, strings[i].data(), strings[i].length());
        current_pos += strings[i].length();
    }

    for (size_t i = 0; i < offsets.size(); ++i) {
        memcpy(unaligned_offset_data + i * sizeof(int32_t), &offsets[i], sizeof(int32_t));
    }

    // 4. Create Arrow array with unaligned memory
    auto value_buffer = arrow::Buffer::Wrap(unaligned_value_data, total_length);
    auto offset_buffer =
            arrow::Buffer::Wrap(unaligned_offset_data, offsets.size() * sizeof(int32_t));
    auto arr = std::make_shared<arrow::StringArray>(strings.size(), offset_buffer, value_buffer);

    const auto* offsets_ptr = arr->raw_value_offsets();
    uintptr_t address = reinterpret_cast<uintptr_t>(offsets_ptr);
    EXPECT_EQ((reinterpret_cast<uintptr_t>(address) % 4), 1);

    // 5.Test read_column_from_arrow
    cctz::time_zone tz;
    auto st = serde_str->read_column_from_arrow(*column_str32, arr.get(), 0, 1, tz);
    EXPECT_TRUE(st.ok());
}

// Run with UBSan enabled to catch misalignment errors.
TEST_F(DataTypeStringSerDeTest, ArrowMemNotAlignedNestedArr) {
    // 1.Prepare the data.
    std::vector<std::string> string_data = {"hello", "world", "test", "a", "b", "c"};
    std::vector<int32_t> string_offsets = {0};
    int32_t current_offset = 0;
    for (const auto& str : string_data) {
        current_offset += static_cast<int32_t>(str.length());
        string_offsets.push_back(current_offset);
    }

    std::vector<int32_t> list_offsets = {0, 2, 3, 6, 6};
    std::vector<uint8_t> value_data;
    for (const auto& str : string_data) {
        value_data.insert(value_data.end(), str.begin(), str.end());
    }

    const int64_t num_lists = list_offsets.size() - 1;
    const int64_t offset_element_size = sizeof(int32_t);

    // 2.Create an unaligned memory buffer.
    std::vector<uint8_t> list_offset_storage(list_offsets.size() * offset_element_size + 10);
    uint8_t* unaligned_list_offsets = list_offset_storage.data() + 1;

    std::vector<uint8_t> string_offset_storage(string_offsets.size() * offset_element_size + 10);
    uint8_t* unaligned_string_offsets = string_offset_storage.data() + 1;

    std::vector<uint8_t> value_storage(value_data.size() + 10);
    uint8_t* unaligned_values = value_storage.data() + 1;

    // 3. Copy data to unaligned memory
    for (size_t i = 0; i < list_offsets.size(); ++i) {
        memcpy(unaligned_list_offsets + i * offset_element_size, &list_offsets[i],
               offset_element_size);
    }

    for (size_t i = 0; i < string_offsets.size(); ++i) {
        memcpy(unaligned_string_offsets + i * offset_element_size, &string_offsets[i],
               offset_element_size);
    }

    memcpy(unaligned_values, value_data.data(), value_data.size());

    // 4. Create Arrow array with unaligned memory
    auto value_buffer = arrow::Buffer::Wrap(unaligned_values, value_data.size());
    auto string_offsets_buffer =
            arrow::Buffer::Wrap(unaligned_string_offsets, string_offsets.size() * sizeof(int32_t));
    auto string_array = std::make_shared<arrow::StringArray>(string_offsets.size() - 1,
                                                             string_offsets_buffer, value_buffer);
    auto list_offsets_buffer =
            arrow::Buffer::Wrap(unaligned_list_offsets, list_offsets.size() * offset_element_size);
    auto list_offsets_array =
            std::make_shared<arrow::Int32Array>(list_offsets.size(), list_offsets_buffer);

    auto arr = std::make_shared<arrow::ListArray>(arrow::list(arrow::utf8()), num_lists,
                                                  list_offsets_buffer, string_array);

    const auto* concrete_array = dynamic_cast<const arrow::ListArray*>(arr.get());
    auto arrow_offsets_array = concrete_array->offsets();
    auto* arrow_offsets = dynamic_cast<arrow::Int32Array*>(arrow_offsets_array.get());

    const auto* offsets_ptr = arrow_offsets->raw_values();
    uintptr_t offsets_address = reinterpret_cast<uintptr_t>(offsets_ptr);
    EXPECT_EQ(offsets_address % 4, 1);

    const auto* values_ptr = string_array->value_data()->data();
    uintptr_t values_address = reinterpret_cast<uintptr_t>(values_ptr);
    EXPECT_EQ(values_address % 4, 1);

    // 5.Test read_column_from_arrow
    auto ser_col = ColumnArray::create(ColumnString::create(), ColumnOffset64::create());
    cctz::time_zone tz;
    auto serde_list = std::make_shared<DataTypeArraySerDe>(serde_str);
    auto st = serde_list->read_column_from_arrow(*ser_col, arr.get(), 0, 1, tz);
    EXPECT_TRUE(st.ok());
}

// Simulate the sender-side bug: the sender upgrades a utf8 builder to large_utf8 (for columns
// exceeding 2MB), producing int64 offsets in the buffer, but forgets to update the schema,
// so the schema still says utf8. This test demonstrates that naive int32 reading of such a
// buffer gives wrong (negative) lengths at every other row, and that our receiver-side fix
// correctly recovers the data.
TEST_F(DataTypeStringSerDeTest, ArrowInt64OffsetsInUtf8Schema_ShowBug) {
    // Build a few strings that are easy to verify.
    std::vector<std::string> strings = {"apple",      "banana",  "cherry",  "doris_db",
                                        "arrow_data", "flight",  "segment", "row_seven",
                                        "eighth_row", "last_row"};
    const int64_t row_count = static_cast<int64_t>(strings.size());

    // Step 1: Build a LargeStringArray — this uses int64 offsets in its buffer.
    arrow::LargeStringBuilder builder;
    for (const auto& s : strings) {
        ASSERT_TRUE(builder.Append(s).ok());
    }
    std::shared_ptr<arrow::Array> large_array;
    ASSERT_TRUE(builder.Finish(&large_array).ok());
    ASSERT_EQ(large_array->type_id(), arrow::Type::LARGE_STRING);

    // Step 2: Simulate the buggy sender — wrap with utf8 type but keep the int64 offsets buffer.
    // This is exactly what block_convertor.cpp did before the sender-side fix.
    auto buggy_data =
            arrow::ArrayData::Make(arrow::utf8(),         // schema says utf8 (int32)
                                   large_array->length(), // but the buffer has int64 offsets!
                                   large_array->data()->buffers, large_array->null_count(), 0);
    auto buggy_array = std::make_shared<arrow::StringArray>(buggy_data);

    // Step 3: Verify the offsets buffer is int64-sized (the mismatch we're testing).
    const int64_t expected_int64_buf = (row_count + 1) * static_cast<int64_t>(sizeof(int64_t));
    const int64_t expected_int32_buf = (row_count + 1) * static_cast<int64_t>(sizeof(int32_t));
    // Print the buffer contents so the int32/int64 mismatch is visually clear.
    std::cout << "\n--- Offsets buffer viewed as int64 vs int32 (the bug) ---\n";
    std::cout << std::left << std::setw(6) << "Row" << std::setw(16) << "int64 offset"
              << std::setw(20) << "int32[2r]  (low)" << std::setw(20) << "int32[2r+1] (high)"
              << "naive length = int32[2r+1-1] - int32[2r-1] (*)\n";
    const int64_t* as_int64 =
            reinterpret_cast<const int64_t*>(buggy_array->value_offsets()->data());
    const int32_t* as_int32 =
            reinterpret_cast<const int32_t*>(buggy_array->value_offsets()->data());
    for (int i = 0; i <= static_cast<int>(strings.size()); ++i) {
        std::cout << std::setw(6) << i << std::setw(16) << as_int64[i] << std::setw(20)
                  << as_int32[2 * i]                      // lower 4 bytes
                  << std::setw(20) << as_int32[2 * i + 1] // upper 4 bytes (always 0 here)
                  << "\n";
    }
    // Show the naive int32 length computation for each row.
    // The receiver calls value_offset(r) = as_int32[r], so:
    //   naive_length(r) = as_int32[r+1] - as_int32[r]
    // This reads ACROSS the int64 boundary: as_int32[2r+1] (high half of offset[r])
    // minus as_int32[2r] (low half of offset[r]), which gives 0 - low_half = negative.
    std::cout << "\n--- Naive int32 length per row (what the buggy receiver computes) ---\n";
    for (int r = 0; r < static_cast<int>(strings.size()); ++r) {
        int32_t naive_start = as_int32[r];
        int32_t naive_end = as_int32[r + 1];
        int32_t naive_len = naive_end - naive_start;
        std::cout << "Row " << r << " (\"" << strings[r] << "\", real len=" << strings[r].size()
                  << "): naive start=" << naive_start << "  end=" << naive_end
                  << "  length=" << naive_len
                  << (naive_len < 0 ? "  ← CRASH (negative length passed to memcpy)" : "") << "\n";
    }
    std::cout << "---\n\n";

    EXPECT_EQ(buggy_array->value_offsets()->size(), expected_int64_buf)
            << "Offsets buffer is int64-sized, but schema says utf8 (int32)";
    EXPECT_NE(buggy_array->value_offsets()->size(), expected_int32_buf)
            << "Should NOT equal int32 expected size";

    // Step 4: Show the bug — naive int32 reading of the int64 buffer gives wrong lengths.
    //
    // The 10-row LargeStringArray has 11 int64 offsets in its buffer (row N's data spans
    // [offset[N], offset[N+1])). With strings "apple"(5), "banana"(6), "cherry"(6), ...,
    // the cumulative byte positions are: 0, 5, 11, 17, 25, 35, 41, 48, 57, 67, 75.
    //
    // These int64 values are laid out in memory (little-endian) as 8 bytes each.
    // For small values (< 2^32), the upper 4 bytes are always 0:
    //
    //   Byte offset  Value  Binary (LE, 8 bytes)           As two int32 words
    //   0            0      00 00 00 00 | 00 00 00 00  →   int32[0]=0,  int32[1]=0
    //   8            5      05 00 00 00 | 00 00 00 00  →   int32[2]=5,  int32[3]=0
    //   16           11     0B 00 00 00 | 00 00 00 00  →   int32[4]=11, int32[5]=0
    //   24           17     11 00 00 00 | 00 00 00 00  →   int32[6]=17, int32[7]=0
    //   ...
    //
    // The buggy receiver treats this buffer as int32 and calls value_offset(r) which returns
    // the r-th int32 word (not the r-th int64 offset).
    //
    // For row 2 ("cherry", expected length 6):
    //   start_offset = value_offset(2) = int32[2] = 5    ← lower half of int64(5)
    //   end_offset   = value_offset(3) = int32[3] = 0    ← upper half of int64(5), always 0
    //   length       = end_offset - start_offset = 0 - 5 = -5  ← NEGATIVE → crash
    //
    // The same pattern repeats for every even row (0, 2, 4, ...) where the upper-half word
    // of the preceding int64 offset (always 0) is used as end_offset.
    const int32_t* naive_int32_offsets =
            reinterpret_cast<const int32_t*>(buggy_array->value_offsets()->data());
    //   int32[2] = lower 4 bytes of int64(5)  = 5  → becomes start_offset of row 2
    //   int32[3] = upper 4 bytes of int64(5)  = 0  → becomes end_offset of row 2 (corrupted)
    //   naive length for row 2 = 0 - 5 = -5
    const int32_t naive_len_row2 = naive_int32_offsets[3] - naive_int32_offsets[2];
    EXPECT_LT(naive_len_row2, 0) << "Naive int32 reading of int64 offsets gives negative length "
                                    "(demonstrating the crash bug): got "
                                 << naive_len_row2;
}

TEST_F(DataTypeStringSerDeTest, ArrowInt64OffsetsInUtf8Schema_ReceiverFix) {
    std::vector<std::string> strings = {"apple",      "banana",  "cherry",  "doris_db",
                                        "arrow_data", "flight",  "segment", "row_seven",
                                        "eighth_row", "last_row"};
    const int64_t row_count = static_cast<int64_t>(strings.size());

    // Build a LargeStringArray (int64 offsets), then rewrap with utf8 type — simulates bug.
    arrow::LargeStringBuilder builder;
    for (const auto& s : strings) {
        ASSERT_TRUE(builder.Append(s).ok());
    }
    std::shared_ptr<arrow::Array> large_array;
    ASSERT_TRUE(builder.Finish(&large_array).ok());

    auto buggy_data =
            arrow::ArrayData::Make(arrow::utf8(), large_array->length(),
                                   large_array->data()->buffers, large_array->null_count(), 0);
    auto buggy_array = std::make_shared<arrow::StringArray>(buggy_data);

    // Our receiver-side fix: read_column_from_arrow detects int64 offsets by comparing
    // offsets buffer size against (N+1)*8 vs (N+1)*4, and falls through to LargeBinaryArray.
    auto result_col = ColumnString::create();
    cctz::time_zone tz;
    auto st = serde_str->read_column_from_arrow(*result_col, buggy_array.get(), 0, row_count, tz);
    ASSERT_TRUE(st.ok()) << "Receiver fix should handle int64 offsets in utf8 array: "
                         << st.to_string();

    // Verify every row is correctly recovered.
    ASSERT_EQ(static_cast<int64_t>(result_col->size()), row_count);
    for (int64_t i = 0; i < row_count; i++) {
        EXPECT_EQ(result_col->get_data_at(i).to_string(), strings[i])
                << "Data mismatch at row " << i;
    }
}

} // namespace doris
