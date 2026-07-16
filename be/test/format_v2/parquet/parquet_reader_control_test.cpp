// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <vector>

#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/reader/global_rowid_column_reader.h"
#include "format_v2/parquet/reader/row_position_column_reader.h"
#include "format_v2/parquet/selection_vector.h"
#include "storage/utils.h"

namespace doris::format::parquet {
namespace {

ParquetColumnSchema int64_schema() {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "mock";
    schema.type = std::make_shared<DataTypeInt64>();
    return schema;
}

class CursorColumnReader final : public ParquetColumnReader {
public:
    CursorColumnReader() : ParquetColumnReader(int64_schema(), std::make_shared<DataTypeInt64>()) {}

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override {
        DORIS_CHECK(column);
        DORIS_CHECK(rows_read != nullptr);
        auto& values = assert_cast<ColumnInt64&>(*column);
        for (int64_t row = 0; row < rows; ++row) {
            values.insert_value(_cursor + row);
        }
        _read_lengths.push_back(rows);
        _cursor += rows;
        *rows_read = rows;
        return Status::OK();
    }

    Status skip(int64_t rows) override {
        DORIS_CHECK(rows >= 0);
        _skip_lengths.push_back(rows);
        _cursor += rows;
        return Status::OK();
    }

    int64_t cursor() const { return _cursor; }
    const std::vector<int64_t>& skip_lengths() const { return _skip_lengths; }
    const std::vector<int64_t>& read_lengths() const { return _read_lengths; }

private:
    int64_t _cursor = 0;
    std::vector<int64_t> _skip_lengths;
    std::vector<int64_t> _read_lengths;
};

GlobalRowLoacationV2 decode_rowid(const ColumnString& column, size_t row) {
    const auto ref = column.get_data_at(row);
    EXPECT_EQ(ref.size, sizeof(GlobalRowLoacationV2));
    GlobalRowLoacationV2 location(0, 0, 0, 0);
    std::memcpy(&location, ref.data, sizeof(GlobalRowLoacationV2));
    return location;
}

} // namespace

TEST(SelectionVectorTest, IdentitySelectionToRanges) {
    SelectionVector selection;
    const auto ranges = selection_to_ranges(selection, 5);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].start, 0);
    EXPECT_EQ(ranges[0].length, 5);
    EXPECT_TRUE(selection.verify(5, 5).ok());
}

TEST(SelectionVectorTest, ExternalBufferSelectionToRanges) {
    SelectionVector::Index indices[] = {0, 1, 4, 6, 7};
    SelectionVector selection(indices, std::size(indices));
    const auto ranges = selection_to_ranges(selection, std::size(indices));
    ASSERT_EQ(ranges.size(), 3);
    EXPECT_EQ(ranges[0].start, 0);
    EXPECT_EQ(ranges[0].length, 2);
    EXPECT_EQ(ranges[1].start, 4);
    EXPECT_EQ(ranges[1].length, 1);
    EXPECT_EQ(ranges[2].start, 6);
    EXPECT_EQ(ranges[2].length, 2);
    EXPECT_TRUE(selection.verify(std::size(indices), 8).ok());
}

TEST(SelectionVectorTest, OutputRangesReuseCapacity) {
    SelectionVector::Index indices[] = {1, 2, 5};
    SelectionVector selection(indices, std::size(indices));
    std::vector<RowRange> ranges;
    ranges.reserve(8);
    const auto retained_capacity = ranges.capacity();

    selection_to_ranges(selection, std::size(indices), &ranges);
    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges.capacity(), retained_capacity);
    selection_to_ranges(selection, 1, &ranges);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges.capacity(), retained_capacity);
}

TEST(SelectionVectorTest, VerifyRejectsInvalidSelection) {
    SelectionVector selection(2);
    EXPECT_FALSE(selection.verify(3, 3).ok());
    EXPECT_FALSE(selection.verify(1, -1).ok());

    selection.set_index(0, 2);
    selection.set_index(1, 1);
    EXPECT_FALSE(selection.verify(2, 3).ok());

    selection.set_index(0, 0);
    selection.set_index(1, 3);
    EXPECT_FALSE(selection.verify(2, 3).ok());
}

TEST(ParquetColumnReaderControlTest, BaseSelectUsesSkipReadRanges) {
    CursorColumnReader reader;
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 2);
    selection.set_index(2, 4);

    auto column = std::make_shared<DataTypeInt64>()->create_column();
    ASSERT_TRUE(reader.select(selection, 3, 6, column).ok());

    const auto& values = assert_cast<const ColumnInt64&>(*column);
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(values.get_element(0), 0);
    EXPECT_EQ(values.get_element(1), 2);
    EXPECT_EQ(values.get_element(2), 4);
    EXPECT_EQ(reader.cursor(), 6);
    EXPECT_EQ(reader.read_lengths(), std::vector<int64_t>({1, 1, 1}));
    EXPECT_EQ(reader.skip_lengths(), std::vector<int64_t>({0, 1, 1, 1}));
}

TEST(ParquetColumnReaderControlTest, BaseSelectZeroRowsConsumesBatch) {
    CursorColumnReader reader;
    SelectionVector selection;
    auto column = std::make_shared<DataTypeInt64>()->create_column();
    ASSERT_TRUE(reader.select(selection, 0, 4, column).ok());
    EXPECT_TRUE(column->empty());
    EXPECT_EQ(reader.cursor(), 4);
    EXPECT_TRUE(reader.read_lengths().empty());
    EXPECT_EQ(reader.skip_lengths(), std::vector<int64_t>({4}));
}

TEST(ParquetVirtualColumnReaderTest, RowPositionReadSkipAndInvalidArgs) {
    RowPositionColumnReader reader(100);
    EXPECT_EQ(reader.file_column_id(), format::ROW_POSITION_COLUMN_ID);
    EXPECT_EQ(reader.parquet_leaf_column_id(), -1);
    EXPECT_EQ(reader.name(), format::ROW_POSITION_COLUMN_NAME);

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    ASSERT_TRUE(reader.read(2, column, &rows_read).ok());
    ASSERT_EQ(rows_read, 2);
    ASSERT_TRUE(reader.skip(3).ok());
    ASSERT_TRUE(reader.read(2, column, &rows_read).ok());

    const auto& values = assert_cast<const ColumnInt64&>(*column);
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(values.get_element(0), 100);
    EXPECT_EQ(values.get_element(1), 101);
    EXPECT_EQ(values.get_element(2), 105);
    EXPECT_EQ(values.get_element(3), 106);

    MutableColumnPtr null_column;
    EXPECT_FALSE(reader.read(1, null_column, &rows_read).ok());
    EXPECT_FALSE(reader.read(-1, column, &rows_read).ok());
    EXPECT_FALSE(reader.read(1, column, nullptr).ok());
}

TEST(ParquetVirtualColumnReaderTest, GlobalRowIdReadSkipSelectAndInvalidArgs) {
    format::GlobalRowIdContext context {.version = 7, .backend_id = 123456789, .file_id = 42};
    GlobalRowIdColumnReader reader(context, 10);
    EXPECT_EQ(reader.file_column_id(), format::GLOBAL_ROWID_COLUMN_ID);
    EXPECT_EQ(reader.parquet_leaf_column_id(), -1);
    EXPECT_EQ(reader.name(), BeConsts::GLOBAL_ROWID_COL);

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    ASSERT_TRUE(reader.read(2, column, &rows_read).ok());
    ASSERT_TRUE(reader.skip(2).ok());
    ASSERT_TRUE(reader.read(1, column, &rows_read).ok());

    const auto& strings = assert_cast<const ColumnString&>(*column);
    ASSERT_EQ(strings.size(), 3);
    const auto first = decode_rowid(strings, 0);
    EXPECT_EQ(first.version, context.version);
    EXPECT_EQ(first.backend_id, context.backend_id);
    EXPECT_EQ(first.file_id, context.file_id);
    EXPECT_EQ(first.row_id, 10);
    EXPECT_EQ(decode_rowid(strings, 1).row_id, 11);
    EXPECT_EQ(decode_rowid(strings, 2).row_id, 14);

    GlobalRowIdColumnReader select_reader(context, 20);
    SelectionVector selection(2);
    selection.set_index(0, 1);
    selection.set_index(1, 3);
    auto selected_column = select_reader.type()->create_column();
    ASSERT_TRUE(select_reader.select(selection, 2, 5, selected_column).ok());
    const auto& selected_strings = assert_cast<const ColumnString&>(*selected_column);
    ASSERT_EQ(selected_strings.size(), 2);
    EXPECT_EQ(decode_rowid(selected_strings, 0).row_id, 21);
    EXPECT_EQ(decode_rowid(selected_strings, 1).row_id, 23);

    MutableColumnPtr null_column;
    EXPECT_FALSE(reader.read(1, null_column, &rows_read).ok());
    EXPECT_FALSE(reader.read(-1, column, &rows_read).ok());
    EXPECT_FALSE(reader.read(1, column, nullptr).ok());
}

} // namespace doris::format::parquet
