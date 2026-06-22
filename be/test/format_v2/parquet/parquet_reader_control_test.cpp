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

#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/reader/global_rowid_column_reader.h"
#include "format_v2/parquet/reader/row_position_column_reader.h"
#include "format_v2/parquet/selection_vector.h"
#include "storage/utils.h"

namespace doris::format::parquet {
namespace {

ParquetColumnSchema int64_schema(std::string name = "mock") {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = std::move(name);
    schema.type = std::make_shared<DataTypeInt64>();
    return schema;
}

std::unique_ptr<ParquetColumnSchema> primitive_child(int local_id, std::string name,
                                                     DataTypePtr type) {
    auto child = std::make_unique<ParquetColumnSchema>();
    child->local_id = local_id;
    child->name = std::move(name);
    child->kind = ParquetColumnSchemaKind::PRIMITIVE;
    child->leaf_column_id = local_id;
    child->type = std::move(type);
    child->type_descriptor.physical_type = ::parquet::Type::INT32;
    child->type_descriptor.doris_type = child->type;
    return child;
}

ParquetColumnSchema struct_schema_for_projection() {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "s";
    schema.kind = ParquetColumnSchemaKind::STRUCT;
    schema.children.push_back(primitive_child(0, "a", std::make_shared<DataTypeInt32>()));
    schema.children.push_back(primitive_child(1, "b", std::make_shared<DataTypeInt32>()));
    DataTypes types = {make_nullable(schema.children[0]->type),
                       make_nullable(schema.children[1]->type)};
    Strings names = {"a", "b"};
    schema.type = std::make_shared<DataTypeStruct>(types, names);
    return schema;
}

ParquetColumnSchema list_schema_for_projection() {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "xs";
    schema.kind = ParquetColumnSchemaKind::LIST;
    schema.children.push_back(primitive_child(0, "element", std::make_shared<DataTypeInt32>()));
    schema.type = std::make_shared<DataTypeArray>(schema.children[0]->type);
    return schema;
}

ParquetColumnSchema map_schema_for_projection() {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "m";
    schema.kind = ParquetColumnSchemaKind::MAP;
    schema.children.push_back(primitive_child(0, "key", std::make_shared<DataTypeString>()));
    schema.children.push_back(primitive_child(1, "value", std::make_shared<DataTypeInt32>()));
    schema.type = std::make_shared<DataTypeMap>(make_nullable(schema.children[0]->type),
                                                make_nullable(schema.children[1]->type));
    return schema;
}

class CursorColumnReader final : public ParquetColumnReader {
public:
    CursorColumnReader() : ParquetColumnReader(int64_schema(), std::make_shared<DataTypeInt64>()) {}

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override {
        if (column.get() == nullptr || rows_read == nullptr) {
            return Status::InvalidArgument("invalid mock read arguments");
        }
        auto* values = assert_cast<ColumnInt64*>(column.get());
        for (int64_t row = 0; row < rows; ++row) {
            values->insert_value(_cursor + row);
        }
        _read_lengths.push_back(rows);
        _cursor += rows;
        *rows_read = rows;
        return Status::OK();
    }

    Status skip(int64_t rows) override {
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

class NestedBuildReader final : public ParquetColumnReader {
public:
    explicit NestedBuildReader(int64_t values_to_build)
            : ParquetColumnReader(int64_schema("nested"), std::make_shared<DataTypeInt64>()),
              _values_to_build(values_to_build) {}

    Status read(int64_t, MutableColumnPtr&, int64_t*) override {
        return Status::NotSupported("unused");
    }

    Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                               int64_t* values_read) override {
        if (column.get() == nullptr || values_read == nullptr) {
            return Status::InvalidArgument("invalid mock nested build arguments");
        }
        _last_length_upper_bound = length_upper_bound;
        auto* values = assert_cast<ColumnInt64*>(column.get());
        for (int64_t value = 0; value < _values_to_build; ++value) {
            values->insert_value(value);
        }
        *values_read = _values_to_build;
        return Status::OK();
    }

    int64_t last_length_upper_bound() const { return _last_length_upper_bound; }

private:
    int64_t _values_to_build = 0;
    int64_t _last_length_upper_bound = 0;
};

class DefaultOnlyReader final : public ParquetColumnReader {
public:
    DefaultOnlyReader()
            : ParquetColumnReader(int64_schema("default_only"), std::make_shared<DataTypeInt64>()) {
    }

    Status read(int64_t, MutableColumnPtr&, int64_t*) override {
        return Status::NotSupported("unused");
    }
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
    EXPECT_EQ(column->size(), 0);
    EXPECT_EQ(reader.cursor(), 4);
    EXPECT_TRUE(reader.read_lengths().empty());
    EXPECT_EQ(reader.skip_lengths(), std::vector<int64_t>({4}));
}

TEST(ParquetColumnReaderControlTest, BaseNestedDefaultsAndSkipNested) {
    DefaultOnlyReader base_reader;
    EXPECT_FALSE(base_reader.skip(1).ok());
    EXPECT_FALSE(base_reader.load_nested_batch(1).ok());

    auto column = std::make_shared<DataTypeInt64>()->create_column();
    int64_t values_read = 0;
    EXPECT_FALSE(base_reader.build_nested_column(1, column, &values_read).ok());

    NestedBuildReader ok_reader(3);
    ASSERT_TRUE(ok_reader.skip_nested_column(3).ok());
    EXPECT_EQ(ok_reader.last_length_upper_bound(), 3);

    NestedBuildReader short_reader(2);
    EXPECT_FALSE(short_reader.skip_nested_column(3).ok());
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

TEST(ParquetColumnReaderFactoryTest, RejectsInvalidLeafIdBeforeCreatingRecordReader) {
    ParquetColumnSchema schema = int64_schema("bad_leaf");
    schema.kind = ParquetColumnSchemaKind::PRIMITIVE;
    schema.leaf_column_id = 3;
    schema.type_descriptor.physical_type = ::parquet::Type::INT64;
    schema.type_descriptor.doris_type = schema.type;

    ParquetColumnReaderFactory factory(nullptr, 1);
    std::unique_ptr<ParquetColumnReader> reader;
    const auto status = factory.create(schema, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid parquet leaf column id"), std::string::npos);
}

TEST(ParquetColumnReaderFactoryTest, RejectsStructInvalidAndEmptyProjection) {
    auto schema = struct_schema_for_projection();
    ParquetColumnReaderFactory factory(nullptr, 0);
    std::unique_ptr<ParquetColumnReader> reader;

    auto invalid_projection = format::LocalColumnIndex::partial_local(0);
    invalid_projection.children.push_back(format::LocalColumnIndex::local(9));
    auto status = factory.create(schema, &invalid_projection, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("invalid child"), std::string::npos);

    auto empty_projection = format::LocalColumnIndex::partial_local(0);
    status = factory.create(schema, &empty_projection, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains no children"), std::string::npos);
}

TEST(ParquetColumnReaderFactoryTest, RejectsListProjectionWithoutElement) {
    auto schema = list_schema_for_projection();
    ParquetColumnReaderFactory factory(nullptr, 0);
    std::unique_ptr<ParquetColumnReader> reader;

    auto projection = format::LocalColumnIndex::partial_local(0);
    const auto status = factory.create(schema, &projection, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains no element"), std::string::npos);
}

TEST(ParquetColumnReaderFactoryTest, RejectsMapInvalidAndKeyOnlyProjection) {
    auto schema = map_schema_for_projection();
    ParquetColumnReaderFactory factory(nullptr, 0);
    std::unique_ptr<ParquetColumnReader> reader;

    auto invalid_projection = format::LocalColumnIndex::partial_local(0);
    invalid_projection.children.push_back(format::LocalColumnIndex::local(1));
    invalid_projection.children.push_back(format::LocalColumnIndex::local(9));
    auto status = factory.create(schema, &invalid_projection, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("invalid child"), std::string::npos);

    auto key_only_projection = format::LocalColumnIndex::partial_local(0);
    key_only_projection.children.push_back(format::LocalColumnIndex::local(0));
    status = factory.create(schema, &key_only_projection, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains no value"), std::string::npos);
}

} // namespace doris::format::parquet
