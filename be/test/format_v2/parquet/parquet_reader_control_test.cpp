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
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_statistics.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/reader/global_rowid_column_reader.h"
#include "format_v2/parquet/reader/list_column_reader.h"
#include "format_v2/parquet/reader/map_column_reader.h"
#include "format_v2/parquet/reader/nested_column_materializer.h"
#include "format_v2/parquet/reader/row_position_column_reader.h"
#include "format_v2/parquet/reader/scalar_column_reader.h"
#include "format_v2/parquet/reader/struct_column_reader.h"
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

ParquetColumnSchema nested_int64_schema(std::string name, int16_t nullable_definition_level,
                                        int16_t definition_level, int16_t repetition_level = 0,
                                        int16_t repeated_ancestor_definition_level = 0) {
    ParquetColumnSchema schema = int64_schema(std::move(name));
    schema.type = make_nullable(std::make_shared<DataTypeInt64>());
    schema.nullable_definition_level = nullable_definition_level;
    schema.definition_level = definition_level;
    schema.repetition_level = repetition_level;
    schema.repeated_repetition_level = repetition_level;
    schema.repeated_ancestor_definition_level = repeated_ancestor_definition_level;
    return schema;
}

ParquetColumnSchema nested_struct_schema() {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "struct";
    schema.kind = ParquetColumnSchemaKind::STRUCT;
    schema.nullable_definition_level = 1;
    schema.definition_level = 2;
    schema.type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt64>()),
                       make_nullable(std::make_shared<DataTypeInt64>())},
            Strings {"a", "b"}));
    return schema;
}

ParquetColumnSchema nested_list_schema(std::string name, DataTypePtr element_type,
                                       int16_t nullable_definition_level, int16_t definition_level,
                                       int16_t repetition_level,
                                       int16_t repeated_ancestor_definition_level) {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = std::move(name);
    schema.kind = ParquetColumnSchemaKind::LIST;
    schema.nullable_definition_level = nullable_definition_level;
    schema.definition_level = definition_level;
    schema.repetition_level = repetition_level;
    schema.repeated_repetition_level = repetition_level;
    schema.repeated_ancestor_definition_level = repeated_ancestor_definition_level;
    schema.type = make_nullable(std::make_shared<DataTypeArray>(std::move(element_type)));
    return schema;
}

ParquetColumnSchema nested_map_schema(
        DataTypePtr value_type = make_nullable(std::make_shared<DataTypeInt64>())) {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "map";
    schema.kind = ParquetColumnSchemaKind::MAP;
    schema.nullable_definition_level = 1;
    schema.definition_level = 2;
    schema.repetition_level = 1;
    schema.repeated_ancestor_definition_level = 2;
    schema.type = make_nullable(std::make_shared<DataTypeMap>(
            make_nullable(std::make_shared<DataTypeInt64>()), std::move(value_type)));
    return schema;
}

ParquetColumnSchema bare_repeated_int64_list_schema() {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "repeated";
    schema.kind = ParquetColumnSchemaKind::LIST;
    schema.definition_level = 1;
    schema.repetition_level = 1;
    schema.repeated_repetition_level = 1;
    schema.repeated_ancestor_definition_level = 1;
    schema.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());
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

class ScriptedNestedReader final : public ParquetColumnReader {
public:
    ScriptedNestedReader(ParquetColumnSchema schema, DataTypePtr type,
                         std::vector<int16_t> def_levels, std::vector<int16_t> rep_levels,
                         bool has_repeated_child = false, bool build_nulls = false)
            : ParquetColumnReader(schema, std::move(type)),
              _def_levels(std::move(def_levels)),
              _rep_levels(std::move(rep_levels)),
              _has_repeated_child(has_repeated_child),
              _build_nulls(build_nulls) {}

    Status read(int64_t, MutableColumnPtr&, int64_t*) override {
        return Status::NotSupported("unused");
    }

    Status load_nested_batch(int64_t rows) override {
        _load_lengths.push_back(rows);
        return Status::OK();
    }

    Status load_nested_levels_batch(int64_t rows) override {
        _level_load_lengths.push_back(rows);
        return Status::OK();
    }

    Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                               int64_t* values_read) override {
        _build_lengths.push_back(length_upper_bound);
        if (column.get() == nullptr || values_read == nullptr) {
            return Status::InvalidArgument("invalid scripted nested build arguments");
        }
        for (int64_t row = 0; row < length_upper_bound; ++row) {
            insert_value(column, _next_value++, _build_nulls);
        }
        *values_read = length_upper_bound;
        return Status::OK();
    }

    Status consume_nested_column(int64_t length_upper_bound, int64_t* values_consumed) override {
        DORIS_CHECK(values_consumed != nullptr);
        _consume_lengths.push_back(length_upper_bound);
        set_nested_build_level_cursor(std::min(nested_build_level_cursor() + length_upper_bound,
                                               static_cast<int64_t>(_def_levels.size())));
        *values_consumed = length_upper_bound;
        return Status::OK();
    }

    const std::vector<int16_t>& nested_definition_levels() const override { return _def_levels; }
    const std::vector<int16_t>& nested_repetition_levels() const override { return _rep_levels; }
    int64_t nested_levels_written() const override {
        return static_cast<int64_t>(_def_levels.size());
    }
    bool is_or_has_repeated_child() const override { return _has_repeated_child; }

    const std::vector<int64_t>& build_lengths() const { return _build_lengths; }
    const std::vector<int64_t>& consume_lengths() const { return _consume_lengths; }
    const std::vector<int64_t>& level_load_lengths() const { return _level_load_lengths; }

private:
    static void insert_value(MutableColumnPtr& column, int64_t value, bool is_null) {
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
            nullable_column != nullptr) {
            if (is_null) {
                nullable_column->insert_default();
                return;
            }
            assert_cast<ColumnInt64&>(nullable_column->get_nested_column()).insert_value(value);
            nullable_column->get_null_map_data().push_back(0);
            return;
        }
        assert_cast<ColumnInt64&>(*column).insert_value(value);
    }

    std::vector<int16_t> _def_levels;
    std::vector<int16_t> _rep_levels;
    bool _has_repeated_child = false;
    bool _build_nulls = false;
    int64_t _next_value = 0;
    std::vector<int64_t> _load_lengths;
    std::vector<int64_t> _level_load_lengths;
    std::vector<int64_t> _build_lengths;
    std::vector<int64_t> _consume_lengths;
};

class ChunkedNestedLeafReader final : public ParquetColumnReader {
public:
    ChunkedNestedLeafReader()
            : ParquetColumnReader(nested_int64_schema("element", 0, 1, 1, 1),
                                  std::make_shared<DataTypeInt64>()) {}

    Status read(int64_t, MutableColumnPtr&, int64_t*) override {
        return Status::NotSupported("unused");
    }

    Status load_nested_batch(int64_t rows) override {
        _load_lengths.push_back(rows);
        _def_levels.assign(static_cast<size_t>(rows), 1);
        _rep_levels.assign(static_cast<size_t>(rows), 0);
        return Status::OK();
    }

    Status load_nested_levels_batch(int64_t rows) override {
        _level_load_lengths.push_back(rows);
        _def_levels.assign(static_cast<size_t>(rows), 1);
        _rep_levels.assign(static_cast<size_t>(rows), 0);
        return Status::OK();
    }

    Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                               int64_t* values_read) override {
        DORIS_CHECK(column.get() != nullptr);
        DORIS_CHECK(values_read != nullptr);
        _initial_column_sizes.push_back(column->size());
        _build_lengths.push_back(length_upper_bound);
        if (auto* nullable = check_and_get_column<ColumnNullable>(*column); nullable != nullptr) {
            auto& values = assert_cast<ColumnInt64&>(nullable->get_nested_column());
            for (int64_t row = 0; row < length_upper_bound; ++row) {
                values.insert_value(row);
                nullable->get_null_map_data().push_back(0);
            }
        } else {
            auto* values = assert_cast<ColumnInt64*>(column.get());
            for (int64_t row = 0; row < length_upper_bound; ++row) {
                values->insert_value(row);
            }
        }
        *values_read = length_upper_bound;
        return Status::OK();
    }

    Status consume_nested_column(int64_t length_upper_bound, int64_t* values_consumed) override {
        DORIS_CHECK(values_consumed != nullptr);
        _consume_lengths.push_back(length_upper_bound);
        *values_consumed = length_upper_bound;
        return Status::OK();
    }

    const std::vector<int16_t>& nested_definition_levels() const override { return _def_levels; }
    const std::vector<int16_t>& nested_repetition_levels() const override { return _rep_levels; }
    int64_t nested_levels_written() const override {
        return static_cast<int64_t>(_def_levels.size());
    }
    bool is_or_has_repeated_child() const override { return true; }

    const std::vector<int64_t>& load_lengths() const { return _load_lengths; }
    const std::vector<int64_t>& build_lengths() const { return _build_lengths; }
    const std::vector<int64_t>& consume_lengths() const { return _consume_lengths; }
    const std::vector<int64_t>& level_load_lengths() const { return _level_load_lengths; }
    const std::vector<size_t>& initial_column_sizes() const { return _initial_column_sizes; }

private:
    std::vector<int16_t> _def_levels;
    std::vector<int16_t> _rep_levels;
    std::vector<int64_t> _load_lengths;
    std::vector<int64_t> _level_load_lengths;
    std::vector<int64_t> _build_lengths;
    std::vector<int64_t> _consume_lengths;
    std::vector<size_t> _initial_column_sizes;
};

} // namespace

struct ScalarColumnReaderTestAccess {
    static void set_nested_batch(ScalarColumnReader* reader,
                                 std::unique_ptr<ParquetNestedScalarBatch> batch) {
        reader->_nested_batch = std::move(batch);
    }

    static int64_t page_filtered_rows_to_skip(const ScalarColumnReader& reader, int64_t rows) {
        return reader.page_filtered_rows_to_skip(rows);
    }

    static void set_row_group_rows_read(ScalarColumnReader* reader, int64_t rows) {
        reader->_row_group_rows_read = rows;
    }

    static Status append_dictionary_filtered_values(
            const ScalarColumnReader& reader,
            const std::vector<std::shared_ptr<::arrow::Array>>& chunks,
            const IColumn::Filter& dictionary_filter, MutableColumnPtr& column,
            IColumn::Filter* row_filter, int64_t* matched_rows, bool* used_filter) {
        return reader.append_dictionary_filtered_values(chunks, dictionary_filter, column,
                                                        row_filter, matched_rows, used_filter);
    }
};

namespace {

ParquetColumnSchema string_schema(std::string name = "string") {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = std::move(name);
    schema.type = std::make_shared<DataTypeString>();
    schema.type_descriptor.physical_type = ::parquet::Type::BYTE_ARRAY;
    schema.type_descriptor.doris_type = schema.type;
    return schema;
}

std::shared_ptr<::arrow::Array> dictionary_array(const std::vector<int8_t>& indices,
                                                 const std::vector<std::string>& values) {
    ::arrow::Int8Builder index_builder;
    EXPECT_TRUE(index_builder.AppendValues(indices).ok());
    auto index_result = index_builder.Finish();
    EXPECT_TRUE(index_result.ok()) << index_result.status();

    ::arrow::StringBuilder dictionary_builder;
    EXPECT_TRUE(dictionary_builder.AppendValues(values).ok());
    auto dictionary_result = dictionary_builder.Finish();
    EXPECT_TRUE(dictionary_result.ok()) << dictionary_result.status();

    auto result = ::arrow::DictionaryArray::FromArrays(
            ::arrow::dictionary(::arrow::int8(), ::arrow::utf8()), *index_result,
            *dictionary_result);
    EXPECT_TRUE(result.ok()) << result.status();
    return *result;
}

std::unique_ptr<ScalarColumnReader> make_scripted_scalar_reader(
        ParquetColumnSchema schema, std::unique_ptr<ParquetNestedScalarBatch> batch) {
    auto reader = std::make_unique<ScalarColumnReader>(schema, nullptr);
    ScalarColumnReaderTestAccess::set_nested_batch(reader.get(), std::move(batch));
    return reader;
}

std::unique_ptr<ParquetNestedScalarBatch> scalar_batch(std::vector<int16_t> def_levels,
                                                       std::vector<int16_t> rep_levels,
                                                       std::vector<int64_t> value_indices,
                                                       std::vector<int64_t> values) {
    auto batch = std::make_unique<ParquetNestedScalarBatch>();
    batch->levels_written = static_cast<int64_t>(def_levels.size());
    batch->def_levels = std::move(def_levels);
    batch->rep_levels = std::move(rep_levels);
    batch->value_indices = std::move(value_indices);
    auto column = ColumnInt64::create();
    for (const auto value : values) {
        column->insert_value(value);
    }
    batch->values_column = std::move(column);
    return batch;
}

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

TEST(ParquetScalarColumnReaderTest, DictionaryIndexOutsideFilterIsCorruption) {
    ScalarColumnReader reader(string_schema("dictionary_value"), nullptr);
    MutableColumnPtr column = ColumnString::create();
    IColumn::Filter row_filter;
    int64_t matched_rows = 0;
    bool used_filter = false;
    const std::vector<std::shared_ptr<::arrow::Array>> chunks = {
            dictionary_array({0, 1}, {"keep", "out-of-range"})};

    const auto status = ScalarColumnReaderTestAccess::append_dictionary_filtered_values(
            reader, chunks, IColumn::Filter {1}, column, &row_filter, &matched_rows, &used_filter);
    EXPECT_EQ(ErrorCode::CORRUPTION, status.code()) << status;
    EXPECT_NE(status.to_string().find("Invalid parquet dictionary index 1"), std::string::npos);
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

    int64_t values_consumed = 0;
    EXPECT_FALSE(base_reader.consume_nested_column(1, &values_consumed).ok());
}

TEST(ParquetColumnReaderControlTest, NestedSkipConsumesBoundedBatchesWithoutMaterializing) {
    auto element_reader = std::make_unique<ChunkedNestedLeafReader>();
    auto* element_reader_ptr = element_reader.get();
    ListColumnReader reader(bare_repeated_int64_list_schema(),
                            bare_repeated_int64_list_schema().type, std::move(element_reader));

    ASSERT_TRUE(reader.skip(8193).ok());
    EXPECT_TRUE(element_reader_ptr->load_lengths().empty());
    EXPECT_EQ(element_reader_ptr->level_load_lengths(), std::vector<int64_t>({4096, 4096, 1}));
    EXPECT_EQ(element_reader_ptr->consume_lengths(), std::vector<int64_t>({4096, 4096, 1}));
    EXPECT_TRUE(element_reader_ptr->build_lengths().empty());
    EXPECT_TRUE(element_reader_ptr->initial_column_sizes().empty());
}

TEST(ParquetColumnReaderControlTest, MapSkipConsumesBothStreamsWithoutMaterializing) {
    const std::vector<int16_t> def_levels {3, 3, 1, 3};
    const std::vector<int16_t> rep_levels {0, 1, 0, 0};
    auto key_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("key", 2, 3, 1, 2),
            make_nullable(std::make_shared<DataTypeInt64>()), def_levels, rep_levels);
    auto* key_reader_ptr = key_reader.get();
    auto value_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("value", 2, 3, 1, 2),
            make_nullable(std::make_shared<DataTypeInt64>()), def_levels, rep_levels);
    auto* value_reader_ptr = value_reader.get();
    MapColumnReader reader(nested_map_schema(), nested_map_schema().type, std::move(key_reader),
                           std::move(value_reader));

    ASSERT_TRUE(reader.skip(3).ok());
    EXPECT_EQ(key_reader_ptr->level_load_lengths(), std::vector<int64_t>({3}));
    EXPECT_EQ(value_reader_ptr->level_load_lengths(), std::vector<int64_t>({3}));
    EXPECT_EQ(key_reader_ptr->consume_lengths(), std::vector<int64_t>({3}));
    EXPECT_EQ(value_reader_ptr->consume_lengths(), std::vector<int64_t>({3}));
    EXPECT_TRUE(key_reader_ptr->build_lengths().empty());
    EXPECT_TRUE(value_reader_ptr->build_lengths().empty());
}

TEST(ParquetColumnReaderControlTest, StructSkipConsumesNullSeparatedChildSpans) {
    const std::vector<int16_t> def_levels {2, 0, 2};
    const std::vector<int16_t> rep_levels {0, 0, 0};
    auto shape_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("shape", 1, 2), make_nullable(std::make_shared<DataTypeInt64>()),
            def_levels, rep_levels);
    auto* shape_reader_ptr = shape_reader.get();
    auto child_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("child", 1, 2), make_nullable(std::make_shared<DataTypeInt64>()),
            def_levels, rep_levels);
    auto* child_reader_ptr = child_reader.get();
    std::vector<std::unique_ptr<ParquetColumnReader>> children;
    children.push_back(std::move(shape_reader));
    children.push_back(std::move(child_reader));
    StructColumnReader reader(nested_struct_schema(), nested_struct_schema().type,
                              std::move(children), {-1, 0});

    ASSERT_TRUE(reader.skip(3).ok());
    EXPECT_EQ(shape_reader_ptr->level_load_lengths(), std::vector<int64_t>({3}));
    EXPECT_EQ(child_reader_ptr->level_load_lengths(), std::vector<int64_t>({3}));
    EXPECT_EQ(shape_reader_ptr->consume_lengths(), std::vector<int64_t>({1, 1}));
    EXPECT_EQ(child_reader_ptr->consume_lengths(), std::vector<int64_t>({1, 1}));
    EXPECT_TRUE(shape_reader_ptr->build_lengths().empty());
    EXPECT_TRUE(child_reader_ptr->build_lengths().empty());
}

TEST(ParquetColumnReaderControlTest, NestedListSkipConsumesRecursivelyWithoutMaterializing) {
    auto leaf_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("leaf", 0, 1, 1, 1), std::make_shared<DataTypeInt64>(),
            std::vector<int16_t> {1, 1}, std::vector<int16_t> {0, 0});
    auto* leaf_reader_ptr = leaf_reader.get();
    const auto inner_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());
    auto inner_reader = std::make_unique<ListColumnReader>(bare_repeated_int64_list_schema(),
                                                           inner_type, std::move(leaf_reader));
    const auto outer_type = std::make_shared<DataTypeArray>(inner_type);
    ListColumnReader reader(bare_repeated_int64_list_schema(), outer_type, std::move(inner_reader));

    ASSERT_TRUE(reader.skip(2).ok());
    EXPECT_EQ(leaf_reader_ptr->level_load_lengths(), std::vector<int64_t>({2}));
    EXPECT_EQ(leaf_reader_ptr->consume_lengths(), std::vector<int64_t>({2}));
    EXPECT_TRUE(leaf_reader_ptr->build_lengths().empty());
}

TEST(ParquetColumnReaderControlTest, NestedMaterializerHelpersAppendOffsetsAndParentNulls) {
    ColumnArray::Offsets64 offsets;
    append_offsets(offsets, {3, 0, 2});
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 3);
    EXPECT_EQ(offsets[1], 3);
    EXPECT_EQ(offsets[2], 5);
    append_offsets(offsets, {1, 4});
    ASSERT_EQ(offsets.size(), 5);
    EXPECT_EQ(offsets[3], 6);
    EXPECT_EQ(offsets[4], 10);

    const NullMap parent_nulls = {0, 1, 0};
    append_parent_nulls(nullptr, parent_nulls);
    NullMap dst = {1};
    append_parent_nulls(&dst, parent_nulls);
    EXPECT_EQ(dst, NullMap({1, 0, 1, 0}));
}

TEST(ParquetColumnReaderControlTest, PageFilteredRowsToSkipUsesOnlyFullSkippedRanges) {
    ParquetPageSkipPlan page_skip_plan;
    page_skip_plan.skipped_ranges = {RowRange {0, 3}, RowRange {5, 2}, RowRange {10, 4}};

    auto schema = nested_int64_schema("page_filtered", 0, 0);
    ScalarColumnReader reader(schema, nullptr, &page_skip_plan);
    EXPECT_EQ(ScalarColumnReaderTestAccess::page_filtered_rows_to_skip(reader, 3), 3);
    EXPECT_EQ(ScalarColumnReaderTestAccess::page_filtered_rows_to_skip(reader, 5), 3);

    ScalarColumnReaderTestAccess::set_row_group_rows_read(&reader, 5);
    EXPECT_EQ(ScalarColumnReaderTestAccess::page_filtered_rows_to_skip(reader, 2), 2);
    EXPECT_EQ(ScalarColumnReaderTestAccess::page_filtered_rows_to_skip(reader, 5), 2);
}

TEST(ParquetColumnReaderControlTest, StructSkipsNullParentForRepeatedChildAndBatchesPresentRows) {
    auto repeated_child = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("repeated_shape", 1, 2, 1),
            make_nullable(std::make_shared<DataTypeInt64>()), std::vector<int16_t> {2, 2, 2, 2},
            std::vector<int16_t> {0, 0, 0, 0}, true);
    auto* repeated_child_ptr = repeated_child.get();
    auto scalar_child = make_scripted_scalar_reader(
            nested_int64_schema("scalar_child", 1, 2),
            scalar_batch({2, 0, 2, 2}, {0, 0, 0, 0}, {0, -1, 1, 2}, {10, 20, 30}));
    auto* scalar_child_ptr = scalar_child.get();

    std::vector<std::unique_ptr<ParquetColumnReader>> children;
    children.push_back(std::move(repeated_child));
    children.push_back(std::move(scalar_child));
    StructColumnReader reader(nested_struct_schema(),
                              make_nullable(std::make_shared<DataTypeStruct>(
                                      DataTypes {make_nullable(std::make_shared<DataTypeInt64>()),
                                                 make_nullable(std::make_shared<DataTypeInt64>())},
                                      Strings {"a", "b"})),
                              std::move(children), {0, 1});

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = reader.build_nested_column(4, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows_read, 4);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 4);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_EQ(repeated_child_ptr->build_lengths(), std::vector<int64_t>({1, 2}));
    EXPECT_EQ(scalar_child_ptr->nested_build_level_cursor(), 4);
}

TEST(ParquetColumnReaderControlTest, StructFallsBackToFirstChildWhenAllChildrenAreRepeated) {
    auto first_child = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("first", 1, 2, 1), make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {2, 0}, std::vector<int16_t> {0, 0}, true);
    auto second_child = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("second", 1, 2, 1),
            make_nullable(std::make_shared<DataTypeInt64>()), std::vector<int16_t> {2, 2},
            std::vector<int16_t> {0, 0}, true);

    std::vector<std::unique_ptr<ParquetColumnReader>> children;
    children.push_back(std::move(first_child));
    children.push_back(std::move(second_child));
    StructColumnReader reader(nested_struct_schema(), nested_struct_schema().type,
                              std::move(children), {0, 1});

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = reader.build_nested_column(2, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(rows_read, 2);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
}

TEST(ParquetColumnReaderControlTest, StructNullParentAdvancesComplexChildShapeOnly) {
    auto shape_child = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("shape", 1, 2), make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {2, 2, 0, 0, 2, 2}, std::vector<int16_t> {0, 0, 0, 0, 0, 0});

    ParquetColumnSchema map_schema = nested_map_schema();
    map_schema.nullable_definition_level = 2;
    map_schema.definition_level = 3;
    map_schema.repeated_ancestor_definition_level = 0;
    auto key_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("key", 3, 3, 1, 0),
            make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {3, 3, 0, 0, 3, 3}, std::vector<int16_t> {0, 0, 0, 0, 0, 0});
    auto value_reader =
            make_scripted_scalar_reader(nested_int64_schema("value", 4, 4, 1, 0),
                                        scalar_batch({4, 4, 0, 0, 4, 4}, {0, 0, 0, 0, 0, 0},
                                                     {0, 1, -1, -1, 2, 3}, {10, 20, 30, 40}));
    auto map_reader = std::make_unique<MapColumnReader>(
            map_schema, map_schema.type, std::move(key_reader), std::move(value_reader));

    std::vector<std::unique_ptr<ParquetColumnReader>> children;
    children.push_back(std::move(shape_child));
    children.push_back(std::move(map_reader));
    auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(DataTypes {map_schema.type},
                                                                      Strings {"partitionValues"}));
    StructColumnReader reader(nested_struct_schema(), struct_type, std::move(children), {-1, 0});

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = reader.build_nested_column(6, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows_read, 6);

    const auto& nullable_struct = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_struct.size(), 6);
    EXPECT_FALSE(nullable_struct.is_null_at(0));
    EXPECT_FALSE(nullable_struct.is_null_at(1));
    EXPECT_TRUE(nullable_struct.is_null_at(2));
    EXPECT_TRUE(nullable_struct.is_null_at(3));
    EXPECT_FALSE(nullable_struct.is_null_at(4));
    EXPECT_FALSE(nullable_struct.is_null_at(5));

    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_struct.get_nested_column());
    const auto& map_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_EQ(map_nullable.size(), 6);
    EXPECT_FALSE(map_nullable.is_null_at(0));
    EXPECT_FALSE(map_nullable.is_null_at(1));
    EXPECT_TRUE(map_nullable.is_null_at(2));
    EXPECT_TRUE(map_nullable.is_null_at(3));
    EXPECT_FALSE(map_nullable.is_null_at(4));
    EXPECT_FALSE(map_nullable.is_null_at(5));
    const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), 6);
    EXPECT_EQ(map_column.get_offsets()[0], 1);
    EXPECT_EQ(map_column.get_offsets()[1], 2);
    EXPECT_EQ(map_column.get_offsets()[2], 2);
    EXPECT_EQ(map_column.get_offsets()[3], 2);
    EXPECT_EQ(map_column.get_offsets()[4], 3);
    EXPECT_EQ(map_column.get_offsets()[5], 4);
}

TEST(ParquetColumnReaderControlTest, StructNullParentAdvancesNestedStructDescendants) {
    auto shape_child = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("shape", 1, 2), make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {2, 0, 2}, std::vector<int16_t> {0, 0, 0});

    auto id_batch = scalar_batch({4, 3, 4}, {0, 0, 0}, {0, -1, 1}, {10, 20});
    id_batch->value_slot_definition_level = 3;
    auto id_reader =
            make_scripted_scalar_reader(nested_int64_schema("id", 3, 4), std::move(id_batch));

    ParquetColumnSchema inner_schema;
    inner_schema.local_id = 0;
    inner_schema.name = "stats_parsed";
    inner_schema.kind = ParquetColumnSchemaKind::STRUCT;
    inner_schema.nullable_definition_level = 2;
    inner_schema.definition_level = 3;
    inner_schema.type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt64>())}, Strings {"id"}));

    std::vector<std::unique_ptr<ParquetColumnReader>> inner_children;
    inner_children.push_back(std::move(id_reader));
    auto inner_reader = std::make_unique<StructColumnReader>(
            inner_schema, inner_schema.type, std::move(inner_children), std::vector<int> {0});

    std::vector<std::unique_ptr<ParquetColumnReader>> outer_children;
    outer_children.push_back(std::move(shape_child));
    outer_children.push_back(std::move(inner_reader));
    auto outer_type = make_nullable(std::make_shared<DataTypeStruct>(DataTypes {inner_schema.type},
                                                                     Strings {"stats_parsed"}));
    StructColumnReader reader(nested_struct_schema(), outer_type, std::move(outer_children),
                              {-1, 0});

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = reader.build_nested_column(3, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows_read, 3);

    const auto& outer_nullable = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(outer_nullable.size(), 3);
    EXPECT_FALSE(outer_nullable.is_null_at(0));
    EXPECT_TRUE(outer_nullable.is_null_at(1));
    EXPECT_FALSE(outer_nullable.is_null_at(2));

    const auto& outer_struct = assert_cast<const ColumnStruct&>(outer_nullable.get_nested_column());
    const auto& inner_nullable = assert_cast<const ColumnNullable&>(outer_struct.get_column(0));
    ASSERT_EQ(inner_nullable.size(), 3);
    EXPECT_FALSE(inner_nullable.is_null_at(0));
    EXPECT_TRUE(inner_nullable.is_null_at(1));
    EXPECT_FALSE(inner_nullable.is_null_at(2));

    const auto& inner_struct = assert_cast<const ColumnStruct&>(inner_nullable.get_nested_column());
    const auto& id_nullable = assert_cast<const ColumnNullable&>(inner_struct.get_column(0));
    const auto& id_values = assert_cast<const ColumnInt64&>(id_nullable.get_nested_column());
    EXPECT_EQ(id_values.get_element(0), 10);
    EXPECT_EQ(id_values.get_element(2), 20);
}

TEST(ParquetColumnReaderControlTest, ListKeepsEmptyBareRepeatedPrimitiveRows) {
    auto element_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("element", 0, 1, 1, 1), std::make_shared<DataTypeInt64>(),
            std::vector<int16_t> {0, 1, 1, 0}, std::vector<int16_t> {0, 0, 1, 0});
    auto* element_reader_ptr = element_reader.get();
    ListColumnReader reader(bare_repeated_int64_list_schema(),
                            bare_repeated_int64_list_schema().type, std::move(element_reader));

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = reader.build_nested_column(3, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows_read, 3);

    const auto& array_column = assert_cast<const ColumnArray&>(*column);
    ASSERT_EQ(array_column.get_offsets().size(), 3);
    EXPECT_EQ(array_column.get_offsets()[0], 0);
    EXPECT_EQ(array_column.get_offsets()[1], 2);
    EXPECT_EQ(array_column.get_offsets()[2], 2);
    EXPECT_EQ(element_reader_ptr->build_lengths(), std::vector<int64_t>({2}));
}

TEST(ParquetColumnReaderControlTest, NestedListSkipsAncestorEmptyRowsButKeepsNullElements) {
    auto element_reader =
            std::make_unique<ScriptedNestedReader>(nested_int64_schema("element", 5, 5, 2, 4),
                                                   make_nullable(std::make_shared<DataTypeInt64>()),
                                                   std::vector<int16_t> {1, 5, 5, 5, 2, 5, 2, 0},
                                                   std::vector<int16_t> {0, 0, 2, 1, 0, 1, 1, 0});
    auto* element_reader_ptr = element_reader.get();

    const auto inner_type = make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt64>())));
    auto inner_reader = std::make_unique<ListColumnReader>(
            nested_list_schema("inner", make_nullable(std::make_shared<DataTypeInt64>()), 3, 4, 2,
                               2),
            inner_type, std::move(element_reader));
    auto outer_type = make_nullable(std::make_shared<DataTypeArray>(inner_type));
    ListColumnReader reader(nested_list_schema("outer", inner_type, 1, 2, 1, 2), outer_type,
                            std::move(inner_reader));

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = reader.build_nested_column(4, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows_read, 4);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 4);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_TRUE(nullable_column.is_null_at(3));

    const auto& outer_array = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& outer_offsets = outer_array.get_offsets();
    ASSERT_EQ(outer_offsets.size(), 4);
    EXPECT_EQ(outer_offsets[0], 0);
    EXPECT_EQ(outer_offsets[1], 2);
    EXPECT_EQ(outer_offsets[2], 5);
    EXPECT_EQ(outer_offsets[3], 5);

    const auto& inner_nullable = assert_cast<const ColumnNullable&>(outer_array.get_data());
    ASSERT_EQ(inner_nullable.size(), 5);
    EXPECT_FALSE(inner_nullable.is_null_at(0));
    EXPECT_FALSE(inner_nullable.is_null_at(1));
    EXPECT_TRUE(inner_nullable.is_null_at(2));
    EXPECT_FALSE(inner_nullable.is_null_at(3));
    EXPECT_TRUE(inner_nullable.is_null_at(4));

    const auto& inner_array = assert_cast<const ColumnArray&>(inner_nullable.get_nested_column());
    const auto& inner_offsets = inner_array.get_offsets();
    ASSERT_EQ(inner_offsets.size(), 5);
    EXPECT_EQ(inner_offsets[0], 2);
    EXPECT_EQ(inner_offsets[1], 3);
    EXPECT_EQ(inner_offsets[2], 3);
    EXPECT_EQ(inner_offsets[3], 4);
    EXPECT_EQ(inner_offsets[4], 4);
    EXPECT_EQ(element_reader_ptr->build_lengths(), std::vector<int64_t>({4}));
}

TEST(ParquetColumnReaderControlTest, MapKeepsEmptyMapRows) {
    auto key_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("key", 1, 2, 1, 2),
            make_nullable(std::make_shared<DataTypeInt64>()), std::vector<int16_t> {1},
            std::vector<int16_t> {0});
    auto value_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("value", 2, 3, 1, 2),
            make_nullable(std::make_shared<DataTypeInt64>()), std::vector<int16_t> {1},
            std::vector<int16_t> {0});
    auto* value_reader_ptr = value_reader.get();
    MapColumnReader reader(nested_map_schema(), nested_map_schema().type, std::move(key_reader),
                           std::move(value_reader));

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = reader.build_nested_column(1, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows_read, 1);

    const auto& nullable_map = assert_cast<const ColumnNullable&>(*column);
    EXPECT_FALSE(nullable_map.is_null_at(0));
    const auto& map_column = assert_cast<const ColumnMap&>(nullable_map.get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), 1);
    EXPECT_EQ(map_column.get_offsets()[0], 0);
    EXPECT_EQ(value_reader_ptr->build_lengths(), std::vector<int64_t>({0}));
}

TEST(ParquetColumnReaderControlTest, ListMapSkipsAncestorEmptyRowsBeforeScalarValues) {
    auto key_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("key", 4, 4, 2, 4),
            make_nullable(std::make_shared<DataTypeInt64>()), std::vector<int16_t> {1, 4},
            std::vector<int16_t> {0, 0});
    auto value_reader = make_scripted_scalar_reader(nested_int64_schema("value", 5, 5, 2, 4),
                                                    scalar_batch({1, 5}, {0, 0}, {-1, 0}, {100}));

    const auto map_type = make_nullable(
            std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeInt64>()),
                                          make_nullable(std::make_shared<DataTypeInt64>())));
    auto map_reader = std::make_unique<MapColumnReader>(
            nested_map_schema(make_nullable(std::make_shared<DataTypeInt64>())), map_type,
            std::move(key_reader), std::move(value_reader));
    auto outer_type = make_nullable(std::make_shared<DataTypeArray>(map_type));
    ListColumnReader reader(nested_list_schema("outer", map_type, 1, 2, 1, 2), outer_type,
                            std::move(map_reader));

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = reader.build_nested_column(2, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows_read, 2);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 2);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));

    const auto& outer_array = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& outer_offsets = outer_array.get_offsets();
    ASSERT_EQ(outer_offsets.size(), 2);
    EXPECT_EQ(outer_offsets[0], 0);
    EXPECT_EQ(outer_offsets[1], 1);

    const auto& map_nullable = assert_cast<const ColumnNullable&>(outer_array.get_data());
    ASSERT_EQ(map_nullable.size(), 1);
    EXPECT_FALSE(map_nullable.is_null_at(0));
    const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), 1);
    EXPECT_EQ(map_column.get_offsets()[0], 1);

    const auto& values = assert_cast<const ColumnNullable&>(map_column.get_values());
    const auto& value_data = assert_cast<const ColumnInt64&>(values.get_nested_column());
    ASSERT_EQ(values.size(), 1);
    EXPECT_FALSE(values.is_null_at(0));
    EXPECT_EQ(value_data.get_element(0), 100);
}

TEST(ParquetColumnReaderControlTest, MapRejectsNullKeysAndMisalignedScalarValueRepLevels) {
    auto key_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("key", 1, 2, 1), make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {2}, std::vector<int16_t> {0}, false, true);
    auto value_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("value", 1, 2, 1), make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {2}, std::vector<int16_t> {0});
    MapColumnReader null_key_reader(nested_map_schema(), nested_map_schema().type,
                                    std::move(key_reader), std::move(value_reader));
    auto column = null_key_reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = null_key_reader.build_nested_column(1, column, &rows_read);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains null key"), std::string::npos);

    auto aligned_key_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("key", 1, 2, 1), make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {2, 2}, std::vector<int16_t> {0, 1});
    auto misaligned_value_reader =
            make_scripted_scalar_reader(nested_int64_schema("value", 2, 3, 1),
                                        scalar_batch({3, 3}, {0, 0}, {0, 1}, {100, 200}));
    MapColumnReader misaligned_reader(nested_map_schema(), nested_map_schema().type,
                                      std::move(aligned_key_reader),
                                      std::move(misaligned_value_reader));
    column = misaligned_reader.type()->create_column();
    status = misaligned_reader.build_nested_column(1, column, &rows_read);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("value repetition level is not aligned"), std::string::npos);
}

TEST(ParquetColumnReaderControlTest, MapConsumePreservesKeyAndValueCorruptionChecks) {
    auto null_key_reader =
            make_scripted_scalar_reader(nested_int64_schema("key", 2, 3, 1, 2),
                                        scalar_batch({2}, {0}, {-1}, std::vector<int64_t> {}));
    auto value_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("value", 2, 3, 1, 2),
            make_nullable(std::make_shared<DataTypeInt64>()), std::vector<int16_t> {2},
            std::vector<int16_t> {0});
    MapColumnReader null_key_reader_map(nested_map_schema(), nested_map_schema().type,
                                        std::move(null_key_reader), std::move(value_reader));
    int64_t values_consumed = 0;
    auto status = null_key_reader_map.consume_nested_column(1, &values_consumed);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains null"), std::string::npos);

    auto key_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("key", 1, 2, 1), make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {2, 2}, std::vector<int16_t> {0, 1});
    auto misaligned_value_reader =
            make_scripted_scalar_reader(nested_int64_schema("value", 2, 3, 1),
                                        scalar_batch({3, 3}, {0, 0}, {0, 1}, {100, 200}));
    MapColumnReader misaligned_reader(nested_map_schema(), nested_map_schema().type,
                                      std::move(key_reader), std::move(misaligned_value_reader));
    status = misaligned_reader.consume_nested_column(1, &values_consumed);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("value repetition level is not aligned"), std::string::npos);
}

TEST(ParquetColumnReaderControlTest, MapBuildsScalarAndComplexValuePaths) {
    auto key_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("key", 1, 2, 1), make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {2, 2}, std::vector<int16_t> {0, 1});
    auto scalar_value_reader =
            make_scripted_scalar_reader(nested_int64_schema("value", 2, 3, 1),
                                        scalar_batch({3, 3}, {0, 1}, {0, 1}, {100, 200}));
    MapColumnReader scalar_reader(nested_map_schema(), nested_map_schema().type,
                                  std::move(key_reader), std::move(scalar_value_reader));
    auto column = scalar_reader.type()->create_column();
    int64_t rows_read = 0;
    auto status = scalar_reader.build_nested_column(1, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable_map = assert_cast<const ColumnNullable&>(*column);
    const auto& map_column = assert_cast<const ColumnMap&>(nullable_map.get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), 1);
    EXPECT_EQ(map_column.get_offsets()[0], 2);
    const auto& values = assert_cast<const ColumnNullable&>(map_column.get_values());
    const auto& value_data = assert_cast<const ColumnInt64&>(values.get_nested_column());
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(value_data.get_element(0), 100);
    EXPECT_EQ(value_data.get_element(1), 200);

    auto complex_key_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("key", 1, 2, 1), make_nullable(std::make_shared<DataTypeInt64>()),
            std::vector<int16_t> {2, 2}, std::vector<int16_t> {0, 1});
    auto complex_value_reader = std::make_unique<ScriptedNestedReader>(
            nested_int64_schema("complex_value", 2, 3, 1),
            make_nullable(std::make_shared<DataTypeInt64>()), std::vector<int16_t> {3, 3},
            std::vector<int16_t> {0, 1});
    auto* complex_value_reader_ptr = complex_value_reader.get();
    MapColumnReader complex_reader(nested_map_schema(), nested_map_schema().type,
                                   std::move(complex_key_reader), std::move(complex_value_reader));
    column = complex_reader.type()->create_column();
    status = complex_reader.build_nested_column(1, column, &rows_read);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(complex_value_reader_ptr->build_lengths(), std::vector<int64_t>({2}));
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

TEST(ParquetColumnReaderFactoryTest, RejectsProjectedUnsupportedLogicalType) {
    ParquetColumnSchema schema = int64_schema("unsupported_time");
    schema.kind = ParquetColumnSchemaKind::PRIMITIVE;
    schema.type_descriptor.unsupported_reason =
            "Parquet TIME with isAdjustedToUTC=true is not supported";

    ParquetColumnReaderFactory factory(nullptr, 1);
    std::unique_ptr<ParquetColumnReader> reader;
    const auto status = factory.create(schema, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(schema.type_descriptor.unsupported_reason),
              std::string::npos);
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
