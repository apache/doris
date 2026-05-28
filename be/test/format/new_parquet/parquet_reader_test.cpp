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

#include "format/new_parquet/parquet_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format/new_parquet/column_reader.h"
#include "format/reader/column_mapper.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"
#include "gen_cpp/Types_types.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/predicate/predicate_creator.h"

namespace doris {
namespace {

constexpr int64_t ROW_COUNT = 5;

class Int32GreaterThanExpr final : public VExpr {
public:
    Int32GreaterThanExpr(int column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& input =
                assert_cast<const ColumnInt32&>(*block->get_by_position(_column_id).column);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = input.get_element(input_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "Int32GreaterThanExpr";
};

class Int32SumGreaterThanExpr final : public VExpr {
public:
    Int32SumGreaterThanExpr(int left_column_id, int right_column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _left_column_id(left_column_id),
              _right_column_id(right_column_id),
              _value(value) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& left_input =
                assert_cast<const ColumnInt32&>(*block->get_by_position(_left_column_id).column);
        const auto& right_input =
                assert_cast<const ColumnInt32&>(*block->get_by_position(_right_column_id).column);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    left_input.get_element(input_row) + right_input.get_element(input_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _left_column_id;
    const int _right_column_id;
    const int32_t _value;
    const std::string _expr_name = "Int32SumGreaterThanExpr";
};

VExprContextSPtr create_int32_greater_than_conjunct(int column_id, int32_t value) {
    auto ctx =
            VExprContext::create_shared(std::make_shared<Int32GreaterThanExpr>(column_id, value));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

VExprContextSPtr create_int32_sum_greater_than_conjunct(int left_column_id, int right_column_id,
                                                        int32_t value) {
    auto ctx = VExprContext::create_shared(
            std::make_shared<Int32SumGreaterThanExpr>(left_column_id, right_column_id, value));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

std::shared_ptr<arrow::Array> finish_array(arrow::ArrayBuilder* builder) {
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder->Finish(&array).ok());
    return array;
}

std::shared_ptr<arrow::Array> build_int32_array(const std::vector<int32_t>& values) {
    arrow::Int32Builder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_string_array(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

void write_parquet_file(const std::string& file_path, int64_t row_group_size = ROW_COUNT) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table = arrow::Table::Make(schema,
                                    {build_int32_array({1, 2, 3, 4, 5}),
                                     build_string_array({"one", "two", "three", "four", "five"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      row_group_size, builder.build()));
}

void write_int_pair_parquet_file(const std::string& file_path, int64_t row_group_size = ROW_COUNT) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("score", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table = arrow::Table::Make(
            schema, {build_int32_array({1, 2, 3, 4, 5}), build_int32_array({1, 2, 3, 4, 5}),
                     build_string_array({"one", "two", "three", "four", "five"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      row_group_size, builder.build()));
}

Block build_file_block(const std::vector<reader::SchemaField>& schema) {
    Block block;
    for (const auto& field : schema) {
        block.insert({field.type->create_column(), field.type, field.name});
    }
    return block;
}

Block build_file_block_with_row_position(const std::vector<reader::SchemaField>& schema) {
    auto block = build_file_block(schema);
    const auto row_position_field =
            parquet::ParquetColumnReaderFactory::row_position_schema_field();
    block.insert({row_position_field.type->create_column(), row_position_field.type,
                  row_position_field.name});
    return block;
}

int64_t parquet_column_start_offset(const ::parquet::ColumnChunkMetaData& column_metadata) {
    return column_metadata.has_dictionary_page()
                   ? static_cast<int64_t>(column_metadata.dictionary_page_offset())
                   : static_cast<int64_t>(column_metadata.data_page_offset());
}

std::pair<int64_t, int64_t> row_group_mid_range(const std::string& file_path, int row_group_idx) {
    auto reader = ::parquet::ParquetFileReader::OpenFile(file_path, false);
    auto metadata = reader->metadata();
    auto row_group_metadata = metadata->RowGroup(row_group_idx);
    auto first_column = row_group_metadata->ColumnChunk(0);
    auto last_column = row_group_metadata->ColumnChunk(row_group_metadata->num_columns() - 1);
    const int64_t row_group_start_offset = parquet_column_start_offset(*first_column);
    const int64_t row_group_end_offset =
            parquet_column_start_offset(*last_column) + last_column->total_compressed_size();
    const int64_t row_group_mid_offset =
            row_group_start_offset + (row_group_end_offset - row_group_start_offset) / 2;
    return {row_group_mid_offset, 1};
}

class TestFileReader final : public reader::FileReader {
public:
    TestFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                   std::unique_ptr<io::FileDescription>& file_description,
                   std::shared_ptr<io::IOContext> io_ctx)
            : reader::FileReader(system_properties, file_description, io_ctx, nullptr) {}

    Status get_schema(std::vector<reader::SchemaField>* file_schema) const override {
        file_schema->clear();
        reader::SchemaField field;
        field.id = 0;
        field.name = "id";
        field.type = std::make_shared<DataTypeInt32>();
        file_schema->push_back(std::move(field));
        return Status::OK();
    }

    bool has_request() const { return _request != nullptr; }

    bool eof() const { return _eof; }
};

TEST(FileReaderTest, OpenStoresRequestAndCloseClearsState) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto file_description = std::make_unique<io::FileDescription>();
    auto io_ctx = std::make_shared<io::IOContext>();
    TestFileReader reader(system_properties, file_description, io_ctx);

    auto request = std::make_unique<reader::FileScanRequest>();
    request->non_predicate_columns.push_back(0);
    ASSERT_TRUE(reader.open(request).ok());
    EXPECT_EQ(request, nullptr);
    EXPECT_TRUE(reader.has_request());

    ASSERT_TRUE(reader.close().ok());
    EXPECT_FALSE(reader.has_request());
    EXPECT_TRUE(reader.eof());
}

TEST(TableColumnMapperTest, CreatesComplexProjectionForStructChildren) {
    reader::SchemaField struct_field;
    struct_field.id = 0;
    struct_field.name = "s";
    struct_field.file_path = {0};
    reader::SchemaField a_field;
    a_field.id = 0;
    a_field.name = "a";
    a_field.type = std::make_shared<DataTypeInt32>();
    a_field.file_path = {0, 0};
    reader::SchemaField b_field;
    b_field.id = 0;
    b_field.name = "b";
    b_field.type = std::make_shared<DataTypeString>();
    b_field.file_path = {0, 1};
    struct_field.children = {a_field, b_field};
    struct_field.type = std::make_shared<DataTypeStruct>(DataTypes {a_field.type, b_field.type},
                                                         Strings {"a", "b"});

    reader::TableColumn table_child;
    table_child.id = 101;
    table_child.name = "b";
    table_child.type = b_field.type;
    reader::TableColumn table_column;
    table_column.id = 100;
    table_column.name = "s";
    table_column.type = std::make_shared<DataTypeStruct>(DataTypes {b_field.type}, Strings {"b"});
    table_column.children = {table_child};

    reader::TableColumnMapperOptions options;
    options.mode = reader::TableColumnMappingMode::BY_NAME;
    reader::TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping({table_column}, {}, {struct_field}).ok());

    auto request = std::make_unique<reader::FileScanRequest>();
    ASSERT_TRUE(mapper.create_scan_request({}, {}, {table_column}, request.get()).ok());
    ASSERT_EQ(request->non_predicate_columns, std::vector<reader::ColumnId>({0}));
    ASSERT_EQ(request->complex_projections.size(), 1);
    const auto& projection = request->complex_projections.at(0);
    EXPECT_EQ(projection.file_path, std::vector<int32_t>({0}));
    ASSERT_FALSE(projection.project_all_children);
    ASSERT_EQ(projection.children.size(), 1);
    EXPECT_EQ(projection.children[0].file_path, std::vector<int32_t>({0, 1}));

    ASSERT_EQ(mapper.mappings().size(), 1);
    const auto* projected_type =
            assert_cast<const DataTypeStruct*>(mapper.mappings()[0].file_type.get());
    ASSERT_EQ(projected_type->get_elements().size(), 1);
    EXPECT_EQ(projected_type->get_element_name(0), "b");
}

class NewParquetReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_new_parquet_reader_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "reader.parquet").string();
        write_parquet_file(_file_path);
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

    std::unique_ptr<parquet::ParquetReader> create_reader(int64_t range_start_offset = 0,
                                                          int64_t range_size = -1) const {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = _file_path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
        file_description->range_start_offset = range_start_offset;
        file_description->range_size = range_size;
        return std::make_unique<parquet::ParquetReader>(system_properties, file_description,
                                                        nullptr, nullptr);
    }

    std::filesystem::path _test_dir;
    std::string _file_path;
};

TEST_F(NewParquetReaderTest, GetSchemaReturnsFileLocalColumns) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(schema[0].id, 0);
    EXPECT_EQ(schema[0].name, "id");
    EXPECT_EQ(schema[0].type->get_primitive_type(), TYPE_INT);
    EXPECT_EQ(schema[1].id, 1);
    EXPECT_EQ(schema[1].name, "value");
    EXPECT_EQ(schema[1].type->get_primitive_type(), TYPE_STRING);
}

TEST_F(NewParquetReaderTest, ReadSingleRowGroupThenEof) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_unique<reader::FileScanRequest>();
    request->non_predicate_columns = {0, 1};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, ROW_COUNT);

    const auto& ids = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& values = assert_cast<const ColumnString&>(*block.get_by_position(1).column);
    ASSERT_EQ(ids.size(), ROW_COUNT);
    ASSERT_EQ(values.size(), ROW_COUNT);
    EXPECT_EQ(ids.get_element(0), 1);
    EXPECT_EQ(ids.get_element(4), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "one");
    EXPECT_EQ(values.get_data_at(4).to_string(), "five");

    rows = 0;
    eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
}

TEST_F(NewParquetReaderTest, ReadMultipleRowGroups) {
    write_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_unique<reader::FileScanRequest>();
    request->non_predicate_columns = {0, 1};
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        const auto& value_column =
                assert_cast<const ColumnString&>(*block.get_by_position(1).column);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 3, 4, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"one", "two", "three", "four", "five"}));
}

TEST_F(NewParquetReaderTest, ReadPredicateAndNonPredicateColumnsWithSelection) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {0};
    request->non_predicate_columns = {1};
    reader::FileExpressionFilter expression_filter;
    expression_filter.conjunct = create_int32_greater_than_conjunct(0, 2);
    request->expression_filters.push_back(std::move(expression_filter));
    reader::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = 0;
    column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GT>(
            0, "id", schema[0].type, Field::create_field<TYPE_INT>(2), false));
    request->column_predicate_filters.push_back(std::move(column_filter));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& values = assert_cast<const ColumnString&>(*block.get_by_position(1).column);
    ASSERT_EQ(ids.size(), 3);
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "three");
    EXPECT_EQ(values.get_data_at(1).to_string(), "four");
    EXPECT_EQ(values.get_data_at(2).to_string(), "five");

    rows = 0;
    eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
}

TEST_F(NewParquetReaderTest, ReadMultiPredicateColumnsBeforeExpressionFilter) {
    write_int_pair_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {0, 1};
    request->non_predicate_columns = {};
    reader::FileExpressionFilter expression_filter;
    expression_filter.conjunct = create_int32_sum_greater_than_conjunct(0, 1, 7);
    request->expression_filters.push_back(std::move(expression_filter));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 2);

    const auto& ids = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& scores = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    ASSERT_EQ(ids.size(), 2);
    ASSERT_EQ(scores.size(), 2);
    EXPECT_EQ(ids.get_element(0), 4);
    EXPECT_EQ(ids.get_element(1), 5);
    EXPECT_EQ(scores.get_element(0), 4);
    EXPECT_EQ(scores.get_element(1), 5);
}

TEST_F(NewParquetReaderTest, PredicateFiltersRowGroupsByStatistics) {
    write_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {0};
    request->non_predicate_columns = {1};
    reader::FileExpressionFilter expression_filter;
    expression_filter.conjunct = create_int32_greater_than_conjunct(0, 2);
    request->expression_filters.push_back(std::move(expression_filter));
    reader::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = 0;
    column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GT>(
            0, "id", schema[0].type, Field::create_field<TYPE_INT>(2), false));
    request->column_predicate_filters.push_back(std::move(column_filter));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        const auto& value_column =
                assert_cast<const ColumnString&>(*block.get_by_position(1).column);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({3, 4, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"three", "four", "five"}));
}

TEST_F(NewParquetReaderTest, RowPositionReaderReturnsFileLocalPositions) {
    write_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_unique<reader::FileScanRequest>();
    request->non_predicate_columns = {parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID,
                                      0};
    request->column_positions = {
            {0, 0},
            {parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID, 2},
    };
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int64_t> row_positions;
    std::vector<int32_t> ids;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block_with_row_position(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        const auto& row_position_column =
                assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            row_positions.push_back(row_position_column.get_element(row));
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 3, 4, 5}));
    EXPECT_EQ(row_positions, std::vector<int64_t>({0, 1, 2, 3, 4}));
}

TEST_F(NewParquetReaderTest, RowPositionReaderKeepsPositionsAfterSelection) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block_with_row_position(schema);

    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {0};
    request->non_predicate_columns = {parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID};
    request->column_positions = {
            {0, 0},
            {parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID, 2},
    };
    reader::FileExpressionFilter expression_filter;
    expression_filter.conjunct = create_int32_greater_than_conjunct(0, 2);
    request->expression_filters.push_back(std::move(expression_filter));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& row_position_column =
            assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(id_column.get_element(1), 4);
    EXPECT_EQ(id_column.get_element(2), 5);
    EXPECT_EQ(row_position_column.get_element(0), 2);
    EXPECT_EQ(row_position_column.get_element(1), 3);
    EXPECT_EQ(row_position_column.get_element(2), 4);
}

TEST_F(NewParquetReaderTest, RowPositionReaderUsesFileLocalPositionsForScanRange) {
    write_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);

    const std::vector<std::vector<int32_t>> expected_ids = {{1, 2}, {3, 4}, {5}};
    const std::vector<std::vector<int64_t>> expected_row_positions = {{0, 1}, {2, 3}, {4}};
    for (int row_group_idx = 0; row_group_idx < 3; ++row_group_idx) {
        const auto [range_start_offset, range_size] =
                row_group_mid_range(_file_path, row_group_idx);
        auto reader = create_reader(range_start_offset, range_size);
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        ASSERT_TRUE(reader->init(&state).ok());

        std::vector<reader::SchemaField> schema;
        ASSERT_TRUE(reader->get_schema(&schema).ok());
        auto request = std::make_unique<reader::FileScanRequest>();
        request->non_predicate_columns = {
                parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID, 0};
        request->column_positions = {
                {0, 0},
                {parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID, 2},
        };
        ASSERT_TRUE(reader->open(request).ok());

        std::vector<int32_t> ids;
        std::vector<int64_t> row_positions;
        bool eof = false;
        while (!eof) {
            Block block = build_file_block_with_row_position(schema);
            size_t rows = 0;
            ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
            if (rows == 0) {
                continue;
            }
            const auto& id_column =
                    assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
            const auto& row_position_column =
                    assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
            for (size_t row = 0; row < rows; ++row) {
                ids.push_back(id_column.get_element(row));
                row_positions.push_back(row_position_column.get_element(row));
            }
        }

        EXPECT_EQ(ids, expected_ids[row_group_idx]);
        EXPECT_EQ(row_positions, expected_row_positions[row_group_idx]);
    }
}

} // namespace
} // namespace doris
