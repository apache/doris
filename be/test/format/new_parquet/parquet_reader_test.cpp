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
#include <parquet/page_index.h>

#include <filesystem>
#include <memory>
#include <numeric>
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
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/parquet_scan.h"
#include "format/new_parquet/reader/column_reader.h"
#include "format/reader/column_mapper.h"
#include "format/reader/expr/delete_predicate.h"
#include "format/reader/expr/slot_ref.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"
#include "gen_cpp/Types_types.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/predicate/predicate_creator.h"

namespace doris {
namespace {

constexpr int64_t ROW_COUNT = 5;

reader::FieldProjection field_projection(reader::ColumnId column_id) {
    return reader::FieldProjection {.file_column_id = column_id};
}

std::vector<reader::ColumnId> projection_ids(
        const std::vector<reader::FieldProjection>& projections) {
    std::vector<reader::ColumnId> ids;
    ids.reserve(projections.size());
    for (const auto& projection : projections) {
        ids.push_back(projection.file_column_id);
    }
    return ids;
}

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

class StringInExpr final : public VExpr {
public:
    StringInExpr(int column_id, std::vector<std::string> values)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _values(std::move(values)) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& input =
                assert_cast<const ColumnString&>(*block->get_by_position(_column_id).column);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            const auto value = input.get_data_at(input_row).to_string();
            result_data[row] = std::find(_values.begin(), _values.end(), value) != _values.end();
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::vector<std::string> _values;
    const std::string _expr_name = "StringInExpr";
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

VExprContextSPtr create_string_in_conjunct(int column_id, std::vector<std::string> values) {
    auto ctx = VExprContext::create_shared(
            std::make_shared<StringInExpr>(column_id, std::move(values)));
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

void write_dictionary_filter_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table =
            arrow::Table::Make(schema, {build_int32_array({1, 2, 3, 4, 5, 6}),
                                        build_string_array({"aa", "az", "lm", "lz", "za", "zz"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.enable_dictionary("value");
    builder.disable_dictionary("id");
    builder.disable_statistics();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_dictionary_edge_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table = arrow::Table::Make(
            schema,
            {build_int32_array({1, 2, 3, 4, 5, 6, 7, 8}),
             build_string_array({"", "same", "other", "long-value", "", "tail", "same", "last"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.enable_dictionary("value");
    builder.disable_dictionary("id");
    builder.disable_statistics();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 2,
                                                      builder.build()));
}

void write_page_index_filter_parquet_file(const std::string& file_path) {
    std::vector<int32_t> ids(128);
    std::iota(ids.begin(), ids.end(), 0);
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.disable_dictionary();
    builder.enable_write_page_index();
    builder.write_batch_size(8);
    builder.data_pagesize(10);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      ids.size(), builder.build()));
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

    bool has_io_context() const { return _io_ctx != nullptr; }

    long io_context_use_count() const { return _io_ctx.use_count(); }
};

TEST(FileReaderTest, OpenStoresRequestAndCloseClearsState) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto file_description = std::make_unique<io::FileDescription>();
    auto io_ctx = std::make_shared<io::IOContext>();
    TestFileReader reader(system_properties, file_description, io_ctx);

    auto request = std::make_unique<reader::FileScanRequest>();
    request->non_predicate_columns.push_back(field_projection(0));
    ASSERT_TRUE(reader.open(request).ok());
    EXPECT_EQ(request, nullptr);
    EXPECT_TRUE(reader.has_request());

    ASSERT_TRUE(reader.close().ok());
    EXPECT_FALSE(reader.has_request());
    EXPECT_TRUE(reader.eof());
}

TEST(FileReaderTest, CloseReleasesSharedIOContext) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto file_description = std::make_unique<io::FileDescription>();
    auto io_ctx = std::make_shared<io::IOContext>();
    std::weak_ptr<io::IOContext> weak_io_ctx = io_ctx;
    TestFileReader reader(system_properties, file_description, io_ctx);

    EXPECT_TRUE(reader.has_io_context());
    EXPECT_EQ(reader.io_context_use_count(), 2);
    io_ctx.reset();
    EXPECT_FALSE(weak_io_ctx.expired());
    EXPECT_EQ(reader.io_context_use_count(), 1);

    ASSERT_TRUE(reader.close().ok());
    EXPECT_FALSE(reader.has_io_context());
    EXPECT_TRUE(weak_io_ctx.expired());
}

TEST(TableColumnMapperTest, CreatesComplexProjectionForStructChildren) {
    reader::SchemaField struct_field;
    struct_field.id = 0;
    struct_field.name = "s";
    reader::SchemaField a_field;
    a_field.id = 0;
    a_field.name = "a";
    a_field.type = std::make_shared<DataTypeInt32>();
    reader::SchemaField b_field;
    b_field.id = 1;
    b_field.name = "b";
    b_field.type = std::make_shared<DataTypeString>();
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
    EXPECT_EQ(projection_ids(request->non_predicate_columns), std::vector<reader::ColumnId>({0}));
    ASSERT_EQ(request->non_predicate_columns.size(), 1);
    const auto& projection = request->non_predicate_columns[0];
    EXPECT_EQ(projection.file_column_id, 0);
    ASSERT_FALSE(projection.project_all_children);
    ASSERT_EQ(projection.children.size(), 1);
    EXPECT_EQ(projection.children[0].file_column_id, 1);

    ASSERT_EQ(mapper.mappings().size(), 1);
    const auto* projected_type =
            assert_cast<const DataTypeStruct*>(mapper.mappings()[0].file_type.get());
    ASSERT_EQ(projected_type->get_elements().size(), 1);
    EXPECT_EQ(projected_type->get_element_name(0), "b");
}

TEST(TableColumnMapperTest, CreatesComplexProjectionForMapValueStructChildren) {
    auto key_type = std::make_shared<DataTypeInt32>();
    auto a_type = std::make_shared<DataTypeInt32>();
    auto b_type = std::make_shared<DataTypeString>();
    auto value_type =
            std::make_shared<DataTypeStruct>(DataTypes {a_type, b_type}, Strings {"a", "b"});

    reader::SchemaField key_field;
    key_field.id = 0;
    key_field.name = "key";
    key_field.type = key_type;
    reader::SchemaField a_field;
    a_field.id = 0;
    a_field.name = "a";
    a_field.type = a_type;
    reader::SchemaField b_field;
    b_field.id = 1;
    b_field.name = "b";
    b_field.type = b_type;
    reader::SchemaField value_field;
    value_field.id = 1;
    value_field.name = "value";
    value_field.type = value_type;
    value_field.children = {a_field, b_field};
    reader::SchemaField entry_field;
    entry_field.id = 0;
    entry_field.name = "entries";
    entry_field.type = std::make_shared<DataTypeStruct>(DataTypes {key_type, value_type},
                                                        Strings {"key", "value"});
    entry_field.children = {key_field, value_field};
    reader::SchemaField map_field;
    map_field.id = 0;
    map_field.name = "m";
    map_field.type = std::make_shared<DataTypeMap>(key_type, value_type);
    map_field.children = {entry_field};

    reader::TableColumn table_value_child;
    table_value_child.id = 103;
    table_value_child.name = "b";
    table_value_child.type = b_type;
    reader::TableColumn table_value;
    table_value.id = 102;
    table_value.name = "value";
    table_value.type = std::make_shared<DataTypeStruct>(DataTypes {b_type}, Strings {"b"});
    table_value.children = {table_value_child};
    reader::TableColumn table_entry;
    table_entry.id = 101;
    table_entry.name = "entries";
    table_entry.type =
            std::make_shared<DataTypeStruct>(DataTypes {table_value.type}, Strings {"value"});
    table_entry.children = {table_value};
    reader::TableColumn table_column;
    table_column.id = 100;
    table_column.name = "m";
    table_column.type = std::make_shared<DataTypeMap>(key_type, table_value.type);
    table_column.children = {table_entry};

    reader::TableColumnMapperOptions options;
    options.mode = reader::TableColumnMappingMode::BY_NAME;
    reader::TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping({table_column}, {}, {map_field}).ok());

    auto request = std::make_unique<reader::FileScanRequest>();
    ASSERT_TRUE(mapper.create_scan_request({}, {}, {table_column}, request.get()).ok());
    EXPECT_EQ(projection_ids(request->non_predicate_columns), std::vector<reader::ColumnId>({0}));
    ASSERT_EQ(request->non_predicate_columns.size(), 1);
    const auto& projection = request->non_predicate_columns[0];
    EXPECT_EQ(projection.file_column_id, 0);
    ASSERT_FALSE(projection.project_all_children);
    ASSERT_EQ(projection.children.size(), 1);
    EXPECT_EQ(projection.children[0].file_column_id, 0);
    ASSERT_EQ(projection.children[0].children.size(), 1);
    EXPECT_EQ(projection.children[0].children[0].file_column_id, 1);
    ASSERT_EQ(projection.children[0].children[0].children.size(), 1);
    EXPECT_EQ(projection.children[0].children[0].children[0].file_column_id, 1);

    ASSERT_EQ(mapper.mappings().size(), 1);
    const auto* projected_type =
            assert_cast<const DataTypeMap*>(mapper.mappings()[0].file_type.get());
    EXPECT_EQ(remove_nullable(projected_type->get_key_type())->get_primitive_type(), TYPE_INT);
    const auto* projected_value =
            assert_cast<const DataTypeStruct*>(projected_type->get_value_type().get());
    ASSERT_EQ(projected_value->get_elements().size(), 1);
    EXPECT_EQ(projected_value->get_element_name(0), "b");
}

TEST(TableColumnMapperTest, ColumnPredicatesDoNotForcePredicateMaterialization) {
    reader::SchemaField id_field;
    id_field.id = 0;
    id_field.name = "id";
    id_field.type = std::make_shared<DataTypeInt32>();

    reader::SchemaField value_field;
    value_field.id = 1;
    value_field.name = "value";
    value_field.type = std::make_shared<DataTypeString>();

    reader::TableColumn table_id;
    table_id.id = 0;
    table_id.name = "id";
    table_id.type = id_field.type;

    reader::TableColumn table_value;
    table_value.id = 1;
    table_value.name = "value";
    table_value.type = value_field.type;

    reader::TableColumnMapperOptions options;
    options.mode = reader::TableColumnMappingMode::BY_NAME;
    reader::TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping({table_id, table_value}, {}, {id_field, value_field}).ok());

    reader::TableColumnPredicates column_predicates;
    column_predicates[0].push_back(create_comparison_predicate<PredicateType::GT>(
            0, "id", id_field.type, Field::create_field<TYPE_INT>(2), false));

    auto request = std::make_unique<reader::FileScanRequest>();
    ASSERT_TRUE(mapper.create_scan_request({}, column_predicates, {table_id, table_value},
                                           request.get())
                        .ok());
    EXPECT_TRUE(request->predicate_columns.empty());
    EXPECT_EQ(projection_ids(request->non_predicate_columns),
              std::vector<reader::ColumnId>({0, 1}));
    ASSERT_EQ(request->column_predicate_filters.size(), 1);
    EXPECT_EQ(request->column_predicate_filters[0].file_column_id, 0);
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
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
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
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
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
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
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

TEST_F(NewParquetReaderTest, ColumnPredicateOnlyPrunesAndDoesNotFilterRowsInsideRowGroup) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
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
    ASSERT_EQ(rows, ROW_COUNT);

    const auto& ids = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& values = assert_cast<const ColumnString&>(*block.get_by_position(1).column);
    ASSERT_EQ(ids.size(), ROW_COUNT);
    ASSERT_EQ(values.size(), ROW_COUNT);
    EXPECT_EQ(ids.get_element(0), 1);
    EXPECT_EQ(ids.get_element(4), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "one");
    EXPECT_EQ(values.get_data_at(4).to_string(), "five");
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
    request->predicate_columns = {field_projection(0), field_projection(1)};
    request->non_predicate_columns = {};
    request->conjuncts.push_back(create_int32_sum_greater_than_conjunct(0, 1, 7));
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

TEST_F(NewParquetReaderTest, ReaderExpressionMapRewritesPredicateColumnBeforeFilter) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 12));
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
    EXPECT_EQ(ids.get_element(0), 13);
    EXPECT_EQ(ids.get_element(1), 14);
    EXPECT_EQ(ids.get_element(2), 15);
    EXPECT_EQ(values.get_data_at(0).to_string(), "three");
    EXPECT_EQ(values.get_data_at(1).to_string(), "four");
    EXPECT_EQ(values.get_data_at(2).to_string(), "five");
}

TEST_F(NewParquetReaderTest, ReaderExpressionMapRewritesNonPredicateColumnAfterSelection) {
    write_int_pair_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& scores = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    ASSERT_EQ(ids.size(), 3);
    ASSERT_EQ(scores.size(), 3);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(scores.get_element(0), 13);
    EXPECT_EQ(scores.get_element(1), 14);
    EXPECT_EQ(scores.get_element(2), 15);
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
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
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

TEST_F(NewParquetReaderTest, PredicateFiltersRowGroupsByDictionary) {
    write_dictionary_filter_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 6);
    for (int row_group_idx = 0; row_group_idx < 6; ++row_group_idx) {
        auto row_group = parquet_file_reader->metadata()->RowGroup(row_group_idx);
        ASSERT_NE(row_group, nullptr);
        auto value_chunk = row_group->ColumnChunk(1);
        ASSERT_NE(value_chunk, nullptr);
        ASSERT_TRUE(value_chunk->has_dictionary_page());
        ASSERT_TRUE(value_chunk->statistics() == nullptr ||
                    !value_chunk->statistics()->HasMinMax());
    }

    std::vector<std::unique_ptr<parquet::ParquetColumnSchema>> file_schema;
    auto schema_descriptor = parquet_file_reader->metadata()->schema();
    ASSERT_NE(schema_descriptor, nullptr);
    ASSERT_TRUE(parquet::build_parquet_column_schema(*schema_descriptor, &file_schema).ok());
    ASSERT_EQ(file_schema.size(), 2);

    reader::FileScanRequest plan_request;
    reader::FileColumnPredicateFilter plan_column_filter;
    plan_column_filter.file_column_id = 1;
    auto value_type = std::make_shared<DataTypeString>();
    plan_column_filter.predicates.push_back(create_comparison_predicate<PredicateType::EQ>(
            1, "value", value_type, Field::create_field<TYPE_STRING>("lm"), false));
    plan_request.column_predicate_filters.push_back(std::move(plan_column_filter));

    parquet::RowGroupScanPlan plan;
    parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                 parquet_file_reader.get(), file_schema,
                                                 plan_request, scan_range, &plan)
                        .ok());
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 6);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_dictionary, 5);
    EXPECT_EQ(plan.pruning_stats.filtered_group_rows, 5);
    EXPECT_EQ(plan.pruning_stats.selected_row_ranges, 1);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_string_in_conjunct(1, {"lm"}));
    reader::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = 1;
    column_filter.predicates.push_back(create_comparison_predicate<PredicateType::EQ>(
            1, "value", schema[1].type, Field::create_field<TYPE_STRING>("lm"), false));
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

    EXPECT_EQ(ids, std::vector<int32_t>({3}));
    EXPECT_EQ(values, std::vector<std::string>({"lm"}));
}

TEST_F(NewParquetReaderTest, PlannerNarrowsRowRangesByPageIndex) {
    write_page_index_filter_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 1);
    auto page_index_reader = parquet_file_reader->GetPageIndexReader();
    ASSERT_NE(page_index_reader, nullptr);
    auto row_group_index_reader = page_index_reader->RowGroup(0);
    ASSERT_NE(row_group_index_reader, nullptr);
    auto offset_index = row_group_index_reader->GetOffsetIndex(0);
    ASSERT_NE(offset_index, nullptr);
    ASSERT_GT(offset_index->page_locations().size(), 1);

    std::vector<std::unique_ptr<parquet::ParquetColumnSchema>> file_schema;
    auto schema_descriptor = parquet_file_reader->metadata()->schema();
    ASSERT_NE(schema_descriptor, nullptr);
    ASSERT_TRUE(parquet::build_parquet_column_schema(*schema_descriptor, &file_schema).ok());
    ASSERT_EQ(file_schema.size(), 1);

    reader::FileScanRequest request;
    reader::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = 0;
    auto id_type = std::make_shared<DataTypeInt32>();
    column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GT>(
            0, "id", id_type, Field::create_field<TYPE_INT>(63), false));
    request.column_predicate_filters.push_back(std::move(column_filter));

    parquet::RowGroupScanPlan plan;
    parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                 parquet_file_reader.get(), file_schema, request,
                                                 scan_range, &plan)
                        .ok());
    ASSERT_EQ(plan.row_groups.size(), 1);
    ASSERT_FALSE(plan.row_groups[0].selected_ranges.empty());
    EXPECT_GT(plan.row_groups[0].selected_ranges.front().start, 0);
    EXPECT_LT(plan.row_groups[0].selected_ranges.front().length, 128);
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_page_index, 0);
    EXPECT_GT(plan.pruning_stats.filtered_page_rows, 0);
    EXPECT_EQ(plan.pruning_stats.selected_row_ranges, plan.row_groups[0].selected_ranges.size());
}

TEST_F(NewParquetReaderTest, InPredicateFiltersRowGroupsByDictionary) {
    write_dictionary_filter_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_string_in_conjunct(1, {"az", "za"}));
    auto set = build_set<TYPE_STRING>();
    set->insert(const_cast<char*>("az"), 2);
    set->insert(const_cast<char*>("za"), 2);
    reader::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = 1;
    column_filter.predicates.push_back(create_in_list_predicate<PredicateType::IN_LIST>(
            1, "value", schema[1].type, set, false));
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

    EXPECT_EQ(ids, std::vector<int32_t>({2, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"az", "za"}));
}

TEST_F(NewParquetReaderTest, DictionaryPageV2StringEdgesSurviveSelection) {
    write_dictionary_edge_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 4);
    for (int row_group_idx = 0; row_group_idx < 4; ++row_group_idx) {
        auto row_group = parquet_file_reader->metadata()->RowGroup(row_group_idx);
        ASSERT_NE(row_group, nullptr);
        ASSERT_TRUE(row_group->ColumnChunk(1)->has_dictionary_page());
    }

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_string_in_conjunct(1, {"", "same"}));
    auto set = build_set<TYPE_STRING>();
    set->insert(const_cast<char*>(""), 0);
    set->insert(const_cast<char*>("same"), 4);
    reader::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = 1;
    column_filter.predicates.push_back(create_in_list_predicate<PredicateType::IN_LIST>(
            1, "value", schema[1].type, set, false));
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

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 5, 7}));
    EXPECT_EQ(values, std::vector<std::string>({"", "same", "", "same"}));
}

TEST_F(NewParquetReaderTest, StatisticsPruningSkipsPrefixRowGroupsAndReadsLaterGroups) {
    write_parquet_file(_file_path, 1);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 5);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 3));
    reader::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = 0;
    column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GE>(
            0, "id", schema[0].type, Field::create_field<TYPE_INT>(4), false));
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

    EXPECT_EQ(ids, std::vector<int32_t>({4, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"four", "five"}));
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
    request->non_predicate_columns = {
            field_projection(parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID),
            field_projection(0)};
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
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {
            field_projection(parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID)};
    request->column_positions = {
            {0, 0},
            {parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID, 2},
    };
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
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

TEST_F(NewParquetReaderTest, DeletePredicateFiltersRowPositions) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block_with_row_position(schema);

    static const std::vector<int64_t> deleted_rows {1, 3};
    auto delete_predicate = std::make_shared<DeletePredicate>(deleted_rows);
    delete_predicate->add_child(TableSlotRef::create_shared(
            2, 2, -1, std::make_shared<DataTypeInt64>(),
            parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_NAME));

    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {
            field_projection(parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID)};
    request->non_predicate_columns = {field_projection(0)};
    request->column_positions = {
            {0, 0},
            {parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID, 2},
    };
    request->delete_conjuncts.push_back(VExprContext::create_shared(std::move(delete_predicate)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& row_position_column =
            assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);
    EXPECT_EQ(id_column.get_element(2), 5);
    EXPECT_EQ(row_position_column.get_element(0), 0);
    EXPECT_EQ(row_position_column.get_element(1), 2);
    EXPECT_EQ(row_position_column.get_element(2), 4);
}

TEST_F(NewParquetReaderTest, QueryPredicateAndDeletePredicateFilterRowPositions) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<reader::SchemaField> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block_with_row_position(schema);

    static const std::vector<int64_t> deleted_rows {3};
    auto delete_predicate = std::make_shared<DeletePredicate>(deleted_rows);
    delete_predicate->add_child(TableSlotRef::create_shared(
            2, 2, -1, std::make_shared<DataTypeInt64>(),
            parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_NAME));

    auto request = std::make_unique<reader::FileScanRequest>();
    request->predicate_columns = {
            field_projection(0),
            field_projection(parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID)};
    request->non_predicate_columns = {};
    request->column_positions = {
            {0, 0},
            {parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID, 2},
    };
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    request->delete_conjuncts.push_back(VExprContext::create_shared(std::move(delete_predicate)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 2);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& row_position_column =
            assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(id_column.get_element(1), 5);
    EXPECT_EQ(row_position_column.get_element(0), 2);
    EXPECT_EQ(row_position_column.get_element(1), 4);
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
                field_projection(parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID),
                field_projection(0)};
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
