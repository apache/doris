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

#include "format/reader/table_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
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
#include "exec/common/endian.h"
#include "exprs/vexpr.h"
#include "format/format_common.h"
#include "format/reader/expr/slot_ref.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg_reader_v2.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "roaring/roaring64map.hh"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/predicate/predicate_creator.h"

namespace doris::reader {
namespace {

FieldProjection field_projection(ColumnId column_id) {
    return FieldProjection {.field_id = column_id};
}

std::vector<ColumnId> projection_ids(const std::vector<FieldProjection>& projections) {
    std::vector<ColumnId> ids;
    ids.reserve(projections.size());
    for (const auto& projection : projections) {
        ids.push_back(projection.field_id);
    }
    return ids;
}

class TableInt32GreaterThanExpr final : public VExpr {
public:
    TableInt32GreaterThanExpr(int slot_id, int column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(value) {
        add_child(TableSlotRef::create_shared(slot_id, column_id, slot_id,
                                              std::make_shared<DataTypeInt32>(), "id"));
        set_node_type(TExprNodeType::BINARY_PRED);
        _opcode = TExprOpcode::GT;
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto* slot_ref = assert_cast<const VSlotRef*>(get_child(0).get());
        const auto& input = assert_cast<const ColumnInt32&>(
                *block->get_by_position(slot_ref->column_id()).column);
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
    const int32_t _value;
    const std::string _expr_name = "TableInt32GreaterThanExpr";
};

class IcebergTableReaderDeleteFileTestHelper final : public doris::iceberg::IcebergTableReader {
public:
    Status parse_deletion_vector_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc* desc,
                                      bool* has_delete_file) {
        return _parse_deletion_vector_file(t_desc, desc, has_delete_file);
    }
};

class IcebergTableReaderScanRequestTestHelper final : public doris::iceberg::IcebergTableReader {
public:
    Status init_for_scan_request_test(std::vector<TableColumn> projected_columns) {
        _query_options = std::make_unique<TQueryOptions>();
        _query_globals = std::make_unique<TQueryGlobals>();
        _state = std::make_unique<RuntimeState>(*_query_options, *_query_globals);
        RETURN_IF_ERROR(init({
                .projected_columns = std::move(projected_columns),
                .column_predicates = {},
                .conjuncts = {},
                .format = FileFormat::PARQUET,
                .scan_params = nullptr,
                .io_ctx = nullptr,
                .runtime_state = _state.get(),
                .scanner_profile = nullptr,
                .allow_missing_columns = true,
                .profile = nullptr,
        }));

        SplitReadOptions split_options;
        split_options.current_range.__set_path("scan-request-test.parquet");
        TTableFormatFileDesc table_format_params;
        TIcebergFileDesc iceberg_params;
        iceberg_params.__set_first_row_id(1000);
        table_format_params.__set_iceberg_params(iceberg_params);
        split_options.current_range.__set_table_format_params(table_format_params);
        RETURN_IF_ERROR(prepare_split(split_options));

        _delete_rows_storage = {1};
        _delete_rows = &_delete_rows_storage;
        return Status::OK();
    }

    Status customize_request(FileScanRequest* request) {
        return customize_file_scan_request(request);
    }

private:
    std::unique_ptr<TQueryOptions> _query_options;
    std::unique_ptr<TQueryGlobals> _query_globals;
    std::unique_ptr<RuntimeState> _state;
    DeleteRows _delete_rows_storage;
};

class TableInt32SumGreaterThanExpr final : public VExpr {
public:
    TableInt32SumGreaterThanExpr(int left_slot_id, int left_column_id, int right_slot_id,
                                 int right_column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(value) {
        add_child(TableSlotRef::create_shared(left_slot_id, left_column_id, left_slot_id,
                                              std::make_shared<DataTypeInt32>(), "id"));
        add_child(TableSlotRef::create_shared(right_slot_id, right_column_id, right_slot_id,
                                              std::make_shared<DataTypeInt32>(), "score"));
        set_node_type(TExprNodeType::BINARY_PRED);
        _opcode = TExprOpcode::GT;
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto* left_slot_ref = assert_cast<const VSlotRef*>(get_child(0).get());
        const auto* right_slot_ref = assert_cast<const VSlotRef*>(get_child(1).get());
        const auto& left_input = assert_cast<const ColumnInt32&>(
                *block->get_by_position(left_slot_ref->column_id()).column);
        const auto& right_input = assert_cast<const ColumnInt32&>(
                *block->get_by_position(right_slot_ref->column_id()).column);
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
    const int32_t _value;
    const std::string _expr_name = "TableInt32SumGreaterThanExpr";
};

class TableInt32SumLessThanExpr final : public VExpr {
public:
    TableInt32SumLessThanExpr(int left_slot_id, int left_column_id, int right_slot_id,
                              int right_column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(value) {
        add_child(TableSlotRef::create_shared(left_slot_id, left_column_id, left_slot_id,
                                              std::make_shared<DataTypeInt32>(), "id"));
        add_child(TableSlotRef::create_shared(right_slot_id, right_column_id, right_slot_id,
                                              std::make_shared<DataTypeInt32>(), "score"));
        set_node_type(TExprNodeType::BINARY_PRED);
        _opcode = TExprOpcode::LT;
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto* left_slot_ref = assert_cast<const VSlotRef*>(get_child(0).get());
        const auto* right_slot_ref = assert_cast<const VSlotRef*>(get_child(1).get());
        const auto& left_input = assert_cast<const ColumnInt32&>(
                *block->get_by_position(left_slot_ref->column_id()).column);
        const auto& right_input = assert_cast<const ColumnInt32&>(
                *block->get_by_position(right_slot_ref->column_id()).column);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    left_input.get_element(input_row) + right_input.get_element(input_row) < _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int32_t _value;
    const std::string _expr_name = "TableInt32SumLessThanExpr";
};

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

std::shared_ptr<arrow::Array> build_int64_array(const std::vector<int64_t>& values) {
    arrow::Int64Builder builder;
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

void write_parquet_file(const std::string& file_path, int32_t id, const std::string& value) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array({id}), build_string_array({value})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_iceberg_equality_delete_parquet_file(const std::string& file_path, int32_t field_id,
                                                int32_t value) {
    const auto metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false)->WithMetadata(metadata),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array({value})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_iceberg_equality_delete_bigint_parquet_file(const std::string& file_path,
                                                       int32_t field_id, int64_t value) {
    const auto metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
    auto schema = arrow::schema({
            arrow::field("id", arrow::int64(), false)->WithMetadata(metadata),
    });
    auto table = arrow::Table::Make(schema, {build_int64_array({value})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_struct_parquet_file(const std::string& file_path, int32_t id) {
    auto struct_type = arrow::struct_({arrow::field("id", arrow::int32(), false)});
    arrow::StructBuilder builder(
            struct_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::Int32Builder>(arrow::default_memory_pool())});
    auto* id_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(id_builder->Append(id).ok());

    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(schema, {finish_array(&builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      writer_builder.build()));
}

void write_struct_parquet_file(const std::string& file_path, const std::vector<int32_t>& ids,
                               int64_t row_group_size = -1) {
    auto struct_type = arrow::struct_({arrow::field("id", arrow::int32(), false)});
    arrow::StructBuilder builder(
            struct_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::Int32Builder>(arrow::default_memory_pool())});
    auto* id_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
    for (const auto id : ids) {
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(id_builder->Append(id).ok());
    }

    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(schema, {finish_array(&builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    const auto write_row_group_size =
            row_group_size > 0 ? row_group_size : static_cast<int64_t>(ids.size());
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      write_row_group_size,
                                                      writer_builder.build()));
}

void write_list_struct_parquet_file(const std::string& file_path) {
    auto struct_type = arrow::struct_(
            {arrow::field("a", arrow::int32(), false), arrow::field("b", arrow::int32(), false)});
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    auto a_array_builder = std::make_unique<arrow::Int32Builder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
    auto b_array_builder = std::make_unique<arrow::Int32Builder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(b_array_builder)));
    auto struct_builder = std::make_shared<arrow::StructBuilder>(
            struct_type, arrow::default_memory_pool(), std::move(field_builders));
    auto list_type = arrow::list(arrow::field("element", struct_type, false));
    arrow::ListBuilder builder(arrow::default_memory_pool(), struct_builder, list_type);
    auto* a_builder = assert_cast<arrow::Int32Builder*>(struct_builder->field_builder(0));
    auto* b_builder = assert_cast<arrow::Int32Builder*>(struct_builder->field_builder(1));

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(10).ok());
    EXPECT_TRUE(b_builder->Append(11).ok());
    EXPECT_TRUE(struct_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(20).ok());
    EXPECT_TRUE(b_builder->Append(21).ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(30).ok());
    EXPECT_TRUE(b_builder->Append(31).ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(40).ok());
    EXPECT_TRUE(b_builder->Append(41).ok());

    auto schema = arrow::schema({
            arrow::field("xs", list_type, false),
    });
    auto table = arrow::Table::Make(schema, {finish_array(&builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 3,
                                                      writer_builder.build()));
}

void write_map_struct_parquet_file(const std::string& file_path) {
    auto key_builder = std::make_shared<arrow::Int32Builder>();
    auto struct_type = arrow::struct_(
            {arrow::field("a", arrow::int32(), false), arrow::field("b", arrow::utf8(), false)});
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    auto a_array_builder = std::make_unique<arrow::Int32Builder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
    auto b_array_builder = std::make_unique<arrow::StringBuilder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(b_array_builder)));
    auto value_builder = std::make_shared<arrow::StructBuilder>(
            struct_type, arrow::default_memory_pool(), std::move(field_builders));
    auto map_type = arrow::map(arrow::int32(), arrow::field("value", struct_type, false));
    arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder, map_type);
    auto* a_builder = assert_cast<arrow::Int32Builder*>(value_builder->field_builder(0));
    auto* b_builder = assert_cast<arrow::StringBuilder*>(value_builder->field_builder(1));

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(key_builder->Append(1).ok());
    EXPECT_TRUE(value_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(10).ok());
    EXPECT_TRUE(b_builder->Append("ma").ok());
    EXPECT_TRUE(key_builder->Append(2).ok());
    EXPECT_TRUE(value_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(20).ok());
    EXPECT_TRUE(b_builder->Append("mb").ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(key_builder->Append(3).ok());
    EXPECT_TRUE(value_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(30).ok());
    EXPECT_TRUE(b_builder->Append("mc").ok());

    EXPECT_TRUE(builder.AppendEmptyValue().ok());

    auto schema = arrow::schema({
            arrow::field("kv", map_type, false),
    });
    auto table = arrow::Table::Make(schema, {finish_array(&builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 3,
                                                      writer_builder.build()));
}

void write_int_pair_parquet_file(const std::string& file_path, const std::vector<int32_t>& ids,
                                 const std::vector<int32_t>& scores,
                                 const std::vector<std::string>& values,
                                 int64_t row_group_size = -1) {
    const auto id_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"0"});
    const auto score_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"1"});
    const auto value_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"2"});
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false)->WithMetadata(id_metadata),
            arrow::field("score", arrow::int32(), false)->WithMetadata(score_metadata),
            arrow::field("value", arrow::utf8(), false)->WithMetadata(value_metadata),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids), build_int32_array(scores),
                                             build_string_array(values)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    const auto write_row_group_size =
            row_group_size > 0 ? row_group_size : static_cast<int64_t>(ids.size());
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      write_row_group_size, builder.build()));
}

void write_position_delete_parquet_file(const std::string& file_path,
                                        const std::vector<std::string>& data_file_paths,
                                        const std::vector<int64_t>& positions) {
    auto schema = arrow::schema({
            arrow::field("file_path", arrow::utf8(), false),
            arrow::field("pos", arrow::int64(), false),
    });
    auto table = arrow::Table::Make(
            schema, {build_string_array(data_file_paths), build_int64_array(positions)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(positions.size()),
                                                      builder.build()));
}

int64_t write_iceberg_deletion_vector_file(const std::string& file_path,
                                           const std::vector<uint64_t>& deleted_positions) {
    roaring::Roaring64Map rows;
    for (const auto position : deleted_positions) {
        rows.add(position);
    }

    const size_t bitmap_size = rows.getSizeInBytes();
    std::vector<char> blob(4 + 4 + bitmap_size + 4);
    rows.write(blob.data() + 8);

    const uint32_t total_length = static_cast<uint32_t>(4 + bitmap_size);
    BigEndian::Store32(blob.data(), total_length);
    constexpr char DV_MAGIC[] = {'\xD1', '\xD3', '\x39', '\x64'};
    memcpy(blob.data() + 4, DV_MAGIC, 4);
    BigEndian::Store32(blob.data() + 8 + bitmap_size, 0);

    std::ofstream output(file_path, std::ios::binary);
    EXPECT_TRUE(output.is_open());
    output.write(blob.data(), static_cast<std::streamsize>(blob.size()));
    EXPECT_TRUE(output.good());
    return static_cast<int64_t>(blob.size());
}

Block build_table_block(const std::vector<TableColumn>& columns) {
    Block block;
    for (const auto& column : columns) {
        block.insert({column.type->create_column(), column.type, column.name});
    }
    return block;
}

void expect_nullable_int64_column_values(const IColumn& column,
                                         const std::vector<int64_t>& expected_values) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*full_column);
    const auto& values =
            assert_cast<const ColumnInt64&>(nullable_column.get_nested_column()).get_data();
    ASSERT_EQ(nullable_column.size(), expected_values.size());
    for (size_t row = 0; row < expected_values.size(); ++row) {
        EXPECT_EQ(nullable_column.get_null_map_data()[row], 0);
        EXPECT_EQ(values[row], expected_values[row]);
    }
}

SplitReadOptions build_split_options(const std::string& file_path) {
    SplitReadOptions options;
    options.current_range.__set_path(file_path);
    options.current_range.__set_file_size(
            static_cast<int64_t>(std::filesystem::file_size(file_path)));
    return options;
}

void set_iceberg_row_lineage_params(SplitReadOptions* split_options, int64_t first_row_id,
                                    int64_t last_updated_sequence_number) {
    TTableFormatFileDesc table_format_params;
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_first_row_id(first_row_id);
    iceberg_params.__set_last_updated_sequence_number(last_updated_sequence_number);
    table_format_params.__set_iceberg_params(iceberg_params);
    split_options->current_range.__set_table_format_params(table_format_params);
}

TIcebergDeleteFileDesc make_iceberg_deletion_vector(const std::string& path, int64_t offset,
                                                    int64_t size) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(3);
    delete_file.__set_path(path);
    delete_file.__set_content_offset(offset);
    delete_file.__set_content_size_in_bytes(size);
    return delete_file;
}

TIcebergDeleteFileDesc make_iceberg_position_delete_file(const std::string& path) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(1);
    delete_file.__set_path(path);
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    return delete_file;
}

TIcebergDeleteFileDesc make_iceberg_equality_delete_file(const std::string& path,
                                                         const std::vector<int32_t>& field_ids) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(2);
    delete_file.__set_path(path);
    delete_file.__set_field_ids(field_ids);
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    return delete_file;
}

TFileScanRangeParams make_local_parquet_scan_params() {
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    return scan_params;
}

std::shared_ptr<io::IOContext> make_io_context(io::FileReaderStats* file_reader_stats,
                                               io::FileCacheStatistics* file_cache_stats) {
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = file_reader_stats;
    io_ctx->file_cache_stats = file_cache_stats;
    return io_ctx;
}

std::unique_ptr<ReadProfile> make_table_read_profile(RuntimeProfile* profile) {
    auto read_profile = std::make_unique<ReadProfile>();
    read_profile->num_delete_files = ADD_COUNTER(profile, "NumDeleteFiles", TUnit::UNIT);
    read_profile->num_delete_rows = ADD_COUNTER(profile, "NumDeleteRows", TUnit::UNIT);
    read_profile->parse_delete_file_time = ADD_TIMER(profile, "ParseDeleteFileTime");
    return read_profile;
}

TTableFormatFileDesc make_iceberg_table_format_desc(
        const std::string& data_file_path,
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    TTableFormatFileDesc table_format_params;
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_format_version(2);
    iceberg_params.__set_original_file_path(data_file_path);
    iceberg_params.__set_delete_files(delete_files);
    table_format_params.__set_iceberg_params(iceberg_params);
    return table_format_params;
}

std::vector<int32_t> read_iceberg_ids(doris::iceberg::IcebergTableReader* reader,
                                      const std::vector<TableColumn>& projected_columns) {
    std::vector<int32_t> ids;
    bool eos = false;
    while (!eos) {
        Block block = build_table_block(projected_columns);
        auto status = reader->get_block(&block, &eos);
        if (!status.ok()) {
            ADD_FAILURE() << status;
            return ids;
        }
        if (block.rows() == 0) {
            continue;
        }
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            ids.push_back(id_column.get_element(row));
        }
    }
    return ids;
}

int64_t parquet_column_start_offset(const ::parquet::ColumnChunkMetaData& column_metadata) {
    return column_metadata.has_dictionary_page()
                   ? static_cast<int64_t>(column_metadata.dictionary_page_offset())
                   : static_cast<int64_t>(column_metadata.data_page_offset());
}

SplitReadOptions build_split_options_for_row_group_mid(const std::string& file_path,
                                                       int row_group_idx) {
    auto options = build_split_options(file_path);
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
    options.current_range.__set_start_offset(row_group_mid_offset);
    options.current_range.__set_size(1);
    return options;
}

TableColumn make_table_column(ColumnId id, const std::string& name, const DataTypePtr& type) {
    TableColumn column;
    column.id = id;
    column.name = name;
    column.type = type;
    return column;
}

SchemaField make_schema_field(ColumnId id, const std::string& name, const DataTypePtr& type) {
    SchemaField field;
    field.id = id;
    field.name = name;
    field.type = type;
    return field;
}

void add_column_predicate(TableColumnPredicates* column_predicates, const TableColumn& column,
                          std::shared_ptr<ColumnPredicate> predicate) {
    auto& entry = (*column_predicates)[column.id];
    entry.first = column;
    entry.second.push_back(std::move(predicate));
}

VExprContextSPtr prepared_conjunct(RuntimeState* state, const VExprSPtr& expr) {
    auto ctx = VExprContext::create_shared(expr);
    auto status = ctx->prepare(state, RowDescriptor());
    EXPECT_TRUE(status.ok()) << status;
    status = ctx->open(state);
    EXPECT_TRUE(status.ok()) << status;
    return ctx;
}

TEST(TableReaderTest, ReopenSplitAfterClose) {
    const auto test_dir = std::filesystem::temp_directory_path() / "doris_table_reader_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const std::vector<std::string> file_paths = {
            (test_dir / "split_1.parquet").string(),
            (test_dir / "split_2.parquet").string(),
            (test_dir / "split_3.parquet").string(),
    };
    write_parquet_file(file_paths[0], 1, "one");
    write_parquet_file(file_paths[1], 2, "two");
    write_parquet_file(file_paths[2], 3, "three");

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(1, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state,
                                            std::make_shared<TableInt32GreaterThanExpr>(0, 0, 0))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    // Simulate the scanner lifecycle for three different splits:
    // init() once, then repeat prepare_split() -> get_block() -> close().
    // This verifies TableReader::close() fully releases the previous low-level reader and task
    // state, so a later prepare_split() can open and read a new split on the same TableReader.
    // The table-level conjunct is also rebuilt for each split. The projection order puts value
    // before id, so the pushed conjunct has to be rewritten to the ParquetReader file-local block
    // position every time a new split is opened.
    std::vector<int32_t> ids;
    std::vector<std::string> values;
    for (const auto& file_path : file_paths) {
        auto split_options = build_split_options(file_path);
        ASSERT_TRUE(reader.prepare_split(split_options).ok());

        Block block = build_table_block(projected_columns);
        bool eos = false;
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        ASSERT_FALSE(eos);

        const auto& value_column =
                assert_cast<const ColumnString&>(*block.get_by_position(0).column);
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
        ASSERT_EQ(id_column.size(), 1);
        ASSERT_EQ(value_column.size(), 1);
        ids.push_back(id_column.get_element(0));
        values.push_back(value_column.get_data_at(0).to_string());

        ASSERT_TRUE(reader.close().ok());
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 3}));
    EXPECT_EQ(values, std::vector<std::string>({"one", "two", "three"}));

    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownCountFromNewParquetReader) {
    const auto test_dir = std::filesystem::temp_directory_path() / "doris_table_reader_count_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"}, 2);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 5);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFromNewParquetReader) {
    const auto test_dir = std::filesystem::temp_directory_path() / "doris_table_reader_minmax_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {3, 1, 5, 2}, {30, 10, 50, 20},
                                {"three", "one", "five", "two"}, 2);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(1, "score", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& score_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 5);
    EXPECT_EQ(score_column.get_element(0), 10);
    EXPECT_EQ(score_column.get_element(1), 50);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxCastsFileValueToTableType) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_cast_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {3, 1, 5, 2}, {30, 10, 50, 20},
                                {"three", "one", "five", "two"}, 2);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt64>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 5);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFromProjectedStructLeaf) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_struct_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_struct_parquet_file(file_path, {3, 1, 5, 2}, 2);

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto id_child = make_table_column(0, "id", int_type);
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"id"});
    auto struct_column = make_table_column(100, "s", struct_type);
    struct_column.children = {id_child};
    const std::vector<TableColumn> projected_columns = {struct_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& struct_result = assert_cast<const ColumnStruct&>(*block.get_by_position(0).column);
    ASSERT_EQ(struct_result.get_columns().size(), 1);
    const auto& ids = assert_cast<const ColumnInt32&>(struct_result.get_column(0));
    EXPECT_EQ(ids.get_element(0), 1);
    EXPECT_EQ(ids.get_element(1), 5);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFallsBackForProjectedListStructLeaf) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_list_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_list_struct_parquet_file(file_path);

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto a_child = make_table_column(0, "a", int_type);
    auto b_child = make_table_column(1, "b", int_type);
    auto element_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type}, Strings {"a", "b"});
    auto element_child = make_table_column(0, "element", element_type);
    element_child.children = {a_child, b_child};
    auto list_column = make_table_column(100, "xs", std::make_shared<DataTypeArray>(element_type));
    list_column.children = {element_child};
    const std::vector<TableColumn> projected_columns = {list_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 3);
    const auto& array_result = assert_cast<const ColumnArray&>(*block.get_by_position(0).column);
    EXPECT_EQ(array_result.get_offsets()[0], 2);
    EXPECT_EQ(array_result.get_offsets()[1], 3);
    EXPECT_EQ(array_result.get_offsets()[2], 4);
    const auto& element_struct = assert_cast<const ColumnStruct&>(array_result.get_data());
    ASSERT_EQ(element_struct.get_columns().size(), 2);
    const auto& a_values = assert_cast<const ColumnInt32&>(element_struct.get_column(0));
    EXPECT_EQ(a_values.get_element(0), 10);
    EXPECT_EQ(a_values.get_element(1), 20);
    EXPECT_EQ(a_values.get_element(2), 30);
    EXPECT_EQ(a_values.get_element(3), 40);
    const auto& b_values = assert_cast<const ColumnInt32&>(element_struct.get_column(1));
    EXPECT_EQ(b_values.get_element(0), 11);
    EXPECT_EQ(b_values.get_element(1), 21);
    EXPECT_EQ(b_values.get_element(2), 31);
    EXPECT_EQ(b_values.get_element(3), 41);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFallsBackForProjectedMapValueStructLeaf) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_map_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_map_struct_parquet_file(file_path);

    const auto key_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    auto b_child = make_table_column(1, "b", string_type);
    auto value_type = std::make_shared<DataTypeStruct>(DataTypes {string_type}, Strings {"b"});
    auto value_child = make_table_column(1, "value", value_type);
    value_child.children = {b_child};
    auto entry_type = std::make_shared<DataTypeStruct>(DataTypes {value_type}, Strings {"value"});
    auto entry_child = make_table_column(0, "entries", entry_type);
    entry_child.children = {value_child};
    auto map_column =
            make_table_column(100, "kv", std::make_shared<DataTypeMap>(key_type, value_type));
    map_column.children = {entry_child};
    const std::vector<TableColumn> projected_columns = {map_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 3);
    const auto& map_result = assert_cast<const ColumnMap&>(*block.get_by_position(0).column);
    EXPECT_EQ(map_result.get_offsets()[0], 2);
    EXPECT_EQ(map_result.get_offsets()[1], 3);
    EXPECT_EQ(map_result.get_offsets()[2], 3);
    const auto& keys = assert_cast<const ColumnInt32&>(map_result.get_keys());
    EXPECT_EQ(keys.get_element(0), 1);
    EXPECT_EQ(keys.get_element(1), 2);
    EXPECT_EQ(keys.get_element(2), 3);
    const auto& value_struct = assert_cast<const ColumnStruct&>(map_result.get_values());
    ASSERT_EQ(value_struct.get_columns().size(), 1);
    const auto& b_values = assert_cast<const ColumnString&>(value_struct.get_column(0));
    EXPECT_EQ(b_values.get_data_at(0).to_string(), "ma");
    EXPECT_EQ(b_values.get_data_at(1).to_string(), "mb");
    EXPECT_EQ(b_values.get_data_at(2).to_string(), "mc");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxOnlyUsesSelectedRowGroupInFileRange) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_range_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {10, 1, 100}, {100, 10, 1000}, {"ten", "one", "hundred"},
                                1);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options_for_row_group_mid(file_path, 1)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownCountOnlyUsesSelectedRowGroupInFileRange) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_count_range_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"}, 1);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options_for_row_group_mid(file_path, 2)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownCountFallsBackWithTableConjunct) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_count_conjunct_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state,
                                            std::make_shared<TableInt32GreaterThanExpr>(0, 0, 2))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 1);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownCountFallsBackWithColumnPredicate) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_count_predicate_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"}, 1);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, projected_columns[0],
                         create_comparison_predicate<PredicateType::GT>(
                                 0, "id", std::make_shared<DataTypeInt32>(),
                                 Field::create_field<TYPE_INT>(2), false));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = std::move(column_predicates),
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 1);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFallsBackWithoutDirectFileMapping) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_minmax_missing_mapping_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(
            make_table_column(99, "missing_id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                                    .profile = nullptr,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 0);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, OpenReaderBuildsTableFiltersFromConjuncts) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_conjunct_filter_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 3, "three");

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(1, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state,
                                            std::make_shared<TableInt32GreaterThanExpr>(0, 0, 2))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // open_reader() should convert the table-level conjunct on projected column id 0 into
    // _table_filters before ColumnMapper creates the FileScanRequest. ColumnMapper then rewrites
    // the conjunct's slot ref from table column id 0 to the file-local block position used by
    // ParquetReader. The projection order intentionally puts value before id, so the id filter
    // column is not at position 0 in the file block.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    ASSERT_EQ(id_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);

    ASSERT_TRUE(reader.close().ok());

    TableReader filtered_reader;
    ASSERT_TRUE(
            filtered_reader
                    .init({
                            .projected_columns = projected_columns,
                            .column_predicates = {},
                            .conjuncts = {prepared_conjunct(
                                    &state, std::make_shared<TableInt32GreaterThanExpr>(0, 0, 4))},
                            .format = FileFormat::PARQUET,
                            .scan_params = nullptr,
                            .io_ctx = nullptr,
                            .runtime_state = &state,
                            .scanner_profile = nullptr,
                            .allow_missing_columns = true,
                            .profile = nullptr,
                    })
                    .ok());
    ASSERT_TRUE(filtered_reader.prepare_split(build_split_options(file_path)).ok());

    block = build_table_block(projected_columns);
    eos = false;
    ASSERT_TRUE(filtered_reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.get_by_position(1).column->size(), 0);

    ASSERT_TRUE(filtered_reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, OpenReaderBuildsColumnPredicateFilters) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_column_predicate_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    // ColumnPredicate is only used for row-group/statistics pruning. Keep one row per row
    // group so the predicate can prune the first two row groups and leave only id = 3.
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {1, 5, 8}, {"one", "two", "three"}, 1);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(2, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, projected_columns[1],
                         create_comparison_predicate<PredicateType::GT>(
                                 0, "id", std::make_shared<DataTypeInt32>(),
                                 Field::create_field<TYPE_INT>(2), false));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = std::move(column_predicates),
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(0).column);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    ASSERT_EQ(id_column.size(), 1);
    ASSERT_EQ(value_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "three");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ColumnPredicateSurvivesReopenSplit) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_predicate_reopen_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const std::vector<std::string> file_paths = {
            (test_dir / "split_1.parquet").string(),
            (test_dir / "split_2.parquet").string(),
    };
    write_int_pair_parquet_file(file_paths[0], {1, 3}, {10, 30}, {"one", "three"}, 1);
    write_int_pair_parquet_file(file_paths[1], {2, 4}, {20, 40}, {"two", "four"}, 1);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, projected_columns[0],
                         create_comparison_predicate<PredicateType::GT>(
                                 0, "id", std::make_shared<DataTypeInt32>(),
                                 Field::create_field<TYPE_INT>(2), false));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = std::move(column_predicates),
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    std::vector<int32_t> ids;
    for (const auto& file_path : file_paths) {
        ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

        Block block = build_table_block(projected_columns);
        bool eos = false;
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        ASSERT_FALSE(eos);
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        ASSERT_EQ(id_column.size(), 1);
        ids.push_back(id_column.get_element(0));

        ASSERT_TRUE(reader.close().ok());
    }

    EXPECT_EQ(ids, std::vector<int32_t>({3, 4}));
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, CreateScanRequestDeduplicatesSharedPredicateColumns) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<TableColumn> projected_columns = {
            make_table_column(0, "a", int_type),
            make_table_column(1, "b", int_type),
            make_table_column(2, "c", int_type),
            make_table_column(3, "value", std::make_shared<DataTypeString>()),
    };
    const std::vector<SchemaField> file_schema = {
            {.id = 0, .name = "a", .type = int_type, .children = {}, .column_type = DATA_COLUMN},
            {.id = 1, .name = "b", .type = int_type, .children = {}, .column_type = DATA_COLUMN},
            {.id = 2, .name = "c", .type = int_type, .children = {}, .column_type = DATA_COLUMN},
            {.id = 3,
             .name = "value",
             .type = std::make_shared<DataTypeString>(),
             .children = {},
             .column_type = DATA_COLUMN},
    };

    TableColumnMapper mapper;
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    std::vector<TableFilter> table_filters;
    table_filters.push_back({
            .conjunct = VExprContext::create_shared(
                    std::make_shared<TableInt32SumGreaterThanExpr>(0, 0, 1, 1, 1)),
            .column_unique_ids = {projected_columns[0], projected_columns[1]},
    });
    table_filters.push_back({
            .conjunct = VExprContext::create_shared(
                    std::make_shared<TableInt32SumLessThanExpr>(0, 0, 2, 2, 3)),
            .column_unique_ids = {projected_columns[0], projected_columns[2]},
    });

    FileScanRequest file_request;
    ASSERT_TRUE(
            mapper.create_scan_request(table_filters, {}, projected_columns, &file_request).ok());

    // Both filters reference column a. It must still be read once as a predicate column, and a
    // predicate column must not be repeated as a non-predicate column.
    EXPECT_EQ(projection_ids(file_request.predicate_columns), std::vector<ColumnId>({0, 1, 2}));
    EXPECT_EQ(projection_ids(file_request.non_predicate_columns), std::vector<ColumnId>({3}));
    ASSERT_EQ(file_request.column_positions.size(), 4);
    EXPECT_EQ(file_request.column_positions.at(3), 0);
    EXPECT_EQ(file_request.column_positions.at(0), 1);
    EXPECT_EQ(file_request.column_positions.at(1), 2);
    EXPECT_EQ(file_request.column_positions.at(2), 3);
    const auto predicate_column_ids = projection_ids(file_request.predicate_columns);
    const auto non_predicate_column_ids = projection_ids(file_request.non_predicate_columns);
    for (const auto predicate_column_id : predicate_column_ids) {
        EXPECT_TRUE(std::find(non_predicate_column_ids.begin(), non_predicate_column_ids.end(),
                              predicate_column_id) == non_predicate_column_ids.end());
    }
}

TEST(TableReaderTest, CreateScanRequestPromotesProjectedColumnToPredicateColumn) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<TableColumn> projected_columns = {
            make_table_column(0, "id", int_type),
            make_table_column(1, "score", int_type),
    };
    const std::vector<SchemaField> file_schema = {
            {.id = 0, .name = "id", .type = int_type, .children = {}, .column_type = DATA_COLUMN},
            {.id = 1,
             .name = "score",
             .type = int_type,
             .children = {},
             .column_type = DATA_COLUMN},
    };

    TableColumnMapper mapper;
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    TableFilter table_filter {
            .conjunct = VExprContext::create_shared(
                    std::make_shared<TableInt32GreaterThanExpr>(0, 0, 1)),
            .column_unique_ids = {projected_columns[0]},
    };

    FileScanRequest file_request;
    ASSERT_TRUE(
            mapper.create_scan_request({table_filter}, {}, projected_columns, &file_request).ok());

    EXPECT_EQ(projection_ids(file_request.predicate_columns), std::vector<ColumnId>({0}));
    EXPECT_EQ(projection_ids(file_request.non_predicate_columns), std::vector<ColumnId>({1}));
    ASSERT_EQ(file_request.column_positions.size(), 2);
    EXPECT_EQ(file_request.column_positions.at(0), 1);
    EXPECT_EQ(file_request.column_positions.at(1), 0);
}

TEST(TableReaderTest, CreateScanRequestUsesColumnNameForByNamePredicateMapping) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<TableColumn> projected_columns = {
            make_table_column(10, "id", int_type),
            make_table_column(11, "score", int_type),
    };
    const std::vector<SchemaField> file_schema = {
            {.id = 0, .name = "ID", .type = int_type, .children = {}, .column_type = DATA_COLUMN},
            {.id = 1,
             .name = "score",
             .type = int_type,
             .children = {},
             .column_type = DATA_COLUMN},
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    TableFilter table_filter {
            .conjunct = VExprContext::create_shared(
                    std::make_shared<TableInt32GreaterThanExpr>(10, 10, 1)),
            .column_unique_ids = {projected_columns[0]},
    };

    FileScanRequest file_request;
    ASSERT_TRUE(
            mapper.create_scan_request({table_filter}, {}, projected_columns, &file_request).ok());

    EXPECT_EQ(projection_ids(file_request.predicate_columns), std::vector<ColumnId>({0}));
    EXPECT_EQ(projection_ids(file_request.non_predicate_columns), std::vector<ColumnId>({1}));
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto* localized_slot = assert_cast<const TableSlotRef*>(
            file_request.conjuncts[0]->root()->children()[0].get());
    EXPECT_EQ(localized_slot->slot_id(), 10);
    EXPECT_EQ(localized_slot->column_id(), 1);
}

TEST(TableReaderTest, ColumnPredicateFilterUsesColumnNameForByNameMapping) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<TableColumn> projected_columns = {
            make_table_column(10, "id", int_type),
            make_table_column(11, "score", int_type),
    };
    const std::vector<SchemaField> file_schema = {
            {.id = 0, .name = "ID", .type = int_type, .children = {}, .column_type = DATA_COLUMN},
            {.id = 1,
             .name = "score",
             .type = int_type,
             .children = {},
             .column_type = DATA_COLUMN},
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, projected_columns[0],
                         create_comparison_predicate<PredicateType::GT>(
                                 10, "id", int_type, Field::create_field<TYPE_INT>(2), false));

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({}, column_predicates, projected_columns, &file_request)
                        .ok());

    ASSERT_EQ(file_request.column_predicate_filters.size(), 1);
    EXPECT_EQ(file_request.column_predicate_filters[0].file_column_id, 0);
    EXPECT_EQ(projection_ids(file_request.non_predicate_columns), std::vector<ColumnId>({0, 1}));
    EXPECT_TRUE(file_request.predicate_columns.empty());
}

TEST(TableReaderTest, OpenReaderPushesMultiColumnConjunctToParquetReader) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_multi_conjunct_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {1, 5, 8}, {"one", "two", "three"});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(2, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(1, "score", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state, std::make_shared<TableInt32SumGreaterThanExpr>(
                                                            0, 0, 1, 1, 8))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // The conjunct references both id and score, so ColumnMapper must put both file columns into
    // predicate_columns and rewrite both slot refs to ParquetReader's file-local block positions.
    // ParquetReader then evaluates the expression after all predicate columns have been read.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(0).column);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    const auto& score_column = assert_cast<const ColumnInt32&>(*block.get_by_position(2).column);
    ASSERT_EQ(id_column.size(), 1);
    ASSERT_EQ(score_column.size(), 1);
    ASSERT_EQ(value_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(score_column.get_element(0), 8);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "three");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedColumnsFillDefaultForParquetSchemaMismatch) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_schema_mismatch_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(
            make_table_column(99, "missing_value", std::make_shared<DataTypeString>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // The table projection asks for field id 99, but the ParquetReader exposes only file-local
    // fields 0 and 1. Missing columns are allowed by the current mapper options, so TableReader
    // should still use the Parquet row count and fill a default column in table schema.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_EQ(block.get_by_position(0).column->size(), 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedColumnsRejectParquetSchemaMismatchWhenMissingColumnsDisallowed) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_schema_mismatch_reject_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(
            make_table_column(99, "missing_value", std::make_shared<DataTypeString>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = false,
                                    .profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // With allow_missing_columns disabled, the same missing projected column should fail while
    // opening the split instead of being materialized as a default column.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    const auto status = reader.get_block(&block, &eos);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("does not have a matching file column"), std::string::npos);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedStructFillsMissingChildWithDefault) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_struct_missing_child_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_struct_parquet_file(file_path, 7);

    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    auto id_child = make_table_column(0, "id", int_type);
    auto missing_child = make_table_column(99, "missing_child", string_type);
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type},
                                                        Strings {"id", "missing_child"});
    auto struct_column = make_table_column(100, "s", struct_type);
    struct_column.children = {id_child, missing_child};
    const std::vector<TableColumn> projected_columns = {struct_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& struct_result = assert_cast<const ColumnStruct&>(*block.get_by_position(0).column);
    ASSERT_EQ(struct_result.get_columns().size(), 2);
    const auto& ids = assert_cast<const ColumnInt32&>(struct_result.get_column(0));
    const auto& missing_values = assert_cast<const ColumnString&>(struct_result.get_column(1));
    ASSERT_EQ(struct_result.size(), 1);
    EXPECT_EQ(ids.get_element(0), 7);
    EXPECT_EQ(missing_values.get_data_at(0).to_string(), "");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedPartitionColumnUsesSplitPartitionValue) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_partition_value_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<TableColumn> projected_columns;
    auto partition_column = make_table_column(1, "value", std::make_shared<DataTypeString>());
    partition_column.is_partition_key = true;
    projected_columns.push_back(std::move(partition_column));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.partition_values.emplace("value", Field::create_field<TYPE_STRING>("p1"));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    // The file has a physical column with the same id/name. The split partition value should still
    // take precedence and be materialized by TableReader.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& partition_value =
            assert_cast<const ColumnString&>(*block.get_by_position(0).column);
    ASSERT_EQ(partition_value.size(), 1);
    EXPECT_EQ(partition_value.get_data_at(0).to_string(), "p1");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergVirtualColumnsUseRowLineageMetadata) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_virtual_columns_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(101, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state,
                                            std::make_shared<TableInt32GreaterThanExpr>(0, 0, 1))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    set_iceberg_row_lineage_params(&split_options, 1000, 77);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(2).column);

    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(id_column.get_element(1), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {1001, 1002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {77, 77});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergVirtualColumnsKeepRowLineageAfterConjunctFiltering) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_virtual_columns_conjunct_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(101, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state,
                                            std::make_shared<TableInt32GreaterThanExpr>(0, 0, 1))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    set_iceberg_row_lineage_params(&split_options, 3000, 88);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(2).column);

    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(id_column.get_element(1), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {3001, 3002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {88, 88});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergVirtualColumnsKeepRowLineageAfterRowGroupPredicatePruning) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_virtual_columns_row_group_predicate_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    // ColumnPredicate is used for row-group/statistics pruning. Keep one row per row group so
    // id > 2 prunes the first two row groups and leaves only the third file-local row.
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"}, 1);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(101, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, projected_columns[2],
                         create_comparison_predicate<PredicateType::GT>(
                                 0, "id", std::make_shared<DataTypeInt32>(),
                                 Field::create_field<TYPE_INT>(2), false));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = std::move(column_predicates),
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    set_iceberg_row_lineage_params(&split_options, 4000, 99);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(2).column);

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {4002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {99});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergDeletionVectorUsesTableReaderDeleteFileInterface) {
    TTableFormatFileDesc table_format_desc;
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_format_version(2);
    iceberg_desc.__set_delete_files({make_iceberg_deletion_vector("dv.bin", 8, 128)});
    table_format_desc.__set_iceberg_params(iceberg_desc);

    IcebergTableReaderDeleteFileTestHelper reader;
    DeleteFileDesc desc;
    bool has_delete_file = false;
    ASSERT_TRUE(reader.parse_deletion_vector_file(table_format_desc, &desc, &has_delete_file).ok());

    EXPECT_TRUE(has_delete_file);
    EXPECT_EQ(desc.path, "dv.bin");
    EXPECT_EQ(desc.start_offset, 8);
    EXPECT_EQ(desc.size, 128);
    EXPECT_EQ(desc.file_size, -1);
    EXPECT_EQ(desc.format, DeleteFileDesc::Format::ICEBERG);
}

TEST(TableReaderTest, IcebergDeletionVectorRejectsMultipleDeleteFiles) {
    TTableFormatFileDesc table_format_desc;
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_format_version(2);
    iceberg_desc.__set_delete_files({make_iceberg_deletion_vector("dv-a.bin", 8, 128),
                                     make_iceberg_deletion_vector("dv-b.bin", 16, 256)});
    table_format_desc.__set_iceberg_params(iceberg_desc);

    IcebergTableReaderDeleteFileTestHelper reader;
    DeleteFileDesc desc;
    bool has_delete_file = false;
    auto status = reader.parse_deletion_vector_file(table_format_desc, &desc, &has_delete_file);

    EXPECT_FALSE(status.ok());
}

TEST(TableReaderTest, IcebergTableReaderAppliesDeletionVectorFile) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_deletion_vector_file_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {0, 4});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_deletion_vector(dv_path, 0, dv_size)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({2, 3, 4}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergTableReaderDoesNotPushDownAggregateWithDeletes) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_aggregate_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {0});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_deletion_vector(dv_path, 0, dv_size)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergTableReaderDoesNotPushDownAggregateWithPositionDelete) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_aggregate_position_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergPositionDeleteFallsBackToSplitPath) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_position_delete_path_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    TTableFormatFileDesc table_format_params;
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_format_version(2);
    iceberg_params.__set_delete_files({make_iceberg_position_delete_file(delete_file_path)});
    table_format_params.__set_iceberg_params(iceberg_params);
    split_options.current_range.__set_table_format_params(table_format_params);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergTableReaderDoesNotPushDownAggregateWithEqualityDelete) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_aggregate_equality_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 2);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergEqualityDeleteCastsDataColumnToDeleteKeyType) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_equality_delete_cast_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_iceberg_equality_delete_bigint_parquet_file(delete_file_path, 0, 2);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergPositionDeleteOnlyMatchesOriginalDataFilePath) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_position_delete_path_match_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto other_file_path = (test_dir / "other.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {other_file_path, file_path}, {0, 1});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergRowLineageRemainsFileLocalAfterDeleteFiltering) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_row_lineage_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    TTableFormatFileDesc table_format_params = make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)});
    table_format_params.iceberg_params.__set_first_row_id(1000);
    split_options.current_range.__set_table_format_params(table_format_params);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {1000, 1002});
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergTableReaderAppliesPositionDeleteFile) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_position_delete_file_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    write_position_delete_parquet_file(delete_file_path, {file_path, file_path}, {1, 3});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3, 5}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergTableReaderMergesDeletionVectorAndPositionDeleteFiles) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_delete_files_merge_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    const auto position_delete_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {0});
    write_position_delete_parquet_file(position_delete_path, {file_path, file_path}, {3, 3});

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .allow_missing_columns = true,
                                    .profile = make_table_read_profile(&profile),
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_deletion_vector(dv_path, 0, dv_size),
                        make_iceberg_position_delete_file(position_delete_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({2, 3, 5}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, RowPositionDeletePredicateColumnIsNotRepeatedAsOutputColumn) {
    const auto row_position_column_id =
            doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID;
    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    IcebergTableReaderScanRequestTestHelper reader;
    ASSERT_TRUE(reader.init_for_scan_request_test(projected_columns).ok());

    FileScanRequest request;
    request.non_predicate_columns.push_back(field_projection(0));
    request.column_positions.emplace(0, 0);

    ASSERT_TRUE(reader.customize_request(&request).ok());

    EXPECT_EQ(projection_ids(request.predicate_columns),
              std::vector<ColumnId>({row_position_column_id}));
    EXPECT_EQ(projection_ids(request.non_predicate_columns), std::vector<ColumnId>({0}));
    ASSERT_TRUE(request.column_positions.contains(row_position_column_id));
    EXPECT_EQ(request.column_positions.at(row_position_column_id), 1);
    ASSERT_TRUE(request.conjuncts.empty());
    ASSERT_EQ(request.delete_conjuncts.size(), 1);
    EXPECT_NE(request.delete_conjuncts[0], nullptr);
}

TEST(TableReaderTest, ParquetReaderReadsOnlyRowGroupsInFileRange) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_file_range_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30},
                                {"range_group_one", "range_group_two", "range_group_three"}, 1);

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(2, "value", std::make_shared<DataTypeString>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options_for_row_group_mid(file_path, 1)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(1).column);
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "range_group_two");

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.rows(), 0);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedColumnsUseMapperExpressionForSameNameDifferentIdParquetSchema) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_same_name_diff_id_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(99, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // The table column has the same name as the Parquet field, but a different field id.
    // ColumnMapper should still resolve it by name and build a SlotRef projection from the file
    // column into the requested table column.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    ASSERT_EQ(id_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedColumnsUseMapperExpressionsForParquetSchemaMismatch) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_mapper_expr_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 7, "seven");

    std::vector<TableColumn> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt64>()));
    projected_columns.push_back(make_table_column(1, "value", std::make_shared<DataTypeString>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .allow_missing_columns = true,
                                    .profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // The table projection requests id as BIGINT instead of the file INT, so ColumnMapper should
    // build a Cast expression. The second field has the same type and should build a SlotRef
    // projection. Both columns should still materialize in table schema order.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    ASSERT_EQ(block.get_by_position(0).name, "id");
    ASSERT_EQ(block.get_by_position(1).name, "value");
    const auto& id_column = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(1).column);
    ASSERT_EQ(id_column.size(), 1);
    ASSERT_EQ(value_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 7);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "seven");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// ---------------------------------------------------------------------------
// BY_INDEX (Hive1 / hive_*_use_column_names=false) column mapping tests.
// These cases exercise `TableColumnMapper::create_mapping` directly to verify top-level
// file-position matching semantics, where `TableColumn::id` is interpreted as the 0-based file
// column position in this mode.
// They do not depend on any real file reads.
// ---------------------------------------------------------------------------

namespace {

// In BY_INDEX mode, `TableColumn::id` directly represents the position of the column in
// `file_schema`. This helper packages `file_index + display name` into one TableColumn.
TableColumn make_index_table_column(int32_t file_index, const std::string& name,
                                    const DataTypePtr& type) {
    return make_table_column(file_index, name, type);
}

} // namespace

TEST(TableColumnMapperByIndexTest, MapsTopLevelColumnsByPositionIgnoringFileNames) {
    // Simulate Hive1 ORC: all file schema names are placeholder values such as `_col0` / `_col1`
    // / `_col2`, so table columns must match by position instead of name. The table projects all
    // three file columns in order.
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto str_type = std::make_shared<DataTypeString>();
    const std::vector<TableColumn> projected_columns = {
            make_index_table_column(0, "user_id", int_type),
            make_index_table_column(1, "user_name", str_type),
            make_index_table_column(2, "age", int_type),
    };
    const std::vector<SchemaField> file_schema = {
            make_schema_field(0, "_col0", int_type),
            make_schema_field(1, "_col1", str_type),
            make_schema_field(2, "_col2", int_type),
    };

    TableColumnMapperOptions options;
    options.mode = TableColumnMappingMode::BY_INDEX;
    TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    const auto& mappings = mapper.mappings();
    ASSERT_EQ(mappings.size(), 3);

    ASSERT_TRUE(mappings[0].field_id.has_value());
    EXPECT_EQ(*mappings[0].field_id, 0);
    EXPECT_EQ(mappings[0].file_column_name, "_col0");
    EXPECT_FALSE(mappings[0].is_constant);

    ASSERT_TRUE(mappings[1].field_id.has_value());
    EXPECT_EQ(*mappings[1].field_id, 1);
    EXPECT_EQ(mappings[1].file_column_name, "_col1");

    ASSERT_TRUE(mappings[2].field_id.has_value());
    EXPECT_EQ(*mappings[2].field_id, 2);
    EXPECT_EQ(mappings[2].file_column_name, "_col2");
}

TEST(TableColumnMapperByIndexTest, SparseProjectionMapsByExplicitFileIndex) {
    // Only project the 2nd and 4th table columns (mapped to `_col2` and `_col4` in the file).
    // BY_INDEX must support sparse projection: file position is determined only by
    // `table_column.id`, independent of the relative order inside `projected_columns`.
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<TableColumn> projected_columns = {
            make_index_table_column(2, "age", int_type),
            make_index_table_column(4, "score", int_type),
    };
    const std::vector<SchemaField> file_schema = {
            make_schema_field(0, "_col0", int_type), make_schema_field(1, "_col1", int_type),
            make_schema_field(2, "_col2", int_type), make_schema_field(3, "_col3", int_type),
            make_schema_field(4, "_col4", int_type),
    };

    TableColumnMapperOptions options;
    options.mode = TableColumnMappingMode::BY_INDEX;
    TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    const auto& mappings = mapper.mappings();
    ASSERT_EQ(mappings.size(), 2);
    ASSERT_TRUE(mappings[0].field_id.has_value());
    EXPECT_EQ(*mappings[0].field_id, 2);
    EXPECT_EQ(mappings[0].file_column_name, "_col2");

    ASSERT_TRUE(mappings[1].field_id.has_value());
    EXPECT_EQ(*mappings[1].field_id, 4);
    EXPECT_EQ(mappings[1].file_column_name, "_col4");
}

TEST(TableColumnMapperByIndexTest, PartitionColumnsTakeConstantAndDoNotConsumeFileIndex) {
    // In BY_INDEX mode, partition columns should take the constant branch using
    // `partition_values` and stay completely independent from file schema. Data columns still
    // index into file positions through `table_column.id`.
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto str_type = std::make_shared<DataTypeString>();

    auto pt_col = make_table_column(/*id=*/-1, "dt", str_type);
    pt_col.is_partition_key = true;
    const std::vector<TableColumn> projected_columns = {
            pt_col, // partition column first
            make_index_table_column(0, "user_id", int_type),
            make_index_table_column(1, "score", int_type),
    };
    const std::vector<SchemaField> file_schema = {
            make_schema_field(0, "_col0", int_type),
            make_schema_field(1, "_col1", int_type),
    };

    std::map<std::string, Field> partition_values;
    partition_values.emplace("dt", Field::create_field<TYPE_STRING>("2025-06-01"));

    TableColumnMapperOptions options;
    options.mode = TableColumnMappingMode::BY_INDEX;
    TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping(projected_columns, partition_values, file_schema).ok());

    const auto& mappings = mapper.mappings();
    ASSERT_EQ(mappings.size(), 3);

    EXPECT_TRUE(mappings[0].is_constant);
    EXPECT_FALSE(mappings[0].field_id.has_value());
    EXPECT_NE(mappings[0].default_expr, nullptr);

    ASSERT_TRUE(mappings[1].field_id.has_value());
    EXPECT_EQ(*mappings[1].field_id, 0);
    EXPECT_EQ(mappings[1].file_column_name, "_col0");

    ASSERT_TRUE(mappings[2].field_id.has_value());
    EXPECT_EQ(*mappings[2].field_id, 1);
    EXPECT_EQ(mappings[2].file_column_name, "_col1");
}

TEST(TableColumnMapperByIndexTest, FileIndexOutOfRangeFallsBackToDefaultOrMissing) {
    // The declared file_index is outside file schema bounds. With `default_expr`, the mapping
    // takes the constant branch. Without a default and with `allow_missing_columns=true`, it
    // falls back to the missing-column branch without error and without a file mapping.
    const auto int_type = std::make_shared<DataTypeInt32>();

    auto with_default = make_index_table_column(5, "extra_default", int_type);
    auto literal_expr = VExprContext::create_shared(
            TableLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(42)));
    with_default.default_expr = literal_expr;

    const std::vector<TableColumn> projected_columns = {
            make_index_table_column(0, "a", int_type),
            with_default, // out-of-range file_index + default
            make_index_table_column(99, "extra_missing", int_type), // out-of-range without default
    };
    const std::vector<SchemaField> file_schema = {
            make_schema_field(0, "_col0", int_type),
            make_schema_field(1, "_col1", int_type),
    };

    TableColumnMapperOptions options;
    options.mode = TableColumnMappingMode::BY_INDEX;
    options.allow_missing_columns = true;
    TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    const auto& mappings = mapper.mappings();
    ASSERT_EQ(mappings.size(), 3);

    ASSERT_TRUE(mappings[0].field_id.has_value());
    EXPECT_EQ(*mappings[0].field_id, 0);

    EXPECT_FALSE(mappings[1].field_id.has_value());
    EXPECT_TRUE(mappings[1].is_constant);
    EXPECT_EQ(mappings[1].default_expr, literal_expr);

    EXPECT_FALSE(mappings[2].field_id.has_value());
    EXPECT_FALSE(mappings[2].is_constant);
    EXPECT_EQ(mappings[2].default_expr, nullptr);
}

TEST(TableColumnMapperByIndexTest, FileIndexOutOfRangeRejectedWhenAllowMissingFalse) {
    // When allow_missing_columns=false, an out-of-range file_index without a default must fail.
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<TableColumn> projected_columns = {
            make_index_table_column(0, "a", int_type),
            make_index_table_column(5, "b", int_type), // out of range and no default
    };
    const std::vector<SchemaField> file_schema = {
            make_schema_field(0, "_col0", int_type),
    };

    TableColumnMapperOptions options;
    options.mode = TableColumnMappingMode::BY_INDEX;
    options.allow_missing_columns = false;
    TableColumnMapper mapper(options);
    const auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    EXPECT_FALSE(status.ok());
}

TEST(TableColumnMapperByIndexTest, ExtraFileColumnsAreSimplyIgnored) {
    // The file may contain more columns than the table projects. Any file column that is not
    // referenced by a table column should simply be ignored without affecting the mapping.
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<TableColumn> projected_columns = {
            make_index_table_column(0, "a", int_type),
    };
    const std::vector<SchemaField> file_schema = {
            make_schema_field(0, "_col0", int_type),
            make_schema_field(1, "_col1", int_type),
            make_schema_field(2, "_col2", int_type),
    };

    TableColumnMapperOptions options;
    options.mode = TableColumnMappingMode::BY_INDEX;
    TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    const auto& mappings = mapper.mappings();
    ASSERT_EQ(mappings.size(), 1);
    ASSERT_TRUE(mappings[0].field_id.has_value());
    EXPECT_EQ(*mappings[0].field_id, 0);
}

TEST(TableColumnMapperByIndexTest, IgnoresFileColumnNames) {
    // BY_INDEX ignores file column names completely. Even if a file column name appears to match a
    // table column name, the mapping must still follow the position specified by
    // `table_column.id`.
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<TableColumn> projected_columns = {
            // The table wants column "a", but file_index=1 means it should map to file column 1
            // (named "b"), not file column 0 that happens to be named "a".
            make_index_table_column(1, "a", int_type),
    };
    const std::vector<SchemaField> file_schema = {
            make_schema_field(10, "a", int_type),
            make_schema_field(20, "b", int_type),
    };

    TableColumnMapperOptions options;
    options.mode = TableColumnMappingMode::BY_INDEX;
    TableColumnMapper mapper(options);
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    const auto& mappings = mapper.mappings();
    ASSERT_EQ(mappings.size(), 1);
    ASSERT_TRUE(mappings[0].field_id.has_value());
    EXPECT_EQ(*mappings[0].field_id, 20);
    EXPECT_EQ(mappings[0].file_column_name, "b");
}

} // namespace
} // namespace doris::reader
