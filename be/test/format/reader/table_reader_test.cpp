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
#include <parquet/arrow/writer.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vexpr.h"
#include "format/reader/expr/slot_ref.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace doris::reader {
namespace {

class TableInt32GreaterThanExpr final : public VExpr {
public:
    TableInt32GreaterThanExpr(int slot_id, int column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(value) {
        add_child(TableSlotRef::create_shared(slot_id, column_id, -1,
                                              std::make_shared<DataTypeInt32>(), "id"));
        set_node_type(TExprNodeType::BINARY_PRED);
        _opcode = TExprOpcode::GT;
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto* slot_ref = assert_cast<const VSlotRef*>(get_child(0).get());
        const auto& input =
                assert_cast<const ColumnInt32&>(
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

void write_parquet_file(const std::string& file_path, int32_t id, const std::string& value) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table =
            arrow::Table::Make(schema, {build_int32_array({id}), build_string_array({value})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(
            *table, arrow::default_memory_pool(), out, 1, builder.build()));
}

Block build_table_block(const std::vector<TableColumn>& columns) {
    Block block;
    for (const auto& column : columns) {
        block.insert({column.type->create_column(), column.type, column.name});
    }
    return block;
}

SplitReadOptions build_split_options(const std::string& file_path) {
    SplitReadOptions options;
    options.current_range.__set_path(file_path);
    options.current_range.__set_file_size(
            static_cast<int64_t>(std::filesystem::file_size(file_path)));
    return options;
}

TableColumn make_table_column(ColumnId id, const std::string& name, const DataTypePtr& type) {
    TableColumn column;
    column.id = id;
    column.name = name;
    column.type = type;
    return column;
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
    ASSERT_TRUE(reader
                        .init({
                                .projected_columns = projected_columns,
                                .conjuncts = VExprContext(
                                        std::make_shared<TableInt32GreaterThanExpr>(0, 0, 0)),
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
    ASSERT_TRUE(reader
                        .init({
                                .projected_columns = projected_columns,
                                .conjuncts = VExprContext(std::make_shared<TableInt32GreaterThanExpr>(
                                        0, 0, 2)),
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
    ASSERT_TRUE(filtered_reader
                        .init({
                                .projected_columns = projected_columns,
                                .conjuncts = VExprContext(std::make_shared<TableInt32GreaterThanExpr>(
                                        0, 0, 4)),
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
    ASSERT_TRUE(reader
                        .init({
                                .projected_columns = projected_columns,
                                .conjuncts = VExprContext(nullptr),
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
    ASSERT_TRUE(reader
                        .init({
                                .projected_columns = projected_columns,
                                .conjuncts = VExprContext(nullptr),
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
    ASSERT_TRUE(reader
                        .init({
                                .projected_columns = projected_columns,
                                .conjuncts = VExprContext(nullptr),
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
    projected_columns.push_back(
            make_table_column(0, "table_id", std::make_shared<DataTypeInt64>()));
    projected_columns.push_back(
            make_table_column(1, "table_value", std::make_shared<DataTypeString>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader
                        .init({
                                .projected_columns = projected_columns,
                                .conjuncts = VExprContext(nullptr),
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

    // The table projection is intentionally different from the Parquet schema:
    // field id 0 is requested as BIGINT instead of the file INT, so ColumnMapper should build a
    // Cast expression; field id 1 has a different table name but the same type, so it should build
    // a SlotRef projection. Both columns should still materialize in table schema order.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    ASSERT_EQ(block.get_by_position(0).name, "table_id");
    ASSERT_EQ(block.get_by_position(1).name, "table_value");
    const auto& id_column = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(1).column);
    ASSERT_EQ(id_column.size(), 1);
    ASSERT_EQ(value_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 7);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "seven");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

} // namespace
} // namespace doris::reader
