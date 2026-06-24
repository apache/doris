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

#include "format_v2/delimited_text/csv_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>

#include "common/consts.h"
#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::format::csv {
namespace {

TFileScanRangeParams csv_scan_params() {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    params.__set_file_type(TFileType::FILE_LOCAL);
    TFileAttributes attributes;
    TFileTextScanRangeParams text_params;
    text_params.__set_column_separator(",");
    text_params.__set_line_delimiter("\n");
    attributes.__set_text_params(std::move(text_params));
    attributes.__set_header_type(BeConsts::CSV_WITH_NAMES);
    params.__set_file_attributes(std::move(attributes));
    params.__set_column_idxs({0, 1, 2});
    return params;
}

std::unique_ptr<io::FileDescription> file_description(const std::string& path,
                                                      int64_t range_start_offset = 0,
                                                      int64_t range_size = -1) {
    auto desc = std::make_unique<io::FileDescription>();
    desc->path = path;
    desc->range_start_offset = range_start_offset;
    desc->range_size = range_size;
    desc->file_size = static_cast<int64_t>(std::filesystem::file_size(path));
    return desc;
}

std::vector<SlotDescriptor*> build_slots(ObjectPool* pool) {
    DescriptorTblBuilder builder(pool);
    builder.declare_tuple()
            << TupleDescBuilder::SlotType {make_nullable(std::make_shared<DataTypeInt32>()), "id"}
            << TupleDescBuilder::SlotType {make_nullable(std::make_shared<DataTypeString>()),
                                           "name"}
            << TupleDescBuilder::SlotType {make_nullable(std::make_shared<DataTypeInt32>()),
                                           "score"};
    auto* desc_tbl = builder.build();
    return desc_tbl->get_tuple_descriptor(0)->slots();
}

SlotDescriptor* make_test_slot(ObjectPool* pool, int slot_id, int slot_idx, DataTypePtr type,
                               const std::string& name) {
    TSlotDescriptor slot_desc;
    slot_desc.__set_id(slot_id);
    slot_desc.__set_parent(0);
    slot_desc.__set_slotType(type->to_thrift());
    slot_desc.__set_columnPos(slot_idx);
    slot_desc.__set_byteOffset(0);
    slot_desc.__set_nullIndicatorByte(slot_idx / 8);
    slot_desc.__set_nullIndicatorBit(slot_idx % 8);
    slot_desc.__set_slotIdx(slot_idx);
    slot_desc.__set_isMaterialized(true);
    slot_desc.__set_colName(name);
    return pool->add(new SlotDescriptor(slot_desc));
}

std::vector<SlotDescriptor*> build_struct_slots(ObjectPool* pool) {
    const auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_int, nullable_int}, Strings {"a", "b"}));
    return {make_test_slot(pool, 0, 0, make_nullable(std::make_shared<DataTypeInt32>()), "id"),
            make_test_slot(pool, 1, 1, struct_type, "s"),
            make_test_slot(pool, 2, 2, make_nullable(std::make_shared<DataTypeInt32>()), "score")};
}

std::unique_ptr<CsvReader> create_reader(const std::string& path, TFileScanRangeParams* params,
                                         const std::vector<SlotDescriptor*>& slots,
                                         MockRuntimeState* state, RuntimeProfile* profile,
                                         int64_t range_start_offset = 0, int64_t range_size = -1) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto desc = file_description(path, range_start_offset, range_size);
    auto reader =
            std::make_unique<CsvReader>(system_properties, desc, nullptr, profile, params, slots);
    EXPECT_TRUE(reader->init(state).ok());
    return reader;
}

Block make_block(const std::vector<ColumnDefinition>& schema,
                 const std::vector<int32_t>& local_ids) {
    Block block;
    for (const auto local_id : local_ids) {
        const auto it = std::find_if(schema.begin(), schema.end(), [&](const auto& column) {
            return column.local_id == local_id;
        });
        EXPECT_TRUE(it != schema.end());
        block.insert({it->type->create_column(), it->type, it->name});
    }
    return block;
}

std::string nullable_string_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnString&>(nullable.get_nested_column());
    return nested.get_data_at(row).to_string();
}

bool is_null_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    return nullable.is_null_at(row);
}

int32_t nullable_int_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    return nested.get_data()[row];
}

int32_t nullable_struct_int_child_at(const IColumn& column, size_t child_index, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& struct_column = assert_cast<const ColumnStruct&>(nullable.get_nested_column());
    const auto& child_nullable =
            assert_cast<const ColumnNullable&>(struct_column.get_column(child_index));
    const auto& nested = assert_cast<const ColumnInt32&>(child_nullable.get_nested_column());
    return nested.get_data()[row];
}

class StructIntChildGreaterThanExpr final : public VExpr {
public:
    StructIntChildGreaterThanExpr(size_t block_position, size_t child_index, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _block_position(block_position),
              _child_index(child_index),
              _value(value) {}

    const std::string& expr_name() const override { return _name; }

    bool is_constant() const override { return false; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_block_position).column);
        const auto& struct_column = assert_cast<const ColumnStruct&>(nullable.get_nested_column());
        const auto& child_nullable =
                assert_cast<const ColumnNullable&>(struct_column.get_column(_child_index));
        const auto& child_data =
                assert_cast<const ColumnInt32&>(child_nullable.get_nested_column());

        auto result = ColumnUInt8::create();
        auto& data = result->get_data();
        data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const auto source_row = selector == nullptr ? row : (*selector)[row];
            data[row] = !nullable.is_null_at(source_row) &&
                        !child_nullable.is_null_at(source_row) &&
                        child_data.get_element(source_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<StructIntChildGreaterThanExpr>(_block_position,
                                                                       _child_index, _value);
        return Status::OK();
    }

private:
    size_t _block_position;
    size_t _child_index;
    int32_t _value;
    const std::string _name = "StructIntChildGreaterThanExpr";
};

VExprContextSPtr prepared_conjunct(RuntimeState* state, const VExprSPtr& expr) {
    auto context = VExprContext::create_shared(expr);
    auto status = context->prepare(state, RowDescriptor());
    EXPECT_TRUE(status.ok()) << status;
    status = context->open(state);
    EXPECT_TRUE(status.ok()) << status;
    return context;
}

class CsvV2ReaderTest : public testing::Test {
public:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_format_v2_csv_reader_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "reader.csv").string();
        std::ofstream output(_file_path, std::ios::binary);
        output << "id,name,score\n";
        output << "1,alice,10\n";
        output << "2,bob,20\n";
        output.close();
        _slots = build_slots(&_pool);
        _params = csv_scan_params();
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

protected:
    ObjectPool _pool;
    MockRuntimeState _state;
    RuntimeProfile _profile {"csv_v2_reader_test"};
    std::filesystem::path _test_dir;
    std::string _file_path;
    std::vector<SlotDescriptor*> _slots;
    TFileScanRangeParams _params;
};

// Scenario: CSV v2 exposes FE-provided file slots as nullable file-local schema using column_idxs
// as CSV field ordinals.
TEST_F(CsvV2ReaderTest, SchemaUsesSlotTypesAndColumnIdxs) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    EXPECT_EQ(schema[0].name, "id");
    EXPECT_EQ(schema[0].local_id, 0);
    EXPECT_TRUE(schema[0].type->is_nullable());
    EXPECT_EQ(schema[1].name, "name");
    EXPECT_EQ(schema[1].local_id, 1);
    EXPECT_TRUE(schema[1].type->is_nullable());
}

// Scenario: CSV v2 honors FileScanRequest local positions, so TableReader can request a subset of
// CSV fields in an order different from the physical CSV field order.
TEST_F(CsvV2ReaderTest, ReadsRequestedColumnsInFileLocalBlockOrder) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1)),
                                      LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1, 0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice");
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 1), "bob");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 0), 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 1), 2);
}

// Scenario: FE column_idxs define the CSV field ordinal for each physical file slot. The mapping
// can be non-identity when FE reorders projected file slots, so the reader must use the local id
// from FileScanRequest instead of the slot vector position.
TEST_F(CsvV2ReaderTest, ColumnIdxsMapSlotsToCsvOrdinals) {
    const auto remap_path = (_test_dir / "remapped.csv").string();
    std::ofstream output(remap_path, std::ios::binary);
    output << "name,score,id\n";
    output << "alice,10,1\n";
    output.close();

    _params.__set_column_idxs({2, 0, 1});
    auto reader = create_reader(remap_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    EXPECT_EQ(schema[0].name, "id");
    EXPECT_EQ(schema[0].local_id, 2);
    EXPECT_EQ(schema[1].name, "name");
    EXPECT_EQ(schema[1].local_id, 0);

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(2)),
                                      LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {2, 0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, 0), "alice");
}

// Scenario: CSV stores one complex column as one text field, so v2 must read the whole struct
// field before evaluating a file-local predicate on one child. This covers `SELECT s.a WHERE
// s.b > 10` style scans after MaterializedColumnMapper has requested the full top-level `s`.
TEST_F(CsvV2ReaderTest, FullStructColumnSupportsChildConjunctFiltering) {
    const auto complex_path = (_test_dir / "complex.csv").string();
    std::ofstream output(complex_path, std::ios::binary);
    output << "id|s|score\n";
    output << "1|{\"a\": 11, \"b\": 5}|10\n";
    output << "2|{\"a\": 22, \"b\": 20}|20\n";
    output.close();

    _params.file_attributes.text_params.__set_column_separator("|");
    _params.__set_column_idxs({0, 1, 2});
    auto slots = build_struct_slots(&_pool);
    auto reader = create_reader(complex_path, &_params, slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    request->conjuncts = {prepared_conjunct(
            &_state, std::make_shared<StructIntChildGreaterThanExpr>(
                             /*block_position=*/0, /*child_index=*/1, /*value=*/10))};
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_struct_int_child_at(*block.get_by_position(0).column, 0, 0), 22);
    EXPECT_EQ(nullable_struct_int_child_at(*block.get_by_position(0).column, 1, 0), 20);
}

// Scenario: a table-level scan can need only partition/default columns, leaving the CSV
// FileScanRequest with no file-local columns. The reader must still report the number of rows read.
TEST_F(CsvV2ReaderTest, EmptyFileLocalProjectionStillReportsRows) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    Block block;
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_FALSE(eof);
}

// Scenario: CSV has no footer row count, so v2 COUNT pushdown scans the split and returns the
// counted row count through FileAggregateResult.
TEST_F(CsvV2ReaderTest, CountAggregateScansRows) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::COUNT;
    FileAggregateResult aggregate_result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &aggregate_result).ok());
    EXPECT_EQ(aggregate_result.count, 2);
}

// Scenario: CSV v2 parses enclosed fields itself instead of delegating to the old CsvReader. A
// separator inside an enclosed string must stay inside the same CSV field.
TEST_F(CsvV2ReaderTest, EnclosedFieldKeepsSeparatorInsideStringValue) {
    const auto quoted_path = (_test_dir / "quoted.csv").string();
    std::ofstream output(quoted_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,\"alice,team\",10\n";
    output.close();

    _params.file_attributes.text_params.__set_enclose('"');
    _params.file_attributes.text_params.__set_escape('\\');
    auto reader = create_reader(quoted_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice,team");
}

// Scenario: when the CSV row has fewer fields than the FE-provided file slot list, v2 fills the
// missing requested field with NULL instead of failing or shifting later columns.
TEST_F(CsvV2ReaderTest, MissingRequestedFieldUsesNullFormat) {
    const auto missing_path = (_test_dir / "missing.csv").string();
    std::ofstream output(missing_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,alice\n";
    output.close();

    auto reader = create_reader(missing_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(2))};
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {2});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 0));
}

// Scenario: the first line may contain UTF-8 BOM and CSV_WITH_NAMES_AND_TYPES has two header
// records. Both must be skipped before materializing the first data row.
TEST_F(CsvV2ReaderTest, HeaderNamesAndTypesSkipsTwoLinesAndBom) {
    const auto header_path = (_test_dir / "header_names_types.csv").string();
    std::ofstream output(header_path, std::ios::binary);
    output.write("\xEF\xBB\xBF", 3);
    output << "id,name,score\n";
    output << "INT,STRING,INT\n";
    output << "7,carol,70\n";
    output.close();

    _params.file_attributes.__set_header_type(BeConsts::CSV_WITH_NAMES_AND_TYPES);
    auto reader = create_reader(header_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 7);
}

// Scenario: when the first returned data line starts with UTF-8 BOM, CSV v2 strips the BOM before
// passing the cell to the serde. This matters for headerless files whose first column is numeric.
TEST_F(CsvV2ReaderTest, BomIsRemovedFromFirstDataLineWithoutHeader) {
    const auto bom_path = (_test_dir / "bom_data.csv").string();
    std::ofstream output(bom_path, std::ios::binary);
    output.write("\xEF\xBB\xBF", 3);
    output << "5,bom,50\n";
    output.close();

    _params.file_attributes.__isset.header_type = false;
    auto reader = create_reader(bom_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 5);
}

// Scenario: when FE does not set header_type, CSV v2 must honor skip_lines exactly as the old
// reader does.
TEST_F(CsvV2ReaderTest, SkipLinesUsedWhenHeaderTypeUnset) {
    const auto skip_path = (_test_dir / "skip_lines.csv").string();
    std::ofstream output(skip_path, std::ios::binary);
    output << "skip me\n";
    output << "skip me too\n";
    output << "3,dan,30\n";
    output.close();

    _params.file_attributes.__isset.header_type = false;
    _params.file_attributes.__set_skip_lines(2);
    auto reader = create_reader(skip_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 3);
}

// Scenario: empty physical lines are skipped by default, but read_csv_empty_line_as_null turns one
// empty line into one all-null logical row.
TEST_F(CsvV2ReaderTest, EmptyLineAsNullWhenQueryOptionEnabled) {
    const auto empty_line_path = (_test_dir / "empty_line.csv").string();
    std::ofstream output(empty_line_path, std::ios::binary);
    output << "id,name,score\n";
    output << "\n";
    output << "4,erin,40\n";
    output.close();

    _state._query_options.__set_read_csv_empty_line_as_null(true);
    auto reader = create_reader(empty_line_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 0));
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 1), 4);
}

// Scenario: FE-provided CSV text parameters define NULL semantics. Explicit null_format and
// empty_field_as_null should both produce nullable values without throwing serde errors.
TEST_F(CsvV2ReaderTest, NullFormatAndEmptyFieldAsNullProduceNullableValues) {
    const auto null_path = (_test_dir / "null_format.csv").string();
    std::ofstream output(null_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,NULL,\n";
    output.close();

    _params.file_attributes.text_params.__set_null_format("NULL");
    _params.file_attributes.text_params.__set_empty_field_as_null(true);
    auto reader = create_reader(null_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1)),
                                      LocalColumnIndex::top_level(LocalColumnId(2))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1, 2});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 0));
    EXPECT_TRUE(is_null_at(*block.get_by_position(1).column, 0));
}

// Scenario: a non-first split starts inside a record. CSV v2 pre-reads enough delimiter bytes and
// skips the partial first line so the split begins at the next complete row.
TEST_F(CsvV2ReaderTest, NonFirstSplitSkipsPartialFirstRecord) {
    const auto split_path = (_test_dir / "split.csv").string();
    std::ofstream output(split_path, std::ios::binary);
    output << "1,skip,10\n";
    output << "2,bob,20\n";
    output.close();

    _params.file_attributes.__isset.header_type = false;
    auto reader = create_reader(split_path, &_params, _slots, &_state, &_profile,
                                /*range_start_offset=*/3);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 2);
}

// Scenario: compressed CSV cannot be split at arbitrary byte offsets because the decompressor needs
// the stream from the beginning. V2 should reject such a split before constructing the line reader.
TEST_F(CsvV2ReaderTest, NonFirstCompressedSplitReturnsError) {
    _params.__set_format_type(TFileFormatType::FORMAT_CSV_GZ);
    _params.file_attributes.__isset.header_type = false;
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile,
                                /*range_start_offset=*/1);

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    EXPECT_FALSE(reader->open(request).ok());
}

// Scenario: FileScanRequest is a TableReader-to-FileReader contract. Unknown CSV ordinals,
// out-of-range block positions, and sparse block-position maps must fail during reader open.
TEST_F(CsvV2ReaderTest, InvalidScanRequestReturnsError) {
    {
        auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
        auto request = std::make_shared<FileScanRequest>();
        request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(99))};
        request->local_positions.emplace(LocalColumnId(99), LocalIndex(0));
        EXPECT_FALSE(reader->open(request).ok());
    }
    {
        auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
        auto request = std::make_shared<FileScanRequest>();
        request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
        request->local_positions.emplace(LocalColumnId(0), LocalIndex(2));
        EXPECT_FALSE(reader->open(request).ok());
    }
    {
        auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
        auto request = std::make_shared<FileScanRequest>();
        request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                          LocalColumnIndex::top_level(LocalColumnId(1))};
        request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
        request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
        EXPECT_FALSE(reader->open(request).ok());
    }
}

// Scenario: CSV v2 can count rows by scanning, but it cannot answer min/max or mixed aggregate
// requests from metadata.
TEST_F(CsvV2ReaderTest, UnsupportedAggregateReturnsNotSupported) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::MINMAX;
    FileAggregateResult aggregate_result;
    EXPECT_FALSE(reader->get_aggregate_result(aggregate_request, &aggregate_result).ok());
}

} // namespace
} // namespace doris::format::csv
