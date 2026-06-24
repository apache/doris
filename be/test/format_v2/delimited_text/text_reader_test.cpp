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

#include "format_v2/delimited_text/text_reader.h"

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
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::format::text {
namespace {

TFileScanRangeParams text_scan_params() {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_TEXT);
    params.__set_file_type(TFileType::FILE_LOCAL);
    TFileAttributes attributes;
    TFileTextScanRangeParams text_params;
    text_params.__set_column_separator(",");
    text_params.__set_line_delimiter("\n");
    text_params.__set_escape('\\');
    attributes.__set_text_params(std::move(text_params));
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

std::unique_ptr<TextReader> create_reader(const std::string& path, TFileScanRangeParams* params,
                                          const std::vector<SlotDescriptor*>& slots,
                                          MockRuntimeState* state, RuntimeProfile* profile,
                                          int64_t range_start_offset = 0, int64_t range_size = -1) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto desc = file_description(path, range_start_offset, range_size);
    auto reader =
            std::make_unique<TextReader>(system_properties, desc, nullptr, profile, params, slots);
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

int32_t nullable_int_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    return nested.get_data()[row];
}

bool is_null_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    return nullable.is_null_at(row);
}

class TextV2ReaderTest : public testing::Test {
public:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_format_v2_text_reader_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "reader.text").string();
        std::ofstream output(_file_path, std::ios::binary);
        output << "1,alice,10\n";
        output << "2,bob,20\n";
        output.close();
        _slots = build_slots(&_pool);
        _params = text_scan_params();
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

protected:
    ObjectPool _pool;
    MockRuntimeState _state;
    RuntimeProfile _profile {"text_v2_reader_test"};
    std::filesystem::path _test_dir;
    std::string _file_path;
    std::vector<SlotDescriptor*> _slots;
    TFileScanRangeParams _params;
};

// Scenario: Text v2 exposes FE-provided file slots as nullable file-local schema using column_idxs
// as Hive text field ordinals.
TEST_F(TextV2ReaderTest, SchemaUsesSlotTypesAndColumnIdxs) {
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

// Scenario: Hive text escapes a field separator inside a string. The splitter keeps the escaped
// separator in the same field, and hive-text serde unescapes the final string value.
TEST_F(TextV2ReaderTest, EscapedSeparatorStaysInsideStringField) {
    const auto escaped_path = (_test_dir / "escaped.text").string();
    std::ofstream output(escaped_path, std::ios::binary);
    output << "1,alice\\,team,10\n";
    output.close();

    auto reader = create_reader(escaped_path, &_params, _slots, &_state, &_profile);
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
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice,team");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 0), 10);
}

// Scenario: Hive text supports multi-character field separators. V2 must not split on partial
// matches and must still honor FileScanRequest output positions.
TEST_F(TextV2ReaderTest, MultiCharacterSeparatorReadsRequestedColumns) {
    const auto multi_path = (_test_dir / "multi.text").string();
    std::ofstream output(multi_path, std::ios::binary);
    output << "3||carol||30\n";
    output.close();

    _params.file_attributes.text_params.__set_column_separator("||");
    auto reader = create_reader(multi_path, &_params, _slots, &_state, &_profile);
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
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "carol");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 0), 3);
}

// Scenario: column_idxs can map table slots to non-identity Hive text field ordinals.
TEST_F(TextV2ReaderTest, ColumnIdxsMapSlotsToTextOrdinals) {
    const auto remap_path = (_test_dir / "remapped.text").string();
    std::ofstream output(remap_path, std::ios::binary);
    output << "doris,40,4\n";
    output.close();

    _params.__set_column_idxs({2, 0, 1});
    auto reader = create_reader(remap_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    EXPECT_EQ(schema[0].local_id, 2);
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
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 4);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, 0), "doris");
}

// Scenario: missing Hive text fields are materialized as NULL rather than shifting later columns.
TEST_F(TextV2ReaderTest, MissingRequestedFieldUsesNullFormat) {
    const auto missing_path = (_test_dir / "missing.text").string();
    std::ofstream output(missing_path, std::ios::binary);
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

// Scenario: Text v2 can scan a request with no materialized columns. This is used by table-level
// COUNT-style paths where the reader must still return the number of logical rows read.
TEST_F(TextV2ReaderTest, EmptyFileLocalProjectionStillReportsRows) {
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

// Scenario: explicit text null_format is honored by Hive-text serde. Unlike CSV
// empty_field_as_null, an empty text field is not NULL unless it equals null_format exactly.
TEST_F(TextV2ReaderTest, NullFormatProducesNullableValue) {
    const auto null_path = (_test_dir / "null_format.text").string();
    std::ofstream output(null_path, std::ios::binary);
    output << "1,NULL,10\n";
    output << "2,,20\n";
    output.close();

    _params.file_attributes.text_params.__set_null_format("NULL");
    auto reader = create_reader(null_path, &_params, _slots, &_state, &_profile);
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
    ASSERT_EQ(rows, 2);
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 0));
    EXPECT_FALSE(is_null_at(*block.get_by_position(0).column, 1));
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 1), "");
}

// Scenario: TEXT_WITH_NAMES_AND_TYPES-style headers share the delimited text base skip path with
// CSV. Both header records must be skipped before the first data row is read.
TEST_F(TextV2ReaderTest, HeaderNamesAndTypesSkipsTwoLines) {
    const auto header_path = (_test_dir / "header_names_types.text").string();
    std::ofstream output(header_path, std::ios::binary);
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

// Scenario: the shared delimited text base removes UTF-8 BOM from the first returned data line.
// This matters for headerless text files whose first column is numeric.
TEST_F(TextV2ReaderTest, BomIsRemovedFromFirstDataLineWithoutHeader) {
    const auto bom_path = (_test_dir / "bom_data.text").string();
    std::ofstream output(bom_path, std::ios::binary);
    output.write("\xEF\xBB\xBF", 3);
    output << "5,bom,50\n";
    output.close();

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

// Scenario: when FE does not set header_type, skip_lines should be honored by the shared
// delimited text base before TextReader starts splitting rows.
TEST_F(TextV2ReaderTest, SkipLinesUsedWhenHeaderTypeUnset) {
    const auto skip_path = (_test_dir / "skip_lines.text").string();
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
// empty text line into one all-null logical row through the shared delimited text base.
TEST_F(TextV2ReaderTest, EmptyLineAsNullWhenQueryOptionEnabled) {
    const auto empty_line_path = (_test_dir / "empty_line.text").string();
    std::ofstream output(empty_line_path, std::ios::binary);
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

// Scenario: Text v2 COUNT pushdown scans rows because text files do not expose row-count metadata.
TEST_F(TextV2ReaderTest, CountAggregateScansRows) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::COUNT;
    FileAggregateResult aggregate_result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &aggregate_result).ok());
    EXPECT_EQ(aggregate_result.count, 2);
}

// Scenario: a non-first split starts inside a text record and must skip the partial first line.
TEST_F(TextV2ReaderTest, NonFirstSplitSkipsPartialFirstRecord) {
    const auto split_path = (_test_dir / "split.text").string();
    std::ofstream output(split_path, std::ios::binary);
    output << "1,skip,10\n";
    output << "2,bob,20\n";
    output.close();

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

// Scenario: compressed text cannot be split at arbitrary byte offsets because the decompressor
// needs the stream from the beginning. V2 should reject such a split before constructing the line
// reader.
TEST_F(TextV2ReaderTest, NonFirstCompressedSplitReturnsError) {
    _params.__set_compress_type(TFileCompressType::GZ);
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile,
                                /*range_start_offset=*/1);

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    EXPECT_FALSE(reader->open(request).ok());
}

// Scenario: FileScanRequest is a TableReader-to-FileReader contract. Unknown TEXT ordinals,
// out-of-range block positions, and sparse block-position maps must fail during reader open.
TEST_F(TextV2ReaderTest, InvalidScanRequestReturnsError) {
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

// Scenario: unsupported aggregate requests must fail explicitly instead of returning partial
// results from the scan path.
TEST_F(TextV2ReaderTest, UnsupportedAggregateReturnsNotSupported) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::MINMAX;
    FileAggregateResult aggregate_result;
    EXPECT_FALSE(reader->get_aggregate_result(aggregate_request, &aggregate_result).ok());
}

} // namespace
} // namespace doris::format::text
