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
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format_v2/column_mapper.h"
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

std::vector<SlotDescriptor*> build_nested_complex_slots(ObjectPool* pool) {
    const auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    const auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_int, nullable_string}, Strings {"a", "b"}));
    const auto array_type = make_nullable(std::make_shared<DataTypeArray>(struct_type));
    const auto map_type =
            make_nullable(std::make_shared<DataTypeMap>(nullable_string, struct_type));
    return {make_test_slot(pool, 0, 0, make_nullable(std::make_shared<DataTypeInt32>()), "id"),
            make_test_slot(pool, 1, 1, array_type, "xs"),
            make_test_slot(pool, 2, 2, map_type, "kv")};
}

std::vector<SlotDescriptor*> build_char_varchar_slots(ObjectPool* pool) {
    const auto nullable_char3 =
            make_nullable(std::make_shared<DataTypeString>(3, PrimitiveType::TYPE_CHAR));
    const auto nullable_varchar4 =
            make_nullable(std::make_shared<DataTypeString>(4, PrimitiveType::TYPE_VARCHAR));
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_char3, nullable_varchar4}, Strings {"city", "country"}));
    return {make_test_slot(pool, 0, 0, make_nullable(std::make_shared<DataTypeInt32>()), "id"),
            make_test_slot(pool, 1, 1, nullable_char3, "city"),
            make_test_slot(pool, 2, 2, struct_type, "region")};
}

std::unique_ptr<TextReader> create_reader(const std::string& path, TFileScanRangeParams* params,
                                          const std::vector<SlotDescriptor*>& slots,
                                          MockRuntimeState* state, RuntimeProfile* profile,
                                          int64_t range_start_offset = 0, int64_t range_size = -1,
                                          std::shared_ptr<io::IOContext> io_ctx = nullptr) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto desc = file_description(path, range_start_offset, range_size);
    auto reader = std::make_unique<TextReader>(system_properties, desc, std::move(io_ctx), profile,
                                               params, slots);
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

int32_t nullable_struct_int_child_at(const IColumn& column, size_t child_index, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& struct_column = assert_cast<const ColumnStruct&>(nullable.get_nested_column());
    const auto& child_nullable =
            assert_cast<const ColumnNullable&>(struct_column.get_column(child_index));
    const auto& nested = assert_cast<const ColumnInt32&>(child_nullable.get_nested_column());
    return nested.get_data()[row];
}

int64_t counter_value(RuntimeProfile* profile, const std::string& name) {
    auto* counter = profile->get_counter(name);
    EXPECT_NE(counter, nullptr) << name;
    return counter == nullptr ? 0 : counter->value();
}

class NullableIntGreaterThanExpr final : public VExpr {
public:
    NullableIntGreaterThanExpr(size_t block_position, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _block_position(block_position),
              _value(value) {}

    const std::string& expr_name() const override { return _name; }

    bool is_constant() const override { return false; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_block_position).column);
        const auto& data = assert_cast<const ColumnInt32&>(nullable.get_nested_column());

        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const auto source_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    !nullable.is_null_at(source_row) && data.get_element(source_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<NullableIntGreaterThanExpr>(_block_position, _value);
        return Status::OK();
    }

private:
    size_t _block_position;
    int32_t _value;
    const std::string _name = "NullableIntGreaterThanExpr";
};

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

// Scenario: FE slot types for Hive text are table target types. CHAR/VARCHAR length is not stored
// in the text file, so the file schema must expose bounded strings as unbounded STRING. Otherwise
// TableReader believes the file value already satisfies the table length and skips truncation.
TEST_F(TextV2ReaderTest, SchemaTreatsCharVarcharSlotsAsUnboundedFileStrings) {
    auto slots = build_char_varchar_slots(&_pool);
    auto reader = create_reader(_file_path, &_params, slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);

    const auto city_type = remove_nullable(schema[1].type);
    EXPECT_EQ(city_type->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(assert_cast<const DataTypeString*>(city_type.get())->len(), -1);

    const auto region_type = remove_nullable(schema[2].type);
    ASSERT_EQ(region_type->get_primitive_type(), TYPE_STRUCT);
    const auto* region_struct = assert_cast<const DataTypeStruct*>(region_type.get());
    ASSERT_EQ(region_struct->get_elements().size(), 2);
    EXPECT_EQ(remove_nullable(region_struct->get_element(0))->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(remove_nullable(region_struct->get_element(1))->get_primitive_type(), TYPE_STRING);
    ASSERT_EQ(schema[2].children.size(), 2);
    EXPECT_EQ(remove_nullable(schema[2].children[0].type)->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(remove_nullable(schema[2].children[1].type)->get_primitive_type(), TYPE_STRING);
}

// Scenario: Hive text is row-oriented and cannot lazy-read predicate columns separately. The
// reader declares that capability by choosing MaterializedColumnMapper itself.
TEST_F(TextV2ReaderTest, CreatesMaterializedColumnMapper) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto mapper = reader->create_column_mapper({.mode = TableColumnMappingMode::BY_NAME});

    ASSERT_NE(dynamic_cast<MaterializedColumnMapper*>(mapper.get()), nullptr);
}

// Scenario: Text v2 exposes delimited-text profile counters for read, parse, deserialize, and
// file-local conjunct filtering, so scanner profiles can explain where row-reader time is spent.
TEST_F(TextV2ReaderTest, ProfileCountersTrackReadParseDeserializeAndFilter) {
    const auto profile_path = (_test_dir / "profile.text").string();
    std::ofstream output(profile_path, std::ios::binary);
    output << "\n";
    output << "1,alice,10\n";
    output << "2,bob,20\n";
    output.close();

    _state._query_options.__set_read_csv_empty_line_as_null(true);
    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_reader(profile_path, &_params, _slots, &_state, &_profile, 0, -1, io_ctx);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                      LocalColumnIndex::top_level(LocalColumnId(2))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(1));
    request->conjuncts = {
            prepared_conjunct(&_state, std::make_shared<NullableIntGreaterThanExpr>(1, 15))};
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0, 2});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 2);

    EXPECT_NE(_profile.get_counter("OpenFileTime"), nullptr);
    EXPECT_NE(_profile.get_counter("CreateLineReaderTime"), nullptr);
    EXPECT_NE(_profile.get_counter("ReadLineTime"), nullptr);
    EXPECT_NE(_profile.get_counter("SplitLineTime"), nullptr);
    EXPECT_NE(_profile.get_counter("DeserializeTime"), nullptr);
    EXPECT_NE(_profile.get_counter("ConjunctFilterTime"), nullptr);
    EXPECT_NE(_profile.get_counter("DeleteConjunctFilterTime"), nullptr);
    EXPECT_EQ(counter_value(&_profile, "RawLinesRead"), 3);
    EXPECT_EQ(counter_value(&_profile, "RowsReadBeforeFilter"), 3);
    EXPECT_EQ(counter_value(&_profile, "RowsFilteredByConjunct"), 2);
    EXPECT_EQ(io_ctx->predicate_filtered_rows, 2);
    EXPECT_EQ(counter_value(&_profile, "RowsFilteredByDeleteConjunct"), 0);
    EXPECT_EQ(counter_value(&_profile, "RowsReturned"), 1);
    EXPECT_EQ(counter_value(&_profile, "EmptyLinesRead"), 1);
    EXPECT_EQ(counter_value(&_profile, "SkippedLines"), 0);
    EXPECT_EQ(counter_value(&_profile, "CellsDeserialized"), 6);
}

// Scenario: Hive text has no embedded nested schema, but TableColumnMapper still needs semantic
// children for complex table columns. The reader synthesizes ARRAY/MAP/STRUCT children from the
// slot type while keeping the top-level local id as the text field ordinal from column_idxs.
TEST_F(TextV2ReaderTest, SchemaSynthesizesComplexChildrenForColumnMapper) {
    _params.__set_column_idxs({4, 7, 9});
    auto slots = build_nested_complex_slots(&_pool);
    auto reader = create_reader(_file_path, &_params, slots, &_state, &_profile);

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);

    EXPECT_EQ(schema[1].name, "xs");
    EXPECT_EQ(schema[1].local_id, 7);
    ASSERT_EQ(schema[1].children.size(), 1);
    EXPECT_EQ(schema[1].children[0].name, "element");
    EXPECT_EQ(schema[1].children[0].local_id, 0);
    ASSERT_EQ(schema[1].children[0].children.size(), 2);
    EXPECT_EQ(schema[1].children[0].children[0].name, "a");
    EXPECT_EQ(schema[1].children[0].children[0].local_id, 0);
    EXPECT_EQ(schema[1].children[0].children[1].name, "b");
    EXPECT_EQ(schema[1].children[0].children[1].local_id, 1);

    EXPECT_EQ(schema[2].name, "kv");
    EXPECT_EQ(schema[2].local_id, 9);
    ASSERT_EQ(schema[2].children.size(), 2);
    EXPECT_EQ(schema[2].children[0].name, "key");
    EXPECT_EQ(schema[2].children[0].local_id, 0);
    EXPECT_EQ(schema[2].children[1].name, "value");
    EXPECT_EQ(schema[2].children[1].local_id, 1);
    ASSERT_EQ(schema[2].children[1].children.size(), 2);
    EXPECT_EQ(schema[2].children[1].children[0].name, "a");
    EXPECT_EQ(schema[2].children[1].children[1].name, "b");
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

// Scenario: Hive text complex values are encoded inside one top-level text field. V2 reads the
// complete struct field first, then evaluates a file-local predicate on one child, covering
// `SELECT s.a WHERE s.b > 10` without pretending that Text has physical nested-column pruning.
TEST_F(TextV2ReaderTest, FullStructColumnSupportsChildConjunctFiltering) {
    const auto complex_path = (_test_dir / "complex.text").string();
    std::ofstream output(complex_path, std::ios::binary);
    output << "1|11,5|10\n";
    output << "2|22,20|20\n";
    output.close();

    _params.file_attributes.text_params.__set_column_separator("|");
    _params.file_attributes.text_params.__set_collection_delimiter(",");
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

// Scenario: stream load/http_stream text input is not backed by a filesystem. If TableReader fails
// to preserve the stream load id, the v2 reader should report that directly instead of calling the
// generic FileFactory path and returning "unsupported file reader type: 2".
TEST_F(TextV2ReaderTest, StreamInputRequiresLoadIdBeforeOpeningPipe) {
    _params.__set_file_type(TFileType::FILE_STREAM);
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    const auto status = reader->open(request);

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("stream reader requires load id"), std::string::npos)
            << status;
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

// Scenario: Hive SerDe can define the empty string itself as NULL. The nullable string fast path
// must match the generic nullable serde behavior instead of treating empty null_format as
// "null format is not configured".
TEST_F(TextV2ReaderTest, EmptyNullFormatProducesNullableValue) {
    const auto null_path = (_test_dir / "empty_null_format.text").string();
    std::ofstream output(null_path, std::ios::binary);
    output << "1,alice,10\n";
    output << "2,,20\n";
    output << "3,NULL,30\n";
    output.close();

    _params.file_attributes.text_params.__set_null_format("");
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
    ASSERT_EQ(rows, 3);
    EXPECT_FALSE(is_null_at(*block.get_by_position(0).column, 0));
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 1));
    EXPECT_FALSE(is_null_at(*block.get_by_position(0).column, 2));
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 2), "NULL");
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
