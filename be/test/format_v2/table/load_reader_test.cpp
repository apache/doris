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

#include "format_v2/table/load_reader.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris::format {
namespace {

TTypeDesc scalar_type(const DataTypePtr& type) {
    TScalarType scalar;
    scalar.__set_type(doris::to_thrift(type->get_primitive_type()));
    TTypeNode node;
    node.__set_type(TTypeNodeType::SCALAR);
    node.__set_scalar_type(scalar);
    TTypeDesc result;
    result.__set_types({node});
    return result;
}

class SourceReader final : public TableReader {
public:
    Status init(TableReadOptions&& options) override {
        columns = options.projected_columns;
        EXPECT_TRUE(options.conjuncts.empty());
        EXPECT_EQ(options.push_down_agg_type, TPushAggOp::NONE);
        return Status::OK();
    }
    Status prepare_split(const SplitReadOptions& options) override {
        EXPECT_TRUE(!options.conjuncts || options.conjuncts->empty());
        calls = 0;
        return Status::OK();
    }
    Status get_block(Block* block, bool* eos) override {
        block->clear_column_data();
        *eos = calls++ != 0;
        if (*eos) {
            return Status::OK();
        }
        auto column = IColumn::mutate(block->get_by_position(0).column);
        auto& nullable = assert_cast<ColumnNullable&>(*column);
        if (all_null) {
            nullable.insert_default();
        } else {
            nullable.get_nested_column().insert_data("value", 5);
            nullable.get_null_map_data().push_back(0);
        }
        nullable.insert_default();
        block->replace_by_position(0, std::move(column));
        for (size_t i = 1; i < block->columns(); ++i) {
            auto other = block->get_by_position(i).type->create_column();
            other->insert_many_defaults(2);
            block->replace_by_position(i, std::move(other));
        }
        _missing_values.clear();
        if (missing_first) {
            _missing_values.emplace(0, std::vector<uint8_t> {1, 0});
        }
        return Status::OK();
    }
    Status close() override { return Status::OK(); }
    std::vector<ColumnDefinition> columns;
    int calls = 0;
    bool all_null = false;
    bool missing_first = false;
};

class LoadReaderTest : public testing::Test {
protected:
    void SetUp() override {
        TTupleDescriptor tuple;
        tuple.__set_id(0);
        tuple.__set_byteSize(0);
        tuple.__set_numNullBytes(1);
        source = std::make_unique<TupleDescriptor>(tuple);
        tuple.__set_id(1);
        destination = std::make_unique<TupleDescriptor>(tuple);
        TSlotDescriptor slot;
        slot.__set_id(10);
        slot.__set_parent(0);
        slot.__set_slotType(scalar_type(make_nullable(std::make_shared<DataTypeString>())));
        slot.__set_columnPos(0);
        slot.__set_slotIdx(0);
        slot.__set_byteOffset(0);
        slot.__set_nullIndicatorByte(0);
        slot.__set_nullIndicatorBit(0);
        slot.__set_isMaterialized(true);
        slot.__set_colName("raw_value");
        source_definition = slot;
        source_slot = std::make_unique<SlotDescriptor>(slot);
        source->add_slot(source_slot.get());
        slot.__set_id(20);
        slot.__set_parent(1);
        slot.__set_colName("value");
        destination_slot = std::make_unique<SlotDescriptor>(slot);
        destination->add_slot(destination_slot.get());
        params.__set_src_tuple_id(0);
        params.__set_dest_tuple_id(1);
        params.__set_num_of_columns_from_file(1);
        TFileScanSlotInfo required;
        required.__set_slot_id(10);
        required.__set_is_file_slot(true);
        params.__set_required_slots({required});
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_type(scalar_type(source_slot->type()));
        node.__set_num_children(0);
        node.__set_is_nullable(true);
        TSlotRef ref;
        ref.__set_slot_id(10);
        ref.__set_tuple_id(0);
        node.__set_slot_ref(ref);
        TExpr expression;
        expression.__set_nodes({node});
        params.__set_expr_of_dest_slot({{20, expression}});
        params.__set_dest_sid_to_src_sid_without_trans({{20, 10}});
    }
    void set_destination_type(DataTypePtr type) {
        TTupleDescriptor tuple;
        tuple.__set_id(1);
        tuple.__set_byteSize(0);
        tuple.__set_numNullBytes(1);
        destination = std::make_unique<TupleDescriptor>(tuple);
        TSlotDescriptor slot;
        slot.__set_id(20);
        slot.__set_parent(1);
        slot.__set_slotType(scalar_type(type));
        slot.__set_columnPos(0);
        slot.__set_slotIdx(0);
        slot.__set_byteOffset(0);
        slot.__set_nullIndicatorByte(0);
        slot.__set_nullIndicatorBit(type->is_nullable() ? 0 : -1);
        slot.__set_isMaterialized(true);
        slot.__set_colName("value");
        destination_slot = std::make_unique<SlotDescriptor>(slot);
        destination->add_slot(destination_slot.get());
    }
    void cast_destination_to_int() {
        set_destination_type(make_nullable(std::make_shared<DataTypeInt32>()));
        TExprNode cast;
        cast.__set_node_type(TExprNodeType::CAST_EXPR);
        cast.__set_type(scalar_type(destination_slot->get_data_type_ptr()));
        cast.__set_is_nullable(true);
        cast.__set_num_children(1);
        params.expr_of_dest_slot[20].nodes.insert(params.expr_of_dest_slot[20].nodes.begin(), cast);
    }
    Status init(LoadTableReader& reader, FileFormat format = FileFormat::CSV) {
        TTupleDescriptor tuple;
        tuple.__set_id(0);
        tuple.__set_byteSize(0);
        tuple.__set_numNullBytes(1);
        TDescriptorTable descriptors;
        descriptors.__set_tupleDescriptors({tuple});
        descriptors.__set_slotDescriptors({source_definition});
        if (source_bitmap) {
            descriptors.slotDescriptors.push_back(bitmap_definition);
        }
        if (anchor_slot) {
            descriptors.slotDescriptors.push_back(anchor_definition);
        }
        DescriptorTbl* table = nullptr;
        RETURN_IF_ERROR(DescriptorTbl::create(&pool, descriptors, &table));
        state.set_desc_tbl(table);
        return reader.init({.projected_columns = {},
                            .conjuncts = {},
                            .format = format,
                            .scan_params = &params,
                            .io_ctx = io,
                            .runtime_state = &state,
                            .scanner_profile = &profile});
    }
    void add_partial_update_columns(bool key) {
        TTupleDescriptor tuple;
        tuple.__set_id(0);
        tuple.__set_byteSize(0);
        tuple.__set_numNullBytes(1);
        source = std::make_unique<TupleDescriptor>(tuple);
        source_definition.__set_col_unique_id(7);
        source_definition.__set_is_key(key);
        source_slot = std::make_unique<SlotDescriptor>(source_definition);
        source->add_slot(source_slot.get());
        TSlotDescriptor bitmap = source_definition;
        bitmap.__set_id(11);
        bitmap.__set_slotIdx(1);
        bitmap.__set_columnPos(1);
        bitmap.__set_is_key(false);
        bitmap.__set_col_unique_id(9);
        bitmap.__set_colName("__DORIS_SKIP_BITMAP_COL__");
        bitmap.__set_slotType(scalar_type(make_nullable(std::make_shared<DataTypeBitMap>())));
        bitmap_definition = bitmap;
        source_bitmap = std::make_unique<SlotDescriptor>(bitmap);
        source->add_slot(source_bitmap.get());
        TFileScanSlotInfo required;
        required.__set_slot_id(11);
        required.__set_is_file_slot(true);
        params.required_slots.push_back(required);
        params.__set_num_of_columns_from_file(2);
        params.__set_sequence_map_col("raw_value");
        bitmap.__set_id(21);
        bitmap.__set_parent(1);
        destination_bitmap = std::make_unique<SlotDescriptor>(bitmap);
        destination->add_slot(destination_bitmap.get());
        TExpr bitmap_expr = params.expr_of_dest_slot[20];
        bitmap_expr.nodes[0].__set_type(bitmap.slotType);
        bitmap_expr.nodes[0].slot_ref.__set_slot_id(11);
        params.expr_of_dest_slot.emplace(21, bitmap_expr);
        TSlotDescriptor sequence = source_definition;
        sequence.__set_id(22);
        sequence.__set_parent(1);
        sequence.__set_slotIdx(2);
        sequence.__set_columnPos(2);
        sequence.__set_is_key(false);
        sequence.__set_col_unique_id(8);
        sequence.__set_colName("__DORIS_SEQUENCE_COL__");
        destination_sequence = std::make_unique<SlotDescriptor>(sequence);
        destination->add_slot(destination_sequence.get());
        params.expr_of_dest_slot.emplace(22, params.expr_of_dest_slot[20]);
    }
    void TearDown() override {
        if (!input_path.empty()) {
            std::filesystem::remove(input_path);
        }
    }
    std::filesystem::path input_path;
    ObjectPool pool;
    TSlotDescriptor source_definition, bitmap_definition, anchor_definition;
    std::unique_ptr<SlotDescriptor> anchor_slot;
    std::unique_ptr<SlotDescriptor> source_bitmap, destination_bitmap, destination_sequence;
    RuntimeState state;
    RuntimeProfile profile {"load"};
    std::shared_ptr<io::IOContext> io = std::make_shared<io::IOContext>();
    TFileScanRangeParams params;
    std::unique_ptr<TupleDescriptor> source, destination;
    std::unique_ptr<SlotDescriptor> source_slot, destination_slot;
};

TEST_F(LoadReaderTest, SourceSlotsAreConvertedBeforeDestinationFiltering) {
    auto child = std::make_unique<SourceReader>();
    auto* input = child.get();
    LoadTableReader reader(std::move(child), source.get(), destination.get());
    auto status = init(reader);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(input->columns.size(), 1);
    EXPECT_EQ(input->columns[0].name, "raw_value");
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "value");
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(output.rows(), 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, StrictCastRejectsInvalidNonNullSourceOnly) {
    cast_destination_to_int();
    params.__set_strict_mode(true);
    LoadTableReader reader(std::make_unique<SourceReader>(), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(eos);
    ASSERT_EQ(output.rows(), 1);
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(0));
    EXPECT_EQ(io->load_filtered_rows, 1);
    EXPECT_EQ(io->load_unselected_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, NonStrictCastPreservesNullableRows) {
    cast_destination_to_int();
    params.__set_strict_mode(false);
    LoadTableReader reader(std::make_unique<SourceReader>(), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(0));
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    EXPECT_EQ(io->load_filtered_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, PreFilterRunsBeforeStrictConversion) {
    cast_destination_to_int();
    params.__set_strict_mode(true);
    TExprNode predicate;
    predicate.__set_node_type(TExprNodeType::BOOL_LITERAL);
    predicate.__set_type(scalar_type(std::make_shared<DataTypeUInt8>()));
    predicate.__set_num_children(0);
    predicate.__set_is_nullable(false);
    TBoolLiteral literal;
    literal.__set_value(false);
    predicate.__set_bool_literal(literal);
    TExpr expression;
    expression.__set_nodes({predicate});
    params.__set_pre_filter_exprs_list({expression});
    LoadTableReader reader(std::make_unique<SourceReader>(), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.rows(), 0);
    EXPECT_EQ(io->load_filtered_rows, 0);
    EXPECT_EQ(io->load_unselected_rows, 2);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, MissingDefaultDoesNotReplaceExplicitNull) {
    TExprNode literal;
    literal.__set_node_type(TExprNodeType::STRING_LITERAL);
    literal.__set_type(scalar_type(source_slot->get_data_type_ptr()));
    literal.__set_num_children(0);
    literal.__set_is_nullable(true);
    TStringLiteral value;
    value.__set_value("fallback");
    literal.__set_string_literal(value);
    TExpr expression;
    expression.__set_nodes({literal});
    params.__set_default_value_of_src_slot({{10, expression}});
    auto child = std::make_unique<SourceReader>();
    child->missing_first = true;
    LoadTableReader reader(std::move(child), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "fallback");
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, NonNullableDestinationFiltersNulls) {
    set_destination_type(std::make_shared<DataTypeString>());
    LoadTableReader reader(std::make_unique<SourceReader>(), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 1);
    EXPECT_FALSE(output.get_by_position(0).column->is_nullable());
    EXPECT_EQ(io->load_filtered_rows, 1);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, FullyRejectedBatchIsNotEofAndNextSplitStillReads) {
    set_destination_type(std::make_shared<DataTypeString>());
    auto child = std::make_unique<SourceReader>();
    auto* input = child.get();
    input->all_null = true;
    LoadTableReader reader(std::move(child), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.rows(), 0);
    EXPECT_EQ(io->load_filtered_rows, 2);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    input->all_null = false;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.rows(), 1);
    EXPECT_EQ(io->load_filtered_rows, 3);
    ASSERT_TRUE(reader.close().ok());
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, FlexibleUpdatePreservesOmittedValueAndSequenceMarks) {
    add_partial_update_columns(false);
    auto child = std::make_unique<SourceReader>();
    child->missing_first = true;
    LoadTableReader reader(std::move(child), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 2);
    const auto& nullable = assert_cast<const ColumnNullable&>(*output.get_by_position(1).column);
    const auto& bitmaps = assert_cast<const ColumnBitmap&>(nullable.get_nested_column()).get_data();
    EXPECT_TRUE(bitmaps[0].contains(7));
    EXPECT_TRUE(bitmaps[0].contains(8));
    EXPECT_FALSE(bitmaps[1].contains(7));
    EXPECT_FALSE(bitmaps[1].contains(8));
    EXPECT_EQ(io->load_filtered_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, FlexibleUpdateRejectsMissingKeyBeforeExpressions) {
    add_partial_update_columns(true);
    auto child = std::make_unique<SourceReader>();
    child->missing_first = true;
    LoadTableReader reader(std::move(child), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(output.rows(), 1);
    EXPECT_EQ(io->load_filtered_rows, 1);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, NativeReadersPreserveValidRowsAroundMalformedRecords) {
    cast_destination_to_int();
    params.__set_strict_mode(true);
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.__set_compress_type(TFileCompressType::PLAIN);
    TFileTextScanRangeParams text;
    text.__set_column_separator(",");
    text.__set_line_delimiter("\n");
    TFileAttributes attributes;
    attributes.__set_text_params(text);
    attributes.__set_read_json_by_line(true);
    params.__set_file_attributes(attributes);
    for (const bool json : {false, true}) {
        SCOPED_TRACE(json ? "JSON" : "CSV");
        params.__set_format_type(json ? TFileFormatType::FORMAT_JSON
                                      : TFileFormatType::FORMAT_CSV_PLAIN);
        input_path = std::filesystem::temp_directory_path() /
                     ("v2_load_reader_" + std::to_string(getpid()));
        {
            std::ofstream file(input_path);
            file << (json ? "{\"raw_value\":1}\n{\"raw_value\":\"bad\"}\nnot-json\n"
                            "{\"raw_value\":null}\n{\"raw_value\":3}\n"
                          : "1\nbad\n2,extra\n\\N\n3\n");
        }
        io->load_filtered_rows = 0;
        const auto format = json ? FileFormat::JSON : FileFormat::CSV;
        LoadTableReader reader(std::make_unique<TableReader>(), source.get(), destination.get());
        auto status = init(reader, format);
        ASSERT_TRUE(status.ok()) << status;
        TFileRangeDesc range;
        range.__set_path(input_path.string());
        range.__set_start_offset(0);
        range.__set_size(std::filesystem::file_size(input_path));
        range.__set_file_size(range.size);
        range.__set_file_type(TFileType::FILE_LOCAL);
        range.__set_format_type(params.format_type);
        SplitReadOptions split;
        split.current_range = range;
        split.current_split_format = format;
        status = reader.prepare_split(split);
        ASSERT_TRUE(status.ok()) << status;
        Block output(destination->slots(), 0);
        bool eos = false;
        status = reader.get_block(&output, &eos);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_FALSE(eos);
        ASSERT_EQ(output.rows(), 3);
        EXPECT_EQ(assert_cast<const ColumnInt32&>(
                          assert_cast<const ColumnNullable&>(*output.get_by_position(0).column)
                                  .get_nested_column())
                          .get_element(0),
                  1);
        EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
        EXPECT_EQ(assert_cast<const ColumnInt32&>(
                          assert_cast<const ColumnNullable&>(*output.get_by_position(0).column)
                                  .get_nested_column())
                          .get_element(2),
                  3);
        EXPECT_EQ(io->load_filtered_rows, 2);
        ASSERT_TRUE(reader.get_block(&output, &eos).ok());
        EXPECT_TRUE(eos);
        ASSERT_TRUE(reader.close().ok());
    }
}

TEST_F(LoadReaderTest, NativeJsonMissingFieldUsesDefaultButExplicitNullDoesNot) {
    anchor_definition = source_definition;
    anchor_definition.__set_id(12);
    anchor_definition.__set_slotIdx(1);
    anchor_definition.__set_columnPos(1);
    anchor_definition.__set_colName("anchor");
    anchor_slot = std::make_unique<SlotDescriptor>(anchor_definition);
    source->add_slot(anchor_slot.get());
    TFileScanSlotInfo required;
    required.__set_slot_id(12);
    required.__set_is_file_slot(true);
    params.required_slots.push_back(required);
    params.__set_num_of_columns_from_file(2);
    params.__set_format_type(TFileFormatType::FORMAT_JSON);
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.__set_compress_type(TFileCompressType::PLAIN);
    TFileTextScanRangeParams text;
    text.__set_line_delimiter("\n");
    TFileAttributes attributes;
    attributes.__set_text_params(text);
    attributes.__set_read_json_by_line(true);
    params.__set_file_attributes(attributes);
    TExprNode literal;
    literal.__set_node_type(TExprNodeType::STRING_LITERAL);
    literal.__set_type(scalar_type(source_slot->get_data_type_ptr()));
    literal.__set_num_children(0);
    literal.__set_is_nullable(true);
    TStringLiteral value;
    value.__set_value("fallback");
    literal.__set_string_literal(value);
    TExpr expression;
    expression.__set_nodes({literal});
    params.__set_default_value_of_src_slot({{10, expression}});
    input_path = std::filesystem::temp_directory_path() /
                 ("v2_load_defaults_" + std::to_string(getpid()));
    {
        std::ofstream file(input_path);
        file << "{\"anchor\":\"a\"}\n{\"anchor\":\"b\",\"raw_value\":null}\n";
    }
    LoadTableReader reader(std::make_unique<TableReader>(), source.get(), destination.get());
    auto status = init(reader, FileFormat::JSON);
    ASSERT_TRUE(status.ok()) << status;
    SplitReadOptions split;
    split.current_split_format = FileFormat::JSON;
    split.current_range.__set_path(input_path.string());
    split.current_range.__set_start_offset(0);
    split.current_range.__set_size(std::filesystem::file_size(input_path));
    split.current_range.__set_file_size(split.current_range.size);
    split.current_range.__set_file_type(TFileType::FILE_LOCAL);
    split.current_range.__set_format_type(params.format_type);
    status = reader.prepare_split(split);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "fallback");
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    EXPECT_EQ(io->load_filtered_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

} // namespace
} // namespace doris::format
