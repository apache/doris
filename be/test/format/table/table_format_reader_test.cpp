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

#include "format/table/table_format_reader.h"

#include <gtest/gtest.h>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "format/partition_column_reader.h"
#include "runtime/descriptors.h"

namespace doris {

class MockTableFormatReader : public TableFormatReader {
public:
    Status _do_get_next_block(Block*, size_t*, bool*) override {
        ++read_calls;
        return Status::OK();
    }

    Status close() override {
        ++close_calls;
        return Status::OK();
    }

    int read_calls = 0;
    int close_calls = 0;

    void set_fill_col_name_to_block_idx(std::unordered_map<std::string, uint32_t>* index) {
        _fill_col_name_to_block_idx = index;
    }

    void set_partition_value(const std::string& col_name, const std::string& value,
                             const SlotDescriptor* slot_desc, bool is_null = false) {
        _fill_partition_values[col_name] = {value, slot_desc};
        _fill_partition_value_is_null[col_name] = is_null;
    }
};

static TTypeDesc create_scalar_type(TPrimitiveType::type primitive_type) {
    TTypeDesc type_desc;
    TTypeNode type_node;
    TScalarType scalar_type;
    scalar_type.__set_type(primitive_type);
    type_node.__set_type(TTypeNodeType::SCALAR);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    return type_desc;
}

static TSlotDescriptor create_slot_descriptor(int slot_id, const std::string& col_name,
                                              TPrimitiveType::type primitive_type,
                                              bool nullable = false) {
    TSlotDescriptor slot_desc;
    slot_desc.__set_id(slot_id);
    slot_desc.__set_parent(1);
    slot_desc.__set_slotType(create_scalar_type(primitive_type));
    slot_desc.__set_columnPos(slot_id);
    slot_desc.__set_colName(col_name);
    slot_desc.__set_col_unique_id(slot_id);
    slot_desc.__set_slotIdx(slot_id);
    slot_desc.__set_isMaterialized(true);
    slot_desc.__set_is_key(false);
    slot_desc.__set_nullIndicatorBit(nullable ? 0 : -1);
    return slot_desc;
}

TEST(TableFormatReaderTest, FillSynthesizedColumnsInvokesRegisteredHandlers) {
    MockTableFormatReader reader;
    size_t handled_rows = 0;
    int handler_calls = 0;

    reader.register_synthesized_column_handler("synthetic",
                                               [&](Block* block, size_t rows) -> Status {
                                                   EXPECT_EQ(block, nullptr);
                                                   handled_rows = rows;
                                                   ++handler_calls;
                                                   return Status::OK();
                                               });

    EXPECT_TRUE(reader.has_synthesized_column_handlers());

    auto status = reader.fill_synthesized_columns(nullptr, 128);

    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(handler_calls, 1);
    EXPECT_EQ(handled_rows, 128u);
}

TEST(TableFormatReaderTest, FillPartitionColumnRestoresSharedColumnOnDeserializeError) {
    MockTableFormatReader reader;
    std::unordered_map<std::string, uint32_t> block_index {{"part_col", 0}};
    reader.set_fill_col_name_to_block_idx(&block_index);

    auto slot_desc = create_slot_descriptor(0, "part_col", TPrimitiveType::INT);
    SlotDescriptor slot(slot_desc);
    reader.set_partition_value("part_col", "not_an_int", &slot);

    auto type = std::make_shared<DataTypeInt32>();
    auto mutable_col = ColumnInt32::create();
    ColumnPtr old_col = mutable_col->get_ptr();

    Block block;
    block.insert({std::move(mutable_col), type, "part_col"});

    auto status = reader.on_fill_partition_columns(&block, 2, {"part_col"});

    EXPECT_FALSE(status.ok());
    ASSERT_EQ(block.columns(), 1);
    ASSERT_NE(block.get_by_position(0).column.get(), nullptr);
    EXPECT_EQ(block.get_by_position(0).column->size(), 0);
    EXPECT_EQ(old_col->size(), 0);
    EXPECT_NE(block.get_by_position(0).column.get(), old_col.get());
    EXPECT_NO_THROW(block.check_number_of_rows());
}

TEST(TableFormatReaderTest, FillMissingNullableColumnDetachesSharedBlockSlot) {
    MockTableFormatReader reader;
    std::unordered_map<std::string, uint32_t> block_index {{"missing_col", 0}};
    reader.set_fill_col_name_to_block_idx(&block_index);

    auto nullable_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto nullable_col = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
    ColumnPtr old_col = nullable_col->get_ptr();

    Block block;
    block.insert({std::move(nullable_col), nullable_type, "missing_col"});

    auto status = reader.on_fill_missing_columns(&block, 3, {"missing_col"});

    EXPECT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(block.columns(), 1);
    ASSERT_NE(block.get_by_position(0).column.get(), nullptr);
    EXPECT_EQ(block.rows(), 3);
    EXPECT_EQ(old_col->size(), 0);
    EXPECT_NE(block.get_by_position(0).column.get(), old_col.get());

    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& null_map = nullable.get_null_map_data();
    ASSERT_EQ(null_map.size(), 3);
    EXPECT_EQ(null_map[0], 1);
    EXPECT_EQ(null_map[1], 1);
    EXPECT_EQ(null_map[2], 1);
}

TEST(TableFormatReaderTest, PartitionValueSupportsColumnarHiveAndHudiRanges) {
    TTableFormatFileDesc table_format;
    table_format.__set_table_format_type("hive");
    TFileRangeDesc range;
    range.__set_table_format_params(table_format);
    range.__set_start_offset(0);
    range.__set_size(1024);
    range.__set_file_size(1024);
    EXPECT_TRUE(PartitionColumnReader::supports_range(range, TFileFormatType::FORMAT_PARQUET));
    EXPECT_TRUE(PartitionColumnReader::supports_range(range, TFileFormatType::FORMAT_ORC));
    // Only formats whose reader can be opened and whose metadata is readable.
    for (const auto format : {TFileFormatType::FORMAT_CSV_PLAIN, TFileFormatType::FORMAT_TEXT,
                              TFileFormatType::FORMAT_JSON, TFileFormatType::FORMAT_JNI}) {
        EXPECT_FALSE(PartitionColumnReader::supports_range(range, format));
    }
    // Formats that can hide physical rows behind deletes are excluded.
    for (const auto* table : {"transactional_hive", "iceberg", "paimon"}) {
        range.table_format_params.__set_table_format_type(table);
        EXPECT_FALSE(PartitionColumnReader::supports_range(range, TFileFormatType::FORMAT_ORC));
    }
    // Hudi COW carries its partition value in the partition path, exactly like Hive.
    range.table_format_params.__set_table_format_type("hudi");
    EXPECT_TRUE(PartitionColumnReader::supports_range(range, TFileFormatType::FORMAT_PARQUET));
    EXPECT_TRUE(PartitionColumnReader::supports_range(range, TFileFormatType::FORMAT_ORC));
    // A split is fine: the reader no longer needs a row count, so a partial range is no longer a
    // correctness problem -- one row per range is the contract.
    range.table_format_params.__set_table_format_type("hive");
    range.__set_start_offset(1);
    EXPECT_TRUE(PartitionColumnReader::supports_range(range, TFileFormatType::FORMAT_PARQUET));
    range.__set_start_offset(0);
    range.__set_size(512);
    EXPECT_TRUE(PartitionColumnReader::supports_range(range, TFileFormatType::FORMAT_PARQUET));
}

TEST(TableFormatReaderTest, PartitionValueEmitsOneRowPerRangeAndFillsTypedNulls) {
    auto value_slot_desc = create_slot_descriptor(0, "part", TPrimitiveType::INT);
    auto null_slot_desc = create_slot_descriptor(1, "null_part", TPrimitiveType::INT, true);
    SlotDescriptor value_slot(value_slot_desc);
    SlotDescriptor null_slot(null_slot_desc);
    std::unordered_map<std::string, uint32_t> block_index {{"part", 0}, {"null_part", 1}};

    // The footer row count is irrelevant: a range emits its partition values whether or not the
    // file turns out to hold rows. (A partition with no file at all emits nothing, simply because
    // it produces no scan range.) Keeping the zero-row case here pins that contract down: an
    // implementation that re-gated on "proven nonempty" would fail it.
    for (const int64_t footer_rows : {0, 1, 10000}) {
        auto inner = std::make_unique<MockTableFormatReader>();
        auto* inner_ptr = inner.get();
        inner->set_fill_col_name_to_block_idx(&block_index);
        inner->set_partition_value("part", "42", &value_slot);
        inner->set_partition_value("null_part", "", &null_slot, true);
        PartitionColumnReader reader(std::move(inner));
        EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::PARTITION_VALUE);

        Block block;
        block.insert(
                {value_slot.get_empty_mutable_column(), value_slot.get_data_type_ptr(), "part"});
        block.insert(
                {null_slot.get_empty_mutable_column(), null_slot.get_data_type_ptr(), "null_part"});
        size_t read_rows = 0;
        bool eof = false;
        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        EXPECT_EQ(read_rows, 1) << "footer_rows=" << footer_rows;
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(eof);
        ASSERT_TRUE(block.check_type_and_column().ok());
        EXPECT_EQ(block.get_by_position(0).column->get_int(0), 42);
        EXPECT_TRUE(block.get_by_position(1).column->is_null_at(0));

        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        EXPECT_EQ(read_rows, 0);
        EXPECT_EQ(block.rows(), 0);
        EXPECT_TRUE(eof);
        EXPECT_EQ(inner_ptr->read_calls, 0);
        ASSERT_TRUE(reader.close().ok());
        EXPECT_EQ(inner_ptr->close_calls, 1);
    }
}

} // namespace doris
