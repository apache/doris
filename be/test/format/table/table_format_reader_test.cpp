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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format/column_descriptor.h"
#include "format/generic_reader.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {

// ============================================================================
// MockTableFormatReader: A testable TableFormatReader subclass.
// ============================================================================
class MockTableFormatReader : public TableFormatReader {
public:
    // Expose protected members for testing.
    using TableFormatReader::_extract_partition_values;
    using TableFormatReader::_fill_col_name_to_block_idx;
    using TableFormatReader::_fill_missing_cols;
    using TableFormatReader::_fill_missing_defaults;
    using TableFormatReader::_fill_partition_values;

    // Fake file columns returned by get_columns().
    std::unordered_map<std::string, DataTypePtr> fake_file_columns;

    // Track calls.
    std::vector<std::string> call_log;

    // Configurable per-batch rows.
    int batches_to_emit = 1;
    size_t rows_per_batch = 10;

protected:
    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override {
        *name_to_type = fake_file_columns;
        return Status::OK();
    }

    Status _do_init_reader(ReaderInitContext* /*ctx*/) override {
        call_log.push_back("_do_init_reader");
        return Status::OK();
    }

    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override {
        call_log.push_back("_do_get_next_block");
        if (batches_to_emit <= 0) {
            *read_rows = 0;
            *eof = true;
            return Status::OK();
        }
        batches_to_emit--;
        *read_rows = rows_per_batch;
        *eof = (batches_to_emit == 0);

        // Simulate reading data: only fill columns that are NOT partition/missing.
        // This matches real reader behavior (Parquet/ORC only fill REGULAR file columns).
        auto mutate_columns = block->mutate_columns();
        for (size_t i = 0; i < mutate_columns.size(); i++) {
            const auto& col_name = block->get_by_position(i).name;
            // Skip columns that will be filled by on_after_read_block.
            if (_fill_partition_values.contains(col_name) ||
                _fill_missing_defaults.contains(col_name)) {
                continue;
            }
            if (mutate_columns[i]->size() == 0) {
                auto type = block->get_by_position(i).type;
                if (type->is_nullable()) {
                    auto* nullable = static_cast<ColumnNullable*>(mutate_columns[i].get());
                    for (size_t r = 0; r < *read_rows; r++) {
                        nullable->insert_data(nullptr, 0);
                    }
                } else {
                    mutate_columns[i]->insert_many_defaults(*read_rows);
                }
            }
        }
        block->set_columns(std::move(mutate_columns));
        return Status::OK();
    }
};

// ============================================================================
// Test fixture
// ============================================================================
class TableFormatReaderTest : public testing::Test {
protected:
    ObjectPool object_pool;

    // Build a TupleDescriptor via DescriptorTblBuilder.
    const TupleDescriptor* build_tuple_desc(
            const std::vector<std::pair<std::string, DataTypePtr>>& cols) {
        DescriptorTblBuilder builder(&object_pool);
        auto& tuple_builder = builder.declare_tuple();
        for (auto& [name, type] : cols) {
            tuple_builder << std::make_tuple(type, name);
        }
        DescriptorTbl* desc_tbl = builder.build();
        return desc_tbl->get_tuple_descriptor(0);
    }

    static DataTypePtr nullable_int64() {
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    }
    static DataTypePtr nullable_string() {
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    // Helper: get column by name from block (via get_position_by_name).
    static const ColumnWithTypeAndName& get_col(const Block& block, const std::string& name) {
        int pos = block.get_position_by_name(name);
        EXPECT_GE(pos, 0) << "Column " << name << " not found in block";
        return block.get_by_position(static_cast<size_t>(pos));
    }

    // Build a Block matching the given tuple descriptor.
    static Block make_block(const TupleDescriptor* tuple_desc) {
        Block block;
        for (auto* slot : tuple_desc->slots()) {
            block.insert({slot->get_data_type_ptr()->create_column(), slot->get_data_type_ptr(),
                          slot->col_name()});
        }
        return block;
    }

    static std::unordered_map<std::string, uint32_t> build_col_name_to_block_idx(
            const TupleDescriptor* tuple_desc) {
        std::unordered_map<std::string, uint32_t> map;
        for (size_t i = 0; i < tuple_desc->slots().size(); i++) {
            map[tuple_desc->slots()[i]->col_name()] = i;
        }
        return map;
    }
};

// ============================================================================
// Test: ColumnOptimizationTypes bitmask operations
// ============================================================================
TEST_F(TableFormatReaderTest, ColumnOptimizationTypesDefault) {
    MockTableFormatReader reader;
    EXPECT_EQ(reader.get_column_optimizations("any_col"),
              TableFormatReader::ColumnOptimizationTypes::DEFAULT);
    EXPECT_TRUE(reader.has_column_optimization(
            "any_col", TableFormatReader::ColumnOptimizationTypes::LAZY_READ));
    EXPECT_TRUE(reader.has_column_optimization(
            "any_col", TableFormatReader::ColumnOptimizationTypes::DICT_FILTER));
    EXPECT_TRUE(reader.has_column_optimization(
            "any_col", TableFormatReader::ColumnOptimizationTypes::MIN_MAX));
    EXPECT_FALSE(reader.disable_column_opt("any_col"));
}

TEST_F(TableFormatReaderTest, ColumnOptimizationTypesSetNone) {
    MockTableFormatReader reader;
    reader.set_column_optimizations("col_a", TableFormatReader::ColumnOptimizationTypes::NONE);
    EXPECT_EQ(reader.get_column_optimizations("col_a"),
              TableFormatReader::ColumnOptimizationTypes::NONE);
    EXPECT_FALSE(reader.has_column_optimization(
            "col_a", TableFormatReader::ColumnOptimizationTypes::LAZY_READ));
    EXPECT_TRUE(reader.disable_column_opt("col_a"));
}

TEST_F(TableFormatReaderTest, ColumnOptimizationTypesPartialBitmask) {
    MockTableFormatReader reader;
    reader.set_column_optimizations("col_b", TableFormatReader::ColumnOptimizationTypes::LAZY_READ);
    EXPECT_TRUE(reader.has_column_optimization(
            "col_b", TableFormatReader::ColumnOptimizationTypes::LAZY_READ));
    EXPECT_FALSE(reader.has_column_optimization(
            "col_b", TableFormatReader::ColumnOptimizationTypes::DICT_FILTER));
    EXPECT_FALSE(reader.has_column_optimization(
            "col_b", TableFormatReader::ColumnOptimizationTypes::MIN_MAX));
    EXPECT_FALSE(reader.disable_column_opt("col_b"));
}

TEST_F(TableFormatReaderTest, ColumnOptimizationTypesResetToDefault) {
    MockTableFormatReader reader;
    reader.set_column_optimizations("col_c", TableFormatReader::ColumnOptimizationTypes::NONE);
    EXPECT_TRUE(reader.disable_column_opt("col_c"));
    // Reset by passing DEFAULT.
    reader.set_column_optimizations("col_c", TableFormatReader::ColumnOptimizationTypes::DEFAULT);
    EXPECT_FALSE(reader.disable_column_opt("col_c"));
    EXPECT_EQ(reader.get_column_optimizations("col_c"),
              TableFormatReader::ColumnOptimizationTypes::DEFAULT);
}

// ============================================================================
// Test: Synthesized column handler registration and filling
// ============================================================================
TEST_F(TableFormatReaderTest, RegisterAndFillSynthesizedColumn) {
    MockTableFormatReader reader;
    EXPECT_FALSE(reader.has_synthesized_column_handlers());

    int synth_call_count = 0;
    reader.register_synthesized_column_handler(
            "synth_col", [&synth_call_count](Block* block, size_t rows) -> Status {
                synth_call_count++;
                return Status::OK();
            });
    EXPECT_TRUE(reader.has_synthesized_column_handlers());

    Block block;
    auto type = nullable_int64();
    block.insert({type->create_column(), type, "synth_col"});
    auto st = reader.fill_synthesized_columns(&block, 5);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(synth_call_count, 1);
}

TEST_F(TableFormatReaderTest, MultipleSynthesizedColumnHandlers) {
    MockTableFormatReader reader;
    std::vector<std::string> called;

    reader.register_synthesized_column_handler("synth_a", [&called](Block*, size_t) -> Status {
        called.push_back("synth_a");
        return Status::OK();
    });
    reader.register_synthesized_column_handler("synth_b", [&called](Block*, size_t) -> Status {
        called.push_back("synth_b");
        return Status::OK();
    });

    Block block;
    auto type = nullable_int64();
    block.insert({type->create_column(), type, "synth_a"});
    block.insert({type->create_column(), type, "synth_b"});

    auto st = reader.fill_synthesized_columns(&block, 3);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(called.size(), 2);
    EXPECT_EQ(called[0], "synth_a");
    EXPECT_EQ(called[1], "synth_b");
}

TEST_F(TableFormatReaderTest, SynthesizedColumnHandlerError) {
    MockTableFormatReader reader;
    reader.register_synthesized_column_handler("bad_synth", [](Block*, size_t) -> Status {
        return Status::InternalError("synth computation failed");
    });

    Block block;
    auto type = nullable_int64();
    block.insert({type->create_column(), type, "bad_synth"});
    auto st = reader.fill_synthesized_columns(&block, 1);
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// Test: Generated column handler registration and optimization flags
// ============================================================================
TEST_F(TableFormatReaderTest, RegisterGeneratedColumnDisablesOptimizations) {
    MockTableFormatReader reader;
    EXPECT_FALSE(reader.has_generated_column_handlers());

    reader.register_generated_column_handler("gen_col",
                                             [](Block*, size_t) -> Status { return Status::OK(); });
    EXPECT_TRUE(reader.has_generated_column_handlers());
    // Generated columns must have ALL optimizations disabled.
    EXPECT_TRUE(reader.disable_column_opt("gen_col"));
    EXPECT_EQ(reader.get_column_optimizations("gen_col"),
              TableFormatReader::ColumnOptimizationTypes::NONE);
}

TEST_F(TableFormatReaderTest, FillGeneratedColumns) {
    MockTableFormatReader reader;
    int gen_call_count = 0;
    reader.register_generated_column_handler(
            "gen_col", [&gen_call_count](Block* block, size_t rows) -> Status {
                gen_call_count++;
                return Status::OK();
            });

    Block block;
    auto type = nullable_int64();
    block.insert({type->create_column(), type, "gen_col"});
    auto st = reader.fill_generated_columns(&block, 10);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(gen_call_count, 1);
}

// ============================================================================
// Test: Clear synthesized/generated columns
// ============================================================================
TEST_F(TableFormatReaderTest, ClearSynthesizedColumns) {
    MockTableFormatReader reader;
    reader.register_synthesized_column_handler(
            "synth_col", [](Block*, size_t) -> Status { return Status::OK(); });

    Block block;
    auto type = nullable_int64();
    auto col = type->create_column();
    col->assume_mutable()->insert_many_defaults(5);
    block.insert({std::move(col), type, "synth_col"});
    ASSERT_EQ(get_col(block, "synth_col").column->size(), 5);

    auto st = reader.clear_synthesized_columns(&block);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(get_col(block, "synth_col").column->size(), 0);
}

TEST_F(TableFormatReaderTest, ClearGeneratedColumns) {
    MockTableFormatReader reader;
    reader.register_generated_column_handler("gen_col",
                                             [](Block*, size_t) -> Status { return Status::OK(); });

    Block block;
    auto type = nullable_int64();
    auto col = type->create_column();
    col->assume_mutable()->insert_many_defaults(3);
    block.insert({std::move(col), type, "gen_col"});
    ASSERT_EQ(get_col(block, "gen_col").column->size(), 3);

    auto st = reader.clear_generated_columns(&block);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(get_col(block, "gen_col").column->size(), 0);
}

// ============================================================================
// Test: on_fill_partition_columns fills column with deserialized values
// ============================================================================
TEST_F(TableFormatReaderTest, FillPartitionColumnsInt64) {
    auto tuple_desc =
            build_tuple_desc({{"data_col", nullable_int64()}, {"part_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;

    // Register partition value "2024" for part_col.
    const SlotDescriptor* part_slot = nullptr;
    for (auto* slot : tuple_desc->slots()) {
        if (slot->col_name() == "part_col") {
            part_slot = slot;
            break;
        }
    }
    ASSERT_NE(part_slot, nullptr);
    reader._fill_partition_values["part_col"] = std::make_tuple("2024", part_slot);

    Block block = make_block(tuple_desc);
    const size_t num_rows = 5;

    auto st = reader.on_fill_partition_columns(&block, num_rows, {"part_col"});
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(get_col(block, "part_col").column->size(), num_rows);
}

TEST_F(TableFormatReaderTest, FillPartitionColumnsString) {
    auto tuple_desc =
            build_tuple_desc({{"data_col", nullable_int64()}, {"region", nullable_string()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;

    const SlotDescriptor* region_slot = nullptr;
    for (auto* slot : tuple_desc->slots()) {
        if (slot->col_name() == "region") {
            region_slot = slot;
            break;
        }
    }
    ASSERT_NE(region_slot, nullptr);
    reader._fill_partition_values["region"] = std::make_tuple("us-east-1", region_slot);

    Block block = make_block(tuple_desc);
    const size_t num_rows = 3;

    auto st = reader.on_fill_partition_columns(&block, num_rows, {"region"});
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(get_col(block, "region").column->size(), num_rows);
}

TEST_F(TableFormatReaderTest, FillPartitionColumnsUnknownNameSkipped) {
    auto tuple_desc = build_tuple_desc({{"col_a", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;
    // No partition values registered.

    Block block = make_block(tuple_desc);
    // Should not fail — unknown name is simply skipped.
    auto st = reader.on_fill_partition_columns(&block, 5, {"unknown_partition"});
    ASSERT_TRUE(st.ok());
}

// ============================================================================
// Test: on_fill_missing_columns with nullptr default_expr → NULL fill
// ============================================================================
TEST_F(TableFormatReaderTest, FillMissingColumnsNullDefault) {
    auto tuple_desc =
            build_tuple_desc({{"data_col", nullable_int64()}, {"missing_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;
    // nullptr default_expr → fill with NULL.
    reader._fill_missing_defaults["missing_col"] = nullptr;

    Block block = make_block(tuple_desc);
    const size_t num_rows = 5;

    auto st = reader.on_fill_missing_columns(&block, num_rows, {"missing_col"});
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto missing_col = get_col(block, "missing_col").column;
    EXPECT_EQ(missing_col->size(), num_rows);
    // All values should be NULL.
    for (size_t i = 0; i < num_rows; i++) {
        EXPECT_TRUE(missing_col->is_null_at(i)) << "Row " << i << " should be NULL";
    }
}

TEST_F(TableFormatReaderTest, FillMissingColumnNotInBlockErrors) {
    auto tuple_desc = build_tuple_desc({{"data_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;
    reader._fill_missing_defaults["nonexistent_col"] = nullptr;

    Block block = make_block(tuple_desc);
    auto st = reader.on_fill_missing_columns(&block, 5, {"nonexistent_col"});
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// Test: fill_remaining_columns integrates partition + missing + synthesized + generated
// ============================================================================
TEST_F(TableFormatReaderTest, FillRemainingColumnsIntegration) {
    auto tuple_desc = build_tuple_desc({{"data_col", nullable_int64()},
                                        {"part_col", nullable_int64()},
                                        {"missing_col", nullable_int64()},
                                        {"synth_col", nullable_int64()},
                                        {"gen_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;

    // Setup partition column.
    const SlotDescriptor* part_slot = nullptr;
    for (auto* slot : tuple_desc->slots()) {
        if (slot->col_name() == "part_col") {
            part_slot = slot;
            break;
        }
    }
    ASSERT_NE(part_slot, nullptr);
    reader._fill_partition_values["part_col"] = std::make_tuple("100", part_slot);

    // Setup missing column (NULL default).
    reader._fill_missing_defaults["missing_col"] = nullptr;

    // Setup synthesized column.
    int synth_calls = 0;
    reader.register_synthesized_column_handler(
            "synth_col", [&synth_calls, &col_idx_map](Block* block, size_t rows) -> Status {
                synth_calls++;
                auto col =
                        block->get_by_position(col_idx_map["synth_col"]).column->assume_mutable();
                auto* nullable = static_cast<ColumnNullable*>(col.get());
                nullable->insert_many_defaults(rows);
                return Status::OK();
            });

    // Setup generated column.
    int gen_calls = 0;
    reader.register_generated_column_handler(
            "gen_col", [&gen_calls, &col_idx_map](Block* block, size_t rows) -> Status {
                gen_calls++;
                auto col = block->get_by_position(col_idx_map["gen_col"]).column->assume_mutable();
                auto* nullable = static_cast<ColumnNullable*>(col.get());
                nullable->insert_many_defaults(rows);
                return Status::OK();
            });

    Block block = make_block(tuple_desc);
    const size_t num_rows = 3;

    auto st = reader.fill_remaining_columns(&block, num_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(get_col(block, "part_col").column->size(), num_rows);
    EXPECT_EQ(get_col(block, "missing_col").column->size(), num_rows);
    EXPECT_EQ(synth_calls, 1);
    EXPECT_EQ(gen_calls, 1);
}

// ============================================================================
// Test: on_after_read_block calls fill_remaining_columns when read_rows > 0
// ============================================================================
TEST_F(TableFormatReaderTest, OnAfterReadBlockFillsColumnsWhenRowsGt0) {
    auto tuple_desc =
            build_tuple_desc({{"data_col", nullable_int64()}, {"missing_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;
    reader._fill_missing_defaults["missing_col"] = nullptr;
    reader.batches_to_emit = 1;
    reader.rows_per_batch = 4;

    Block block = make_block(tuple_desc);
    size_t read_rows = 0;
    bool eof = false;
    auto st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(read_rows, 4);
    // Missing column should be filled by on_after_read_block.
    EXPECT_EQ(get_col(block, "missing_col").column->size(), 4);
}

// ============================================================================
// Test: on_after_read_block skips fill when read_rows == 0
// ============================================================================
TEST_F(TableFormatReaderTest, OnAfterReadBlockSkipsWhenZeroRows) {
    auto tuple_desc =
            build_tuple_desc({{"data_col", nullable_int64()}, {"missing_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;
    reader._fill_missing_defaults["missing_col"] = nullptr;
    reader.batches_to_emit = 0; // Immediate EOF.

    Block block = make_block(tuple_desc);
    size_t read_rows = 0;
    bool eof = false;
    auto st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(read_rows, 0);
    EXPECT_TRUE(eof);
    // Missing column should NOT be filled (0 rows).
    EXPECT_EQ(get_col(block, "missing_col").column->size(), 0);
}

// ============================================================================
// Test: on_after_read_block skips fill for COUNT pushdown
// ============================================================================
TEST_F(TableFormatReaderTest, OnAfterReadBlockSkipsForCountPushDown) {
    auto tuple_desc =
            build_tuple_desc({{"data_col", nullable_int64()}, {"missing_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;
    reader._fill_missing_defaults["missing_col"] = nullptr;
    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);
    reader.batches_to_emit = 1;
    reader.rows_per_batch = 4;

    Block block = make_block(tuple_desc);
    size_t read_rows = 0;
    bool eof = false;
    auto st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(read_rows, 4);
    // Missing column should NOT be filled (COUNT pushdown).
    EXPECT_EQ(get_col(block, "missing_col").column->size(), 0);
}

// ============================================================================
// Test: _extract_partition_values from TFileRangeDesc
// ============================================================================
TEST_F(TableFormatReaderTest, ExtractPartitionValues) {
    auto tuple_desc = build_tuple_desc({{"data_col", nullable_int64()},
                                        {"year", nullable_int64()},
                                        {"region", nullable_string()}});

    TFileRangeDesc range;
    range.__set_columns_from_path_keys({"year", "region"});
    range.__set_columns_from_path({"2024", "us-west-2"});

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_values;
    auto st = TableFormatReader::_extract_partition_values(range, tuple_desc, partition_values);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(partition_values.size(), 2);

    auto [year_val, year_slot] = partition_values["year"];
    EXPECT_EQ(year_val, "2024");
    EXPECT_EQ(year_slot->col_name(), "year");

    auto [region_val, region_slot] = partition_values["region"];
    EXPECT_EQ(region_val, "us-west-2");
    EXPECT_EQ(region_slot->col_name(), "region");
}

TEST_F(TableFormatReaderTest, ExtractPartitionValuesNoPath) {
    auto tuple_desc = build_tuple_desc({{"col_a", nullable_int64()}});

    TFileRangeDesc range;
    // No columns_from_path_keys set.

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_values;
    auto st = TableFormatReader::_extract_partition_values(range, tuple_desc, partition_values);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(partition_values.size(), 0);
}

TEST_F(TableFormatReaderTest, ExtractPartitionValuesNullTupleDesc) {
    TFileRangeDesc range;
    range.__set_columns_from_path_keys({"year"});
    range.__set_columns_from_path({"2024"});

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_values;
    auto st = TableFormatReader::_extract_partition_values(range, nullptr, partition_values);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(partition_values.size(), 0);
}

// ============================================================================
// Test: on_before_init_reader for simple readers (default implementation)
// ============================================================================
TEST_F(TableFormatReaderTest, OnBeforeInitReaderDefaultImpl) {
    auto tuple_desc = build_tuple_desc(
            {{"id", nullable_int64()}, {"name", nullable_string()}, {"year", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    // Simulate file columns: file has "id" and "name" but NOT "year".
    reader.fake_file_columns["id"] = nullable_int64();
    reader.fake_file_columns["name"] = nullable_string();

    TFileRangeDesc range;
    range.__set_columns_from_path_keys({"year"});
    range.__set_columns_from_path({"2024"});

    // Build column_descs.
    std::vector<ColumnDescriptor> col_descs = {
            {"id", nullptr, ColumnCategory::REGULAR, nullptr},
            {"name", nullptr, ColumnCategory::REGULAR, nullptr},
            {"year", nullptr, ColumnCategory::PARTITION_KEY, nullptr},
    };

    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.col_name_to_block_idx = &col_idx_map;
    ctx.tuple_descriptor = tuple_desc;
    ctx.range = &range;

    auto st = reader.on_before_init_reader(&ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // column_names should only contain REGULAR columns (id, name), not partition (year).
    ASSERT_EQ(ctx.column_names.size(), 2);
    EXPECT_EQ(ctx.column_names[0], "id");
    EXPECT_EQ(ctx.column_names[1], "name");

    // table_info_node should be set (StructNode with entries).
    EXPECT_NE(ctx.table_info_node, nullptr);
}

// ============================================================================
// Test: on_before_init_reader detects missing columns
// ============================================================================
TEST_F(TableFormatReaderTest, OnBeforeInitReaderMissingColumns) {
    auto tuple_desc =
            build_tuple_desc({{"id", nullable_int64()}, {"missing_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    // File only has "id".
    reader.fake_file_columns["id"] = nullable_int64();

    TFileRangeDesc range;

    std::vector<ColumnDescriptor> col_descs = {
            {"id", nullptr, ColumnCategory::REGULAR, nullptr},
            {"missing_col", nullptr, ColumnCategory::REGULAR, nullptr},
    };

    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.col_name_to_block_idx = &col_idx_map;
    ctx.tuple_descriptor = tuple_desc;
    ctx.range = &range;

    auto st = reader.on_before_init_reader(&ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // missing_col should be detected as missing.
    EXPECT_TRUE(reader.missing_cols().count("missing_col"));
    EXPECT_FALSE(reader.missing_cols().count("id"));
}

// ============================================================================
// Test: on_before_init_reader case-insensitive column matching
// ============================================================================
TEST_F(TableFormatReaderTest, OnBeforeInitReaderCaseInsensitive) {
    auto tuple_desc = build_tuple_desc({{"Name", nullable_string()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    // File has column "name" (lowercase).
    reader.fake_file_columns["name"] = nullable_string();

    TFileRangeDesc range;

    std::vector<ColumnDescriptor> col_descs = {
            {"Name", nullptr, ColumnCategory::REGULAR, nullptr},
    };

    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.col_name_to_block_idx = &col_idx_map;
    ctx.tuple_descriptor = tuple_desc;
    ctx.range = &range;

    auto st = reader.on_before_init_reader(&ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();
    // "Name" matches "name" case-insensitively → NOT missing.
    EXPECT_FALSE(reader.missing_cols().count("Name"));
}

// ============================================================================
// Test: on_before_init_reader with GENERATED columns
// ============================================================================
TEST_F(TableFormatReaderTest, OnBeforeInitReaderGeneratedColumn) {
    auto tuple_desc = build_tuple_desc({{"id", nullable_int64()}, {"gen_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader.fake_file_columns["id"] = nullable_int64();
    // gen_col NOT in file.

    TFileRangeDesc range;

    std::vector<ColumnDescriptor> col_descs = {
            {"id", nullptr, ColumnCategory::REGULAR, nullptr},
            {"gen_col", nullptr, ColumnCategory::GENERATED, nullptr},
    };

    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.col_name_to_block_idx = &col_idx_map;
    ctx.tuple_descriptor = tuple_desc;
    ctx.range = &range;

    auto st = reader.on_before_init_reader(&ctx);
    ASSERT_TRUE(st.ok());

    // GENERATED columns appear in column_names (they may exist in file).
    ASSERT_EQ(ctx.column_names.size(), 2);
    EXPECT_EQ(ctx.column_names[0], "id");
    EXPECT_EQ(ctx.column_names[1], "gen_col");

    // gen_col is detected as missing because it's not in the file.
    EXPECT_TRUE(reader.missing_cols().count("gen_col"));
}

// ============================================================================
// Test: on_before_init_reader skips SYNTHESIZED columns from column_names
// ============================================================================
TEST_F(TableFormatReaderTest, OnBeforeInitReaderSynthesizedExcluded) {
    auto tuple_desc = build_tuple_desc({{"id", nullable_int64()}, {"synth_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader.fake_file_columns["id"] = nullable_int64();

    TFileRangeDesc range;

    std::vector<ColumnDescriptor> col_descs = {
            {"id", nullptr, ColumnCategory::REGULAR, nullptr},
            {"synth_col", nullptr, ColumnCategory::SYNTHESIZED, nullptr},
    };

    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.col_name_to_block_idx = &col_idx_map;
    ctx.tuple_descriptor = tuple_desc;
    ctx.range = &range;

    auto st = reader.on_before_init_reader(&ctx);
    ASSERT_TRUE(st.ok());

    // Only REGULAR shows up in column_names.
    ASSERT_EQ(ctx.column_names.size(), 1);
    EXPECT_EQ(ctx.column_names[0], "id");

    // SYNTHESIZED cols are not tracked as missing.
    EXPECT_FALSE(reader.missing_cols().count("synth_col"));
}

// ============================================================================
// Test: missing_cols() returns accumulated set
// ============================================================================
TEST_F(TableFormatReaderTest, MissingColsAccessor) {
    MockTableFormatReader reader;
    EXPECT_TRUE(reader.missing_cols().empty());

    // Manually set for direct testing.
    reader._fill_missing_cols.insert("col_a");
    reader._fill_missing_cols.insert("col_b");
    EXPECT_EQ(reader.missing_cols().size(), 2);
    EXPECT_TRUE(reader.missing_cols().count("col_a"));
    EXPECT_TRUE(reader.missing_cols().count("col_b"));
}

// ============================================================================
// Test: Multi-batch get_next_block with on_after_read_block auto-fill
// ============================================================================
TEST_F(TableFormatReaderTest, MultiBatchWithAutoFill) {
    auto tuple_desc =
            build_tuple_desc({{"data_col", nullable_int64()}, {"missing_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;
    reader._fill_missing_defaults["missing_col"] = nullptr;
    reader.batches_to_emit = 3;
    reader.rows_per_batch = 5;

    size_t total_rows = 0;
    bool eof = false;
    int batch_count = 0;
    while (!eof) {
        Block block = make_block(tuple_desc);
        size_t read_rows = 0;
        auto st = reader.get_next_block(&block, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st.to_string();
        if (read_rows > 0) {
            // Verify missing_col was filled by on_after_read_block each batch.
            EXPECT_EQ(get_col(block, "missing_col").column->size(), read_rows);
            for (size_t i = 0; i < read_rows; i++) {
                EXPECT_TRUE(get_col(block, "missing_col").column->is_null_at(i));
            }
        }
        total_rows += read_rows;
        batch_count++;
    }
    EXPECT_EQ(total_rows, 15);
    EXPECT_EQ(batch_count, 3);
}

// ============================================================================
// Test: Full lifecycle: init_reader → get_next_block → auto-fill all types
// ============================================================================
TEST_F(TableFormatReaderTest, FullLifecycleWithInit) {
    auto tuple_desc = build_tuple_desc(
            {{"id", nullable_int64()}, {"year", nullable_int64()}, {"name", nullable_string()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    // Simulate: file has "id" and "name" but NOT "year".
    reader.fake_file_columns["id"] = nullable_int64();
    reader.fake_file_columns["name"] = nullable_string();

    TFileRangeDesc range;
    range.__set_columns_from_path_keys({"year"});
    range.__set_columns_from_path({"2024"});

    std::vector<ColumnDescriptor> col_descs = {
            {"id", nullptr, ColumnCategory::REGULAR, nullptr},
            {"name", nullptr, ColumnCategory::REGULAR, nullptr},
            {"year", nullptr, ColumnCategory::PARTITION_KEY, nullptr},
    };

    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.col_name_to_block_idx = &col_idx_map;
    ctx.tuple_descriptor = tuple_desc;
    ctx.range = &range;

    // Init should extract partition values and compute column_names.
    auto st = reader.init_reader(&ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Verify column_names only has REGULAR columns.
    ASSERT_EQ(ctx.column_names.size(), 2);
    EXPECT_EQ(ctx.column_names[0], "id");
    EXPECT_EQ(ctx.column_names[1], "name");

    // Read one batch.
    reader.batches_to_emit = 1;
    reader.rows_per_batch = 3;

    Block block = make_block(tuple_desc);
    size_t read_rows = 0;
    bool eof = false;
    st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(read_rows, 3);

    // Partition column "year" should be filled with "2024" by on_after_read_block.
    auto year_col = get_col(block, "year").column;
    EXPECT_EQ(year_col->size(), 3);
}

// ============================================================================
// Test: Synthesized column handler error stops fill_remaining_columns
// ============================================================================
TEST_F(TableFormatReaderTest, FillRemainingColumnsStopsOnSynthError) {
    auto tuple_desc = build_tuple_desc({{"data_col", nullable_int64()},
                                        {"synth_col", nullable_int64()},
                                        {"gen_col", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;

    reader.register_synthesized_column_handler("synth_col", [](Block*, size_t) -> Status {
        return Status::InternalError("synth error");
    });

    int gen_calls = 0;
    reader.register_generated_column_handler("gen_col", [&gen_calls](Block*, size_t) -> Status {
        gen_calls++;
        return Status::OK();
    });

    Block block = make_block(tuple_desc);
    auto st = reader.fill_remaining_columns(&block, 3);
    ASSERT_FALSE(st.ok());
    // Generated handler should NOT have been called (synthesized error aborted early).
    EXPECT_EQ(gen_calls, 0);
}

// ============================================================================
// Test: Clear columns on non-existent column name does not crash
// ============================================================================
TEST_F(TableFormatReaderTest, ClearNonExistentColumnSafe) {
    MockTableFormatReader reader;
    reader.register_synthesized_column_handler(
            "ghost_col", [](Block*, size_t) -> Status { return Status::OK(); });

    Block block;
    auto type = nullable_int64();
    block.insert({type->create_column(), type, "other_col"});

    // "ghost_col" not in block — clear should still succeed (get_position_by_name returns -1).
    auto st = reader.clear_synthesized_columns(&block);
    ASSERT_TRUE(st.ok());
}

// ============================================================================
// Test: Multiple partition columns filled correctly
// ============================================================================
TEST_F(TableFormatReaderTest, MultiplePartitionColumns) {
    auto tuple_desc = build_tuple_desc({{"data_col", nullable_int64()},
                                        {"year", nullable_int64()},
                                        {"month", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader._fill_col_name_to_block_idx = &col_idx_map;

    const SlotDescriptor* year_slot = nullptr;
    const SlotDescriptor* month_slot = nullptr;
    for (auto* slot : tuple_desc->slots()) {
        if (slot->col_name() == "year") year_slot = slot;
        if (slot->col_name() == "month") month_slot = slot;
    }
    ASSERT_NE(year_slot, nullptr);
    ASSERT_NE(month_slot, nullptr);

    reader._fill_partition_values["year"] = std::make_tuple("2024", year_slot);
    reader._fill_partition_values["month"] = std::make_tuple("12", month_slot);

    Block block = make_block(tuple_desc);
    const size_t num_rows = 4;

    auto st = reader.on_fill_partition_columns(&block, num_rows, {"year", "month"});
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(get_col(block, "year").column->size(), num_rows);
    EXPECT_EQ(get_col(block, "month").column->size(), num_rows);
}

// ============================================================================
// Test: on_before_init_reader with all column categories mixed
// ============================================================================
TEST_F(TableFormatReaderTest, OnBeforeInitReaderAllCategories) {
    auto tuple_desc = build_tuple_desc({{"id", nullable_int64()},
                                        {"year", nullable_int64()},
                                        {"$row_id", nullable_int64()},
                                        {"_row_id", nullable_int64()},
                                        {"name", nullable_string()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader.fake_file_columns["id"] = nullable_int64();
    reader.fake_file_columns["name"] = nullable_string();

    TFileRangeDesc range;
    range.__set_columns_from_path_keys({"year"});
    range.__set_columns_from_path({"2024"});

    std::vector<ColumnDescriptor> col_descs = {
            {"id", nullptr, ColumnCategory::REGULAR, nullptr},
            {"year", nullptr, ColumnCategory::PARTITION_KEY, nullptr},
            {"$row_id", nullptr, ColumnCategory::SYNTHESIZED, nullptr},
            {"_row_id", nullptr, ColumnCategory::GENERATED, nullptr},
            {"name", nullptr, ColumnCategory::REGULAR, nullptr},
    };

    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.col_name_to_block_idx = &col_idx_map;
    ctx.tuple_descriptor = tuple_desc;
    ctx.range = &range;

    auto st = reader.on_before_init_reader(&ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // column_names: REGULAR + GENERATED only.
    ASSERT_EQ(ctx.column_names.size(), 3);
    EXPECT_EQ(ctx.column_names[0], "id");
    EXPECT_EQ(ctx.column_names[1], "_row_id");
    EXPECT_EQ(ctx.column_names[2], "name");

    // _row_id is GENERATED but not in file → detected as missing.
    EXPECT_TRUE(reader.missing_cols().count("_row_id"));
    // $row_id is SYNTHESIZED → not in missing.
    EXPECT_FALSE(reader.missing_cols().count("$row_id"));
    // year is PARTITION_KEY → not in missing.
    EXPECT_FALSE(reader.missing_cols().count("year"));
    // id and name are in file → not missing.
    EXPECT_FALSE(reader.missing_cols().count("id"));
    EXPECT_FALSE(reader.missing_cols().count("name"));
}

// ============================================================================
// Test: _extract_partition_values ignores keys not in tuple descriptor
// ============================================================================
TEST_F(TableFormatReaderTest, ExtractPartitionValuesExtraKeysIgnored) {
    auto tuple_desc = build_tuple_desc({{"year", nullable_int64()}});

    TFileRangeDesc range;
    range.__set_columns_from_path_keys({"year", "nonexistent_key"});
    range.__set_columns_from_path({"2024", "ignored_value"});

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_values;
    auto st = MockTableFormatReader::_extract_partition_values(range, tuple_desc, partition_values);
    ASSERT_TRUE(st.ok());
    // Only "year" matches a slot, "nonexistent_key" is ignored.
    EXPECT_EQ(partition_values.size(), 1);
    EXPECT_TRUE(partition_values.count("year"));
}

// ============================================================================
// Test: ColumnOptimizationTypes bitwise OR combination
// ============================================================================
TEST_F(TableFormatReaderTest, ColumnOptimizationTypesCombination) {
    MockTableFormatReader reader;
    auto combined = TableFormatReader::ColumnOptimizationTypes::LAZY_READ |
                    TableFormatReader::ColumnOptimizationTypes::MIN_MAX;
    reader.set_column_optimizations("col_x", combined);

    EXPECT_TRUE(reader.has_column_optimization(
            "col_x", TableFormatReader::ColumnOptimizationTypes::LAZY_READ));
    EXPECT_FALSE(reader.has_column_optimization(
            "col_x", TableFormatReader::ColumnOptimizationTypes::DICT_FILTER));
    EXPECT_TRUE(reader.has_column_optimization(
            "col_x", TableFormatReader::ColumnOptimizationTypes::MIN_MAX));
    EXPECT_FALSE(reader.disable_column_opt("col_x")); // Not NONE.
}

// ============================================================================
// Test: on_before_init_reader partition column is not added to missing
// ============================================================================
TEST_F(TableFormatReaderTest, PartitionColumnNotMissing) {
    auto tuple_desc = build_tuple_desc({{"id", nullable_int64()}, {"year", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader.fake_file_columns["id"] = nullable_int64();
    // "year" is NOT in file, but it's a PARTITION_KEY column.

    TFileRangeDesc range;
    range.__set_columns_from_path_keys({"year"});
    range.__set_columns_from_path({"2024"});

    std::vector<ColumnDescriptor> col_descs = {
            {"id", nullptr, ColumnCategory::REGULAR, nullptr},
            {"year", nullptr, ColumnCategory::PARTITION_KEY, nullptr},
    };

    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.col_name_to_block_idx = &col_idx_map;
    ctx.tuple_descriptor = tuple_desc;
    ctx.range = &range;

    auto st = reader.on_before_init_reader(&ctx);
    ASSERT_TRUE(st.ok());
    // Partition columns should NOT appear in missing_cols.
    EXPECT_FALSE(reader.missing_cols().count("year"));
}

// ============================================================================
// Test: on_before_init_reader with empty column_descs
// ============================================================================
TEST_F(TableFormatReaderTest, OnBeforeInitReaderEmptyColumnDescs) {
    auto tuple_desc = build_tuple_desc({{"id", nullable_int64()}});
    auto col_idx_map = build_col_name_to_block_idx(tuple_desc);

    MockTableFormatReader reader;
    reader.fake_file_columns["id"] = nullable_int64();

    TFileRangeDesc range;

    std::vector<ColumnDescriptor> col_descs; // Empty.

    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.col_name_to_block_idx = &col_idx_map;
    ctx.tuple_descriptor = tuple_desc;
    ctx.range = &range;

    auto st = reader.on_before_init_reader(&ctx);
    ASSERT_TRUE(st.ok());
    EXPECT_TRUE(ctx.column_names.empty());
    EXPECT_TRUE(reader.missing_cols().empty());
}

} // namespace doris
