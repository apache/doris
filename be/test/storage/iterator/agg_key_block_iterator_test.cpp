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

// AggKeyBlockIterator::ValueAggregator folds key groups given as row ranges of merged Blocks: one
// result row per group, NULLs skipped by the nullable functions, the last value that is not NULL
// for REPLACE_IF_NOT_NULL, and whether one-row groups may be copied instead of folded.
// AggKeyBlockIterator turns every key group of the merged Blocks into one row: the keys and the
// REPLACE values of the group's last row, the other values folded, and output Blocks that end in
// front of a new key.

#include "storage/iterator/agg_key_block_iterator.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_quantilestate.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

TabletColumnPtr make_column(const std::string& name, FieldAggregationMethod aggregation,
                            FieldType type, bool is_nullable, int32_t unique_id, bool is_key) {
    auto column = std::make_shared<TabletColumn>(aggregation, type, is_nullable, unique_id,
                                                 sizeof(int64_t));
    column->set_name(name);
    column->set_is_key(is_key);
    return column;
}

// k BIGINT key, v BIGINT with `aggregation`, nullable when `nullable`.
ReadSchemaSPtr make_read_schema(FieldAggregationMethod aggregation, bool nullable = false) {
    auto key = make_column("k", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                           FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0, true);
    auto value =
            make_column("v", aggregation, FieldType::OLAP_FIELD_TYPE_BIGINT, nullable, 1, false);
    DataTypePtr int_type = std::make_shared<DataTypeInt64>();
    DataTypePtr value_type = nullable ? make_nullable(int_type) : int_type;
    return std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {key, value},
                                        std::vector<DataTypePtr> {int_type, value_type});
}

// A Block of `values` in column 1 (NULL where nullopt) with key 1 in column 0.
Block make_block(const ReadSchema& schema, const std::vector<std::optional<int64_t>>& values) {
    Block block = schema.create_read_block();
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (const auto& value : values) {
        assert_cast<ColumnInt64&>(*columns[0]).insert_value(1);
        if (auto* nullable = check_and_get_column<ColumnNullable>(*columns[1])) {
            nullable->get_null_map_data().push_back(value.has_value() ? 0 : 1);
            assert_cast<ColumnInt64&>(nullable->get_nested_column())
                    .insert_value(value.value_or(0));
        } else {
            assert_cast<ColumnInt64&>(*columns[1]).insert_value(*value);
        }
    }
    return block;
}

std::unique_ptr<AggKeyBlockIterator::ValueAggregator> make_aggregator(const ReadSchema& schema) {
    auto aggregator = std::make_unique<AggKeyBlockIterator::ValueAggregator>();
    EXPECT_TRUE(aggregator->init(schema, {1}, BeExecVersionManager::get_newest_version()).ok());
    return aggregator;
}

std::optional<int64_t> value_at(const IColumn& column, size_t row) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        if (nullable->is_null_at(row)) {
            return std::nullopt;
        }
        return assert_cast<const ColumnInt64&>(nullable->get_nested_column()).get_data()[row];
    }
    return assert_cast<const ColumnInt64&>(column).get_data()[row];
}

// A row of the k, r, s schema below.
using Row = std::vector<int64_t>;

// k BIGINT key, r BIGINT REPLACE, s BIGINT SUM.
ReadSchemaSPtr make_agg_schema() {
    return std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {
            make_column("k", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                        FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0, true),
            make_column("r", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE,
                        FieldType::OLAP_FIELD_TYPE_BIGINT, false, 1, false),
            make_column("s", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                        FieldType::OLAP_FIELD_TYPE_BIGINT, false, 2, false)});
}

// Hands out the merged Blocks `blocks`, one per next_batch(), then EndOfFile.
class MergedBlocksUtIterator final : public RowwiseIterator {
public:
    MergedBlocksUtIterator(ReadSchemaSPtr schema, std::vector<std::vector<Row>> blocks)
            : _schema(std::move(schema)), _blocks(std::move(blocks)) {}

    Status next_batch(Block* block) override {
        if (_next == _blocks.size()) {
            return Status::EndOfFile("no more merged blocks");
        }
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (const Row& row : _blocks[_next]) {
            for (size_t c = 0; c < columns.size(); ++c) {
                assert_cast<ColumnInt64&>(*columns[c]).insert_value(row[c]);
            }
        }
        ++_next;
        return Status::OK();
    }

    const ReadSchema& schema() const override { return *_schema; }

private:
    const ReadSchemaSPtr _schema;
    std::vector<std::vector<Row>> _blocks;
    size_t _next = 0;
};

// Fold the merged Blocks `blocks` with output Blocks of `max_rows` rows at most; the rows of one
// key are told apart by the key when `merged`. Return the rows of every output Block.
std::vector<std::vector<Row>> fold(std::vector<std::vector<Row>> blocks, bool merged,
                                   size_t max_rows, uint64_t* merged_rows) {
    auto schema = make_agg_schema();
    AggKeyBlockIterator iter(std::make_unique<MergedBlocksUtIterator>(schema, std::move(blocks)),
                             merged, max_rows, /*preferred_block_size_bytes=*/0, schema,
                             BeExecVersionManager::get_newest_version(), merged_rows);
    Status st = iter.init();
    EXPECT_TRUE(st.ok()) << st;
    std::vector<std::vector<Row>> output;
    bool eof = !st.ok();
    while (!eof) {
        Block block = schema->create_read_block();
        st = iter.next_block(&block, &eof);
        EXPECT_TRUE(st.ok()) << st;
        if (!st.ok() || eof) {
            EXPECT_EQ(block.rows(), 0);
            break;
        }
        auto& rows = output.emplace_back();
        for (size_t r = 0; r < block.rows(); ++r) {
            rows.push_back({value_at(*block.get_by_position(0).column, r).value(),
                            value_at(*block.get_by_position(1).column, r).value(),
                            value_at(*block.get_by_position(2).column, r).value()});
        }
    }
    return output;
}

} // namespace

TEST(AggKeyBlockIteratorValueAggregatorTest, FoldsEachGroupFromItsRowRange) {
    auto schema = make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM);
    auto aggregator = make_aggregator(*schema);
    Block block = make_block(*schema, {1, 2, 3, 4, 5, 6});
    MutableColumns out = schema->create_read_block().mutate_columns();

    // Groups: rows [0,2) -> 1+2, rows [2,5) -> 3+4+5, row 5 -> 6.
    aggregator->start_block(block);
    aggregator->fold_group(block, 0, 2, out);
    aggregator->fold_group(block, 2, 5, out);
    aggregator->fold_group(block, 5, 6, out);

    ASSERT_EQ(out[1]->size(), 3);
    EXPECT_EQ(value_at(*out[1], 0), 3);
    EXPECT_EQ(value_at(*out[1], 1), 12);
    EXPECT_EQ(value_at(*out[1], 2), 6);
}

// The rows of a key come in the order they were written: REPLACE_IF_NOT_NULL takes the last value
// that is not NULL, and NULL for a group of NULLs only.
TEST(AggKeyBlockIteratorValueAggregatorTest, ReplaceIfNotNullTakesTheLastValueThatIsNotNull) {
    auto schema = make_read_schema(
            FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, /*nullable=*/true);
    auto aggregator = make_aggregator(*schema);
    Block block = make_block(*schema, {1, 2, std::nullopt, std::nullopt, std::nullopt, 3});
    MutableColumns out = schema->create_read_block().mutate_columns();

    // Groups: rows [0,3) -> 2, rows [3,5) -> NULL, row 5 -> 3.
    aggregator->start_block(block);
    aggregator->fold_group(block, 0, 3, out);
    aggregator->fold_group(block, 3, 5, out);
    aggregator->fold_group(block, 5, 6, out);

    ASSERT_EQ(out[1]->size(), 3);
    EXPECT_EQ(value_at(*out[1], 0), 2);
    EXPECT_EQ(value_at(*out[1], 1), std::nullopt);
    EXPECT_EQ(value_at(*out[1], 2), 3);
}

TEST(AggKeyBlockIteratorValueAggregatorTest, NullableSumSkipsNulls) {
    auto schema = make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                                   /*nullable=*/true);
    auto aggregator = make_aggregator(*schema);
    // The first Block holds NULLs, so the nullable function adds row by row; the second holds
    // none and takes the batch path.
    Block with_nulls = make_block(*schema, {1, std::nullopt, 3, std::nullopt, std::nullopt});
    Block without_nulls = make_block(*schema, {10, 20});
    MutableColumns out = schema->create_read_block().mutate_columns();

    aggregator->start_block(with_nulls);
    aggregator->fold_group(with_nulls, 0, 3, out);
    aggregator->fold_group(with_nulls, 3, 5, out);
    aggregator->start_block(without_nulls);
    aggregator->fold_group(without_nulls, 0, 2, out);

    ASSERT_EQ(out[1]->size(), 3);
    EXPECT_EQ(value_at(*out[1], 0), 4);
    EXPECT_EQ(value_at(*out[1], 1), std::nullopt);
    EXPECT_EQ(value_at(*out[1], 2), 30);
}

TEST(AggKeyBlockIteratorValueAggregatorTest,
     OneRowGroupsAreCopiedOnlyForFunctionsReturningTheirInput) {
    for (auto aggregation : {FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL}) {
        auto schema = make_read_schema(aggregation);
        EXPECT_TRUE(make_aggregator(*schema)->returns_single_row_unchanged())
                << static_cast<int>(aggregation);
    }

    auto key = make_column("k", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                           FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0, true);
    auto quantile = make_column("q", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_QUANTILE_UNION,
                                FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, false, 1, false);
    ReadSchema schema(std::vector<TabletColumnPtr> {key, quantile},
                      std::vector<DataTypePtr> {std::make_shared<DataTypeInt64>(),
                                                std::make_shared<DataTypeQuantileState>()});
    EXPECT_FALSE(make_aggregator(schema)->returns_single_row_unchanged());
}

// Every merged Block holds whole key groups, the rows of a key in the order they were written:
// each group becomes one row with the REPLACE value of its last row, the newest one, and the SUM
// of all, also the group that ends a merged Block.
TEST(AggKeyBlockIteratorTest, FoldsEveryKeyGroupOnce) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 10, 1}, {1, 11, 2}, {2, 20, 3}},
                              {{3, 30, 4}, {3, 31, 5}, {3, 32, 6}},
                              {{4, 40, 7}}},
                             /*merged=*/true, /*max_rows=*/100, &merged_rows);
    EXPECT_EQ(output,
              (std::vector<std::vector<Row>> {{{1, 11, 3}, {2, 20, 3}, {3, 32, 15}, {4, 40, 7}}}));
    EXPECT_EQ(merged_rows, 3);
}

// An output Block of two rows at most ends in front of a new key and never splits a group.
TEST(AggKeyBlockIteratorTest, EndsOutputBlocksInFrontOfANewKey) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 10, 1}, {2, 20, 2}, {3, 30, 3}, {3, 31, 4}}, {{4, 40, 5}}},
                             /*merged=*/true, /*max_rows=*/2, &merged_rows);
    EXPECT_EQ(output,
              (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 20, 2}}, {{3, 31, 7}, {4, 40, 5}}}));
    EXPECT_EQ(merged_rows, 1);
}

// Rowsets read one after another share no key: every row is a group of its own.
TEST(AggKeyBlockIteratorTest, KeepsEveryRowOfInputsThatShareNoKey) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 10, 1}, {2, 20, 2}}, {{3, 30, 3}}}, /*merged=*/false,
                             /*max_rows=*/100, &merged_rows);
    EXPECT_EQ(output, (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 20, 2}, {3, 30, 3}}}));
    EXPECT_EQ(merged_rows, 0);
}

} // namespace doris
