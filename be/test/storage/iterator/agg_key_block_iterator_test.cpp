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

// AggKeyBlockIterator::ValueAggregator folds key groups given as row counts of input Blocks: one
// result row per group, NULLs skipped by the nullable functions group by group, the first value
// that is not NULL for REPLACE_IF_NOT_NULL, and a group that goes on from one input Block to the
// next. AggKeyBlockIterator turns every key into one row, the keys and the REPLACE values of its
// first row, the newest, and the other values folded, also a key that goes on from one input
// Block to the next; it copies keys that have one row as they are, and ends an output Block in
// front of a new key at its row limit, or at its byte budget counted as rows.

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

// Hands out the input Blocks `blocks`, one per next_batch(), then EndOfFile.
class InputBlocksUtIterator final : public RowwiseIterator {
public:
    InputBlocksUtIterator(ReadSchemaSPtr schema, std::vector<std::vector<Row>> blocks)
            : _schema(std::move(schema)), _blocks(std::move(blocks)) {}

    Status next_batch(Block* block) override {
        if (_next == _blocks.size()) {
            return Status::EndOfFile("no more input blocks");
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

// Fold the input Blocks `blocks`, the rows of a key newest first, into output Blocks of `max_rows`
// rows and about `preferred_block_size_bytes` bytes at most. Return the rows of every output Block.
std::vector<std::vector<Row>> fold(std::vector<std::vector<Row>> blocks, size_t max_rows,
                                   size_t preferred_block_size_bytes, uint64_t* merged_rows) {
    auto schema = make_agg_schema();
    AggKeyBlockIterator iter(std::make_unique<InputBlocksUtIterator>(schema, std::move(blocks)),
                             /*key_ordinals=*/ {0}, max_rows, preferred_block_size_bytes, schema,
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

TEST(AggKeyBlockIteratorValueAggregatorTest, FoldsEachGroupFromItsRowCount) {
    auto schema = make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM);
    auto aggregator = make_aggregator(*schema);
    Block block = make_block(*schema, {1, 2, 3, 4, 5, 6});
    MutableColumns out = schema->create_read_block().mutate_columns();

    // Groups: rows [0,2) -> 1+2, rows [2,5) -> 3+4+5, row 5 -> 6.
    const std::vector<uint32_t> group_rows = {2, 3, 1};
    aggregator->fold_groups(block, 0, group_rows, out);

    ASSERT_EQ(out[1]->size(), 3);
    EXPECT_EQ(value_at(*out[1], 0), 3);
    EXPECT_EQ(value_at(*out[1], 1), 12);
    EXPECT_EQ(value_at(*out[1], 2), 6);
}

// The rows of a key come newest first: REPLACE_IF_NOT_NULL takes the first value that is not NULL,
// and NULL for a group of NULLs only.
TEST(AggKeyBlockIteratorValueAggregatorTest, ReplaceIfNotNullTakesTheFirstValueThatIsNotNull) {
    auto schema = make_read_schema(
            FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, /*nullable=*/true);
    auto aggregator = make_aggregator(*schema);
    Block block = make_block(*schema, {std::nullopt, 2, 1, std::nullopt, std::nullopt, 3});
    MutableColumns out = schema->create_read_block().mutate_columns();

    // Groups: rows [0,3) -> 2, rows [3,5) -> NULL, row 5 -> 3.
    const std::vector<uint32_t> group_rows = {3, 2, 1};
    aggregator->fold_groups(block, 0, group_rows, out);

    ASSERT_EQ(out[1]->size(), 3);
    EXPECT_EQ(value_at(*out[1], 0), 2);
    EXPECT_EQ(value_at(*out[1], 1), std::nullopt);
    EXPECT_EQ(value_at(*out[1], 2), 3);
}

// Each group is checked for NULLs on its own rows: the nullable function adds the rows of a group
// with a NULL one by one, and those of a group without NULLs as one batch. MIN shows a NULL added
// as a value, since a NULL row holds 0 underneath; the NULL is the last row of the first group and
// the first row of the second.
TEST(AggKeyBlockIteratorValueAggregatorTest, NullableMinSkipsTheNullsOfEachGroup) {
    auto schema = make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN,
                                   /*nullable=*/true);
    auto aggregator = make_aggregator(*schema);
    Block block = make_block(
            *schema, {5, 7, std::nullopt, std::nullopt, 6, 8, 9, std::nullopt, std::nullopt});
    MutableColumns out = schema->create_read_block().mutate_columns();

    // Groups: rows [0,3) -> 5, rows [3,5) -> 6, rows [5,7) -> 8, rows [7,9) -> NULL.
    const std::vector<uint32_t> group_rows = {3, 2, 2, 2};
    aggregator->fold_groups(block, 0, group_rows, out);

    ASSERT_EQ(out[1]->size(), 4);
    EXPECT_EQ(value_at(*out[1], 0), 5);
    EXPECT_EQ(value_at(*out[1], 1), 6);
    EXPECT_EQ(value_at(*out[1], 2), 8);
    EXPECT_EQ(value_at(*out[1], 3), std::nullopt);
}

// An open group takes rows from several input Blocks. REPLACE_IF_NOT_NULL keeps a copy of its
// value: the Block it came from is refilled with other values before the group is closed.
TEST(AggKeyBlockIteratorValueAggregatorTest, AnOpenGroupKeepsItsValueAcrossInputBlocks) {
    auto schema = make_read_schema(
            FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, /*nullable=*/true);
    auto aggregator = make_aggregator(*schema);
    Block block = make_block(*schema, {std::nullopt, 4});
    aggregator->add_to_open_group(block, 0, 2);
    block = make_block(*schema, {9, 9});
    aggregator->add_to_open_group(block, 0, 1);
    MutableColumns out = schema->create_read_block().mutate_columns();
    aggregator->close_open_group(out);

    ASSERT_EQ(out[1]->size(), 1);
    EXPECT_EQ(value_at(*out[1], 0), 4);

    auto sum_schema = make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM);
    auto sum = make_aggregator(*sum_schema);
    sum->add_to_open_group(make_block(*sum_schema, {1, 2}), 0, 2);
    sum->add_to_open_group(make_block(*sum_schema, {3}), 0, 1);
    MutableColumns sum_out = sum_schema->create_read_block().mutate_columns();
    sum->close_open_group(sum_out);
    ASSERT_EQ(sum_out[1]->size(), 1);
    EXPECT_EQ(value_at(*sum_out[1], 0), 6);
}

// The `_reader` storage function of every aggregation a folded column can have resolves.
// make_aggregator() expects init() to succeed.
TEST(AggKeyBlockIteratorValueAggregatorTest, ResolvesTheFunctionOfEveryAggregation) {
    for (auto aggregation : {FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL}) {
        SCOPED_TRACE(static_cast<int>(aggregation));
        make_aggregator(*make_read_schema(aggregation));
    }

    auto key = make_column("k", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                           FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0, true);
    auto quantile = make_column("q", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_QUANTILE_UNION,
                                FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, false, 1, false);
    ReadSchema schema(std::vector<TabletColumnPtr> {key, quantile},
                      std::vector<DataTypePtr> {std::make_shared<DataTypeInt64>(),
                                                std::make_shared<DataTypeQuantileState>()});
    make_aggregator(schema);
}

// Every key becomes one row with the REPLACE value of its first row, the newest one, and the SUM
// of all.
TEST(AggKeyBlockIteratorTest, FoldsEveryKeyGroupOnce) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 11, 2}, {1, 10, 1}, {2, 20, 3}},
                              {{3, 32, 6}, {3, 31, 5}, {3, 30, 4}},
                              {{4, 40, 7}}},
                             /*max_rows=*/100, /*preferred_block_size_bytes=*/0, &merged_rows);
    EXPECT_EQ(output,
              (std::vector<std::vector<Row>> {{{1, 11, 3}, {2, 20, 3}, {3, 32, 15}, {4, 40, 7}}}));
    EXPECT_EQ(merged_rows, 3);
}

// The merge may split the rows of a key between its Blocks: key 2 goes on through three input
// Blocks and still comes out once, with the REPLACE value of its first row.
TEST(AggKeyBlockIteratorTest, CarriesAKeyFromOneInputBlockToTheNext) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 12, 1}, {2, 22, 2}}, {{2, 21, 3}}, {{2, 20, 4}, {3, 30, 5}}},
                             /*max_rows=*/100, /*preferred_block_size_bytes=*/0, &merged_rows);
    EXPECT_EQ(output, (std::vector<std::vector<Row>> {{{1, 12, 1}, {2, 22, 9}, {3, 30, 5}}}));
    EXPECT_EQ(merged_rows, 2);
}

// A key of one row that ends an input Block is copied as it is when the next input Block starts
// with another key, and folded when the next one goes on with it.
TEST(AggKeyBlockIteratorTest, KeepsAOneRowKeyAtTheEndOfAnInputBlockAsItIs) {
    uint64_t merged_rows = 0;
    EXPECT_EQ(fold({{{1, 10, 1}, {2, 20, 2}}, {{3, 30, 3}}}, /*max_rows=*/100,
                   /*preferred_block_size_bytes=*/0, &merged_rows),
              (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 20, 2}, {3, 30, 3}}}));
    EXPECT_EQ(merged_rows, 0);
    EXPECT_EQ(fold({{{1, 10, 1}, {2, 21, 2}}, {{2, 20, 3}, {3, 30, 4}}}, /*max_rows=*/100,
                   /*preferred_block_size_bytes=*/0, &merged_rows),
              (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 21, 5}, {3, 30, 4}}}));
    EXPECT_EQ(merged_rows, 1);
}

// An output Block of two keys at most ends in front of the third key, in the middle of an input
// Block, which the next output Block goes on with.
TEST(AggKeyBlockIteratorTest, FillsOutputBlocksToTheRowLimit) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 10, 1}, {2, 20, 2}, {3, 30, 3}, {4, 41, 4}}, {{4, 40, 5}}},
                             /*max_rows=*/2, /*preferred_block_size_bytes=*/0, &merged_rows);
    EXPECT_EQ(output,
              (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 20, 2}}, {{3, 30, 3}, {4, 41, 9}}}));
    EXPECT_EQ(merged_rows, 1);
}

// An output Block that holds its two keys still takes the rows of its last key from the next
// input Block, so that the key comes out once.
TEST(AggKeyBlockIteratorTest, EndsAnOutputBlockInFrontOfANewKeyOnly) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 10, 1}, {2, 22, 2}}, {{2, 21, 3}, {2, 20, 4}, {3, 30, 5}}},
                             /*max_rows=*/2, /*preferred_block_size_bytes=*/0, &merged_rows);
    EXPECT_EQ(output, (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 22, 9}}, {{3, 30, 5}}}));
    EXPECT_EQ(merged_rows, 2);
}

// The byte budget counts as rows: 24 bytes a row in the input Block give 60 bytes two keys. A
// budget smaller than a row gives every key an output Block of its own.
TEST(AggKeyBlockIteratorTest, EndsOutputBlocksAtTheByteBudget) {
    const std::vector<std::vector<Row>> blocks = {{{1, 13, 4}, {1, 12, 3}, {1, 11, 2}, {1, 10, 1}},
                                                  {{2, 21, 6}, {2, 20, 5}},
                                                  {{3, 31, 8}, {3, 30, 7}}};
    uint64_t merged_rows = 0;
    EXPECT_EQ(fold(blocks, /*max_rows=*/100, /*preferred_block_size_bytes=*/60, &merged_rows),
              (std::vector<std::vector<Row>> {{{1, 13, 10}, {2, 21, 11}}, {{3, 31, 15}}}));
    EXPECT_EQ(merged_rows, 5);
    EXPECT_EQ(fold(blocks, /*max_rows=*/100, /*preferred_block_size_bytes=*/1, &merged_rows),
              (std::vector<std::vector<Row>> {{{1, 13, 10}}, {{2, 21, 11}}, {{3, 31, 15}}}));
}

// Keys that have one row are copied as they are, also right in front of a key whose rows are
// folded.
TEST(AggKeyBlockIteratorTest, CopiesKeysThatHaveOneRowAsTheyAre) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 10, 1}, {2, 20, 2}, {3, 31, 4}, {3, 30, 3}}, {{4, 40, 5}}},
                             /*max_rows=*/100, /*preferred_block_size_bytes=*/0, &merged_rows);
    EXPECT_EQ(output,
              (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 20, 2}, {3, 31, 7}, {4, 40, 5}}}));
    EXPECT_EQ(merged_rows, 1);
}

} // namespace doris
