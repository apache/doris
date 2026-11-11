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

// VHeapMergeIterator sorts the rows of its children into one stream: by the key columns, each
// ascending or descending, then by the sequence column, then by child order (the older child
// first), in output Blocks of at most block_row_max rows or of the byte budget, past which only
// the rows of the last key go on with `whole_key_groups`, reading on past a child's empty batches.

#include "storage/iterator/heap_merge_iterator.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"

namespace doris {

namespace {

using Row = std::vector<int64_t>;
using Batch = std::vector<Row>;

// BIGINT columns: the key k, then the sequence column s when `with_sequence`, then the value v.
ReadSchemaSPtr make_schema(bool with_sequence) {
    std::vector<TabletColumnPtr> columns;
    auto add = [&](const std::string& name, bool is_key) {
        auto column = std::make_shared<TabletColumn>(
                FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                FieldType::OLAP_FIELD_TYPE_BIGINT, false, static_cast<int32_t>(columns.size()),
                sizeof(int64_t));
        column->set_name(name);
        column->set_is_key(is_key);
        columns.push_back(column);
    };
    add("k", true);
    if (with_sequence) {
        add("s", false);
    }
    add("v", false);
    return std::make_shared<ReadSchema>(std::move(columns));
}

// Hands out `batches`, one per next_batch(), then EndOfFile. An empty batch comes out as an OK
// batch without rows, like the batch of a segment whose predicates filtered out every row.
class BatchesUtIterator final : public RowwiseIterator {
public:
    BatchesUtIterator(const ReadSchema& schema, std::vector<Batch> batches)
            : _schema(schema), _batches(std::move(batches)) {}

    Status next_batch(Block* block) override {
        if (_next == _batches.size()) {
            return Status::EndOfFile("no more batches");
        }
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (const Row& row : _batches[_next]) {
            for (size_t c = 0; c < columns.size(); ++c) {
                assert_cast<ColumnInt64&>(*columns[c]).insert_value(row[c]);
            }
        }
        ++_next;
        return Status::OK();
    }

    const ReadSchema& schema() const override { return _schema; }

private:
    const ReadSchema& _schema;
    std::vector<Batch> _batches;
    size_t _next = 0;
};

// By the key k, ascending (descending with `descending`), NULLs first.
MergeDecision key_decision(bool descending = false) {
    MergeDecision decision;
    decision.keys.emplace_back(0, descending ? -1 : 1, -1);
    return decision;
}

// Merge `children`, the batches of each child, and return the rows of every output Block.
std::vector<std::vector<Row>> merge(const ReadSchemaSPtr& schema,
                                    const std::vector<std::vector<Batch>>& children,
                                    const MergeDecision& decision, uint32_t block_row_max = 4064,
                                    size_t preferred_block_size_bytes = 0) {
    std::vector<RowwiseIteratorUPtr> inputs;
    for (const auto& batches : children) {
        inputs.push_back(std::make_unique<BatchesUtIterator>(*schema, batches));
    }
    auto iter = new_heap_merge_iterator(std::move(inputs), schema, decision);
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    opts.block_row_max = block_row_max;
    opts.preferred_block_size_bytes = preferred_block_size_bytes;
    Status st = iter->init(opts);
    EXPECT_TRUE(st.ok()) << st;

    std::vector<std::vector<Row>> blocks;
    while (st.ok()) {
        Block block = schema->create_read_block();
        st = iter->next_batch(&block);
        if (!st.ok()) {
            EXPECT_EQ(block.rows(), 0);
            break;
        }
        EXPECT_GT(block.rows(), 0);
        std::vector<Row> rows;
        for (size_t r = 0; r < block.rows(); ++r) {
            Row row;
            for (size_t c = 0; c < block.columns(); ++c) {
                row.push_back(block.get_by_position(c).column->get_int(r));
            }
            rows.push_back(std::move(row));
        }
        blocks.push_back(std::move(rows));
    }
    EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st;
    return blocks;
}

std::vector<Row> flatten(const std::vector<std::vector<Row>>& blocks) {
    std::vector<Row> rows;
    for (const auto& block : blocks) {
        rows.insert(rows.end(), block.begin(), block.end());
    }
    return rows;
}

} // namespace

// Rows of one key go by child order, the older child first, in the order they were written; runs
// of a child cross its Block boundaries, and the last child left hands out its rows without
// comparisons.
TEST(HeapMergeIteratorTest, MergesByKeyWithTheOlderChildFirst) {
    auto schema = make_schema(false);
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {3, 30}}, {{5, 50}}},
            {{{1, 11}, {2, 21}}, {{3, 31}, {6, 61}, {7, 71}}},
            {{{4, 40}}},
    };
    EXPECT_EQ(flatten(merge(schema, children, key_decision())), (std::vector<Row> {{1, 10},
                                                                                   {1, 11},
                                                                                   {2, 21},
                                                                                   {3, 30},
                                                                                   {3, 31},
                                                                                   {4, 40},
                                                                                   {5, 50},
                                                                                   {6, 61},
                                                                                   {7, 71}}));
}

// Rows of one key go by the sequence column before child order, ascending: the row with the
// largest value comes last.
TEST(HeapMergeIteratorTest, OrdersRowsOfOneKeyBySequence) {
    auto schema = make_schema(true);
    const std::vector<std::vector<Batch>> children = {
            {{{1, 5, 10}, {2, 1, 20}}},
            {{{1, 3, 11}, {2, 1, 21}}},
    };
    MergeDecision decision = key_decision();
    decision.sequence = SortColumnDescription(1, 1, -1);
    EXPECT_EQ(flatten(merge(schema, children, decision)),
              (std::vector<Row> {{1, 3, 11}, {1, 5, 10}, {2, 1, 20}, {2, 1, 21}}));
}

TEST(HeapMergeIteratorTest, ReverseMergesKeysDescending) {
    auto schema = make_schema(false);
    const std::vector<std::vector<Batch>> children = {
            {{{5, 50}, {3, 30}}, {{1, 10}}},
            {{{4, 41}, {3, 31}, {2, 21}}},
    };
    EXPECT_EQ(flatten(merge(schema, children, key_decision(/*descending=*/true))),
              (std::vector<Row> {{5, 50}, {4, 41}, {3, 30}, {3, 31}, {2, 21}, {1, 10}}));
}

TEST(HeapMergeIteratorTest, KeepsToTheRowLimit) {
    auto schema = make_schema(false);
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {2, 20}, {3, 30}, {4, 40}}},
            {{{1, 11}, {5, 51}}, {{6, 61}, {7, 71}}},
    };
    const auto blocks = merge(schema, children, key_decision(), /*block_row_max=*/3);
    ASSERT_EQ(blocks.size(), 3);
    EXPECT_EQ(blocks[0].size(), 3);
    EXPECT_EQ(blocks[1].size(), 3);
    EXPECT_EQ(blocks[2].size(), 2);
    EXPECT_EQ(flatten(blocks),
              (std::vector<Row> {
                      {1, 10}, {1, 11}, {2, 20}, {3, 30}, {4, 40}, {5, 51}, {6, 61}, {7, 71}}));
}

// With `whole_key_groups`, an output Block goes past the row limit with the rows of its last key
// only, so the rows of one key never land in two Blocks; without it, the Block ends at the limit.
TEST(HeapMergeIteratorTest, KeepsTheRowsOfOneKeyInOneBlock) {
    auto schema = make_schema(false);
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {2, 20}}, {{3, 30}}},
            {{{2, 21}, {3, 31}}},
            {{{2, 22}, {4, 42}}},
    };
    MergeDecision decision = key_decision();
    decision.whole_key_groups = true;
    EXPECT_EQ(merge(schema, children, decision, /*block_row_max=*/2),
              (std::vector<std::vector<Row>> {
                      {{1, 10}, {2, 20}, {2, 21}, {2, 22}}, {{3, 30}, {3, 31}}, {{4, 42}}}));

    EXPECT_EQ(merge(schema, children, key_decision(), /*block_row_max=*/2),
              (std::vector<std::vector<Row>> {
                      {{1, 10}, {2, 20}}, {{2, 21}, {2, 22}}, {{3, 30}, {3, 31}}, {{4, 42}}}));
}

// With a byte budget, an output Block holds the budget divided by the average row size of the
// children's Blocks: two BIGINT columns take 16 bytes a row, so 48 bytes give 3 rows.
TEST(HeapMergeIteratorTest, KeepsToTheByteBudget) {
    const bool adaptive_batch_size = config::enable_adaptive_batch_size;
    config::enable_adaptive_batch_size = true;
    Defer restore {[&]() { config::enable_adaptive_batch_size = adaptive_batch_size; }};
    auto schema = make_schema(false);
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {3, 30}, {5, 50}, {7, 70}}},
            {{{2, 20}, {4, 40}, {6, 60}}},
    };
    const auto blocks = merge(schema, children, key_decision(), /*block_row_max=*/4064,
                              /*preferred_block_size_bytes=*/48);
    ASSERT_EQ(blocks.size(), 3);
    EXPECT_EQ(blocks[0].size(), 3);
    EXPECT_EQ(blocks[1].size(), 3);
    EXPECT_EQ(blocks[2].size(), 1);
    EXPECT_EQ(flatten(blocks).size(), 7);
}

// An empty batch of a child does not end it: the merge reads on to its next batch. A child
// without rows is left out.
TEST(HeapMergeIteratorTest, ReadsOnAfterEmptyBatches) {
    auto schema = make_schema(false);
    const std::vector<std::vector<Batch>> children = {
            {{}, {}, {{2, 20}}, {}, {{4, 40}}},
            {{{1, 11}, {3, 31}}},
            {{}},
    };
    EXPECT_EQ(flatten(merge(schema, children, key_decision())),
              (std::vector<Row> {{1, 11}, {2, 20}, {3, 31}, {4, 40}}));
}

// A key column must be one, and every compared column must be in the merged Blocks: otherwise
// the read fails instead of merging by the wrong column.
TEST(HeapMergeIteratorTest, RejectsColumnsItCannotCompareBy) {
    auto schema = make_schema(false);
    auto init = [&](const MergeDecision& decision) {
        std::vector<RowwiseIteratorUPtr> inputs;
        inputs.push_back(
                std::make_unique<BatchesUtIterator>(*schema, std::vector<Batch> {{{1, 10}}}));
        auto iter = new_heap_merge_iterator(std::move(inputs), schema, decision);
        OlapReaderStatistics stats;
        StorageReadOptions opts(stats);
        return iter->init(opts);
    };
    for (const std::vector<int>& key_columns : {std::vector<int> {1}, std::vector<int> {0, 2}}) {
        MergeDecision decision;
        for (int column : key_columns) {
            decision.keys.emplace_back(column, 1, -1);
        }
        Status st = init(decision);
        EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>()) << st;
    }
    MergeDecision decision = key_decision();
    decision.sequence = SortColumnDescription(2, 1, -1);
    Status st = init(decision);
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>()) << st;
}

} // namespace doris
