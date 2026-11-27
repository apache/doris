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

// VHeapMergeIterator sorts the rows of its children by the key columns, the newer child first, in
// output Blocks of block_row_max rows or of the byte budget, also inside the rows of one key, and
// reads on past a child's empty batches. A child's Block whose rows all go first passes whole, by
// its columns.

#include "storage/iterator/heap_merge_iterator.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "runtime/runtime_state.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

using Row = std::vector<int64_t>;
using Batch = std::vector<Row>;

// BIGINT columns: the key k, then the value v.
ReadSchemaSPtr make_schema() {
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
    add("v", false);
    return std::make_shared<ReadSchema>(std::move(columns));
}

// Hands out `batches`, one per next_batch(), then `end`. An empty batch comes out as an OK batch
// without rows, like the batch of a segment whose predicates filtered out every row.
class BatchesUtIterator final : public RowwiseIterator {
public:
    BatchesUtIterator(const ReadSchema& schema, std::vector<Batch> batches,
                      Status end = Status::EndOfFile("no more batches"))
            : _schema(schema), _batches(std::move(batches)), _end(std::move(end)) {}

    Status next_batch(Block* block) override {
        if (_next == _batches.size()) {
            return _end;
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
    const Status _end;
    size_t _next = 0;
};

// Merge `children`, the batches of each child, and return the rows of every output Block. With
// `taken`, also whether each output Block took the columns of a child's Block, instead of getting
// the rows copied into the columns it was passed.
std::vector<std::vector<Row>> merge(const ReadSchemaSPtr& schema,
                                    const std::vector<std::vector<Batch>>& children,
                                    uint32_t block_row_max = 4064,
                                    size_t preferred_block_size_bytes = 0,
                                    std::vector<bool>* taken = nullptr) {
    std::vector<RowwiseIteratorUPtr> inputs;
    for (const auto& batches : children) {
        inputs.push_back(std::make_unique<BatchesUtIterator>(*schema, batches));
    }
    auto iter = std::make_unique<VHeapMergeIterator>(std::move(inputs), schema);
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    opts.block_row_max = block_row_max;
    opts.preferred_block_size_bytes = preferred_block_size_bytes;
    Status st = iter->init(opts);
    EXPECT_TRUE(st.ok()) << st;

    std::vector<std::vector<Row>> blocks;
    while (st.ok()) {
        Block block = schema->create_read_block();
        const IColumn* passed = block.get_by_position(0).column.get();
        st = iter->next_batch(&block);
        if (!st.ok()) {
            EXPECT_EQ(block.rows(), 0);
            break;
        }
        EXPECT_GT(block.rows(), 0);
        if (taken != nullptr) {
            taken->push_back(block.get_by_position(0).column.get() != passed);
        }
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

// Rows of one key go by child order, the newer child first; runs of a child cross its Block
// boundaries, and the last child left hands out its rows without comparisons.
TEST(HeapMergeIteratorTest, MergesByKeyWithTheNewerChildFirst) {
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {3, 30}}, {{5, 50}}},
            {{{1, 11}, {2, 21}}, {{3, 31}, {6, 61}, {7, 71}}},
            {{{4, 40}}},
    };
    EXPECT_EQ(flatten(merge(schema, children)), (std::vector<Row> {{1, 11},
                                                                   {1, 10},
                                                                   {2, 21},
                                                                   {3, 31},
                                                                   {3, 30},
                                                                   {4, 40},
                                                                   {5, 50},
                                                                   {6, 61},
                                                                   {7, 71}}));
}

TEST(HeapMergeIteratorTest, KeepsToTheRowLimit) {
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {2, 20}, {3, 30}, {4, 40}}},
            {{{1, 11}, {5, 51}}, {{6, 61}, {7, 71}}},
    };
    const auto blocks = merge(schema, children, /*block_row_max=*/3);
    ASSERT_EQ(blocks.size(), 3);
    EXPECT_EQ(blocks[0].size(), 3);
    EXPECT_EQ(blocks[1].size(), 3);
    EXPECT_EQ(blocks[2].size(), 2);
    EXPECT_EQ(flatten(blocks),
              (std::vector<Row> {
                      {1, 11}, {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 51}, {6, 61}, {7, 71}}));
}

// An output Block ends at its row limit also inside the rows of one key: key 2, newest row first,
// is split between the first two Blocks.
TEST(HeapMergeIteratorTest, EndsABlockAtTheRowLimitInsideTheRowsOfAKey) {
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {2, 20}}, {{3, 30}}},
            {{{2, 21}, {3, 31}}},
            {{{2, 22}, {4, 42}}},
    };
    EXPECT_EQ(merge(schema, children, /*block_row_max=*/2),
              (std::vector<std::vector<Row>> {
                      {{1, 10}, {2, 22}}, {{2, 21}, {2, 20}}, {{3, 31}}, {{3, 30}}, {{4, 42}}}));
}

// An output Block that starts with a child's Block whose rows all go before the next child takes
// that Block's columns instead of copying its rows, and nothing else; an output Block that holds
// rows ends in front of such a Block. A Block that the next child cuts into is copied.
TEST(HeapMergeIteratorTest, TakesTheColumnsOfABlockThatGoesFirstWhole) {
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {2, 20}}, {{3, 30}, {9, 90}}},
            {{{4, 41}}},
    };
    std::vector<bool> taken;
    EXPECT_EQ(
            merge(schema, children, /*block_row_max=*/2,
                  /*preferred_block_size_bytes=*/0, &taken),
            (std::vector<std::vector<Row>> {{{1, 10}, {2, 20}}, {{3, 30}}, {{4, 41}}, {{9, 90}}}));
    EXPECT_EQ(taken, (std::vector<bool> {true, false, true, false}));
}

// A child's Block whose rows all go before the next child is not split: an output Block that holds
// rows ends in front of it, and the next output Block takes it.
TEST(HeapMergeIteratorTest, EndsABlockInFrontOfABlockThatGoesFirstWhole) {
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> children = {
            {{{2, 20}, {3, 30}, {4, 40}}},
            {{{1, 11}, {9, 91}}},
    };
    std::vector<bool> taken;
    EXPECT_EQ(merge(schema, children, /*block_row_max=*/3,
                    /*preferred_block_size_bytes=*/0, &taken),
              (std::vector<std::vector<Row>> {{{1, 11}}, {{2, 20}, {3, 30}, {4, 40}}, {{9, 91}}}));
    EXPECT_EQ(taken, (std::vector<bool> {false, true, false}));
}

// A run that reaches the end of its child's Block is found by the Block's last row: child 0 runs
// 3, 4, 5 to the end of its Block in the first merge. In the second, its run stops at 5, in front
// of 6, and is scanned row by row.
TEST(HeapMergeIteratorTest, FindsTheEndOfARunOfAChild) {
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> to_the_end = {
            {{{1, 10}, {3, 30}, {4, 40}, {5, 50}}},
            {{{2, 20}, {9, 90}}},
    };
    EXPECT_EQ(flatten(merge(schema, to_the_end)),
              (std::vector<Row> {{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {9, 90}}));
    const std::vector<std::vector<Batch>> inside = {
            {{{1, 10}, {3, 30}, {4, 40}, {5, 50}, {8, 80}}},
            {{{2, 20}, {6, 60}, {9, 90}}},
    };
    EXPECT_EQ(flatten(merge(schema, inside)),
              (std::vector<Row> {
                      {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}, {8, 80}, {9, 90}}));
}

// With a byte budget, an output Block holds the budget divided by the average row size of the
// children's Blocks: two BIGINT columns take 16 bytes a row, so 48 bytes give 3 rows.
TEST(HeapMergeIteratorTest, KeepsToTheByteBudget) {
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {3, 30}, {5, 50}, {7, 70}}},
            {{{2, 20}, {4, 40}, {6, 60}}},
    };
    const auto blocks = merge(schema, children, /*block_row_max=*/4064,
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
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> children = {
            {{}, {}, {{2, 20}}, {}, {{4, 40}}},
            {{{1, 11}, {3, 31}}},
            {{}},
    };
    EXPECT_EQ(flatten(merge(schema, children)),
              (std::vector<Row> {{1, 11}, {2, 20}, {3, 31}, {4, 40}}));
}

// A child's Block that goes first but has more rows than an output Block can take is copied up to
// the row limit, and the rest of it goes on in the next output Block.
TEST(HeapMergeIteratorTest, CopiesABlockThatGoesFirstButExceedsTheRowLimit) {
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {2, 20}, {3, 30}}},
            {{{9, 90}}},
    };
    std::vector<bool> taken;
    EXPECT_EQ(
            merge(schema, children, /*block_row_max=*/2, /*preferred_block_size_bytes=*/0, &taken),
            (std::vector<std::vector<Row>> {{{1, 10}, {2, 20}}, {{3, 30}}, {{9, 90}}}));
    EXPECT_EQ(taken, (std::vector<bool> {false, false, true}));
}

// A run of a child that is still going at the row limit ends there: child 0 runs 1, 2 and would go
// on to 4 but for the row limit of 2.
TEST(HeapMergeIteratorTest, EndsARunOfAChildAtTheRowLimit) {
    auto schema = make_schema();
    const std::vector<std::vector<Batch>> children = {
            {{{1, 10}, {2, 20}, {4, 40}}},
            {{{3, 30}}},
    };
    EXPECT_EQ(merge(schema, children, /*block_row_max=*/2),
              (std::vector<std::vector<Row>> {{{1, 10}, {2, 20}}, {{3, 30}}, {{4, 40}}}));
}

// An error of a child, such as a cancelled read, comes out of the merge: from init(), which reads
// the first Block of every child, and from next_batch() when it reads the next Block of a child,
// after the child's Block went out whole or after its last rows were copied.
TEST(HeapMergeIteratorTest, PassesOnTheErrorOfAChild) {
    auto schema = make_schema();
    auto make_merge = [&](std::vector<Batch> first, Status first_end, std::vector<Batch> second) {
        std::vector<RowwiseIteratorUPtr> children;
        children.push_back(
                std::make_unique<BatchesUtIterator>(*schema, std::move(first), first_end));
        children.push_back(std::make_unique<BatchesUtIterator>(*schema, std::move(second)));
        return std::make_unique<VHeapMergeIterator>(std::move(children), schema);
    };
    auto next_until_error = [&](VHeapMergeIterator& iter) {
        Status st;
        while (st.ok()) {
            Block block = schema->create_read_block();
            st = iter.next_batch(&block);
        }
        return st;
    };
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    opts.block_row_max = 4064;

    auto on_init = make_merge({}, Status::Cancelled("cancelled"), {{{1, 10}}});
    EXPECT_TRUE(on_init->init(opts).is<ErrorCode::CANCELLED>());

    // Child 0's Block {1} goes out whole, then reading its next Block fails.
    auto after_whole_block = make_merge({{{1, 10}}}, Status::Cancelled("cancelled"), {{{2, 20}}});
    ASSERT_TRUE(after_whole_block->init(opts).ok());
    EXPECT_TRUE(next_until_error(*after_whole_block).is<ErrorCode::CANCELLED>());

    // Child 0's rows 1 and 3 are copied around child 1's 2, then reading its next Block fails.
    auto after_copied_rows =
            make_merge({{{1, 10}, {3, 30}}}, Status::Cancelled("cancelled"), {{{2, 20}}});
    ASSERT_TRUE(after_copied_rows->init(opts).ok());
    EXPECT_TRUE(next_until_error(*after_copied_rows).is<ErrorCode::CANCELLED>());
}

// The merge checks whether the query is cancelled each time it reads a Block of a child: it merges
// as usual while the query runs, and returns the reason once the query is cancelled.
TEST(HeapMergeIteratorTest, StopsOnceTheQueryIsCancelled) {
    auto schema = make_schema();
    std::vector<RowwiseIteratorUPtr> children;
    children.push_back(std::make_unique<BatchesUtIterator>(
            *schema, std::vector<Batch> {{{1, 10}}, {{3, 30}}, {{5, 50}}}));
    children.push_back(
            std::make_unique<BatchesUtIterator>(*schema, std::vector<Batch> {{{2, 20}}}));
    VHeapMergeIterator iter(std::move(children), schema);
    RuntimeState runtime_state;
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    opts.block_row_max = 4064;
    opts.runtime_state = &runtime_state;
    ASSERT_TRUE(iter.init(opts).ok());
    Block block = schema->create_read_block();
    ASSERT_TRUE(iter.next_batch(&block).ok());
    EXPECT_EQ(block.rows(), 1);

    // Child 0 reads {5} after its Block {3} goes out.
    runtime_state.cancel(Status::Cancelled("query cancelled"));
    Status st;
    while (st.ok()) {
        Block next = schema->create_read_block();
        st = iter.next_batch(&next);
    }
    EXPECT_TRUE(st.is<ErrorCode::CANCELLED>()) << st;
}

namespace {

std::vector<Row> merge_two_children(const ReadSchemaSPtr& schema, std::vector<Batch> first,
                                    std::vector<Batch> second, HeapMergeOrder order) {
    std::vector<RowwiseIteratorUPtr> children;
    children.push_back(std::make_unique<BatchesUtIterator>(*schema, std::move(first)));
    children.push_back(std::make_unique<BatchesUtIterator>(*schema, std::move(second)));
    VHeapMergeIterator iter(std::move(children), schema, order);
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    opts.block_row_max = 4064;
    DORIS_CHECK(iter.init(opts).ok());
    std::vector<Row> out;
    while (true) {
        Block block = schema->create_read_block();
        Status st = iter.next_batch(&block);
        if (st.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
        DORIS_CHECK(st.ok());
        for (size_t r = 0; r < block.rows(); ++r) {
            out.push_back({block.get_by_position(0).column->get_int(r),
                           block.get_by_position(1).column->get_int(r)});
        }
    }
    return out;
}

} // namespace

TEST(HeapMergeIteratorTest, SupportsExplicitSortOrderFactories) {
    auto schema = make_schema();
    // Older child first when keys match.
    EXPECT_EQ(merge_two_children(schema, {{{1, 10}, {1, 20}, {2, 5}}}, {{{1, 20}, {1, 15}, {2, 9}}},
                                 HeapMergeOrder::by_all_keys(SortDirection::ASC)),
              (std::vector<Row> {{1, 10}, {1, 20}, {1, 20}, {1, 15}, {2, 5}, {2, 9}}));

    // Same-key sort column ascending (like TSO) with older child first on ties.
    EXPECT_EQ(merge_two_children(schema, {{{1, 10}, {1, 20}}}, {{{1, 15}, {1, 20}}},
                                 HeapMergeOrder::by_all_keys_and_same_key_col(1, SortDirection::ASC,
                                                                              SortDirection::ASC)),
              (std::vector<Row> {{1, 10}, {1, 15}, {1, 20}, {1, 20}}));

    // Same-key sort column descending (like sequence column) with newer child first on ties.
    EXPECT_EQ(merge_two_children(schema, {{{1, 30}, {1, 10}}}, {{{1, 20}, {1, 10}}},
                                 HeapMergeOrder::by_all_keys_and_same_key_col(
                                         1, SortDirection::DESC, SortDirection::DESC)),
              (std::vector<Row> {{1, 30}, {1, 20}, {1, 10}, {1, 10}}));
}

} // namespace doris
