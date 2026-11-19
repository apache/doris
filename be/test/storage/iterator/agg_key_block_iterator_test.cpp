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

// AggKeyBlockAggregator folds the rows of each key with the `_reader` function of each value
// column: REPLACE_IF_NOT_NULL takes the first value that is not NULL, nullable MIN skips the NULLs
// key by key, and the states of the last key of a source keep their values once the next source
// takes its place. It resolves the function of every aggregation, carries the open key to the
// next source and to the next batch, and takes rows for one key only when they are equal on every
// key column.
// AggKeyBlockIterator, which feeds it whole input Blocks, turns every key into one row, also a key
// that goes on from one input Block to the next, and returns the complete keys of a batch once it
// holds more keys than its row limit, or its byte budget counted as rows, even when every input
// Block goes on with the last key of the one before.

#include "storage/iterator/agg_key_block_iterator.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
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
#include "core/data_type/data_type_string.h"
#include "storage/iterator/agg_key_block_aggregator.h"
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

std::optional<int64_t> value_at(const IColumn& column, size_t row) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        if (nullable->is_null_at(row)) {
            return std::nullopt;
        }
        return assert_cast<const ColumnInt64&>(nullable->get_nested_column()).get_data()[row];
    }
    return assert_cast<const ColumnInt64&>(column).get_data()[row];
}

// A row of the k, v schema above: the key and the value, NULL where nullopt.
using KeyValue = std::pair<int64_t, std::optional<int64_t>>;

// Put `rows` of the k, v schema into `block` in place of its rows, in the columns it has, as
// AggKeyBlockIterator refills its input Block.
void refill(Block* block, const std::vector<KeyValue>& rows) {
    block->clear_column_data();
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (const auto& [key, value] : rows) {
        assert_cast<ColumnInt64&>(*columns[0]).insert_value(key);
        if (auto* nullable = check_and_get_column<ColumnNullable>(*columns[1])) {
            nullable->get_null_map_data().push_back(value.has_value() ? 0 : 1);
            assert_cast<ColumnInt64&>(nullable->get_nested_column())
                    .insert_value(value.value_or(0));
        } else {
            assert_cast<ColumnInt64&>(*columns[1]).insert_value(*value);
        }
    }
}

// Aggregate `sources`, the rows of one input Block each, of the k, v schema `schema` into one
// batch, and return the value of each of its rows. The sources take turns in one Block, so a state
// that kept a pointer into a source would see the values of the next one.
std::vector<std::optional<int64_t>> aggregate_values(
        const ReadSchemaSPtr& schema, const std::vector<std::vector<KeyValue>>& sources) {
    uint64_t merged_rows = 0;
    AggKeyBlockAggregator aggregator(schema, &merged_rows);
    const Status st = aggregator.init(BeExecVersionManager::get_newest_version());
    EXPECT_TRUE(st.ok()) << st;
    if (!st.ok()) {
        return {};
    }
    Block source = schema->create_read_block();
    for (const auto& rows : sources) {
        refill(&source, rows);
        aggregator.aggregate(source);
    }
    Block batch = schema->create_read_block();
    aggregator.output_all_keys(&batch);
    std::vector<std::optional<int64_t>> values;
    for (size_t row = 0; row < batch.rows(); ++row) {
        values.push_back(value_at(*batch.get_by_position(1).column, row));
    }
    return values;
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

// A Block of the k, r, s schema holding `rows`.
Block make_rows(const ReadSchema& schema, const std::vector<Row>& rows) {
    Block block = schema.create_read_block();
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (const Row& row : rows) {
        for (size_t c = 0; c < columns.size(); ++c) {
            assert_cast<ColumnInt64&>(*columns[c]).insert_value(row[c]);
        }
    }
    return block;
}

// The rows of a Block of the k, r, s schema.
std::vector<Row> rows_of(const Block& block) {
    std::vector<Row> rows;
    for (size_t r = 0; r < block.rows(); ++r) {
        rows.push_back({value_at(*block.get_by_position(0).column, r).value(),
                        value_at(*block.get_by_position(1).column, r).value(),
                        value_at(*block.get_by_position(2).column, r).value()});
    }
    return rows;
}

// Hands out the input Blocks `blocks`, one per next_batch(), then `end`.
class InputBlocksUtIterator final : public RowwiseIterator {
public:
    InputBlocksUtIterator(ReadSchemaSPtr schema, std::vector<std::vector<Row>> blocks,
                          Status end = Status::EndOfFile("no more input blocks"))
            : _schema(std::move(schema)), _blocks(std::move(blocks)), _end(std::move(end)) {}

    Status next_batch(Block* block) override {
        if (_next == _blocks.size()) {
            return _end;
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
    const Status _end;
    size_t _next = 0;
};

// Fold the input Blocks `blocks`, the rows of a key newest first, into output Blocks with a row
// limit of `block_row_max`, or of the byte budget `preferred_block_size_bytes`. Return the rows of
// every output Block.
std::vector<std::vector<Row>> fold(std::vector<std::vector<Row>> blocks, size_t block_row_max,
                                   size_t preferred_block_size_bytes, uint64_t* merged_rows) {
    auto schema = make_agg_schema();
    AggKeyBlockIterator iter(std::make_unique<InputBlocksUtIterator>(schema, std::move(blocks)),
                             block_row_max, preferred_block_size_bytes, schema,
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
        output.push_back(rows_of(block));
    }
    return output;
}

} // namespace

// The rows of a key come newest first: REPLACE_IF_NOT_NULL takes the first value that is not NULL,
// and NULL for a key of NULLs only.
TEST(AggKeyBlockAggregatorTest, ReplaceIfNotNullTakesTheFirstValueThatIsNotNull) {
    auto schema = make_read_schema(
            FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, /*nullable=*/true);
    EXPECT_EQ(aggregate_values(schema, {{{1, std::nullopt},
                                         {1, 2},
                                         {1, 1},
                                         {2, std::nullopt},
                                         {2, std::nullopt},
                                         {3, 3}}}),
              (std::vector<std::optional<int64_t>> {2, std::nullopt, 3}));
}

// Each key is checked for NULLs on its own rows: the nullable function adds the rows of a key with
// a NULL one by one, and those of a key without NULLs as one batch. MIN shows a NULL added as a
// value, since a NULL row holds 0 underneath; the NULL is the last row of the first key and
// the first row of the second.
TEST(AggKeyBlockAggregatorTest, NullableMinSkipsTheNullsOfEachKey) {
    auto schema = make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN,
                                   /*nullable=*/true);
    EXPECT_EQ(aggregate_values(schema, {{{1, 5},
                                         {1, 7},
                                         {1, std::nullopt},
                                         {2, std::nullopt},
                                         {2, 6},
                                         {3, 8},
                                         {3, 9},
                                         {4, std::nullopt},
                                         {4, std::nullopt}}}),
              (std::vector<std::optional<int64_t>> {5, 6, 8, std::nullopt}));
}

// Key 1 ends the first source and goes on in the second, which takes the place of the first in
// the same Block: REPLACE and REPLACE_IF_NOT_NULL keep the first value they took, SUM its sum.
TEST(AggKeyBlockAggregatorTest, AnOpenKeyKeepsItsValueWhenItsSourceIsRefilled) {
    EXPECT_EQ(aggregate_values(
                      make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE),
                      {{{1, 4}, {1, 5}}, {{1, 9}, {2, 8}}}),
              (std::vector<std::optional<int64_t>> {4, 8}));
    EXPECT_EQ(aggregate_values(
                      make_read_schema(
                              FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL,
                              /*nullable=*/true),
                      {{{1, std::nullopt}, {1, 4}}, {{1, 9}, {2, 9}}}),
              (std::vector<std::optional<int64_t>> {4, 9}));
    EXPECT_EQ(aggregate_values(make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM),
                               {{{1, 1}, {1, 2}}, {{1, 3}, {2, 5}}}),
              (std::vector<std::optional<int64_t>> {6, 5}));
}

// REPLACE keeps a copy of the string of an open key: the next source overwrites the bytes of the
// refilled Block before the key is closed.
TEST(AggKeyBlockAggregatorTest, AnOpenKeyKeepsItsReplaceStringWhenItsSourceIsRefilled) {
    auto key = make_column("k", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                           FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0, true);
    auto value = make_column("v", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE,
                             FieldType::OLAP_FIELD_TYPE_VARCHAR, false, 1, false);
    auto schema = std::make_shared<ReadSchema>(
            std::vector<TabletColumnPtr> {key, value},
            std::vector<DataTypePtr> {std::make_shared<DataTypeInt64>(),
                                      std::make_shared<DataTypeString>()});
    uint64_t merged_rows = 0;
    AggKeyBlockAggregator aggregator(schema, &merged_rows);
    ASSERT_TRUE(aggregator.init(BeExecVersionManager::get_newest_version()).ok());

    Block source = schema->create_read_block();
    auto refill_strings = [&](const std::vector<std::pair<int64_t, std::string>>& rows) {
        source.clear_column_data();
        auto columns_guard = source.mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (const auto& [k, v] : rows) {
            assert_cast<ColumnInt64&>(*columns[0]).insert_value(k);
            columns[1]->insert_data(v.data(), v.size());
        }
    };
    refill_strings({{1, "newest"}, {1, "older"}});
    aggregator.aggregate(source);
    refill_strings({{1, "oldest"}, {2, "second"}});
    aggregator.aggregate(source);

    Block batch = schema->create_read_block();
    aggregator.output_all_keys(&batch);
    ASSERT_EQ(batch.rows(), 2);
    EXPECT_EQ(batch.get_by_position(1).column->get_data_at(0).to_string(), "newest");
    EXPECT_EQ(batch.get_by_position(1).column->get_data_at(1).to_string(), "second");
    EXPECT_EQ(merged_rows, 2);
}

// The `_reader` storage function of every aggregation a value column can have resolves.
TEST(AggKeyBlockAggregatorTest, ResolvesTheFunctionOfEveryAggregation) {
    auto expect_init = [](const ReadSchemaSPtr& schema) {
        uint64_t merged_rows = 0;
        AggKeyBlockAggregator aggregator(schema, &merged_rows);
        const Status st = aggregator.init(BeExecVersionManager::get_newest_version());
        EXPECT_TRUE(st.ok()) << st;
    };
    for (auto aggregation : {FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL}) {
        SCOPED_TRACE(static_cast<int>(aggregation));
        expect_init(make_read_schema(aggregation));
    }

    auto key = make_column("k", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                           FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0, true);
    auto quantile = make_column("q", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_QUANTILE_UNION,
                                FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, false, 1, false);
    expect_init(std::make_shared<ReadSchema>(
            std::vector<TabletColumnPtr> {key, quantile},
            std::vector<DataTypePtr> {std::make_shared<DataTypeInt64>(),
                                      std::make_shared<DataTypeQuantileState>()}));
}

// A value column whose aggregation has no reader function fails init() instead of folding.
TEST(AggKeyBlockAggregatorTest, FailsToInitWithoutTheFunctionOfAnAggregation) {
    uint64_t merged_rows = 0;
    AggKeyBlockAggregator aggregator(
            make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE), &merged_rows);
    const Status st = aggregator.init(BeExecVersionManager::get_newest_version());
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("Failed to init the aggregate function"), std::string::npos)
            << st;
}

// The last key of a source stays open: key 2 goes on in the second source, and the batch holds
// it once, with the REPLACE value of its first row and the SUM of both rows.
TEST(AggKeyBlockAggregatorTest, CarriesTheOpenKeyToTheNextSource) {
    auto schema = make_agg_schema();
    uint64_t merged_rows = 0;
    AggKeyBlockAggregator aggregator(schema, &merged_rows);
    ASSERT_TRUE(aggregator.init(BeExecVersionManager::get_newest_version()).ok());

    Block first = make_rows(*schema, {{1, 11, 1}, {2, 22, 2}});
    aggregator.aggregate(first);
    EXPECT_EQ(aggregator.batch_rows(), 2);
    Block second = make_rows(*schema, {{2, 21, 3}, {3, 30, 4}});
    aggregator.aggregate(second);
    ASSERT_EQ(aggregator.batch_rows(), 3);

    Block batch = schema->create_read_block();
    aggregator.output_all_keys(&batch);
    EXPECT_EQ(rows_of(batch), (std::vector<Row> {{1, 11, 1}, {2, 22, 5}, {3, 30, 4}}));
    EXPECT_EQ(merged_rows, 1);
    EXPECT_EQ(aggregator.batch_rows(), 0);
}

// Handing over the complete keys keeps the open last key, key 2, for the next batch, which folds
// the rows of the next source into it.
TEST(AggKeyBlockAggregatorTest, KeepsTheOpenKeyForTheNextBatch) {
    auto schema = make_agg_schema();
    uint64_t merged_rows = 0;
    AggKeyBlockAggregator aggregator(schema, &merged_rows);
    ASSERT_TRUE(aggregator.init(BeExecVersionManager::get_newest_version()).ok());

    Block first = make_rows(*schema, {{1, 11, 1}, {2, 22, 2}});
    aggregator.aggregate(first);
    Block complete = schema->create_read_block();
    aggregator.output_finished_keys(&complete);
    EXPECT_EQ(rows_of(complete), (std::vector<Row> {{1, 11, 1}}));
    EXPECT_EQ(aggregator.batch_rows(), 1);

    Block second = make_rows(*schema, {{2, 21, 3}, {3, 30, 4}});
    aggregator.aggregate(second);
    Block rest = schema->create_read_block();
    aggregator.output_all_keys(&rest);
    EXPECT_EQ(rows_of(rest), (std::vector<Row> {{2, 22, 5}, {3, 30, 4}}));
    EXPECT_EQ(merged_rows, 1);
}

// Rows are one key only when they are equal on every key column: rows whose last key column
// repeats are different keys where the first one changes, and rows whose last key column changes
// on every row are different keys whatever the first one holds.
TEST(AggKeyBlockAggregatorTest, FoldsTheRowsThatAreEqualOnEveryKeyColumn) {
    // a BIGINT key, b BIGINT key, s BIGINT SUM.
    auto schema = std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {
            make_column("a", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                        FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0, true),
            make_column("b", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                        FieldType::OLAP_FIELD_TYPE_BIGINT, false, 1, true),
            make_column("s", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                        FieldType::OLAP_FIELD_TYPE_BIGINT, false, 2, false)});
    uint64_t merged_rows = 0;
    AggKeyBlockAggregator aggregator(schema, &merged_rows);
    ASSERT_TRUE(aggregator.init(BeExecVersionManager::get_newest_version()).ok());

    Block first = make_rows(*schema, {{1, 1, 1}, {2, 1, 2}, {2, 1, 3}, {2, 2, 4}});
    aggregator.aggregate(first);
    Block second = make_rows(*schema, {{3, 2, 5}, {3, 3, 6}, {4, 4, 7}});
    aggregator.aggregate(second);

    Block batch = schema->create_read_block();
    aggregator.output_all_keys(&batch);
    EXPECT_EQ(
            rows_of(batch),
            (std::vector<Row> {{1, 1, 1}, {2, 1, 5}, {2, 2, 4}, {3, 2, 5}, {3, 3, 6}, {4, 4, 7}}));
    EXPECT_EQ(merged_rows, 1);
}

// Every key becomes one row with the REPLACE value of its first row, the newest one, and the SUM
// of all.
TEST(AggKeyBlockIteratorTest, FoldsEveryKeyOnce) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 11, 2}, {1, 10, 1}, {2, 20, 3}},
                              {{3, 32, 6}, {3, 31, 5}, {3, 30, 4}},
                              {{4, 40, 7}}},
                             /*block_row_max=*/100, /*preferred_block_size_bytes=*/0, &merged_rows);
    EXPECT_EQ(output,
              (std::vector<std::vector<Row>> {{{1, 11, 3}, {2, 20, 3}, {3, 32, 15}, {4, 40, 7}}}));
    EXPECT_EQ(merged_rows, 3);
}

// The merge may split the rows of a key between its Blocks: key 2 goes on through three input
// Blocks and still comes out once, with the REPLACE value of its first row.
TEST(AggKeyBlockIteratorTest, CarriesAKeyFromOneInputBlockToTheNext) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 12, 1}, {2, 22, 2}}, {{2, 21, 3}}, {{2, 20, 4}, {3, 30, 5}}},
                             /*block_row_max=*/100, /*preferred_block_size_bytes=*/0, &merged_rows);
    EXPECT_EQ(output, (std::vector<std::vector<Row>> {{{1, 12, 1}, {2, 22, 9}, {3, 30, 5}}}));
    EXPECT_EQ(merged_rows, 2);
}

// The last key of an input Block is complete when the next input Block starts with another key,
// and goes on when the next one starts with the same key.
TEST(AggKeyBlockIteratorTest, ClosesTheLastKeyOfAnInputBlockAtTheNextKey) {
    uint64_t merged_rows = 0;
    EXPECT_EQ(fold({{{1, 10, 1}, {2, 20, 2}}, {{3, 30, 3}}}, /*block_row_max=*/100,
                   /*preferred_block_size_bytes=*/0, &merged_rows),
              (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 20, 2}, {3, 30, 3}}}));
    EXPECT_EQ(merged_rows, 0);
    EXPECT_EQ(fold({{{1, 10, 1}, {2, 21, 2}}, {{2, 20, 3}, {3, 30, 4}}}, /*block_row_max=*/100,
                   /*preferred_block_size_bytes=*/0, &merged_rows),
              (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 21, 5}, {3, 30, 4}}}));
    EXPECT_EQ(merged_rows, 1);
}

// A batch takes whole input Blocks: once it holds more keys than its row limit of two, it returns
// all but the last, which may go on in the next input Block and starts the next batch.
TEST(AggKeyBlockIteratorTest, ReturnsTheCompleteKeysOnceABatchExceedsItsRowLimit) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 10, 1}, {2, 20, 2}, {3, 30, 3}}, {{4, 40, 4}}, {{5, 50, 5}}},
                             /*block_row_max=*/2, /*preferred_block_size_bytes=*/0, &merged_rows);
    EXPECT_EQ(output, (std::vector<std::vector<Row>> {
                              {{1, 10, 1}, {2, 20, 2}}, {{3, 30, 3}, {4, 40, 4}}, {{5, 50, 5}}}));
    EXPECT_EQ(merged_rows, 0);
}

// Every input Block goes on with the last key of the one before, as when the merge cuts each key
// of two rows: the batches still end at their row limit of two keys, and every key comes out once.
TEST(AggKeyBlockIteratorTest, BoundsBatchesWhenEveryInputBlockGoesOnWithTheLastKey) {
    uint64_t merged_rows = 0;
    const auto output = fold({{{1, 10, 1}, {2, 21, 2}},
                              {{2, 20, 3}, {3, 31, 4}},
                              {{3, 30, 5}, {4, 41, 6}},
                              {{4, 40, 7}, {5, 51, 8}},
                              {{5, 50, 9}, {6, 61, 10}},
                              {{6, 60, 11}}},
                             /*block_row_max=*/2, /*preferred_block_size_bytes=*/0, &merged_rows);
    EXPECT_EQ(output, (std::vector<std::vector<Row>> {{{1, 10, 1}, {2, 21, 5}},
                                                      {{3, 31, 9}, {4, 41, 13}},
                                                      {{5, 51, 17}, {6, 61, 21}}}));
    EXPECT_EQ(merged_rows, 5);
}

// The byte budget counts as rows: 24 bytes a row in the input Block give 60 bytes two keys. A
// budget smaller than a row gives every key an output Block of its own.
TEST(AggKeyBlockIteratorTest, EndsOutputBlocksAtTheByteBudget) {
    const std::vector<std::vector<Row>> blocks = {{{1, 13, 4}, {1, 12, 3}, {1, 11, 2}, {1, 10, 1}},
                                                  {{2, 21, 6}, {2, 20, 5}},
                                                  {{3, 31, 8}, {3, 30, 7}}};
    uint64_t merged_rows = 0;
    EXPECT_EQ(fold(blocks, /*block_row_max=*/100, /*preferred_block_size_bytes=*/60, &merged_rows),
              (std::vector<std::vector<Row>> {{{1, 13, 10}, {2, 21, 11}}, {{3, 31, 15}}}));
    EXPECT_EQ(merged_rows, 5);
    EXPECT_EQ(fold(blocks, /*block_row_max=*/100, /*preferred_block_size_bytes=*/1, &merged_rows),
              (std::vector<std::vector<Row>> {{{1, 13, 10}}, {{2, 21, 11}}, {{3, 31, 15}}}));
}

// An error of the input, such as a cancelled read, comes out of next_block().
TEST(AggKeyBlockIteratorTest, PassesOnTheErrorOfItsInput) {
    auto schema = make_agg_schema();
    uint64_t merged_rows = 0;
    AggKeyBlockIterator iter(std::make_unique<InputBlocksUtIterator>(
                                     schema, std::vector<std::vector<Row>> {{{1, 10, 1}}},
                                     Status::Cancelled("cancelled")),
                             /*block_row_max=*/100, /*preferred_block_size_bytes=*/0, schema,
                             BeExecVersionManager::get_newest_version(), &merged_rows);
    ASSERT_TRUE(iter.init().ok());
    Block block = schema->create_read_block();
    bool eof = false;
    EXPECT_TRUE(iter.next_block(&block, &eof).is<ErrorCode::CANCELLED>());
}

} // namespace doris
