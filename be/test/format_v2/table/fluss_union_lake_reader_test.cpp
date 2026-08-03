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

#include "format_v2/table/fluss_union_lake_reader.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format/format_common.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::fluss {
namespace {

using Tail = FlussUnionLakeReader::Tail;

// The lake half, stood in for. Everything about how a paimon split is read is the sibling's answer;
// what this reader adds is only what happens to the block afterwards, which is what these tests are.
class RecordingLakeReader final : public format::TableReader {
public:
    Status prepare_split(const format::SplitReadOptions& options) override {
        prepared_ranges.push_back(options.current_range);
        _current_split_pruned = prune_next_split;
        return Status::OK();
    }

    Status get_block(Block* block, bool* eos) override {
        if (blocks.empty()) {
            *eos = true;
            return Status::OK();
        }
        *eos = false;
        *block = std::move(blocks.front());
        blocks.erase(blocks.begin());
        return Status::OK();
    }

    Status close() override {
        closed = true;
        return Status::OK();
    }

    std::vector<TFileRangeDesc> prepared_ranges;
    std::vector<Block> blocks;
    bool prune_next_split = false;
    bool closed = false;
};

DataTypePtr int_type() {
    return std::make_shared<DataTypeInt32>();
}

DataTypePtr string_type() {
    return std::make_shared<DataTypeString>();
}

format::ColumnDefinition make_column(const std::string& name, DataTypePtr type) {
    format::ColumnDefinition column;
    column.name = name;
    column.type = std::move(type);
    return column;
}

// id (int) / name (string) / amount (int); the primary key is (id, name) unless stated otherwise.
std::vector<format::ColumnDefinition> projected_columns() {
    return {make_column("id", int_type()), make_column("name", string_type()),
            make_column("amount", int_type())};
}

TFileScanRangeParams make_scan_params(std::map<std::string, std::string> fluss_properties) {
    TFileScanRangeParams scan_params;
    scan_params.__set_fluss_properties(std::move(fluss_properties));
    return scan_params;
}

TFileScanRangeParams union_scan_params(const std::string& pk_names = "id,name",
                                       const std::string& max_tail_rows = "2000000") {
    return make_scan_params({{"fluss.db_name", "db"},
                             {"fluss.table_name", "lake_pk"},
                             {"fluss.union.pk_names", pk_names},
                             {"fluss.union.max_tail_rows", max_tail_rows}});
}

TFileRangeDesc wrapped_lake_split(const std::string& tail) {
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("fluss_union");
    table_format_params.__set_fluss_params(
            {{"fluss.range_type", "LAKE_SUPPRESS"}, {"fluss.union.tail", tail}});
    TFileRangeDesc range;
    range.__set_table_format_params(std::move(table_format_params));
    range.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    return range;
}

Status init_reader(FlussUnionLakeReader* reader, TFileScanRangeParams* scan_params,
                   std::vector<format::ColumnDefinition> columns = projected_columns()) {
    return reader->init({
            .projected_columns = std::move(columns),
            .conjuncts = {},
            .format = format::FileFormat::PARQUET,
            .scan_params = scan_params,
            .io_ctx = nullptr,
            .runtime_state = nullptr,
            .scanner_profile = nullptr,
    });
}

// Replaces the paimon child after init(), which is where the real one is built and asked what it
// makes of the scan's options.
RecordingLakeReader* install_lake_reader(FlussUnionLakeReader* reader) {
    auto lake_reader = std::make_unique<RecordingLakeReader>();
    auto* raw = lake_reader.get();
    reader->_lake_reader = std::move(lake_reader);
    return raw;
}

Block make_block(const std::vector<int32_t>& ids, const std::vector<std::string>& names,
                 const std::vector<int32_t>& amounts) {
    auto id_column = ColumnInt32::create();
    auto name_column = ColumnString::create();
    auto amount_column = ColumnInt32::create();
    for (size_t row = 0; row < ids.size(); ++row) {
        id_column->insert_value(ids[row]);
        name_column->insert_data(names[row].data(), names[row].size());
        amount_column->insert_value(amounts[row]);
    }
    Block block;
    block.insert({std::move(id_column), int_type(), "id"});
    block.insert({std::move(name_column), string_type(), "name"});
    block.insert({std::move(amount_column), int_type(), "amount"});
    return block;
}

// The tail as this reader receives it: one row per change log record, key columns only, in the order
// FE listed the key.
Block make_key_block(const std::vector<int32_t>& ids, const std::vector<std::string>& names) {
    auto id_column = ColumnInt32::create();
    auto name_column = ColumnString::create();
    for (size_t row = 0; row < ids.size(); ++row) {
        id_column->insert_value(ids[row]);
        name_column->insert_data(names[row].data(), names[row].size());
    }
    Block block;
    block.insert({std::move(id_column), int_type(), "id"});
    block.insert({std::move(name_column), string_type(), "name"});
    return block;
}

std::vector<int32_t> ids_of(const Block& block) {
    std::vector<int32_t> ids;
    const auto& column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    for (size_t row = 0; row < column.size(); ++row) {
        ids.push_back(column.get_data()[row]);
    }
    return ids;
}

// ------------------------------------------------------------------ the tail descriptor

// The four fields are how a split says which slice of which bucket's log may contradict it. A tail
// read from the wrong bucket suppresses rows nothing has superseded and leaves the superseded ones.
TEST(FlussUnionLakeReaderTest, ParsesTheTailOfBothPartitionedAndUnpartitionedTables) {
    Tail partitioned;
    ASSERT_TRUE(FlussUnionLakeReader::parse_tail("1234:3:17:42", &partitioned).ok());
    EXPECT_EQ(partitioned.partition_id, "1234");
    EXPECT_EQ(partitioned.bucket_id, 3);
    EXPECT_EQ(partitioned.start_offset, 17);
    EXPECT_EQ(partitioned.stop_offset, 42);
    EXPECT_EQ(partitioned.spec, "1234:3:17:42");

    Tail unpartitioned;
    ASSERT_TRUE(FlussUnionLakeReader::parse_tail(":3:0:9", &unpartitioned).ok());
    // Empty rather than a sentinel number, so that bucket 3 of an unpartitioned table and bucket 3
    // of some partition cannot become one entry in the cache that holds their keys.
    EXPECT_TRUE(unpartitioned.partition_id.empty());
    EXPECT_EQ(unpartitioned.bucket_id, 3);
}

TEST(FlussUnionLakeReaderTest, RefusesATailItCannotReadInFull) {
    Tail tail;
    EXPECT_FALSE(FlussUnionLakeReader::parse_tail("3:0:9", &tail).ok());
    EXPECT_FALSE(FlussUnionLakeReader::parse_tail("1:3:0:9:extra", &tail).ok());
    EXPECT_FALSE(FlussUnionLakeReader::parse_tail("::0:9", &tail).ok());
    EXPECT_FALSE(FlussUnionLakeReader::parse_tail(":bucket:0:9", &tail).ok());
    EXPECT_FALSE(FlussUnionLakeReader::parse_tail("x:3:0:9", &tail).ok());
    EXPECT_FALSE(FlussUnionLakeReader::parse_tail(":3:0:", &tail).ok());
}

// An empty range is never planned: FE leaves a bucket whose lake has caught up with its log
// unwrapped. One arriving here means the two halves were bounded by different offsets, which
// duplicates or loses rows - and suppressing nothing would look exactly like working.
TEST(FlussUnionLakeReaderTest, RefusesATailThatSupersedesNothing) {
    Tail tail;
    EXPECT_FALSE(FlussUnionLakeReader::parse_tail(":3:9:9", &tail).ok());
    EXPECT_FALSE(FlussUnionLakeReader::parse_tail(":3:10:9", &tail).ok());
}

// The range that reads the tail is synthesized here, not planned by FE, so nothing downstream would
// notice it naming the wrong bucket or the wrong offsets. Pinned as a whole map for that reason.
TEST(FlussUnionLakeReaderTest, ReadsTheTailAsABoundedLogRangeOfThatBucket) {
    Tail tail;
    ASSERT_TRUE(FlussUnionLakeReader::parse_tail("77:3:17:42", &tail).ok());
    const auto range = FlussUnionLakeReader::tail_scan_range(tail);
    ASSERT_TRUE(range.__isset.table_format_params);
    EXPECT_EQ(range.table_format_params.table_format_type, "fluss");
    EXPECT_EQ(range.format_type, TFileFormatType::FORMAT_JNI);
    const std::map<std::string, std::string> expected {{"fluss.range_type", "LOG"},
                                                       {"fluss.partition_id", "77"},
                                                       {"fluss.bucket_id", "3"},
                                                       {"fluss.log_start_offset", "17"},
                                                       {"fluss.log_stop_offset", "42"}};
    EXPECT_EQ(range.table_format_params.fluss_params, expected);
}

// LOG, not PK_TAIL. The suppression set is every key the tail touched, including the ones it ended
// by deleting: a lake row whose key was deleted has to disappear too, and PK_TAIL - which returns
// the surviving state - would not name it.
TEST(FlussUnionLakeReaderTest, ReadsTheTailAsAWholeChangeLogNotAsItsSurvivingState) {
    Tail tail;
    ASSERT_TRUE(FlussUnionLakeReader::parse_tail(":0:0:5", &tail).ok());
    const auto range = FlussUnionLakeReader::tail_scan_range(tail);
    EXPECT_EQ(range.table_format_params.fluss_params.at("fluss.range_type"), "LOG");
}

// An unpartitioned table's bucket is subscribed to by a different fluss call than a partitioned
// one's, chosen by whether the range carries a partition id at all.
TEST(FlussUnionLakeReaderTest, LeavesOutThePartitionIdOfAnUnpartitionedTable) {
    Tail tail;
    ASSERT_TRUE(FlussUnionLakeReader::parse_tail(":3:0:9", &tail).ok());
    const auto range = FlussUnionLakeReader::tail_scan_range(tail);
    EXPECT_EQ(range.table_format_params.fluss_params.count("fluss.partition_id"), 0);
}

// ------------------------------------------------------------------ the key columns

// The key columns are the projected ones, reused. FE keeps them in the scan's tuple for this read,
// which is what lets the two halves be compared at all: same tuple, same types, already mapped.
TEST(FlussUnionLakeReaderTest, TakesItsKeyColumnsFromTheProjectionInTheOrderFeListedThem) {
    auto scan_params = union_scan_params("name,id");
    FlussUnionLakeReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());

    // The key order is FE's, not the projection's: the tail's key block arrives in that same order.
    ASSERT_EQ(reader._key_column_indexes.size(), 2);
    EXPECT_EQ(reader._key_column_indexes[0], 1);
    EXPECT_EQ(reader._key_column_indexes[1], 0);
    ASSERT_EQ(reader._key_columns.size(), 2);
    EXPECT_EQ(reader._key_columns[0].name, "name");
    EXPECT_TRUE(reader._key_columns[0].type->equals(DataTypeString {}));
    EXPECT_EQ(reader._key_columns[1].name, "id");
}

// The one thing this reader cannot work around. FE promises the key columns are projected whenever
// it plans a union read; if that promise is broken, suppressing nothing returns every superseded
// lake row a second time, and no count or assertion downstream would show it.
TEST(FlussUnionLakeReaderTest, RefusesAScanThatDoesNotProjectAKeyColumn) {
    auto scan_params = union_scan_params("id,name");
    FlussUnionLakeReader reader;
    const auto status =
            init_reader(&reader, &scan_params,
                        {make_column("id", int_type()), make_column("amount", int_type())});
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("name"), std::string::npos);
}

TEST(FlussUnionLakeReaderTest, RefusesAScanWithoutTheUnionProperties) {
    TFileScanRangeParams no_properties;
    FlussUnionLakeReader without_properties;
    EXPECT_FALSE(init_reader(&without_properties, &no_properties).ok());

    auto no_names = make_scan_params({{"fluss.db_name", "db"}});
    FlussUnionLakeReader without_names;
    EXPECT_FALSE(init_reader(&without_names, &no_names).ok());

    auto no_limit = make_scan_params({{"fluss.union.pk_names", "id"}});
    FlussUnionLakeReader without_limit;
    EXPECT_FALSE(init_reader(&without_limit, &no_limit).ok());

    auto zero_limit = union_scan_params("id", "0");
    FlussUnionLakeReader with_zero_limit;
    EXPECT_FALSE(init_reader(&with_zero_limit, &zero_limit).ok());

    auto negative_limit = union_scan_params("id", "-1");
    FlussUnionLakeReader with_negative_limit;
    EXPECT_FALSE(init_reader(&with_negative_limit, &negative_limit).ok());
}

// A COUNT answered from paimon's file metadata would count the rows this reader exists to remove,
// and would do it without ever producing a block to remove them from.
TEST(FlussUnionLakeReaderTest, WithholdsAggregatePushdownFromTheLakeHalf) {
    auto scan_params = union_scan_params();
    FlussUnionLakeReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns(),
                                    .conjuncts = {},
                                    .format = format::FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = nullptr,
                                    .runtime_state = nullptr,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());
    ASSERT_TRUE(reader._lake_reader != nullptr);
    EXPECT_EQ(reader._lake_reader->_push_down_agg_type, TPushAggOp::type::NONE);
    EXPECT_EQ(reader._push_down_agg_type, TPushAggOp::type::COUNT);
}

// ------------------------------------------------------------------ the suppression

struct SuppressionFixture {
    FlussUnionLakeReader reader;
    TFileScanRangeParams scan_params = union_scan_params();
    ShardedKVCache cache {2};
    RecordingLakeReader* lake = nullptr;
    std::vector<std::string> tails_read;

    Status open(Block keys) {
        RETURN_IF_ERROR(init_reader(&reader, &scan_params));
        lake = install_lake_reader(&reader);
        reader._test_tail_reader = [this, keys](const Tail& tail, Block* out) mutable {
            tails_read.push_back(tail.spec);
            *out = keys;
            return Status::OK();
        };
        return Status::OK();
    }

    format::SplitReadOptions split(const std::string& tail) {
        format::SplitReadOptions options;
        options.cache = &cache;
        options.current_range = wrapped_lake_split(tail);
        return options;
    }
};

// The whole point, stated once: a lake row whose key the tail touched is gone, and every other lake
// row is untouched. What the tail ended up saying about those keys arrives separately.
TEST(FlussUnionLakeReaderTest, DropsTheLakeRowsTheTailNamesAndKeepsTheRest) {
    SuppressionFixture fixture;
    ASSERT_TRUE(fixture.open(make_key_block({2, 4}, {"b", "d"})).ok());
    ASSERT_TRUE(fixture.reader.prepare_split(fixture.split(":0:10:20")).ok());
    fixture.lake->blocks.push_back(
            make_block({1, 2, 3, 4}, {"a", "b", "c", "d"}, {10, 20, 30, 40}));

    Block block = make_block({}, {}, {});
    bool eos = false;
    ASSERT_TRUE(fixture.reader.get_block(&block, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(ids_of(block), (std::vector<int32_t> {1, 3}));
    // The block still has the shape its consumer expects; only rows went away.
    EXPECT_EQ(block.columns(), 3);
}

// The same key column values in different rows are not the same key. Comparing one column of a
// composite key would drop rows nothing superseded.
TEST(FlussUnionLakeReaderTest, MatchesOnTheWholeKeyNotOnOneColumnOfIt) {
    SuppressionFixture fixture;
    ASSERT_TRUE(fixture.open(make_key_block({1}, {"b"})).ok());
    ASSERT_TRUE(fixture.reader.prepare_split(fixture.split(":0:10:20")).ok());
    // (1, "a") shares its id with the tail's key and (2, "b") shares its name; neither is that key.
    fixture.lake->blocks.push_back(make_block({1, 2, 1}, {"a", "b", "b"}, {10, 20, 30}));

    Block block = make_block({}, {}, {});
    bool eos = false;
    ASSERT_TRUE(fixture.reader.get_block(&block, &eos).ok());
    EXPECT_EQ(ids_of(block), (std::vector<int32_t> {1, 2}));
}

TEST(FlussUnionLakeReaderTest, LeavesASplitWhoseKeysTheTailNeverTouchedAlone) {
    SuppressionFixture fixture;
    ASSERT_TRUE(fixture.open(make_key_block({7, 8}, {"g", "h"})).ok());
    ASSERT_TRUE(fixture.reader.prepare_split(fixture.split(":0:10:20")).ok());
    fixture.lake->blocks.push_back(make_block({1, 2}, {"a", "b"}, {10, 20}));

    Block block = make_block({}, {}, {});
    bool eos = false;
    ASSERT_TRUE(fixture.reader.get_block(&block, &eos).ok());
    EXPECT_EQ(ids_of(block), (std::vector<int32_t> {1, 2}));
}

// One bucket's tail is one read, however many of its splits this BE was given. The read is a network
// round trip to fluss; doing it per split would multiply it by the number of files in the bucket.
TEST(FlussUnionLakeReaderTest, ReadsOneBucketsTailOnceHoweverManySplitsItHas) {
    SuppressionFixture fixture;
    ASSERT_TRUE(fixture.open(make_key_block({2}, {"b"})).ok());
    ASSERT_TRUE(fixture.reader.prepare_split(fixture.split(":0:10:20")).ok());
    ASSERT_TRUE(fixture.reader.prepare_split(fixture.split(":0:10:20")).ok());
    EXPECT_EQ(fixture.tails_read, (std::vector<std::string> {":0:10:20"}));
}

// Two buckets, two tails, and each split suppressed by its own. Binding them by bucket is what makes
// the tail small; getting the binding wrong is invisible unless the two tails differ.
TEST(FlussUnionLakeReaderTest, SuppressesEachBucketWithItsOwnTail) {
    FlussUnionLakeReader reader;
    auto scan_params = union_scan_params();
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    auto* lake = install_lake_reader(&reader);
    ShardedKVCache cache {2};
    std::vector<std::string> tails_read;
    reader._test_tail_reader = [&](const Tail& tail, Block* out) {
        tails_read.push_back(tail.spec);
        *out = tail.bucket_id == 0 ? make_key_block({1}, {"a"}) : make_key_block({2}, {"b"});
        return Status::OK();
    };
    const auto split = [&](const std::string& tail) {
        format::SplitReadOptions options;
        options.cache = &cache;
        options.current_range = wrapped_lake_split(tail);
        return options;
    };

    ASSERT_TRUE(reader.prepare_split(split(":0:10:20")).ok());
    lake->blocks.push_back(make_block({1, 2}, {"a", "b"}, {10, 20}));
    Block bucket0 = make_block({}, {}, {});
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&bucket0, &eos).ok());
    EXPECT_EQ(ids_of(bucket0), (std::vector<int32_t> {2}));

    ASSERT_TRUE(reader.prepare_split(split(":1:30:40")).ok());
    lake->blocks.push_back(make_block({1, 2}, {"a", "b"}, {10, 20}));
    Block bucket1 = make_block({}, {}, {});
    ASSERT_TRUE(reader.get_block(&bucket1, &eos).ok());
    EXPECT_EQ(ids_of(bucket1), (std::vector<int32_t> {1}));
    EXPECT_EQ(tails_read, (std::vector<std::string> {":0:10:20", ":1:30:40"}));
}

// The limit exists because a BE is a long-lived process shared by every query on it. Both halves of
// one table count the same thing - change log records over the same offsets - so a table that is
// over the limit is over it on both sides rather than in whichever half ran first.
TEST(FlussUnionLakeReaderTest, RefusesATailLargerThanTheConfiguredLimit) {
    SuppressionFixture over_limit;
    over_limit.scan_params = union_scan_params("id,name", "2");
    ASSERT_TRUE(over_limit.open(make_key_block({1, 2, 3}, {"a", "b", "c"})).ok());
    const auto status = over_limit.reader.prepare_split(over_limit.split(":0:10:20"));
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("max_tail_rows"), std::string::npos);

    SuppressionFixture at_limit;
    at_limit.scan_params = union_scan_params("id,name", "3");
    ASSERT_TRUE(at_limit.open(make_key_block({1, 2, 3}, {"a", "b", "c"})).ok());
    EXPECT_TRUE(at_limit.reader.prepare_split(at_limit.split(":0:10:20")).ok());
}

// A tail that reaches this BE in several batches is one tail. Accumulating it batch by batch is
// what keeps the limit from being reached only after the whole of an oversized tail is in memory.
TEST(FlussUnionLakeReaderTest, AccumulatesATailThatArrivesInSeveralBatches) {
    FlussUnionLakeReader reader;
    auto scan_params = union_scan_params();
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    Tail tail;
    ASSERT_TRUE(FlussUnionLakeReader::parse_tail(":0:10:20", &tail).ok());

    std::vector<Block> batches {make_key_block({1}, {"a"}), make_key_block({}, {}),
                                make_key_block({3}, {"c"})};
    size_t delivered = 0;
    Block keys;
    ASSERT_TRUE(reader._accumulate_tail_keys(
                              tail,
                              [&](Block* batch, bool* eos) {
                                  if (delivered < batches.size()) {
                                      *batch = batches[delivered++];
                                  }
                                  *eos = delivered >= batches.size();
                                  return Status::OK();
                              },
                              &keys)
                        .ok());
    EXPECT_EQ(keys.rows(), 2);
    EXPECT_EQ(ids_of(keys), (std::vector<int32_t> {1, 3}));
}

// A split that says nothing about which tail supersedes it cannot be read: reading it as-is returns
// the superseded rows a second time.
TEST(FlussUnionLakeReaderTest, RefusesALakeSplitWithNoTailBoundToIt) {
    FlussUnionLakeReader reader;
    auto scan_params = union_scan_params();
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    install_lake_reader(&reader);
    ShardedKVCache cache {2};

    const auto prepare = [&](TFileRangeDesc range) {
        format::SplitReadOptions options;
        options.cache = &cache;
        options.current_range = std::move(range);
        return reader.prepare_split(options);
    };

    TFileRangeDesc no_params;
    EXPECT_FALSE(prepare(no_params).ok());

    TTableFormatFileDesc empty_fluss_params;
    empty_fluss_params.__set_table_format_type("fluss_union");
    empty_fluss_params.__set_fluss_params({});
    TFileRangeDesc without_tail;
    without_tail.__set_table_format_params(empty_fluss_params);
    EXPECT_FALSE(prepare(without_tail).ok());

    TTableFormatFileDesc wrong_type;
    wrong_type.__set_table_format_type("fluss_union");
    wrong_type.__set_fluss_params({{"fluss.range_type", "LOG"}, {"fluss.union.tail", ":0:10:20"}});
    TFileRangeDesc not_a_suppression;
    not_a_suppression.__set_table_format_params(wrong_type);
    EXPECT_FALSE(prepare(not_a_suppression).ok());

    TTableFormatFileDesc no_tail_key;
    no_tail_key.__set_table_format_type("fluss_union");
    no_tail_key.__set_fluss_params({{"fluss.range_type", "LAKE_SUPPRESS"}});
    TFileRangeDesc suppression_without_tail;
    suppression_without_tail.__set_table_format_params(no_tail_key);
    EXPECT_FALSE(prepare(suppression_without_tail).ok());
}

// Reading a block with no suppression built would return the superseded rows. This is the state that
// a future refactor could reach by accident, so it fails rather than passing the block through.
TEST(FlussUnionLakeReaderTest, RefusesToReturnABlockWithNoSuppressionBuilt) {
    FlussUnionLakeReader reader;
    auto scan_params = union_scan_params();
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    auto* lake = install_lake_reader(&reader);
    lake->blocks.push_back(make_block({1}, {"a"}, {10}));

    Block block = make_block({}, {}, {});
    bool eos = false;
    EXPECT_FALSE(reader.get_block(&block, &eos).ok());
}

// A pruned split returns no rows, so its bucket's tail is not worth a network round trip.
TEST(FlussUnionLakeReaderTest, DoesNotReadTheTailOfAPrunedSplit) {
    SuppressionFixture fixture;
    ASSERT_TRUE(fixture.open(make_key_block({1}, {"a"})).ok());
    fixture.lake->prune_next_split = true;
    ASSERT_TRUE(fixture.reader.prepare_split(fixture.split(":0:10:20")).ok());
    EXPECT_TRUE(fixture.tails_read.empty());
    EXPECT_TRUE(fixture.reader.current_split_pruned());
}

// Every split reaches the sibling exactly as it was planned - its path, its byte extent, its paimon
// payload. Rewriting any of it here would reshape a read this reader has no business reshaping.
TEST(FlussUnionLakeReaderTest, ForwardsTheSplitToTheSiblingUntouched) {
    SuppressionFixture fixture;
    ASSERT_TRUE(fixture.open(make_key_block({1}, {"a"})).ok());
    auto range = wrapped_lake_split(":0:10:20");
    range.__set_path("/warehouse/db.db/lake_pk/bucket-0/data-1.parquet");
    range.__set_start_offset(4);
    range.__set_size(1024);
    format::SplitReadOptions options;
    options.cache = &fixture.cache;
    options.current_range = range;
    ASSERT_TRUE(fixture.reader.prepare_split(options).ok());

    ASSERT_EQ(fixture.lake->prepared_ranges.size(), 1);
    EXPECT_EQ(fixture.lake->prepared_ranges[0], range);
}

TEST(FlussUnionLakeReaderTest, ClosesTheLakeHalf) {
    SuppressionFixture fixture;
    ASSERT_TRUE(fixture.open(make_key_block({1}, {"a"})).ok());
    ASSERT_TRUE(fixture.reader.prepare_split(fixture.split(":0:10:20")).ok());
    ASSERT_TRUE(fixture.reader.close().ok());
    EXPECT_TRUE(fixture.lake->closed);
    EXPECT_TRUE(fixture.reader._suppression == nullptr);
}

} // namespace
} // namespace doris::format::fluss
