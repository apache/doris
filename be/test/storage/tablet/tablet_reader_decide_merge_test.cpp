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

// The decision table of TabletReader::decide_merge: for every table model and read shape, what
// the rows are compared by, which column orders the rows of one key, and whether the older
// versions of a key are dropped. Every consumer of a merge (the rowset merge, a rowset's segment
// merge, the TopN read, the schema changes) takes these answers from here and nowhere else.

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

namespace {

struct ColumnSpec {
    std::string name;
    bool is_key = false;
};

struct SchemaSpec {
    KeysType keys_type = KeysType::DUP_KEYS;
    std::vector<ColumnSpec> columns;
    // Pairs of (sequence column, value column) of a sequence map.
    std::vector<std::pair<std::string, std::string>> sequence_map;
    // Names of the cluster key columns.
    std::vector<std::string> cluster_keys;
};

// A BIGINT-only tablet schema; the column named SEQUENCE_COL is the sequence column and the one
// named CHANGELOG_TSO_COL the changelog TSO, which is how ReadSchema finds both.
TabletSchemaSPtr make_schema(const SchemaSpec& spec) {
    TabletSchemaPB pb;
    pb.set_keys_type(spec.keys_type);
    pb.set_num_short_key_columns(1);
    pb.set_num_rows_per_row_block(1024);
    pb.set_compress_kind(COMPRESS_NONE);
    pb.set_next_column_unique_id(static_cast<int32_t>(spec.columns.size()));
    auto uid_of = [&](const std::string& name) -> int32_t {
        for (size_t i = 0; i < spec.columns.size(); ++i) {
            if (spec.columns[i].name == name) {
                return static_cast<int32_t>(i);
            }
        }
        DORIS_CHECK(false) << "unknown column " << name;
        return -1;
    };
    for (size_t i = 0; i < spec.columns.size(); ++i) {
        ColumnPB* column = pb.add_column();
        column->set_unique_id(static_cast<int32_t>(i));
        column->set_name(spec.columns[i].name);
        column->set_type("BIGINT");
        column->set_is_key(spec.columns[i].is_key);
        column->set_length(8);
        column->set_index_length(8);
        column->set_is_nullable(false);
        if (spec.columns[i].name == SEQUENCE_COL) {
            pb.set_sequence_col_idx(static_cast<int32_t>(i));
        }
    }
    for (const auto& [sequence_column, value_column] : spec.sequence_map) {
        ColumnGroupPB* group = pb.mutable_seq_map()->add_cg();
        group->set_sequence_column(uid_of(sequence_column));
        group->add_columns_in_group(uid_of(value_column));
    }
    for (const auto& name : spec.cluster_keys) {
        pb.add_cluster_key_uids(uid_of(name));
    }
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(pb);
    return schema;
}

// decide_merge() for a read of the whole schema compared by `compare_ordinals` (empty: the
// whole key), with the three pass-through flags set to recognizable values.
MergeDecision decide(const SchemaSpec& spec, std::vector<uint32_t> compare_ordinals = {}) {
    auto schema = make_schema(spec);
    ReadSchema read_schema(schema->columns());
    return TabletReader::decide_merge(*schema, read_schema, std::move(compare_ordinals),
                                      /*key_ordered=*/true, /*reverse=*/false,
                                      /*older_first=*/false);
}

const SchemaSpec kUnique {.keys_type = KeysType::UNIQUE_KEYS,
                          .columns = {{"k1", true}, {"k2", true}, {"v"}},
                          .sequence_map = {},
                          .cluster_keys = {}};
const SchemaSpec kUniqueWithSequence {
        .keys_type = KeysType::UNIQUE_KEYS,
        .columns = {{"k1", true}, {"k2", true}, {"v"}, {SEQUENCE_COL}},
        .sequence_map = {},
        .cluster_keys = {}};
const SchemaSpec kSequenceMap {.keys_type = KeysType::UNIQUE_KEYS,
                               .columns = {{"k1", true}, {"v1"}, {"s1"}, {"v2"}, {"s2"}},
                               .sequence_map = {{"s1", "v1"}, {"s2", "v2"}},
                               .cluster_keys = {}};
const SchemaSpec kClusterKey {
        .keys_type = KeysType::UNIQUE_KEYS,
        .columns = {{"k1", true}, {"k2", true}, {"c1"}, {"v"}, {SEQUENCE_COL}},
        .sequence_map = {},
        .cluster_keys = {"c1"}};
const SchemaSpec kAgg {.keys_type = KeysType::AGG_KEYS,
                       .columns = {{"k1", true}, {"v"}},
                       .sequence_map = {},
                       .cluster_keys = {}};
const SchemaSpec kChangelog {.keys_type = KeysType::DUP_KEYS,
                             .columns = {{"k1", true},
                                         {"v"},
                                         {CHANGELOG_TSO_COL},
                                         {CHANGELOG_LSN_COL},
                                         {CHANGELOG_OP_COL}},
                             .sequence_map = {},
                             .cluster_keys = {}};

} // namespace

// The whole key is the key prefix of the read schema; UNIQUE_KEYS without a sequence map keeps
// the newest row of a key, ordered by nothing but the child order without a sequence column.
TEST(TabletReaderDecideMergeTest, UniqueKeysByWholeKey) {
    const MergeDecision decision = decide(kUnique);
    EXPECT_TRUE(decision.key_ordered);
    EXPECT_FALSE(decision.reverse);
    EXPECT_FALSE(decision.older_first);
    EXPECT_EQ(decision.compare_ordinals, (std::vector<uint32_t> {0, 1}));
    EXPECT_TRUE(decision.compare_whole_key);
    EXPECT_EQ(decision.sequence_ordinal, -1);
    EXPECT_FALSE(decision.small_sequence_first);
    EXPECT_TRUE(decision.drop_older_versions);
}

// The sequence column orders the rows of one key, the larger value first.
TEST(TabletReaderDecideMergeTest, UniqueKeysWithSequenceColumn) {
    const MergeDecision decision = decide(kUniqueWithSequence);
    EXPECT_EQ(decision.sequence_ordinal, 3);
    EXPECT_FALSE(decision.small_sequence_first);
    EXPECT_TRUE(decision.drop_older_versions);
}

// A sequence map needs every version of a key: any of them may hold the winning value of a
// column group. This holds whoever asks, a query, a compaction or a schema change.
TEST(TabletReaderDecideMergeTest, SequenceMapKeepsEveryVersion) {
    const MergeDecision decision = decide(kSequenceMap);
    EXPECT_TRUE(decision.compare_whole_key);
    EXPECT_FALSE(decision.drop_older_versions);
}

// A read compared by a key prefix (ORDER BY k1 LIMIT n pushed down): rows equal on the prefix
// are not versions of one key, so none is dropped. Before, the segment merge of a
// merge-on-write TopN read dropped every row but one per prefix value.
TEST(TabletReaderDecideMergeTest, KeyPrefixNeverDropsRows) {
    const MergeDecision decision = decide(kUnique, {0});
    EXPECT_EQ(decision.compare_ordinals, (std::vector<uint32_t> {0}));
    EXPECT_FALSE(decision.compare_whole_key);
    EXPECT_FALSE(decision.drop_older_versions);
}

// A cluster key table is merged by its cluster key (a schema change), whose rows are not
// versions of one key: no sequence column and nothing dropped.
TEST(TabletReaderDecideMergeTest, ClusterKeyIgnoresTheSequenceColumn) {
    const MergeDecision decision = decide(kClusterKey, {2});
    EXPECT_EQ(decision.compare_ordinals, (std::vector<uint32_t> {2}));
    EXPECT_FALSE(decision.compare_whole_key);
    EXPECT_EQ(decision.sequence_ordinal, -1);
    EXPECT_FALSE(decision.drop_older_versions);
}

// AGG_KEYS folds every version of a key into one row.
TEST(TabletReaderDecideMergeTest, AggKeysKeepsEveryVersion) {
    const MergeDecision decision = decide(kAgg);
    EXPECT_EQ(decision.compare_ordinals, (std::vector<uint32_t> {0}));
    EXPECT_EQ(decision.sequence_ordinal, -1);
    EXPECT_FALSE(decision.drop_older_versions);
}

// A changelog read orders the changes of a key by their TSO, the smaller one first, and keeps
// every change.
TEST(TabletReaderDecideMergeTest, ChangelogOrdersByTso) {
    const MergeDecision decision = decide(kChangelog);
    EXPECT_EQ(decision.sequence_ordinal, 2);
    EXPECT_TRUE(decision.small_sequence_first);
    EXPECT_FALSE(decision.drop_older_versions);
}

// The flags the caller decides are passed through untouched.
TEST(TabletReaderDecideMergeTest, PassesTheCallerFlagsThrough) {
    auto schema = make_schema(kUnique);
    ReadSchema read_schema(schema->columns());
    const MergeDecision decision = TabletReader::decide_merge(
            *schema, read_schema, {}, /*key_ordered=*/false, /*reverse=*/true,
            /*older_first=*/true);
    EXPECT_FALSE(decision.key_ordered);
    EXPECT_TRUE(decision.reverse);
    EXPECT_TRUE(decision.older_first);
}

} // namespace doris
