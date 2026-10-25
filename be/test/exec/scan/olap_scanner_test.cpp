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

#include "exec/scan/olap_scanner.h"

#include <gtest/gtest.h>

#include <string>

#include "cloud/config.h"
#include "common/config.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "storage/schema.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

class CloudFileCacheConfigGuard {
public:
    CloudFileCacheConfigGuard()
            : _cloud_unique_id(config::cloud_unique_id),
              _enable_file_cache(config::enable_file_cache) {}

    ~CloudFileCacheConfigGuard() {
        config::cloud_unique_id = _cloud_unique_id;
        config::enable_file_cache = _enable_file_cache;
    }

private:
    std::string _cloud_unique_id;
    bool _enable_file_cache;
};

TUniqueId make_query_id() {
    TUniqueId query_id;
    query_id.hi = 100;
    query_id.lo = 200;
    return query_id;
}

} // namespace

TEST(OlapScannerTest, BuildScoreRuntimeCollectionIoContextPropagatesQueryLimiter) {
    CloudFileCacheConfigGuard config_guard;
    config::cloud_unique_id = "olap_scanner_score_runtime_io_context_ut";
    config::enable_file_cache = true;

    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::SELECT);
    query_options.__set_file_cache_query_limit_bytes(1024);

    auto query_id = make_query_id();
    TNetworkAddress fe_addr;
    fe_addr.hostname = "127.0.0.1";
    fe_addr.port = 9030;
    auto query_ctx = MockQueryContext::create(query_id, ExecEnv::GetInstance(), query_options,
                                              fe_addr, true, fe_addr);
    ASSERT_NE(query_ctx->remote_scan_cache_write_limiter(), nullptr);

    MockRuntimeState state;
    state._query_id = query_id;
    state._query_ctx_uptr = query_ctx;
    state._query_ctx = query_ctx.get();

    io::FileCacheStatistics stats;
    auto io_ctx = build_score_runtime_collection_io_context(&state, ReaderType::READER_QUERY, 3600,
                                                            &stats);

    EXPECT_EQ(io_ctx.reader_type, ReaderType::READER_QUERY);
    EXPECT_EQ(io_ctx.expiration_time, 3600);
    EXPECT_EQ(io_ctx.query_id, &state.query_id());
    EXPECT_EQ(io_ctx.file_cache_stats, &stats);
    EXPECT_TRUE(io_ctx.is_inverted_index);
    EXPECT_EQ(io_ctx.remote_scan_cache_write_limiter, query_ctx->remote_scan_cache_write_limiter());
}

TEST(OlapScannerTest, BuildScoreRuntimeCollectionIoContextAllowsMissingQueryContext) {
    auto query_id = make_query_id();
    MockRuntimeState state;
    state._query_id = query_id;
    state._query_ctx = nullptr;

    io::FileCacheStatistics stats;
    auto io_ctx = build_score_runtime_collection_io_context(&state, ReaderType::READER_QUERY, 3600,
                                                            &stats);

    EXPECT_EQ(io_ctx.query_id, &state.query_id());
    EXPECT_EQ(io_ctx.file_cache_stats, &stats);
    EXPECT_TRUE(io_ctx.is_inverted_index);
    EXPECT_EQ(io_ctx.remote_scan_cache_write_limiter, nullptr);
    EXPECT_FALSE(io_ctx.is_disposable);
    EXPECT_FALSE(io_ctx.inverted_index_snii_read_no_write_file_cache);
}

// The statistics read the index before the scan, with the query's file cache choices.
TEST(OlapScannerTest, BuildScoreRuntimeCollectionIoContextFollowsQueryFileCacheOptions) {
    MockRuntimeState state;
    state._query_id = make_query_id();
    state._query_ctx = nullptr;
    state._query_options.__set_disable_file_cache(true);
    state._query_options.__set_inverted_index_snii_read_no_write_file_cache(true);

    io::FileCacheStatistics stats;
    auto io_ctx = build_score_runtime_collection_io_context(&state, ReaderType::READER_QUERY, 3600,
                                                            &stats);
    EXPECT_TRUE(io_ctx.is_disposable);
    EXPECT_TRUE(io_ctx.inverted_index_snii_read_no_write_file_cache);
}

namespace {

struct ChangelogReadFixture {
    TabletSchema tablet;
    std::vector<std::unique_ptr<SlotDescriptor>> owned_slots;
    std::vector<SlotDescriptor*> slots;
    std::unique_ptr<ReadSchema> read_schema;
    TOlapScanNode scan_node;

    explicit ChangelogReadFixture(bool historical = true) {
        for (const auto& [uid, name] : {std::pair<int, std::string> {10, "key"},
                                        {11, "current"},
                                        {12, "unrelated_before_name"},
                                        {13, CHANGELOG_TSO_COL},
                                        {14, CHANGELOG_OP_COL}}) {
            if (!historical && uid == 12) {
                continue;
            }
            TabletColumn column;
            column.set_unique_id(uid);
            column.set_name(name);
            column.set_is_key(uid == 10);
            column.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
            tablet.append_column(column);
        }
        // The scan tuple order and IDs deliberately differ from both UID and tablet ordinal.
        std::vector<TabletColumnPtr> columns;
        for (int uid : {14, 12, 10, 13, 11}) {
            if (!historical && uid == 12) {
                continue;
            }
            auto column = tablet.columns()[tablet.field_index(uid)];
            TSlotDescriptor slot;
            slot.__set_id(100 - uid);
            slot.__set_col_unique_id(uid);
            slot.__set_colName("slot_alias_" + std::to_string(uid));
            slot.__set_nullIndicatorBit(-1);
            column->get_vec_type()->to_thrift(slot.slotType);
            owned_slots.push_back(std::make_unique<SlotDescriptor>(slot));
            slots.push_back(owned_slots.back().get());
            columns.push_back(column);
        }
        read_schema = std::make_unique<ReadSchema>(std::move(columns));
        TChangelogReadColumnMappings mappings;
        std::vector<TChangelogReadColumnPair> pairs;
        if (historical) {
            TChangelogReadColumnPair pair;
            pair.__set_current_slot_id(89);
            pair.__set_before_slot_id(88);
            pairs.push_back(pair);
        }
        mappings.__set_pairs(pairs);
        scan_node.__set_changelog_column_mappings(mappings);
    }

    Status init(TChangelogScanType::type mode) {
        return OlapScanner::_init_changelog_read_mappings(scan_node, slots, tablet, mode,
                                                          *read_schema);
    }
};

} // namespace

TEST(OlapScannerTest, ExplicitChangelogPairsResolveFinalTupleSlots) {
    ChangelogReadFixture fixture;
    ASSERT_TRUE(fixture.init(TChangelogScanType::MIN_DELTA).ok());
    EXPECT_EQ(1, fixture.read_schema->before_column_ordinal(4));
    EXPECT_EQ(3, fixture.read_schema->tso_ordinal());
    EXPECT_EQ(0, fixture.read_schema->op_ordinal());
    EXPECT_EQ(-1, fixture.read_schema->lsn_ordinal());
}

TEST(OlapScannerTest, ChangelogMappingsDistinguishMissingAndEmpty) {
    ChangelogReadFixture fixture(false);
    EXPECT_TRUE(fixture.init(TChangelogScanType::DETAIL).ok());
    EXPECT_TRUE(fixture.read_schema->changelog_value_column_pairs().empty());
    fixture.scan_node.changelog_column_mappings.__isset.pairs = false;
    EXPECT_FALSE(fixture.init(TChangelogScanType::DETAIL).ok());
    fixture.scan_node.__isset.changelog_column_mappings = false;
    EXPECT_FALSE(fixture.init(TChangelogScanType::MIN_DELTA).ok());
    EXPECT_TRUE(fixture.init(TChangelogScanType::APPEND_ONLY).ok());
}

TEST(OlapScannerTest, ChangelogMappingsRejectUnknownAndIncompletePairs) {
    ChangelogReadFixture fixture;
    auto& pair = fixture.scan_node.changelog_column_mappings.pairs[0];
    pair.__set_before_slot_id(999);
    EXPECT_FALSE(fixture.init(TChangelogScanType::DETAIL).ok());
    pair.__set_before_slot_id(88);
    pair.__isset.current_slot_id = false;
    EXPECT_FALSE(fixture.init(TChangelogScanType::DETAIL).ok());
    pair.__set_current_slot_id(89);
    pair.__set_before_slot_id(90); // key column
    EXPECT_FALSE(fixture.init(TChangelogScanType::DETAIL).ok());
    fixture.scan_node.changelog_column_mappings.pairs.clear();
    EXPECT_FALSE(fixture.init(TChangelogScanType::MIN_DELTA).ok());
}

TEST(OlapScannerTest, MinDeltaRequiresUnprojectedValues) {
    ChangelogReadFixture fixture;
    TabletColumn missing;
    missing.set_unique_id(15);
    missing.set_name("unprojected_value");
    missing.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    fixture.tablet.append_column(missing);
    EXPECT_FALSE(fixture.init(TChangelogScanType::MIN_DELTA).ok());
    EXPECT_TRUE(fixture.init(TChangelogScanType::DETAIL).ok());
}

TEST(OlapScannerTest, AppendOnlyAllowsNestedPruningButMinDeltaRequiresFullShape) {
    ChangelogReadFixture fixture;
    for (int uid : {11, 12}) {
        auto column = fixture.tablet.columns()[fixture.tablet.field_index(uid)];
        column->set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
        for (const std::string& name : {"a", "b"}) {
            TabletColumn child;
            child.set_name(name);
            child.set_type(FieldType::OLAP_FIELD_TYPE_INT);
            column->add_sub_column(child);
        }
    }
    DataTypes types;
    for (const auto& column : fixture.read_schema->columns()) {
        if (column->unique_id() == 11 || column->unique_id() == 12) {
            types.push_back(std::make_shared<DataTypeStruct>(
                    DataTypes {std::make_shared<DataTypeInt32>()}, Strings {"a"}));
        } else {
            types.push_back(column->get_vec_type());
        }
    }
    fixture.read_schema =
            std::make_unique<ReadSchema>(fixture.read_schema->columns(), std::move(types));
    EXPECT_FALSE(fixture.init(TChangelogScanType::MIN_DELTA).ok());
    fixture.scan_node.changelog_column_mappings.pairs.clear();
    EXPECT_TRUE(fixture.init(TChangelogScanType::APPEND_ONLY).ok());
    auto pruned = std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeInt32>()},
                                                   Strings {"a"});
    EXPECT_TRUE(fixture.read_schema->data_type(4)->equals(*pruned));
}

TEST(OlapScannerTest, ChangelogScanRequiresSystemColumns) {
    ChangelogReadFixture fixture;
    fixture.slots.erase(fixture.slots.begin()); // omit OP from the final tuple
    auto columns = fixture.read_schema->columns();
    columns.erase(columns.begin());
    fixture.read_schema = std::make_unique<ReadSchema>(std::move(columns));
    EXPECT_FALSE(fixture.init(TChangelogScanType::DETAIL).ok());
}

TEST(OlapScannerTest, KeyOnlyMinDeltaAcceptsExplicitEmptyMappings) {
    ChangelogReadFixture fixture;
    std::vector<TabletColumnPtr> columns;
    std::vector<SlotDescriptor*> slots;
    for (size_t ordinal = 0; ordinal < fixture.slots.size(); ++ordinal) {
        auto column = fixture.read_schema->columns()[ordinal];
        if (column->unique_id() == 11 || column->unique_id() == 12) {
            continue;
        }
        columns.push_back(column);
        slots.push_back(fixture.slots[ordinal]);
    }
    fixture.tablet.clear_columns();
    for (const auto& column : columns) {
        fixture.tablet.append_column(*column);
    }
    fixture.read_schema = std::make_unique<ReadSchema>(std::move(columns));
    fixture.slots = std::move(slots);
    fixture.scan_node.changelog_column_mappings.pairs.clear();
    EXPECT_TRUE(fixture.init(TChangelogScanType::MIN_DELTA).ok());
}

} // namespace doris
