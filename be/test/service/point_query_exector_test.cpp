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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <unordered_set>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "exprs/vexpr.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/point_query_executor.h"
#include "storage/read_time_hidden_column.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

// Helper class for setting up Reusable objects to test LookupConnectionCache
class ReusableTestHelper {
public:
    static TDescriptorTable create_descriptor_tablet() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_TINYINT)
                                       .column_name("k1")
                                       .column_pos(0)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_SMALLINT)
                                       .column_name("k2")
                                       .column_pos(1)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).column_name("k3").column_pos(2).build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k4").column_pos(3).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_LARGEINT)
                                       .column_name("k5")
                                       .column_pos(4)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_DATE).column_name("k6").column_pos(5).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATETIME)
                                       .column_name("k7")
                                       .column_pos(6)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(4).column_name("k8").column_pos(7).build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(65).column_name("k9").column_pos(8).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .decimal_type(6, 3)
                                       .column_name("k10")
                                       .column_pos(9)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATEV2)
                                       .column_name("k11")
                                       .column_pos(10)
                                       .build());

        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_TINYINT)
                                       .column_name("v1")
                                       .column_pos(11)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_SMALLINT)
                                       .column_name("v2")
                                       .column_pos(12)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .column_name("v3")
                                       .column_pos(13)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_BIGINT)
                                       .column_name("v4")
                                       .column_pos(14)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_LARGEINT)
                                       .column_name("v5")
                                       .column_pos(15)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATE)
                                       .column_name("v6")
                                       .column_pos(16)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATETIME)
                                       .column_name("v7")
                                       .column_pos(17)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(4)
                                       .column_name("v8")
                                       .column_pos(18)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65)
                                       .column_name("v9")
                                       .column_pos(19)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .decimal_type(6, 3)
                                       .column_name("v10")
                                       .column_pos(20)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATEV2)
                                       .column_name("v11")
                                       .column_pos(21)
                                       .nullable(false)
                                       .build());
        tuple_builder.build(&dtb);

        return dtb.desc_tbl();
    }

    static std::shared_ptr<Reusable> create_reusable() {
        auto obj_pool = std::make_unique<ObjectPool>();
        auto runtime_state = RuntimeState::create_unique();
        auto reusable = std::make_shared<Reusable>();

        // Create a simple TupleDescriptor for testing
        TDescriptorTable t_desc_tbl = create_descriptor_tablet();

        // Initialize Reusable
        Status st = reusable->init(t_desc_tbl, output_exprs, query_options, *tablet_schema, 2);
        if (!st.ok()) {
            return nullptr;
        }

        return reusable;
    }

    static std::vector<TExpr> output_exprs;
    static TQueryOptions query_options;
    static std::shared_ptr<TabletSchema> tablet_schema;
};

std::vector<TExpr> ReusableTestHelper::output_exprs = []() {
    std::vector<TExpr> list;
    TExpr expr;
    expr.nodes.emplace_back(TExprNode());
    expr.nodes[0].node_type = TExprNodeType::SLOT_REF;
    auto type = TTypeDesc();
    type.types.emplace_back(TTypeNode());
    type.types[0].type = TTypeNodeType::SCALAR;
    type.types[0].__isset.scalar_type = true;
    type.types[0].scalar_type.type = TPrimitiveType::BIGINT;
    expr.nodes[0].type = type;
    expr.nodes[0].num_children = 0;
    list.push_back(expr);
    return list;
}();

TQueryOptions ReusableTestHelper::query_options = TQueryOptions();
std::shared_ptr<TabletSchema> ReusableTestHelper::tablet_schema = []() {
    auto schema = std::make_shared<TabletSchema>();
    for (int i = 0; i < 11; ++i) {
        schema->append_column(TabletColumn(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                           FieldType::OLAP_FIELD_TYPE_BIGINT));
    }
    return schema;
}();

class PointQueryHiddenColumnTest : public testing::Test {
protected:
    static constexpr int32_t kKeyUid = 10;
    static constexpr int32_t kValueUid = 11;
    static constexpr int32_t kVersionUid = 12;
    static constexpr int32_t kCommitTsoUid = 13;
    static constexpr int32_t kBinlogTsoUid = 14;
    static constexpr int32_t kDeleteSignUid = 15;
    static constexpr int32_t kRowStoreUid = 16;

    void SetUp() override {
        add_column(kKeyUid, "k1");
        add_column(kValueUid, "v1");
        add_column(kVersionUid, VERSION_COL);
        add_column(kCommitTsoUid, COMMIT_TSO_COL);
        add_column(kBinlogTsoUid, BINLOG_TSO_COL);
        add_column(kDeleteSignUid, DELETE_SIGN);
    }

    void add_column(int32_t uid, const std::string& name) {
        TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                            FieldType::OLAP_FIELD_TYPE_BIGINT, false);
        column.set_unique_id(uid);
        column.set_name(name);
        _schema.append_column(column);
    }

    void add_row_store(const std::vector<int32_t>& row_column_uids = {}) {
        TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                            FieldType::OLAP_FIELD_TYPE_STRING, false);
        column.set_unique_id(kRowStoreUid);
        column.set_name(BeConsts::ROW_STORE_COL);
        _schema.append_column(column);
        _schema._row_store_column_unique_ids = row_column_uids;
    }

    void init_reusable(const std::vector<int32_t>& tuple_uids,
                       const std::vector<int32_t>& output_uids) {
        TDescriptorTableBuilder descriptor_builder;
        TTupleDescriptorBuilder tuple_builder;
        for (size_t i = 0; i < tuple_uids.size(); ++i) {
            const int32_t uid = tuple_uids[i];
            auto slot = TSlotDescriptorBuilder()
                                .type(TYPE_BIGINT)
                                .column_name(_schema.column_by_uid(uid).name())
                                .column_pos(i)
                                .nullable(false)
                                .build();
            slot.__set_col_unique_id(uid);
            tuple_builder.add_slot(slot);
        }
        tuple_builder.build(&descriptor_builder);
        const auto descriptor_table = descriptor_builder.desc_tbl();
        std::vector<TExpr> output_exprs;
        for (int32_t uid : output_uids) {
            for (const auto& slot : descriptor_table.slotDescriptors) {
                if (slot.col_unique_id == uid) {
                    TExprNode node;
                    node.__set_node_type(TExprNodeType::SLOT_REF);
                    node.__set_type(slot.slotType);
                    node.__set_num_children(0);
                    TSlotRef slot_ref;
                    slot_ref.__set_slot_id(slot.id);
                    slot_ref.__set_tuple_id(slot.parent);
                    node.__set_slot_ref(slot_ref);
                    TExpr expr;
                    expr.nodes.push_back(node);
                    output_exprs.push_back(expr);
                    break;
                }
            }
        }
        const auto status =
                _reusable.init(descriptor_table, output_exprs, TQueryOptions(), _schema);
        ASSERT_TRUE(status.ok()) << status;
    }

    TabletSchema _schema;
    Reusable _reusable;
};

TEST_F(PointQueryHiddenColumnTest, FullRowStoreLeavesHiddenColumnsOutOfJsonb) {
    add_row_store();
    ASSERT_NO_FATAL_FAILURE(init_reusable({kVersionUid, kKeyUid, kCommitTsoUid, kDeleteSignUid},
                                          {kVersionUid, kKeyUid, kCommitTsoUid, kVersionUid}));
    EXPECT_TRUE(_reusable.missing_col_uids().empty());
    // The JSONB decode is narrowed to the slots it still serves.
    EXPECT_EQ((std::unordered_set<int32_t> {kKeyUid, kDeleteSignUid}),
              _reusable.include_col_uids());
    EXPECT_EQ((std::unordered_set<int32_t> {kVersionUid, kCommitTsoUid}),
              _reusable.column_store_col_uids());
    EXPECT_TRUE(_reusable.has_rowset_derived_hidden_columns());
    EXPECT_TRUE(_reusable.decode_row_store());
    EXPECT_EQ(kRowStoreUid, _reusable.rs_column_uid());
    EXPECT_EQ(3, _reusable.delete_sign_idx());
}

TEST_F(PointQueryHiddenColumnTest, FullRowStoreHiddenOnlyProjectionSkipsJsonb) {
    add_row_store();
    ASSERT_NO_FATAL_FAILURE(
            init_reusable({kVersionUid, kCommitTsoUid}, {kCommitTsoUid, kVersionUid}));
    EXPECT_TRUE(_reusable.missing_col_uids().empty());
    EXPECT_TRUE(_reusable.include_col_uids().empty());
    EXPECT_EQ((std::unordered_set<int32_t> {kVersionUid, kCommitTsoUid}),
              _reusable.column_store_col_uids());
    EXPECT_TRUE(_reusable.has_rowset_derived_hidden_columns());
    // An empty include set would decode every slot again, so the JSONB is not decoded at all.
    EXPECT_FALSE(_reusable.decode_row_store());
    EXPECT_EQ(kRowStoreUid, _reusable.rs_column_uid());
}

TEST_F(PointQueryHiddenColumnTest, PartialRowStoreDoesNotTrustStoredHiddenColumns) {
    add_row_store({kKeyUid, kVersionUid, kCommitTsoUid});
    ASSERT_NO_FATAL_FAILURE(init_reusable({kKeyUid, kVersionUid, kCommitTsoUid, kValueUid},
                                          {kKeyUid, kVersionUid, kCommitTsoUid, kValueUid}));
    EXPECT_EQ((std::unordered_set<int32_t> {kValueUid}), _reusable.missing_col_uids());
    EXPECT_EQ((std::unordered_set<int32_t> {kKeyUid}), _reusable.include_col_uids());
    EXPECT_EQ((std::unordered_set<int32_t> {kVersionUid, kCommitTsoUid, kValueUid}),
              _reusable.column_store_col_uids());
    EXPECT_TRUE(_reusable.decode_row_store());
}

TEST_F(PointQueryHiddenColumnTest, PartialRowStoreKeepsMissingHiddenColumnsInColumnStore) {
    add_row_store({kKeyUid});
    ASSERT_NO_FATAL_FAILURE(init_reusable({kKeyUid, kVersionUid}, {kKeyUid, kVersionUid}));
    EXPECT_EQ((std::unordered_set<int32_t> {kVersionUid}), _reusable.missing_col_uids());
    EXPECT_EQ((std::unordered_set<int32_t> {kKeyUid}), _reusable.include_col_uids());
    EXPECT_EQ((std::unordered_set<int32_t> {kVersionUid}), _reusable.column_store_col_uids());
}

TEST_F(PointQueryHiddenColumnTest, OrdinaryProjectionKeepsFullRowStoreFastPath) {
    add_row_store();
    ASSERT_NO_FATAL_FAILURE(init_reusable({kKeyUid, kValueUid}, {kValueUid}));
    EXPECT_TRUE(_reusable.missing_col_uids().empty());
    EXPECT_TRUE(_reusable.include_col_uids().empty());
    EXPECT_TRUE(_reusable.column_store_col_uids().empty());
    EXPECT_FALSE(_reusable.has_rowset_derived_hidden_columns());
    EXPECT_TRUE(_reusable.decode_row_store());
    EXPECT_EQ(kRowStoreUid, _reusable.rs_column_uid());
}

TEST_F(PointQueryHiddenColumnTest, NoRowStoreReadsAllProjectedColumnsIndependently) {
    ASSERT_NO_FATAL_FAILURE(init_reusable({kVersionUid, kKeyUid, kCommitTsoUid},
                                          {kKeyUid, kCommitTsoUid, kVersionUid}));
    EXPECT_EQ((std::unordered_set<int32_t> {kVersionUid, kKeyUid, kCommitTsoUid}),
              _reusable.missing_col_uids());
    EXPECT_EQ(_reusable.missing_col_uids(), _reusable.column_store_col_uids());
    EXPECT_TRUE(_reusable.include_col_uids().empty());
    EXPECT_TRUE(_reusable.has_rowset_derived_hidden_columns());
    EXPECT_FALSE(_reusable.decode_row_store());
    EXPECT_EQ(-1, _reusable.rs_column_uid());
}

TEST_F(PointQueryHiddenColumnTest, BinlogTsoStaysInJsonbAndKeepsRowCache) {
    add_row_store();
    ASSERT_NO_FATAL_FAILURE(init_reusable({kBinlogTsoUid, kKeyUid}, {kBinlogTsoUid, kKeyUid}));
    EXPECT_TRUE(_reusable.missing_col_uids().empty());
    EXPECT_TRUE(_reusable.include_col_uids().empty());
    EXPECT_TRUE(_reusable.column_store_col_uids().empty());
    EXPECT_FALSE(_reusable.has_rowset_derived_hidden_columns());
    EXPECT_TRUE(_reusable.decode_row_store());
}

TEST_F(PointQueryHiddenColumnTest, RowsetDerivedValueOnlyForSingletonRowsets) {
    // A compacted rowset keeps its materialized per-row values.
    EXPECT_FALSE(get_read_time_hidden_column_value(ReadTimeHiddenColumnType::VERSION, Version(2, 3),
                                                   TsoRange(2, 3), false)
                         .has_value());
    EXPECT_FALSE(get_read_time_hidden_column_value(ReadTimeHiddenColumnType::COMMIT_TSO,
                                                   Version(2, 3), TsoRange(2, 3), false)
                         .has_value());
    // A singleton rowset answers with its own version and assigned commit TSO.
    EXPECT_EQ(7, get_read_time_hidden_column_value(ReadTimeHiddenColumnType::VERSION, Version(7, 7),
                                                   TsoRange(8, 8), false)
                         ->get<TYPE_BIGINT>());
    EXPECT_EQ(8, get_read_time_hidden_column_value(ReadTimeHiddenColumnType::COMMIT_TSO,
                                                   Version(7, 7), TsoRange(8, 8), false)
                         ->get<TYPE_BIGINT>());
    // An unassigned commit TSO and BINLOG_TSO outside a row-binlog read keep the stored value.
    EXPECT_FALSE(get_read_time_hidden_column_value(ReadTimeHiddenColumnType::COMMIT_TSO,
                                                   Version(7, 7), TsoRange(), false)
                         .has_value());
    EXPECT_FALSE(get_read_time_hidden_column_value(ReadTimeHiddenColumnType::BINLOG_TSO,
                                                   Version(7, 7), TsoRange(8, 8), false)
                         .has_value());
}

// RowCache test class
class RowCacheTest : public testing::Test {
protected:
    void SetUp() override {
        // Create RowCache instance
        _row_cache = new RowCache(1024, 4); // 1KB cache, 4 shards
    }

    void TearDown() override { delete _row_cache; }

    RowCache* _row_cache = nullptr;
};

// Test basic RowCache functionality
TEST_F(RowCacheTest, PQTestBasicOperations) {
    // Create test data
    const int64_t tablet_id = 12345;
    const std::string key_data = "test_key";
    Slice key_slice(key_data.c_str(), key_data.size());

    const std::string value_data = "test_value";
    Slice value_slice(value_data.c_str(), value_data.size());

    RowCache::RowCacheKey cache_key(tablet_id, key_slice);

    // Test insertion
    _row_cache->insert(cache_key, value_slice);

    // Test lookup
    RowCache::CacheHandle handle;
    bool found = _row_cache->lookup(cache_key, &handle);
    ASSERT_TRUE(found) << "Cache entry should be found";
    ASSERT_TRUE(handle.valid()) << "Cache handle should be valid";

    // Verify data correctness
    Slice cached_data = handle.data();
    ASSERT_EQ(cached_data.size, value_slice.size) << "Cache data size should match";
    ASSERT_EQ(memcmp(cached_data.data, value_slice.data, value_slice.size), 0)
            << "Cache data content should match";

    // Test deletion
    _row_cache->erase(cache_key);
    RowCache::CacheHandle handle2;
    found = _row_cache->lookup(cache_key, &handle2);
    ASSERT_FALSE(found) << "Cache entry should be deleted";
    ASSERT_FALSE(handle2.valid()) << "Cache handle should be invalid";
}

// Test RowCache LRU mechanism
TEST_F(RowCacheTest, PQTestLRUEviction) {
    // Use small capacity cache to test LRU eviction
    RowCache small_cache(100, 1); // 100 bytes capacity, 1 shard

    // Create enough entries to trigger LRU eviction
    for (int i = 0; i < 10; i++) {
        const int64_t tablet_id = 12345;
        std::string key_data = "key_" + std::to_string(i);
        Slice key_slice(key_data.c_str(), key_data.size());

        // Create values large enough, about 20 bytes each
        std::string value_data = "value_" + std::to_string(i) + std::string(10, 'x');
        Slice value_slice(value_data.c_str(), value_data.size());

        RowCache::RowCacheKey cache_key(tablet_id, key_slice);
        small_cache.insert(cache_key, value_slice);
    }

    // Check if some earlier entries have been evicted
    bool at_least_one_evicted = false;
    for (int i = 0; i < 5; i++) {
        const int64_t tablet_id = 12345;
        std::string key_data = "key_" + std::to_string(i);
        Slice key_slice(key_data.c_str(), key_data.size());

        RowCache::RowCacheKey cache_key(tablet_id, key_slice);
        RowCache::CacheHandle handle;
        if (!small_cache.lookup(cache_key, &handle)) {
            at_least_one_evicted = true;
            break;
        }
    }

    ASSERT_TRUE(at_least_one_evicted) << "At least one cache entry should be evicted";

    // Ensure the most recently accessed entry is still in cache
    const int64_t tablet_id = 12345;
    std::string key_data = "key_9"; // Last inserted key
    Slice key_slice(key_data.c_str(), key_data.size());

    RowCache::RowCacheKey cache_key(tablet_id, key_slice);
    RowCache::CacheHandle handle;
    bool found = small_cache.lookup(cache_key, &handle);
    //evicted
    ASSERT_TRUE(!found);
}

// LookupConnectionCache test class
class LookupConnectionCacheTest : public testing::Test {
protected:
    void SetUp() override {
        // Create LookupConnectionCache instance
        _lookup_cache = new LookupConnectionCache(1024); // 1KB cache
    }

    void TearDown() override { delete _lookup_cache; }

    LookupConnectionCache* _lookup_cache = nullptr;
};

TEST_F(LookupConnectionCacheTest, PQTestLRUEvictionPolicy) {
    LookupConnectionCache cache(40);

    for (int i = 1; i <= 45; ++i) {
        auto reusable = std::make_shared<Reusable>();
        reusable->_block_pool.resize(10);
        cache.add(i, reusable);
    }
    EXPECT_LT(cache.get_element_count(), 45) << "capacity " << cache.get_capacity();
    auto entry2 = cache.get(10);

    auto reusable5 = std::make_shared<Reusable>();
    reusable5->_block_pool.resize(10);
    cache.add(41, reusable5);
    cache.add(42, reusable5);
    cache.add(43, reusable5);
    EXPECT_LT(cache.get_element_count(), 45) << "capacity " << cache.get_capacity();
}

// Test cache capacity boundary and LRU eviction policy
TEST_F(LookupConnectionCacheTest, PQTestCapacityBoundary) {
    LookupConnectionCache cache(40);
    const int num_entries = 45;

    // Insert 15 entries to exceed capacity
    for (int i = 0; i < num_entries; ++i) {
        auto reusable = ReusableTestHelper::create_reusable();
        cache.add(i, reusable);
    }

    // Verify at least 5 entries are evicted
    int found_count = 0;
    for (int i = 0; i < num_entries; ++i) {
        if (cache.get(i) != nullptr) found_count++;
    }
    EXPECT_LT(found_count, 45) << "LRU eviction should maintain capacity limit";
}

// Test thread safety with concurrent operations
TEST_F(LookupConnectionCacheTest, PQTestConcurrentAccess) {
    const int num_threads = 4;
    const int num_ops_per_thread = 1000;
    LookupConnectionCache cache(1024 * 1024 * 100); // 100MB buffer

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            // Mixed operations: 50% insert, 50% query
            for (int j = 0; j < num_ops_per_thread; ++j) {
                int64_t key = i * 1000 + j;
                auto reusable = ReusableTestHelper::create_reusable();

                if (j % 2 == 0) {
                    cache.add(key, reusable); // Insert
                } else {
                    auto entry = cache.get(key); // Query
                    EXPECT_TRUE(entry == nullptr || entry.use_count() == 2);
                }
            }
        });
    }

    for (auto& t : threads) t.join();
}

// Test exceptional input handling
TEST_F(LookupConnectionCacheTest, PQTestInvalidKeys) {
    LookupConnectionCache cache(1024 * 1024);

    // Null value test
    cache.add(0, nullptr);

    // Oversized entry test (1MB limit)
    auto large_reusable = std::make_shared<Reusable>();
    large_reusable->_block_pool.resize(1024 * 1024); // 1MB block
    cache.add(1, large_reusable);
}

// Test key collision handling
TEST_F(LookupConnectionCacheTest, PQTestDuplicateAdd) {
    LookupConnectionCache cache(1024 * 1024);
    auto reusable1 = ReusableTestHelper::create_reusable();
    auto reusable2 = ReusableTestHelper::create_reusable();

    // Overwrite existing key
    cache.add(123, reusable1);
    cache.add(123, reusable2);

    auto entry = cache.get(123);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.get(), reusable2.get()) << "Last write should win in key collision";
}

// Test reference counting mechanism
TEST_F(LookupConnectionCacheTest, PQTestEntryLifetime) {
    LookupConnectionCache cache(1024 * 1024);
    {
        auto reusable = ReusableTestHelper::create_reusable();
        cache.add(123, reusable);
        auto entry = cache.get(123);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry.use_count(), 3); // Cache + local reference
    }                                    // Local reference released

    // Verify cache maintains ownership
    auto entry = cache.get(123);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.use_count(), 2) << "Cache should maintain sole ownership after scope exit";
}

} // namespace doris
