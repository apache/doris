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

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>

#include "core/block/block.h"
#include "exec/operator/cache_sink_operator.h"
#include "exec/operator/cache_source_operator.h"
#include "exec/operator/repeat_operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
namespace doris {

class QueryCacheMockChildOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, Block* block, bool* eos) override {
        return Status::OK();
    }

    Status get_block_impl(RuntimeState* state, Block* block, bool* eos) override {
        return Status::OK();
    }
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        return Status::OK();
    }

    const RowDescriptor& row_desc() const override { return *_mock_row_desc; }

private:
    std::unique_ptr<MockRowDescriptor> _mock_row_desc;
};

struct QueryCacheOperatorTest : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        state->_batch_size = 10;
        child_op = std::make_unique<QueryCacheMockChildOperator>();
        query_cache_uptr.reset(QueryCache::create_global_cache(1024 * 1024 * 1024));
        query_cache = query_cache_uptr.get();
        scan_ranges.clear();
        TScanRangeParams scan_range;
        TPaloScanRange palp_scan_range;
        palp_scan_range.__set_tablet_id(42);
        palp_scan_range.__set_version("114514");
        scan_range.scan_range.__set_palo_scan_range(palp_scan_range);
        scan_ranges.push_back(scan_range);
    }
    void TearDown() override {
        // Must release every QueryCacheHandle holder before query_cache_uptr is
        // destroyed. C++ destroys members in reverse declaration order, so
        // `state` (local states) and `source`/`sink` (whose QueryCacheRuntime
        // owns the decisions that pin cache entries) would otherwise outlive
        // the cache and release their handles into a destroyed QueryCache.
        // Local states built manually and never moved into `state` are
        // released here too, for the same reason.
        source_local_state_uptr.reset();
        sink_local_state_uptr.reset();
        state.reset();
        source.reset();
        sink.reset();
    }
    void create_local_state() {
        shared_state = sink->create_shared_state();
        {
            sink_local_state_uptr = CacheSinkLocalState ::create_unique(sink.get(), state.get());
            sink_local_state = sink_local_state_uptr.get();
            LocalSinkStateInfo info {.task_idx = 0,
                                     .parent_profile = &profile,
                                     .sender_id = 0,
                                     .shared_state = shared_state.get(),
                                     .shared_state_map = {},
                                     .tsink = TDataSink {}};
            EXPECT_TRUE(sink_local_state_uptr->init(state.get(), info).ok());
            state->emplace_sink_local_state(0, std::move(sink_local_state_uptr));
        }

        {
            source_local_state_uptr =
                    CacheSourceLocalState::create_unique(state.get(), source.get());
            source_local_state = source_local_state_uptr.get();
            source_local_state->_global_cache = query_cache;
            LocalStateInfo info {.parent_profile = &profile,
                                 .scan_ranges = scan_ranges,
                                 .shared_state = shared_state.get(),
                                 .shared_state_map = {},
                                 .task_idx = 0};

            EXPECT_TRUE(source_local_state_uptr->init(state.get(), info).ok());
            state->resize_op_id_to_local_state(-100);
            state->emplace_local_state(source->operator_id(), std::move(source_local_state_uptr));
        }

        { EXPECT_TRUE(sink_local_state->open(state.get()).ok()); }

        { EXPECT_TRUE(source_local_state->open(state.get()).ok()); }
    }

    RuntimeProfile profile {"test"};
    std::unique_ptr<CacheSinkOperatorX> sink;
    std::unique_ptr<CacheSourceOperatorX> source;

    std::unique_ptr<CacheSinkLocalState> sink_local_state_uptr;

    CacheSinkLocalState* sink_local_state;

    std::unique_ptr<CacheSourceLocalState> source_local_state_uptr;
    CacheSourceLocalState* source_local_state;

    std::shared_ptr<MockRuntimeState> state;

    std::shared_ptr<QueryCacheMockChildOperator> child_op;

    ObjectPool pool;

    std::shared_ptr<BasicSharedState> shared_state;

    std::unique_ptr<QueryCache> query_cache_uptr;
    QueryCache* query_cache;

    std::vector<TScanRangeParams> scan_ranges;
};

TEST_F(QueryCacheOperatorTest, test_no_hit_cache1) {
    sink = std::make_unique<CacheSinkOperatorX>();
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 1000;

    // Exercise the production constructor once; the other tests use the
    // BE_TEST default constructor and assign the members directly.
    source = std::make_unique<CacheSourceOperatorX>(
            &pool, /*plan_node_id=*/0, /*operator_id=*/0, cache_param,
            std::make_shared<QueryCacheRuntime>(cache_param, query_cache));
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &pool});
    create_local_state();

    std::cout << query_cache->get_element_count() << std::endl;

    {
        auto block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        std::cout << block.dump_data() << std::endl;
        std::cout << query_cache->get_element_count() << std::endl;
        EXPECT_EQ(block.rows(), 5);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5})));
        EXPECT_EQ(query_cache->get_element_count(), 1);
    }
    EXPECT_TRUE(source_local_state->_need_insert_cache);
}

TEST_F(QueryCacheOperatorTest, test_no_hit_cache2) {
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 3;

    source->_cache_param = cache_param;
    source->_query_cache_runtime = std::make_shared<QueryCacheRuntime>(cache_param, query_cache);
    create_local_state();

    std::cout << query_cache->get_element_count() << std::endl;

    {
        auto block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        std::cout << block.dump_data() << std::endl;
        std::cout << query_cache->get_element_count() << std::endl;
        EXPECT_EQ(block.rows(), 5);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5})));
        EXPECT_EQ(query_cache->get_element_count(), 0);
    }
    EXPECT_FALSE(source_local_state->_need_insert_cache);
}

TEST_F(QueryCacheOperatorTest, test_hit_cache) {
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 3;

    {
        int64_t version = 0;
        std::string cache_key;
        EXPECT_TRUE(
                QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        query_cache->insert(cache_key, version, result, {0, 2, 3}, 1);
    }

    source->_cache_param = cache_param;
    source->_query_cache_runtime = std::make_shared<QueryCacheRuntime>(cache_param, query_cache);
    create_local_state();

    std::cout << query_cache->get_element_count() << std::endl;
    EXPECT_EQ(source_local_state->_slot_orders.size(), 1);
    EXPECT_EQ(source_local_state->_slot_orders[0], 0);

    {
        auto block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        std::cout << block.dump_data() << std::endl;
        std::cout << query_cache->get_element_count() << std::endl;
        EXPECT_EQ(block.rows(), 5);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5})));
    }

    {
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_TRUE(block.empty());
    }
}

TEST_F(QueryCacheOperatorTest, test_stale_full_recompute) {
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 1000;

    std::string cache_key;
    {
        // A stale entry (version 100 < scan version 114514) without
        // allow_incremental: plain miss, full recompute, write back overwrites.
        int64_t version = 0;
        EXPECT_TRUE(
                QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = ColumnHelper::create_block<DataTypeInt64>({100, 200});
        query_cache->insert(cache_key, 100, result, {0}, 1);
    }

    source->_cache_param = cache_param;
    source->_query_cache_runtime = std::make_shared<QueryCacheRuntime>(cache_param, query_cache);
    create_local_state();

    EXPECT_TRUE(source_local_state->_need_insert_cache);
    EXPECT_FALSE(source_local_state->_is_incremental);

    {
        auto block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 5);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5})));
    }

    // The stale entry is overwritten by the recomputed one.
    doris::QueryCacheHandle handle;
    EXPECT_TRUE(query_cache->lookup(cache_key, 114514, &handle));
    EXPECT_EQ(handle.get_cache_delta_count(), 0);
}

TEST_F(QueryCacheOperatorTest, test_incremental_merge) {
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 1000;
    cache_param.__set_allow_incremental(true);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    {
        // The cached partial blocks of version 100, already merged once before.
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        query_cache->insert(cache_key, 100, result, {0}, 1, /*delta_count=*/1);
    }

    source->_cache_param = cache_param;
    source->_query_cache_runtime = std::make_shared<QueryCacheRuntime>(cache_param, query_cache);
    // Hand-craft the INCREMENTAL decision (capturing a real delta read source
    // needs a storage engine, which unit tests do not have). The scan side
    // would scan only (100, 114514] and feed the delta through the sink.
    {
        auto decision = std::make_shared<QueryCacheInstanceDecision>();
        decision->mode = QueryCacheInstanceDecision::Mode::INCREMENTAL;
        decision->key_valid = true;
        decision->cache_key = cache_key;
        decision->current_version = version;
        decision->cached_version = 100;
        decision->cached_delta_count = 1;
        EXPECT_TRUE(query_cache->lookup_any_version(cache_key, &decision->handle));
        source->_query_cache_runtime->inject_decision_for_test(cache_key, decision);
    }
    create_local_state();

    EXPECT_TRUE(source_local_state->_need_insert_cache);
    EXPECT_TRUE(source_local_state->_is_incremental);
    const std::string* stale =
            source_local_state->custom_profile()->get_info_string("HitCacheStale");
    ASSERT_NE(stale, nullptr);
    EXPECT_EQ(*stale, "1");
    const std::string* delta_versions =
            source_local_state->custom_profile()->get_info_string("IncrementalDeltaVersions");
    ASSERT_NE(delta_versions, nullptr);
    EXPECT_EQ(*delta_versions, "(100, 114514]");

    {
        // The delta partial result produced by scanning only (100, 114514].
        auto block = ColumnHelper::create_block<DataTypeInt64>({6, 7});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        // First the cached blocks are emitted ...
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 5);
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5})));
    }
    {
        // ... then the delta from the data queue; both are partial aggregation
        // states merged by the upstream aggregation.
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 2);
        EXPECT_TRUE(ColumnHelper::block_equal(block,
                                              ColumnHelper::create_block<DataTypeInt64>({6, 7})));
    }

    // The merged entry is written back under the new version with an increased
    // delta count, holding both the cached and the delta blocks.
    doris::QueryCacheHandle handle;
    EXPECT_TRUE(query_cache->lookup(cache_key, 114514, &handle));
    EXPECT_EQ(handle.get_cache_delta_count(), 2);
    int64_t total_rows = 0;
    for (const auto& cached_block : *handle.get_cache_result()) {
        total_rows += cached_block->rows();
    }
    EXPECT_EQ(total_rows, 7);
}

TEST_F(QueryCacheOperatorTest, test_incremental_over_entry_limit) {
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    // The cached part alone (5 rows) already exceeds the limit, so the merged
    // entry must not be written back; the stale entry stays untouched.
    cache_param.entry_max_rows = 3;
    cache_param.__set_allow_incremental(true);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    {
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        query_cache->insert(cache_key, 100, result, {0}, 1, /*delta_count=*/0);
    }

    source->_cache_param = cache_param;
    source->_query_cache_runtime = std::make_shared<QueryCacheRuntime>(cache_param, query_cache);
    {
        auto decision = std::make_shared<QueryCacheInstanceDecision>();
        decision->mode = QueryCacheInstanceDecision::Mode::INCREMENTAL;
        decision->key_valid = true;
        decision->cache_key = cache_key;
        decision->current_version = version;
        decision->cached_version = 100;
        decision->cached_delta_count = 0;
        EXPECT_TRUE(query_cache->lookup_any_version(cache_key, &decision->handle));
        source->_query_cache_runtime->inject_decision_for_test(cache_key, decision);
    }
    create_local_state();

    EXPECT_TRUE(source_local_state->_need_insert_cache);

    {
        auto block = ColumnHelper::create_block<DataTypeInt64>({6, 7});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        // Emitting the cached blocks overruns entry_max_rows: caching stops but
        // the data still flows.
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 5);
        EXPECT_FALSE(source_local_state->_need_insert_cache);
    }
    {
        // The delta passes through unchanged.
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 2);
        EXPECT_TRUE(ColumnHelper::block_equal(block,
                                              ColumnHelper::create_block<DataTypeInt64>({6, 7})));
    }

    // No write back happened: the stale entry still carries version 100.
    doris::QueryCacheHandle handle;
    EXPECT_FALSE(query_cache->lookup(cache_key, 114514, &handle));
    doris::QueryCacheHandle stale_handle;
    EXPECT_TRUE(query_cache->lookup_any_version(cache_key, &stale_handle));
    EXPECT_EQ(stale_handle.get_cache_version(), 100);
}

TEST_F(QueryCacheOperatorTest, test_incremental_write_back_infeasible) {
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 1000;
    cache_param.__set_allow_incremental(true);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    {
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        query_cache->insert(cache_key, 100, result, {0}, 1);
    }

    source->_cache_param = cache_param;
    source->_query_cache_runtime = std::make_shared<QueryCacheRuntime>(cache_param, query_cache);
    // The decision already knows the merged entry could never fit the limits:
    // scan only the delta, but never clone blocks for a doomed write back.
    {
        auto decision = std::make_shared<QueryCacheInstanceDecision>();
        decision->mode = QueryCacheInstanceDecision::Mode::INCREMENTAL;
        decision->key_valid = true;
        decision->write_back_feasible = false;
        decision->cache_key = cache_key;
        decision->current_version = version;
        decision->cached_version = 100;
        decision->cached_delta_count = 0;
        EXPECT_TRUE(query_cache->lookup_any_version(cache_key, &decision->handle));
        source->_query_cache_runtime->inject_decision_for_test(cache_key, decision);
    }
    create_local_state();

    EXPECT_FALSE(source_local_state->_need_insert_cache);
    EXPECT_TRUE(source_local_state->_is_incremental);

    {
        auto block = ColumnHelper::create_block<DataTypeInt64>({6, 7});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    {
        // Cached blocks flow through without being cloned for a write back.
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        EXPECT_EQ(block.rows(), 5);
        EXPECT_TRUE(source_local_state->_local_cache_blocks.empty());
    }
    {
        Block block;
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 2);
        EXPECT_TRUE(source_local_state->_local_cache_blocks.empty());
    }

    // No write back: the stale entry still carries version 100.
    doris::QueryCacheHandle handle;
    EXPECT_FALSE(query_cache->lookup(cache_key, 114514, &handle));
    doris::QueryCacheHandle stale_handle;
    EXPECT_TRUE(query_cache->lookup_any_version(cache_key, &stale_handle));
    EXPECT_EQ(stale_handle.get_cache_version(), 100);
}

TEST_F(QueryCacheOperatorTest, test_incremental_fallback_reason_in_profile) {
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &pool});
    // Use a tablet id that exists nowhere so fetching the tablet fails even if
    // another suite left a storage engine registered in this test process.
    constexpr int64_t kMissingTabletId = 424242424242;
    scan_ranges.clear();
    TScanRangeParams scan_range;
    TPaloScanRange palo_scan_range;
    palo_scan_range.__set_tablet_id(kMissingTabletId);
    palo_scan_range.__set_version("114514");
    scan_range.scan_range.__set_palo_scan_range(palo_scan_range);
    scan_ranges.push_back(scan_range);

    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({kMissingTabletId, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 1000;
    cache_param.__set_allow_incremental(true);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    {
        // A stale entry with incremental allowed, but the tablet does not exist
        // in this test process: the decision falls back to a full recompute and
        // reports why in the query profile.
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = ColumnHelper::create_block<DataTypeInt64>({100, 200});
        query_cache->insert(cache_key, 100, result, {0}, 1);
    }

    source->_cache_param = cache_param;
    source->_query_cache_runtime = std::make_shared<QueryCacheRuntime>(cache_param, query_cache);
    create_local_state();

    EXPECT_FALSE(source_local_state->_is_incremental);
    EXPECT_TRUE(source_local_state->_need_insert_cache);
    const std::string* reason =
            source_local_state->custom_profile()->get_info_string("IncrementalFallbackReason");
    ASSERT_NE(reason, nullptr);
    EXPECT_EQ(*reason, "tablet not found");
}

TEST_F(QueryCacheOperatorTest, test_missing_runtime_fails_init) {
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 1000;

    // No runtime injected: a pass-through here could silently drop data if
    // the paired scan still made a HIT decision, so init must fail loudly.
    source->_cache_param = cache_param;
    shared_state = sink->create_shared_state();
    source_local_state_uptr = CacheSourceLocalState::create_unique(state.get(), source.get());
    source_local_state = source_local_state_uptr.get();
    source_local_state->_global_cache = query_cache;
    LocalStateInfo info {.parent_profile = &profile,
                         .scan_ranges = scan_ranges,
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};
    Status st = source_local_state_uptr->init(state.get(), info);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("query cache runtime is absent at the cache source") !=
                std::string::npos)
            << st.to_string();
    EXPECT_EQ(query_cache->get_element_count(), 0);
}

} // namespace doris
