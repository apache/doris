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
    // A successful commit latches the flag off (the insert is exactly-once);
    // that the query cached at all is asserted by the element count above,
    // in contrast to test_no_hit_cache2 where the over-limit entry is
    // dropped and the count stays 0.
    EXPECT_FALSE(source_local_state->_need_insert_cache);
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

TEST_F(QueryCacheOperatorTest, test_hit_cache_multi_block_reordered_slots) {
    // Two normalized-equivalent queries can share one digest with different
    // output slot orders, so the entry is served through a column
    // permutation. With MORE THAN ONE cached block the permutation must be
    // applied to each cached block before merging it into the reused output
    // block: the pipeline's clear_column_data() keeps the already-permuted
    // schema, so merging a cached-order block into it positionally breaks --
    // a type error for heterogeneous slots like these (BIGINT/INT swapped),
    // silently misplaced data for same-typed ones.
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    auto* row_desc = new MockRowDescriptor {
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt32>()}, &pool};
    child_op->_mock_row_desc.reset(row_desc);
    // The mock descriptor leaves every slot id at its default; give the two
    // output slots distinct ids so the slot-order mapping is meaningful.
    auto& slots = static_cast<MockTupleDescriptor*>(row_desc->tuple_desc_map.front())->Slots;
    slots[0]->_id = 100;
    slots[1]->_id = 101;

    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[100] = 10;
    cache_param.output_slot_mapping[101] = 11;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 1000;

    {
        // The entry was written by the sibling query whose output order was
        // (INT32 slot 11, INT64 slot 10) -- the reverse of this query.
        int64_t version = 0;
        std::string cache_key;
        EXPECT_TRUE(
                QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = Block({ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2}),
                                ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20})});
        result.push_back(std::make_unique<Block>());
        *result.back() = Block({ColumnHelper::create_column_with_name<DataTypeInt32>({3}),
                                ColumnHelper::create_column_with_name<DataTypeInt64>({30})});
        query_cache->insert(cache_key, version, result, {11, 10}, 1);
    }

    source->_cache_param = cache_param;
    source->_query_cache_runtime = std::make_shared<QueryCacheRuntime>(cache_param, query_cache);
    // In production this operator is also built without a plan node, so its
    // _row_descriptor reports zero materialized slots and clear_column_data()
    // wipes the block every pull -- the schema-carrying reused-block shape is
    // accidentally unreachable today. Report a real slot count here to pin
    // the permute-before-merge invariant against the natural cleanups (a real
    // row descriptor, or removing the redundant per-operator wipe) that would
    // make that shape live.
    source->_row_descriptor._num_materialized_slots = 2;
    create_local_state();

    EXPECT_EQ(source_local_state->_slot_orders, (std::vector<int> {10, 11}));
    EXPECT_EQ(source_local_state->_hit_cache_column_orders, (std::vector<int> {1, 0}));

    {
        auto block = ColumnHelper::create_block<DataTypeInt64>({0});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    // ONE output block reused across pulls, exactly like the pipeline driver
    // (PipelineTask feeds the same block every iteration and
    // clear_column_data() keeps its schema): pull 2 is the shape that broke,
    // because the reused block already carries the permuted schema.
    Block block;
    {
        // First cached block, permuted to this query's order.
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        Block expected({ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20}),
                        ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2})});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected)) << block.dump_data();
    }
    {
        // Second cached block through the schema-carrying reused block.
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
        Block expected({ColumnHelper::create_column_with_name<DataTypeInt64>({30}),
                        ColumnHelper::create_column_with_name<DataTypeInt32>({3})});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected)) << block.dump_data();
    }
}

TEST_F(QueryCacheOperatorTest, test_incremental_reordered_write_back) {
    // An INCREMENTAL merge through a permuted entry: the cached blocks are
    // emitted in this query's slot order and the written-back entry must
    // hold "cached + delta" under this query's slot order as one consistent
    // whole, so a later exact hit through it permutes correctly again.
    sink = std::make_unique<CacheSinkOperatorX>();
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    auto* row_desc = new MockRowDescriptor {
            {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt32>()}, &pool};
    child_op->_mock_row_desc.reset(row_desc);
    auto& slots = static_cast<MockTupleDescriptor*>(row_desc->tuple_desc_map.front())->Slots;
    slots[0]->_id = 100;
    slots[1]->_id = 101;

    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.digest = "test_digest";
    cache_param.output_slot_mapping[100] = 10;
    cache_param.output_slot_mapping[101] = 11;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 1000;
    cache_param.__set_allow_incremental(true);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    {
        // Two stale cached blocks written by the reverse-ordered sibling.
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = Block({ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2}),
                                ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20})});
        result.push_back(std::make_unique<Block>());
        *result.back() = Block({ColumnHelper::create_column_with_name<DataTypeInt32>({3}),
                                ColumnHelper::create_column_with_name<DataTypeInt64>({30})});
        query_cache->insert(cache_key, 100, result, {11, 10}, 1, /*delta_count=*/0);
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
    // Pin the schema-carrying reused-block shape; see the rationale in
    // test_hit_cache_multi_block_reordered_slots.
    source->_row_descriptor._num_materialized_slots = 2;
    create_local_state();

    EXPECT_TRUE(source_local_state->_is_incremental);
    EXPECT_TRUE(source_local_state->_need_insert_cache);
    EXPECT_EQ(source_local_state->_hit_cache_column_orders, (std::vector<int> {1, 0}));

    {
        // The delta partial result arrives in THIS query's slot order.
        auto block = Block({ColumnHelper::create_column_with_name<DataTypeInt64>({40}),
                            ColumnHelper::create_column_with_name<DataTypeInt32>({4})});
        auto st = sink->sink(state.get(), &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    // ONE output block reused across pulls, like the pipeline driver: the
    // second cached pull must permute correctly into the schema-carrying
    // reused block before its write-back snapshot is taken.
    Block block;
    for (int i = 0; i < 2; ++i) {
        // Both cached blocks flow out permuted, without merge errors.
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_FALSE(eos);
    }
    // The write-back snapshots are zero-copy views over the pinned entry's
    // columns (in this query's order), not payload copies: the single
    // materialization happens at insert time.
    const auto& pinned_blocks = *source_local_state->_cache_decision->handle.get_cache_result();
    const auto& kept_blocks = source_local_state->_local_cache_blocks;
    ASSERT_EQ(kept_blocks.size(), 2);
    const auto* pinned_first_int64 = pinned_blocks[0]->get_by_position(1).column.get();
    EXPECT_EQ(kept_blocks[0]->get_by_position(0).column.get(), pinned_first_int64);
    EXPECT_EQ(kept_blocks[0]->get_by_position(1).column.get(),
              pinned_blocks[0]->get_by_position(0).column.get());
    EXPECT_EQ(kept_blocks[1]->get_by_position(0).column.get(),
              pinned_blocks[1]->get_by_position(1).column.get());
    {
        // Then the delta.
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 1);
    }

    // The merged entry holds every block under THIS query's slot order.
    doris::QueryCacheHandle handle;
    EXPECT_TRUE(query_cache->lookup(cache_key, 114514, &handle));
    EXPECT_EQ(handle.get_cache_delta_count(), 1);
    EXPECT_EQ(*handle.get_cache_slot_orders(), (std::vector<int> {10, 11}));
    const auto& cached_blocks = *handle.get_cache_result();
    EXPECT_EQ(cached_blocks.size(), 3);
    // The insert materialized one cache-owned copy: the new entry does not
    // alias the old pinned columns.
    EXPECT_NE(cached_blocks[0]->get_by_position(0).column.get(), pinned_first_int64);
    Block expected0({ColumnHelper::create_column_with_name<DataTypeInt64>({10, 20}),
                     ColumnHelper::create_column_with_name<DataTypeInt32>({1, 2})});
    EXPECT_TRUE(ColumnHelper::block_equal(*cached_blocks[0], expected0))
            << cached_blocks[0]->dump_data();
    Block expected1({ColumnHelper::create_column_with_name<DataTypeInt64>({30}),
                     ColumnHelper::create_column_with_name<DataTypeInt32>({3})});
    EXPECT_TRUE(ColumnHelper::block_equal(*cached_blocks[1], expected1))
            << cached_blocks[1]->dump_data();
    Block expected2({ColumnHelper::create_column_with_name<DataTypeInt64>({40}),
                     ColumnHelper::create_column_with_name<DataTypeInt32>({4})});
    EXPECT_TRUE(ColumnHelper::block_equal(*cached_blocks[2], expected2))
            << cached_blocks[2]->dump_data();

    {
        // A spurious re-poll after eos must not re-publish: the commit is
        // latched off after the insert, so with the write-back set already
        // cleared the merged entry keeps its three blocks instead of being
        // replaced by an EMPTY set under the same version.
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        doris::QueryCacheHandle repoll_handle;
        EXPECT_TRUE(query_cache->lookup(cache_key, 114514, &repoll_handle));
        EXPECT_EQ(repoll_handle.get_cache_result()->size(), 3);
    }
}

TEST_F(QueryCacheOperatorTest, test_failed_final_delta_merge_publishes_nothing) {
    // On the final delta block, eos is observed BEFORE the block is merged:
    // if that merge fails, the entry must NOT be committed, or an incomplete
    // prefix (cached blocks plus earlier deltas, missing the failed block)
    // would be served as the current version by later exact hits. The
    // malformed two-column final block makes the merge fail determinately.
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
        *result.back() = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
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
    // Pin the schema-carrying reused-block shape (rationale in
    // test_hit_cache_multi_block_reordered_slots): with the default empty row
    // descriptor the reused block is wiped every pull and re-cloned from the
    // incoming block, so the malformed final block would merge into a clone
    // of itself and SUCCEED, unbinding this test from the bug it guards.
    source->_row_descriptor._num_materialized_slots = 1;
    create_local_state();
    EXPECT_TRUE(source_local_state->_need_insert_cache);

    {
        // A good delta block, then a malformed final one.
        auto good = ColumnHelper::create_block<DataTypeInt64>({6, 7});
        EXPECT_TRUE(sink->sink(state.get(), &good, false).ok());
        auto bad = Block({ColumnHelper::create_column_with_name<DataTypeInt64>({8}),
                          ColumnHelper::create_column_with_name<DataTypeInt64>({9})});
        EXPECT_TRUE(sink->sink(state.get(), &bad, true).ok());
    }
    Block block;
    {
        // Cached block, then the good delta block.
        bool eos = false;
        EXPECT_TRUE(source->get_block(state.get(), &block, &eos).ok());
        EXPECT_FALSE(eos);
        EXPECT_TRUE(source->get_block(state.get(), &block, &eos).ok());
        EXPECT_FALSE(eos);
    }
    {
        // The final block: eos flips to true first, then the merge fails.
        bool eos = false;
        auto st = source->get_block(state.get(), &block, &eos);
        EXPECT_FALSE(st.ok());
    }

    // Nothing was committed: no entry at the current version, and the stale
    // entry still carries version 100 untouched.
    doris::QueryCacheHandle handle;
    EXPECT_FALSE(query_cache->lookup(cache_key, 114514, &handle));
    doris::QueryCacheHandle stale_handle;
    EXPECT_TRUE(query_cache->lookup_any_version(cache_key, &stale_handle));
    EXPECT_EQ(stale_handle.get_cache_version(), 100);
}

} // namespace doris
