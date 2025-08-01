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

#include "pipeline/exec/cache_sink_operator.h"
#include "pipeline/exec/cache_source_operator.h"
#include "pipeline/exec/repeat_operator.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
namespace doris::pipeline {

using namespace vectorized;

class QueryCacheMockChildOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                    bool* eos) override {
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
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
        state->batsh_size = 10;
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

            EXPECT_TRUE(source_local_state_uptr->init(state.get(), info));
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
    source = std::make_unique<CacheSourceOperatorX>();
    EXPECT_TRUE(source->set_child(child_op));
    child_op->_mock_row_desc.reset(
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 1000;

    source->_cache_param = cache_param;
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
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 3;

    source->_cache_param = cache_param;
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
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>()}, &pool});
    TQueryCacheParam cache_param;
    cache_param.node_id = 0;
    cache_param.output_slot_mapping[0] = 0;
    cache_param.tablet_to_range.insert({42, "test"});
    cache_param.force_refresh_query_cache = false;
    cache_param.entry_max_bytes = 1024 * 1024;
    cache_param.entry_max_rows = 3;

    {
        int64_t version = 0;
        std::string cache_key;
        EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version));
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4, 5});
        query_cache->insert(cache_key, version, result, {0, 2, 3}, 1);
    }

    source->_cache_param = cache_param;
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

    query_cache_uptr.release();
}

} // namespace doris::pipeline