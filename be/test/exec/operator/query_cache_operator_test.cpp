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
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
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
        // Must clear state before query_cache_uptr is destroyed, because
        // local states inside `state` hold QueryCacheHandle which references
        // the QueryCache. C++ destroys members in reverse declaration order,
        // so state (declared before query_cache_uptr) would outlive the cache.
        state.reset();
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

TEST_F(QueryCacheOperatorTest, FilePayloadMissCommitAndRepeatedHit) {
    const auto file_type = make_nullable(std::make_shared<DataTypeFile>());
    const auto array_type = make_nullable(std::make_shared<DataTypeArray>(file_type));
    const auto payload_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {array_type}, Strings {"files"}));
    const auto id_type = std::make_shared<DataTypeInt64>();
    const std::vector<std::string> inline_bytes {
            std::string("\0\xff\x80", 3) + std::string(4096, 'a'),
            std::string("\xff\0z", 3) + std::string(4096, 'b')};

    auto make_block = [&](size_t batch) {
        File file {Field::create_field<TYPE_STRING>("urn:query-cache:nested"),
                   Field::create_field<TYPE_BIGINT>(7),
                   Field::create_field<TYPE_BIGINT>(4099),
                   Field::create_field<TYPE_STRING>("application/octet-stream"),
                   Field::create_field<TYPE_STRING>("ETAG:nested-cache"),
                   Field::create_field<TYPE_VARBINARY>(StringView(inline_bytes[batch]))};
        Array files {Field::create_field<TYPE_FILE>(file), Field()};
        file[5] = Field::create_field<TYPE_VARBINARY>(StringView());
        files.push_back(Field::create_field<TYPE_FILE>(file));
        file[5] = Field();
        files.push_back(Field::create_field<TYPE_FILE>(file));
        auto payload = payload_type->create_column();
        payload->insert_default(); // NULL struct ancestor.
        payload->insert(Field::create_field<TYPE_STRUCT>(Struct {Field()})); // NULL array ancestor.
        payload->insert(Field::create_field<TYPE_STRUCT>(
                Struct {Field::create_field<TYPE_ARRAY>(Array {})}));
        payload->insert(Field::create_field<TYPE_STRUCT>(
                Struct {Field::create_field<TYPE_ARRAY>(std::move(files))}));
        auto ids = id_type->create_column();
        for (int64_t row = 0; row < 4; ++row) {
            ids->insert(Field::create_field<TYPE_BIGINT>(static_cast<int64_t>(batch) * 10 + row));
        }
        Block block;
        block.insert({std::move(ids), id_type, "id"});
        block.insert({std::move(payload), payload_type, "payload"});
        return block;
    };
    auto expect_block = [&](const Block& block, size_t batch, bool reordered) {
        ASSERT_EQ(block.rows(), 4);
        ASSERT_EQ(block.columns(), 2);
        const auto& ids = block.get_by_position(reordered ? 1 : 0);
        const auto& payload = block.get_by_position(reordered ? 0 : 1);
        ASSERT_TRUE(ids.type->equals(*id_type));
        ASSERT_TRUE(payload.type->equals(*payload_type));
        for (int64_t row = 0; row < 4; ++row) {
            EXPECT_EQ((*ids.column)[row].get<TYPE_BIGINT>(),
                      static_cast<int64_t>(batch) * 10 + row);
        }
        ASSERT_TRUE(payload.column->is_null_at(0));
        for (size_t row = 1; row < 4; ++row) {
            ASSERT_FALSE(payload.column->is_null_at(row));
        }
        const auto null_array = (*payload.column)[1];
        EXPECT_TRUE(null_array.get<TYPE_STRUCT>()[0].is_null());
        const auto empty_array = (*payload.column)[2];
        EXPECT_TRUE(empty_array.get<TYPE_STRUCT>()[0].get<TYPE_ARRAY>().empty());
        const auto populated = (*payload.column)[3];
        const auto& files = populated.get<TYPE_STRUCT>()[0].get<TYPE_ARRAY>();
        ASSERT_EQ(files.size(), 4);
        EXPECT_TRUE(files[1].is_null());
        for (size_t item : {0, 2, 3}) {
            ASSERT_EQ(files[item].get_type(), TYPE_FILE);
            const auto& fields = files[item].get<TYPE_FILE>();
            ASSERT_EQ(fields.size(), 6);
            EXPECT_EQ(fields[0].get<TYPE_STRING>(), "urn:query-cache:nested");
            EXPECT_EQ(fields[1].get<TYPE_BIGINT>(), 7);
            EXPECT_EQ(fields[2].get<TYPE_BIGINT>(), 4099);
            EXPECT_EQ(fields[3].get<TYPE_STRING>(), "application/octet-stream");
            EXPECT_EQ(fields[4].get<TYPE_STRING>(), "ETAG:nested-cache");
            if (item == 3) {
                EXPECT_TRUE(fields[5].is_null());
            } else {
                ASSERT_FALSE(fields[5].is_null());
                EXPECT_EQ(fields[5].get<TYPE_VARBINARY>().to_string_ref().to_string(),
                          item == 0 ? inline_bytes[batch] : std::string());
            }
        }
    };

    auto start_query = [&](bool reordered) {
        // Release the old query's handles/local state; keep only the global cache.
        state.reset();
        source.reset();
        sink.reset();
        state = std::make_shared<MockRuntimeState>();
        state->_batch_size = 10;
        sink = std::make_unique<CacheSinkOperatorX>();
        auto row_desc = std::make_unique<MockRowDescriptor>(
                reordered ? DataTypes {payload_type, id_type} : DataTypes {id_type, payload_type},
                &pool);
        auto& slots = static_cast<MockTupleDescriptor*>(row_desc->tuple_desc_map.front())->Slots;
        slots[0]->_id = 100;
        slots[1]->_id = 101;
        TQueryCacheParam cache_param;
        cache_param.node_id = 0;
        cache_param.digest = "file-payload-query";
        cache_param.output_slot_mapping[100] = reordered ? 11 : 10;
        cache_param.output_slot_mapping[101] = reordered ? 10 : 11;
        cache_param.tablet_to_range.insert({42, "test"});
        cache_param.force_refresh_query_cache = false;
        cache_param.entry_max_bytes = 1024 * 1024;
        cache_param.entry_max_rows = 1000;
        source = std::make_unique<CacheSourceOperatorX>(&pool, 0, 0, cache_param);
        // Keep the output schema on subsequent pulls, as the pipeline driver does.
        source->_row_descriptor = *row_desc;
        child_op->_mock_row_desc = std::move(row_desc);
        ASSERT_TRUE(source->set_child(child_op));
        create_local_state();
    };

    ASSERT_NO_FATAL_FAILURE(start_query(false));
    ASSERT_EQ(source_local_state->_hit_cache_results, nullptr);
    ASSERT_TRUE(source_local_state->_need_insert_cache);
    for (size_t batch = 0; batch < inline_bytes.size(); ++batch) {
        auto input = make_block(batch);
        const auto st = sink->sink(state.get(), &input, batch == 1);
        ASSERT_TRUE(st.ok()) << st;
    }
    Block output;
    for (size_t batch = 0; batch < inline_bytes.size(); ++batch) {
        bool eos = false;
        const auto st = source->get_block(state.get(), &output, &eos);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_EQ(eos, batch == 1);
        ASSERT_NO_FATAL_FAILURE(expect_block(output, batch, false));
        output.clear_column_data(2);
    }
    ASSERT_EQ(query_cache->get_element_count(), 1);
    EXPECT_TRUE(source_local_state->_need_insert_cache);

    for (int query = 0; query < 2; ++query) {
        SCOPED_TRACE(query);
        ASSERT_NO_FATAL_FAILURE(start_query(true));
        ASSERT_NE(source_local_state->_hit_cache_results, nullptr);
        const auto* hit = source_local_state->custom_profile()->get_info_string("HitCache");
        ASSERT_NE(hit, nullptr);
        EXPECT_EQ(*hit, "1");
        EXPECT_EQ(source_local_state->_hit_cache_column_orders, (std::vector<int> {1, 0}));
        // A hit skips the scan: only eos arrives from the sink, never replacement rows.
        Block empty;
        ASSERT_TRUE(sink->sink(state.get(), &empty, true).ok());
        output.clear();
        for (size_t batch = 0; batch < inline_bytes.size(); ++batch) {
            bool eos = false;
            const auto st = source->get_block(state.get(), &output, &eos);
            ASSERT_TRUE(st.ok()) << st;
            EXPECT_FALSE(eos);
            ASSERT_NO_FATAL_FAILURE(expect_block(output, batch, true));
            // Clearing a returned block must not mutate the next cache hit. The 4.1
            // source clones the cached schema before applying the requested slot order.
            output.clear_column_data(2);
            output.clear();
        }
        bool eos = false;
        ASSERT_TRUE(source->get_block(state.get(), &output, &eos).ok());
        EXPECT_TRUE(eos);
        EXPECT_EQ(output.rows(), 0);
        EXPECT_EQ(query_cache->get_element_count(), 1);
    }
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

} // namespace doris
