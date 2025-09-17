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

#include "pipeline/exec/multi_cast_data_streamer.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "olap/olap_define.h"
#include "pipeline/dependency.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

class MultiCastDataStreamerTest : public testing::Test {
public:
    MultiCastDataStreamerTest() = default;
    ~MultiCastDataStreamerTest() override = default;
    void SetUp() override {
        profile = std::make_unique<RuntimeProfile>("MultiCastDataStreamerTest");
        custom_profile = std::make_unique<RuntimeProfile>("CustomCounters");
        common_profile = std::make_unique<RuntimeProfile>("CommonCounters");

        {
            ADD_COUNTER_WITH_LEVEL(common_profile.get(), "MemoryUsage", TUnit::BYTES, 1);
            ADD_TIMER_WITH_LEVEL(common_profile.get(), "ExecTime", 1);
            ADD_TIMER_WITH_LEVEL(custom_profile.get(), "SpillTotalTime", 1);
            ADD_TIMER_WITH_LEVEL(custom_profile.get(), "SpillWriteTime", 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillWriteTaskWaitInQueueCount",
                                   TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillWriteTaskCount", TUnit::UNIT, 1);
            ADD_TIMER_WITH_LEVEL(custom_profile.get(), "SpillWriteTaskWaitInQueueTime", 1);
            ADD_TIMER_WITH_LEVEL(custom_profile.get(), "SpillWriteFileTime", 1);
            ADD_TIMER_WITH_LEVEL(custom_profile.get(), "SpillWriteSerializeBlockTime", 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillWriteBlockCount", TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillWriteBlockBytes", TUnit::BYTES, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillWriteFileBytes", TUnit::BYTES, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillWriteRows", TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillReadFileTime", TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillReadDerializeBlockTime", TUnit::UNIT,
                                   1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillReadBlockCount", TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillReadBlockBytes", TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillReadFileBytes", TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillReadRows", TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillReadFileCount", TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillWriteFileTotalCount", TUnit::UNIT,
                                   1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillWriteFileCurrentCount", TUnit::UNIT,
                                   1);
            ADD_COUNTER_WITH_LEVEL(custom_profile.get(), "SpillWriteFileCurrentBytes", TUnit::UNIT,
                                   1);
        }

        profile->add_child(custom_profile.get(), true);
        profile->add_child(common_profile.get(), true);
        shared_state = std::make_shared<MultiCastSharedState>(&pool, cast_sender_count, 0);
        multi_cast_data_streamer =
                std::make_unique<MultiCastDataStreamer>(&pool, cast_sender_count, 0);
        shared_state->setup_shared_profile(profile.get());
        multi_cast_data_streamer->set_sink_profile(profile.get());

        source_profiles.resize(cast_sender_count);
        for (int i = 0; i < cast_sender_count; i++) {
            auto dep = Dependency::create_shared(1, 1, "MultiCastDataStreamerTest", true);
            deps.push_back(dep);
            multi_cast_data_streamer->set_dep_by_sender_idx(i, dep.get());
            source_profiles[i] =
                    std::make_unique<RuntimeProfile>(fmt::format("source_profile_{}", i));
            source_common_profiles.push_back(std::make_unique<RuntimeProfile>("CommonCounters"));
            source_custom_profiles.push_back(std::make_unique<RuntimeProfile>("CustomCounters"));
            source_profiles[i]->add_child(source_common_profiles[i].get(), true);
            source_profiles[i]->add_child(source_custom_profiles[i].get(), true);
            ADD_TIMER_WITH_LEVEL(source_common_profiles[i].get(), "ExecTime", 1);
            ADD_TIMER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillTotalTime", 1);
            ADD_TIMER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillRecoverTime", 1);
            ADD_COUNTER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadTaskWaitInQueueCount",
                                   TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadTaskCount",
                                   TUnit::UNIT, 1);
            ADD_TIMER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadTaskWaitInQueueTime",
                                 1);
            ADD_TIMER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadFileTime", 1);
            ADD_TIMER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadDerializeBlockTime", 1);
            ADD_COUNTER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadBlockCount",
                                   TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadBlockBytes",
                                   TUnit::BYTES, 1);
            ADD_COUNTER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadFileBytes",
                                   TUnit::BYTES, 1);
            ADD_COUNTER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadRows", TUnit::UNIT,
                                   1);
            ADD_COUNTER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillReadFileCount",
                                   TUnit::UNIT, 1);
            ADD_COUNTER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillWriteFileCurrentBytes",
                                   TUnit::BYTES, 1);
            ADD_COUNTER_WITH_LEVEL(source_custom_profiles[i].get(), "SpillWriteFileCurrentCount",
                                   TUnit::UNIT, 1);
            multi_cast_data_streamer->set_source_profile(i, source_profiles[i].get());
        }

        write_dependency =
                Dependency::create_shared(1, 1, "MultiCastDataStreamerTestWriteDep", true);

        multi_cast_data_streamer->set_write_dependency(write_dependency.get());
        fragment_mgr = ExecEnv::GetInstance()->_fragment_mgr;
        ExecEnv::GetInstance()->_fragment_mgr =
                new MockFragmentManager(spill_status, ExecEnv::GetInstance());

        auto spill_data_dir =
                std::make_unique<vectorized::SpillDataDir>("./ut_dir/spill_test", 1024L * 1024 * 4);
        auto st = io::global_local_filesystem()->create_directory(spill_data_dir->path(), false);
        EXPECT_TRUE(st.ok()) << "create directory: " << spill_data_dir->path()
                             << " failed: " << st.to_string();
        std::unordered_map<std::string, std::unique_ptr<vectorized::SpillDataDir>> data_map;
        data_map.emplace("test", std::move(spill_data_dir));
        auto* spill_stream_manager = new vectorized::SpillStreamManager(std::move(data_map));
        ExecEnv::GetInstance()->_spill_stream_mgr = spill_stream_manager;
        st = spill_stream_manager->init();
        EXPECT_TRUE(st.ok()) << "init spill stream manager failed: " << st.to_string();

        EXPECT_EQ(state.enable_spill(), false);
    }

    void TearDown() override {
        ExecEnv::GetInstance()->_fragment_mgr->stop();
        SAFE_DELETE(ExecEnv::GetInstance()->_fragment_mgr);
        ExecEnv::GetInstance()->_fragment_mgr = fragment_mgr;
        doris::ExecEnv::GetInstance()->spill_stream_mgr()->stop();
        SAFE_DELETE(ExecEnv::GetInstance()->_spill_stream_mgr);
    }

    ObjectPool pool;
    std::unique_ptr<MultiCastDataStreamer> multi_cast_data_streamer = nullptr;
    std::vector<std::shared_ptr<Dependency>> deps;
    std::shared_ptr<Dependency> write_dependency;
    int cast_sender_count = 3;
    MockRuntimeState state;
    Status spill_status;
    FragmentMgr* fragment_mgr {nullptr};
    std::shared_ptr<MultiCastSharedState> shared_state;
    std::unique_ptr<RuntimeProfile> profile;
    std::vector<std::unique_ptr<RuntimeProfile>> source_profiles;
    std::vector<std::unique_ptr<RuntimeProfile>> source_common_profiles;
    std::vector<std::unique_ptr<RuntimeProfile>> source_custom_profiles;
    std::unique_ptr<RuntimeProfile> custom_profile;
    std::unique_ptr<RuntimeProfile> common_profile;
};

TEST_F(MultiCastDataStreamerTest, NormTest) {
    using namespace vectorized;

    for (auto dep : deps) {
        EXPECT_FALSE(dep->ready());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});

        EXPECT_TRUE(multi_cast_data_streamer->push(&state, &block, false).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeString>({"a", "b", "c"});
        EXPECT_TRUE(multi_cast_data_streamer->push(&state, &block, true).ok());
    }

    for (auto dep : deps) {
        EXPECT_TRUE(dep->ready());
    }

    {
        for (int id = 0; id < cast_sender_count; id++) {
            Block block1;
            bool eos = false;
            EXPECT_TRUE(multi_cast_data_streamer->pull(&state, id, &block1, &eos).ok());
            EXPECT_FALSE(eos);
            EXPECT_TRUE(ColumnHelper::block_equal(
                    block1, ColumnHelper::create_block<DataTypeInt64>({1, 2, 3})));

            Block block2;
            EXPECT_TRUE(multi_cast_data_streamer->pull(&state, id, &block2, &eos).ok());
            EXPECT_TRUE(eos);
            EXPECT_TRUE(ColumnHelper::block_equal(
                    block2, ColumnHelper::create_block<DataTypeString>({"a", "b", "c"})));
        }
    }
}

TEST_F(MultiCastDataStreamerTest, MultiTest) {
    using namespace vectorized;

    std::vector<Block> blocks;
    const auto input_count = 50;
    for (int i = 0; i < input_count; i++) {
        Block block = ColumnHelper::create_block<DataTypeInt64>({i, i + 1, i + 2});
        blocks.push_back(block);
    }

    for (auto dep : deps) {
        EXPECT_FALSE(dep->ready());
    }

    std::vector<std::vector<Block>> output_blocks(cast_sender_count);

    auto output_func = [&](int id) {
        while (true) {
            bool eos = false;
            Block block;
            if (deps[id]->ready()) {
                EXPECT_TRUE(multi_cast_data_streamer->pull(&state, id, &block, &eos).ok());
                output_blocks[id].push_back(block);
            }
            if (eos) {
                break;
            }
        }
    };

    std::thread output1(output_func, 0);
    std::thread output2(output_func, 1);
    std::thread output3(output_func, 2);
    std::thread input([&] {
        for (int i = 0; i < input_count; i++) {
            EXPECT_TRUE(
                    multi_cast_data_streamer->push(&state, &blocks[i], i == input_count - 1).ok());
        }
    });
    input.join();
    output1.join();
    output2.join();
    output3.join();

    for (int i = 0; i < input_count; i++) {
        EXPECT_TRUE(ColumnHelper::block_equal(
                output_blocks[0][i], ColumnHelper::create_block<DataTypeInt64>({i, i + 1, i + 2})));
        EXPECT_TRUE(ColumnHelper::block_equal(
                output_blocks[1][i], ColumnHelper::create_block<DataTypeInt64>({i, i + 1, i + 2})));
        EXPECT_TRUE(ColumnHelper::block_equal(
                output_blocks[2][i], ColumnHelper::create_block<DataTypeInt64>({i, i + 1, i + 2})));
    }
}

TEST_F(MultiCastDataStreamerTest, SpillTest) {
    using namespace vectorized;

    state.set_enable_spill(true);
    auto exchg_node_buffer_size_bytes = config::exchg_node_buffer_size_bytes;
    config::exchg_node_buffer_size_bytes = 1;
    Defer defer {[&] { config::exchg_node_buffer_size_bytes = exchg_node_buffer_size_bytes; }};

    std::vector<Block> blocks;
    const auto input_count = 50;
    for (int i = 0; i < input_count; i++) {
        Block block = ColumnHelper::create_block<DataTypeInt64>({i, i + 1, i + 2});
        blocks.push_back(block);
    }

    for (auto dep : deps) {
        EXPECT_FALSE(dep->ready());
    }

    std::vector<std::vector<Block>> output_blocks(cast_sender_count);

    auto output_func = [&](int id) {
        SCOPED_INIT_THREAD_CONTEXT();
        while (true) {
            if (!deps[id]->ready()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            bool eos = false;
            Block block;
            EXPECT_TRUE(multi_cast_data_streamer->pull(&state, id, &block, &eos).ok());
            if (!block.empty()) {
                output_blocks[id].push_back(block);
            }
            if (eos) {
                break;
            }
        }
    };

    std::thread output1(output_func, 0);

    std::thread input([&] {
        for (int i = 0; i < input_count;) {
            if (!write_dependency->ready()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            EXPECT_TRUE(
                    multi_cast_data_streamer->push(&state, &blocks[i], i == input_count - 1).ok());
            i++;
        }
    });
    input.join();
    std::cout << "profile: " << profile->pretty_print() << std::endl;
    ASSERT_TRUE(spill_status.ok()) << "spill status: " << spill_status.to_string();

    std::thread output2(output_func, 1);
    std::thread output3(output_func, 2);
    output1.join();
    output2.join();
    output3.join();

    ASSERT_EQ(multi_cast_data_streamer->_multi_cast_blocks.size(), 0);
    ASSERT_EQ(multi_cast_data_streamer->_cached_blocks[0].size(), 0);
    ASSERT_EQ(multi_cast_data_streamer->_cached_blocks[1].size(), 0);
    ASSERT_EQ(multi_cast_data_streamer->_cached_blocks[2].size(), 0);
    ASSERT_EQ(multi_cast_data_streamer->_spill_readers[0].size(), 0);
    ASSERT_EQ(multi_cast_data_streamer->_spill_readers[1].size(), 0);
    ASSERT_EQ(multi_cast_data_streamer->_spill_readers[2].size(), 0);

    auto debug_string = multi_cast_data_streamer->debug_string();
    EXPECT_TRUE(debug_string.find("MemSize:") != std::string::npos);

    for (int i = 0; i < input_count; i++) {
        // std::cout << output_blocks[0][i].dump_data() << std::endl;
        ASSERT_TRUE(ColumnHelper::block_equal(
                output_blocks[0][i], ColumnHelper::create_block<DataTypeInt64>({i, i + 1, i + 2})))
                << "i: " << i;
        // std::cout << output_blocks[1][i].dump_data() << std::endl;
        ASSERT_TRUE(ColumnHelper::block_equal(
                output_blocks[1][i], ColumnHelper::create_block<DataTypeInt64>({i, i + 1, i + 2})))
                << "i: " << i;
        // std::cout << output_blocks[2][i].dump_data() << std::endl;
        ASSERT_TRUE(ColumnHelper::block_equal(
                output_blocks[2][i], ColumnHelper::create_block<DataTypeInt64>({i, i + 1, i + 2})))
                << "i: " << i;
    }
}

// ./run-be-ut.sh --run --filter=MultiCastDataStreamerTest.*

} // namespace doris::pipeline
