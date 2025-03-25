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

#include "pipeline/dependency.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::pipeline {

class MultiCastDataStreamerTest : public testing::Test {
public:
    MultiCastDataStreamerTest() = default;
    ~MultiCastDataStreamerTest() override = default;
    void SetUp() override {
        multi_cast_data_streamer =
                std::make_unique<MultiCastDataStreamer>(nullptr, &pool, cast_sender_count, 0);
        for (int i = 0; i < cast_sender_count; i++) {
            auto dep = Dependency::create_shared(1, 1, "MultiCastDataStreamerTest", true);
            deps.push_back(dep);
            multi_cast_data_streamer->set_dep_by_sender_idx(i, dep.get());
        }

        write_dependency =
                Dependency::create_shared(1, 1, "MultiCastDataStreamerTestWriteDep", true);

        multi_cast_data_streamer->set_write_dependency(write_dependency.get());

        // TODO: support testing with spill
        EXPECT_EQ(state.enable_spill(), false);
    }

    ObjectPool pool;
    std::unique_ptr<MultiCastDataStreamer> multi_cast_data_streamer = nullptr;
    std::vector<std::shared_ptr<Dependency>> deps;
    std::shared_ptr<Dependency> write_dependency;
    int cast_sender_count = 3;
    MockRuntimeState state;
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

// ./run-be-ut.sh --run --filter=MultiCastDataStreamerTest.*

} // namespace doris::pipeline
