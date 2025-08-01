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

#include "pipeline/exec/data_queue.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "pipeline/dependency.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {

class DataQueueTest : public testing::Test {
public:
    DataQueueTest() = default;
    ~DataQueueTest() override = default;
    void SetUp() override {
        data_queue = std::make_unique<DataQueue>(child_count);
        for (int i = 0; i < child_count; i++) {
            auto dep = Dependency::create_shared(1, 1, "DataQueueTest", true);
            sink_deps.push_back(dep);
            data_queue->set_sink_dependency(dep.get(), i);
        }
        source_dep = Dependency::create_shared(1, 1, "DataQueueTest", true);
        data_queue->set_source_dependency(source_dep);
    }
    std::unique_ptr<DataQueue> data_queue = nullptr;
    std::vector<std::shared_ptr<Dependency>> sink_deps;
    std::shared_ptr<Dependency> source_dep;
    const int child_count = 3;
};

TEST_F(DataQueueTest, MultiTest) {
    using namespace vectorized;

    int output_count = 0;
    auto output_func = [&]() {
        while (true) {
            bool eos = false;
            if (source_dep->ready()) {
                Defer set_eos {[&]() {
                    if (data_queue->remaining_has_data()) {
                        eos = false;
                    } else if (data_queue->is_all_finish()) {
                        eos = !data_queue->remaining_has_data();
                    } else {
                        eos = false;
                    }
                }};
                std::unique_ptr<Block> output_block;
                int child_idx = 0;
                EXPECT_TRUE(data_queue->get_block_from_queue(&output_block, &child_idx));
                if (output_block) {
                    output_count++;
                }
            }
            if (eos) {
                break;
            }
        }
    };

    std::vector<std::unique_ptr<Block>> input_blocks[3];

    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 50; j++) {
            auto block = std::make_unique<Block>();
            block->insert(ColumnWithTypeAndName {ColumnUInt8::create(1),
                                                 std::make_shared<DataTypeUInt8>(), ""});
            input_blocks[i].push_back(std::move(block));
        }
    }

    auto input_func = [&](int id) {
        int i = 0;
        while (i < 50) {
            if (sink_deps[id]->ready()) {
                data_queue->push_block(std::move(input_blocks[id][i]), id);
                i++;
            }
        }
        data_queue->set_finish(id);
    };

    std::thread input1(input_func, 0);
    std::thread input2(input_func, 1);
    std::thread input3(input_func, 2);
    std::thread output1(output_func);
    input1.join();
    input2.join();
    input3.join();
    output1.join();

    EXPECT_EQ(output_count, 150);
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(data_queue->is_finish(i));
    }
    EXPECT_TRUE(data_queue->is_all_finish());
    data_queue->clear_free_blocks();
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(data_queue->_free_blocks[i].empty());
    }
}

// ./run-be-ut.sh --run --filter=DataQueueTest.*

} // namespace doris::pipeline
