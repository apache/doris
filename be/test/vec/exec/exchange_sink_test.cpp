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

#include "exchange_sink_test.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "pipeline/exec/exchange_sink_buffer.h"

namespace doris::vectorized {
using namespace pipeline;
TEST_F(ExchangeSInkTest, test_normal_end) {
    {
        auto state = create_runtime_state();
        auto buffer = create_buffer(state);

        auto sink1 = create_sink(state, buffer);
        auto sink2 = create_sink(state, buffer);
        auto sink3 = create_sink(state, buffer);

        EXPECT_EQ(sink1.add_block(dest_ins_id_1, true), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, true), Status::OK());

        EXPECT_EQ(sink2.add_block(dest_ins_id_1, true), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_3, true), Status::OK());

        EXPECT_EQ(sink3.add_block(dest_ins_id_1, true), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_3, true), Status::OK());

        for (auto [id, count] : buffer->_running_sink_count) {
            EXPECT_EQ(count, 3) << "id : " << id;
        }

        for (auto [id, is_turn_off] : buffer->_rpc_channel_is_turn_off) {
            EXPECT_EQ(is_turn_off, false) << "id : " << id;
        }

        pop_block(dest_ins_id_1, PopState::accept);
        pop_block(dest_ins_id_1, PopState::accept);
        pop_block(dest_ins_id_1, PopState::accept);

        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_2, PopState::accept);

        pop_block(dest_ins_id_3, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);

        for (auto [id, count] : buffer->_running_sink_count) {
            EXPECT_EQ(count, 0) << "id : " << id;
        }

        for (auto [id, is_turn_off] : buffer->_rpc_channel_is_turn_off) {
            EXPECT_EQ(is_turn_off, true) << "id : " << id;
        }
        clear_all_done();
    }
}

TEST_F(ExchangeSInkTest, test_eof_end) {
    {
        auto state = create_runtime_state();
        auto buffer = create_buffer(state);

        auto sink1 = create_sink(state, buffer);
        auto sink2 = create_sink(state, buffer);
        auto sink3 = create_sink(state, buffer);

        EXPECT_EQ(sink1.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink2.add_block(dest_ins_id_1, true), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_3, true), Status::OK());

        EXPECT_EQ(sink3.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_3, false), Status::OK());

        for (auto [id, count] : buffer->_running_sink_count) {
            EXPECT_EQ(count, 3) << "id : " << id;
        }

        for (auto [id, is_turn_off] : buffer->_rpc_channel_is_turn_off) {
            EXPECT_EQ(is_turn_off, false) << "id : " << id;
        }

        pop_block(dest_ins_id_1, PopState::eof);
        EXPECT_EQ(buffer->_rpc_channel_is_turn_off[dest_ins_id_1], true);
        EXPECT_TRUE(buffer->_instance_to_package_queue[dest_ins_id_1].empty());

        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_2, PopState::accept);

        pop_block(dest_ins_id_3, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);

        EXPECT_EQ(buffer->_rpc_channel_is_turn_off[dest_ins_id_1], true);
        EXPECT_EQ(buffer->_rpc_channel_is_turn_off[dest_ins_id_2], false) << "not all eos";
        EXPECT_EQ(buffer->_rpc_channel_is_turn_off[dest_ins_id_3], false) << " not all eos";

        EXPECT_TRUE(sink1.add_block(dest_ins_id_1, true).is<ErrorCode::END_OF_FILE>());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, true), Status::OK());
        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);

        EXPECT_EQ(buffer->_rpc_channel_is_turn_off[dest_ins_id_1], true);
        EXPECT_EQ(buffer->_rpc_channel_is_turn_off[dest_ins_id_2], true);
        EXPECT_EQ(buffer->_rpc_channel_is_turn_off[dest_ins_id_3], false);
        EXPECT_EQ(buffer->_running_sink_count[dest_ins_id_3], 1);

        clear_all_done();
    }
}

TEST_F(ExchangeSInkTest, test_error_end) {
    {
        auto state = create_runtime_state();
        auto buffer = create_buffer(state);

        auto sink1 = create_sink(state, buffer);
        auto sink2 = create_sink(state, buffer);
        auto sink3 = create_sink(state, buffer);

        EXPECT_EQ(sink1.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink2.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink3.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_3, false), Status::OK());

        for (auto [id, count] : buffer->_running_sink_count) {
            EXPECT_EQ(count, 3) << "id : " << id;
        }

        for (auto [id, is_turn_off] : buffer->_rpc_channel_is_turn_off) {
            EXPECT_EQ(is_turn_off, false) << "id : " << id;
        }

        pop_block(dest_ins_id_2, PopState::error);

        auto orgin_queue_1_size = done_map[dest_ins_id_1].size();
        auto orgin_queue_2_size = done_map[dest_ins_id_2].size();
        auto orgin_queue_3_size = done_map[dest_ins_id_3].size();

        EXPECT_EQ(sink1.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink2.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink3.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(orgin_queue_1_size, done_map[dest_ins_id_1].size());
        EXPECT_EQ(orgin_queue_2_size, done_map[dest_ins_id_2].size());
        EXPECT_EQ(orgin_queue_3_size, done_map[dest_ins_id_3].size());

        clear_all_done();
    }
}

} // namespace doris::vectorized
