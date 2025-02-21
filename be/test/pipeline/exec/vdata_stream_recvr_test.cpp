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

#include "vec/runtime/vdata_stream_recvr.h"

#include <gtest/gtest.h>

#include <memory>

#include "pipeline/dependency.h"
#include "pipeline/exec/multi_cast_data_streamer.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::pipeline {
using namespace vectorized;

struct MockVDataStreamRecvr : public VDataStreamRecvr {
    MockVDataStreamRecvr(RuntimeState* state, RuntimeProfile::HighWaterMarkCounter* counter,
                         RuntimeProfile* profile, int num_senders, bool is_merging)
            : VDataStreamRecvr(nullptr, counter, state, TUniqueId(), 0, num_senders, is_merging,
                               profile, 1) {};
};

class DataStreamRecvrTest : public testing::Test {
public:
    DataStreamRecvrTest() = default;
    ~DataStreamRecvrTest() override = default;
    void SetUp() override {}

    void create_recvr(int num_senders, bool is_merging) {
        _mock_counter =
                std::make_unique<RuntimeProfile::HighWaterMarkCounter>(TUnit::UNIT, 0, "test");
        _mock_state = std::make_unique<MockRuntimeState>();
        _mock_profile = std::make_unique<RuntimeProfile>("test");
        recvr = std::make_unique<MockVDataStreamRecvr>(_mock_state.get(), _mock_counter.get(),
                                                       _mock_profile.get(), num_senders,
                                                       is_merging);
    }

    std::unique_ptr<MockVDataStreamRecvr> recvr;

    std::unique_ptr<RuntimeProfile::HighWaterMarkCounter> _mock_counter;

    std::unique_ptr<MockRuntimeState> _mock_state;

    std::unique_ptr<RuntimeProfile> _mock_profile;

    std::shared_ptr<Dependency> sink_dep;
};

TEST_F(DataStreamRecvrTest, TestCreateSenderQueue) {
    {
        create_recvr(3, false);
        EXPECT_EQ(recvr->sender_queues().size(), 1);
        EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);
    }

    {
        create_recvr(3, true);
        EXPECT_EQ(recvr->sender_queues().size(), 3);
        for (auto& queue : recvr->sender_queues()) {
            EXPECT_EQ(queue->_num_remaining_senders, 1);
        }
    }
}
} // namespace doris::pipeline
