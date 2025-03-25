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

#include <memory>
#include <vector>

#include "common/status.h"
#include "pipeline/exec/exchange_sink_buffer.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "vec/sink/writer/vhive_utils.h"

namespace doris::pipeline {

std::map<int64_t, std::queue<AutoReleaseClosure<PTransmitDataParams,
                                                ExchangeSendCallback<PTransmitDataResult>>*>>

        done_map;

void add_request(int64_t id, auto* done) {
    done_map[id].push(done);
}

void clear_all_done() {
    for (auto& [id, dones] : done_map) {
        while (!dones.empty()) {
            dones.front()->Run();
            dones.pop();
        }
    }
}

enum PopState : int {
    eof,
    error,
    accept,
};

void pop_block(int64_t id, PopState state) {
    if (done_map[id].empty()) {
        return;
    }
    auto* done = done_map[id].front();
    done_map[id].pop();
    switch (state) {
    case PopState::eof: {
        Status st = Status::EndOfFile("Mock eof");
        st.to_protobuf(done->response_->mutable_status());
        done->Run();
        break;
    }
    case error: {
        done->cntl_->SetFailed("Mock error");
        done->Run();
        break;
    }
    case accept: {
        done->Run();
        break;
    }
    }
}
void transmit_blockv2(PBackendService_Stub& stub,
                      std::unique_ptr<AutoReleaseClosure<PTransmitDataParams,
                                                         ExchangeSendCallback<PTransmitDataResult>>>
                              closure) {
    std::cout << "mock transmit_blockv2 dest ins id :" << closure->request_->finst_id().lo()
              << "\n";
    add_request(closure->request_->finst_id().lo(), closure.release());
}
}; // namespace doris::pipeline

namespace doris::vectorized {

using namespace pipeline;
class ExchangeSInkTest : public testing::Test {
public:
    ExchangeSInkTest() = default;
    ~ExchangeSInkTest() override = default;
};

class MockContext : public TaskExecutionContext {};

std::shared_ptr<MockContext> _mock_context = std::make_shared<MockContext>();

auto create_runtime_state() {
    auto state = RuntimeState::create_shared();

    state->set_task_execution_context(_mock_context);
    return state;
}
constexpr int64_t recvr_fragment_id = 2;
constexpr int64_t sender_fragment_id = 2;

TUniqueId create_TUniqueId(int64_t hi, int64_t lo) {
    TUniqueId t {};
    t.hi = hi;
    t.lo = lo;
    return t;
}

const auto dest_fragment_ins_id_1 = create_TUniqueId(recvr_fragment_id, 1);
const auto dest_fragment_ins_id_2 = create_TUniqueId(recvr_fragment_id, 2);
const auto dest_fragment_ins_id_3 = create_TUniqueId(recvr_fragment_id, 3);
const auto dest_ins_id_1 = dest_fragment_ins_id_1.lo;
const auto dest_ins_id_2 = dest_fragment_ins_id_2.lo;
const auto dest_ins_id_3 = dest_fragment_ins_id_3.lo;

class MockSinkBuffer : public ExchangeSinkBuffer {
public:
    MockSinkBuffer(RuntimeState* state, int64_t sinknum) : ExchangeSinkBuffer(state, sinknum) {};
    void _failed(InstanceLoId id, const std::string& err) override {
        _is_failed = true;
        std::cout << "_failed\n";
    }
};

struct SinkWithChannel {
    std::shared_ptr<ExchangeSinkLocalState> sink;
    std::shared_ptr<MockSinkBuffer> buffer;
    std::map<int64_t, std::shared_ptr<Channel>> channels;
    Status add_block(int64_t id, bool eos) {
        auto channel = channels[id];
        TransmitInfo transmitInfo {.channel = channel.get(),
                                   .block = std::make_unique<PBlock>(),
                                   .eos = eos,
                                   .exec_status = Status::OK()};
        return buffer->add_block(std::move(transmitInfo));
    }
};

auto create_buffer(std::shared_ptr<RuntimeState> state) {
    auto sink_buffer = std::make_shared<MockSinkBuffer>(state.get(), 3);

    sink_buffer->construct_request(dest_fragment_ins_id_1);
    sink_buffer->construct_request(dest_fragment_ins_id_2);
    sink_buffer->construct_request(dest_fragment_ins_id_3);
    return sink_buffer;
}

auto create_sink(std::shared_ptr<RuntimeState> state, std::shared_ptr<MockSinkBuffer> sink_buffer) {
    SinkWithChannel sink_with_channel;
    sink_with_channel.sink = ExchangeSinkLocalState::create_shared(state.get());
    sink_with_channel.buffer = sink_buffer;
    {
        auto channel = std::make_shared<vectorized::Channel>(
                sink_with_channel.sink.get(), TNetworkAddress {}, dest_fragment_ins_id_1, 0);
        sink_with_channel.channels[channel->dest_ins_id()] = channel;
    }
    {
        auto channel = std::make_shared<vectorized::Channel>(
                sink_with_channel.sink.get(), TNetworkAddress {}, dest_fragment_ins_id_2, 0);
        sink_with_channel.channels[channel->dest_ins_id()] = channel;
    }
    {
        auto channel = std::make_shared<vectorized::Channel>(
                sink_with_channel.sink.get(), TNetworkAddress {}, dest_fragment_ins_id_3, 0);
        sink_with_channel.channels[channel->dest_ins_id()] = channel;
    }
    return sink_with_channel;
}

} // namespace doris::vectorized
