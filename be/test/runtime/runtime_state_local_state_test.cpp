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
#include <string>

#include "exec/operator/operator.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

// Local states keep counters of profiles allocated in the state's object pool and may still use
// them in their destructors, e.g. a spill writer that closes its last part when a cancelled
// query tears down its operators.
class CounterTouchingSinkLocalState final : public PipelineXSinkLocalStateBase {
public:
    CounterTouchingSinkLocalState(RuntimeState* state, RuntimeProfile::Counter* counter,
                                  bool* destroyed)
            : PipelineXSinkLocalStateBase(nullptr, state),
              _counter(counter),
              _destroyed(destroyed) {}
    ~CounterTouchingSinkLocalState() override {
        COUNTER_UPDATE(_counter, 1);
        *_destroyed = true;
    }

    Status init(RuntimeState*, LocalSinkStateInfo&) override { return Status::OK(); }
    Status prepare(RuntimeState*) override { return Status::OK(); }
    Status open(RuntimeState*) override { return Status::OK(); }
    Status terminate(RuntimeState*) override { return Status::OK(); }
    Status close(RuntimeState*, Status) override { return Status::OK(); }
    std::string debug_string(int) const override { return "CounterTouchingSinkLocalState"; }

private:
    RuntimeProfile::Counter* _counter;
    bool* _destroyed;
};

class CounterTouchingLocalState final : public PipelineXLocalStateBase {
public:
    CounterTouchingLocalState(RuntimeState* state, RuntimeProfile::Counter* counter,
                              bool* destroyed)
            : PipelineXLocalStateBase(state, nullptr), _counter(counter), _destroyed(destroyed) {}
    ~CounterTouchingLocalState() override {
        COUNTER_UPDATE(_counter, 1);
        *_destroyed = true;
    }

    Status init(RuntimeState*, LocalStateInfo&) override { return Status::OK(); }
    Status prepare(RuntimeState*) override { return Status::OK(); }
    Status open(RuntimeState*) override { return Status::OK(); }
    Status close(RuntimeState*) override { return Status::OK(); }
    Status terminate(RuntimeState*) override { return Status::OK(); }
    std::string debug_string(int) const override { return "CounterTouchingLocalState"; }

private:
    RuntimeProfile::Counter* _counter;
    bool* _destroyed;
};

} // namespace

// The object pool, and the profiles in it, must outlive the local states. ASAN reports a
// heap-use-after-free here when the pool is cleared first.
TEST(RuntimeStateLocalStateTest, LocalStatesAreReleasedBeforeTheObjectPool) {
    auto state = std::make_unique<RuntimeState>();
    auto* profile = state->obj_pool()->add(new RuntimeProfile("CustomCounters"));
    auto* sink_counter = ADD_COUNTER(profile, "SinkDestroyed", TUnit::UNIT);
    auto* source_counter = ADD_COUNTER(profile, "SourceDestroyed", TUnit::UNIT);

    bool sink_destroyed = false;
    bool source_destroyed = false;
    state->emplace_sink_local_state(0, std::make_unique<CounterTouchingSinkLocalState>(
                                               state.get(), sink_counter, &sink_destroyed));
    state->resize_op_id_to_local_state(-1);
    state->emplace_local_state(0, std::make_unique<CounterTouchingLocalState>(
                                          state.get(), source_counter, &source_destroyed));

    state.reset();
    ASSERT_TRUE(sink_destroyed);
    ASSERT_TRUE(source_destroyed);
}

} // namespace doris
