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

#pragma once

#include <vector>

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/data_queue.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

class MultiCastDataStreamSinkOperatorX;
class MultiCastDataStreamSinkLocalState final
        : public PipelineXSpillSinkLocalState<MultiCastSharedState> {
    ENABLE_FACTORY_CREATOR(MultiCastDataStreamSinkLocalState);
    MultiCastDataStreamSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    friend class MultiCastDataStreamSinkOperatorX;
    friend class DataSinkOperatorX<MultiCastDataStreamSinkLocalState>;
    using Base = PipelineXSpillSinkLocalState<MultiCastSharedState>;
    using Parent = MultiCastDataStreamSinkOperatorX;
    std::string name_suffix() override;

    Status open(RuntimeState* state) override;

    std::vector<Dependency*> dependencies() const override;

    std::string debug_string(int indentation_level) const override;
};

class MultiCastDataStreamSinkOperatorX final
        : public DataSinkOperatorX<MultiCastDataStreamSinkLocalState> {
    using Base = DataSinkOperatorX<MultiCastDataStreamSinkLocalState>;

public:
    MultiCastDataStreamSinkOperatorX(int sink_id, int node_id, std::vector<int>& sources,
                                     ObjectPool* pool, const TMultiCastDataStreamSink& sink)
            : Base(sink_id, node_id, sources),
              _pool(pool),
              _cast_sender_count(sources.size()),
              _sink(sink),
              _num_dests(sources.size()) {}
    ~MultiCastDataStreamSinkOperatorX() override = default;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    std::shared_ptr<BasicSharedState> create_shared_state() const override;

    const TMultiCastDataStreamSink& sink_node() { return _sink; }
    bool count_down_destination() override {
        DCHECK_GT(_num_dests, 0);
        return _num_dests.fetch_sub(1) == 1;
    }

private:
    friend class MultiCastDataStreamSinkLocalState;
    ObjectPool* _pool;
    const size_t _cast_sender_count;
    const TMultiCastDataStreamSink& _sink;
    friend class MultiCastDataStreamSinkLocalState;
    std::atomic<size_t> _num_dests;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
