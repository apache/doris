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

#include "operator.h"

namespace doris::pipeline {

class MultiCastDataStreamSinkOperatorX;
class MultiCastDataStreamSinkLocalState final
        : public PipelineXSinkLocalState<MultiCastSharedState> {
    ENABLE_FACTORY_CREATOR(MultiCastDataStreamSinkLocalState);
    MultiCastDataStreamSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    friend class MultiCastDataStreamSinkOperatorX;
    friend class DataSinkOperatorX<MultiCastDataStreamSinkLocalState>;
    using Base = PipelineXSinkLocalState<MultiCastSharedState>;
    using Parent = MultiCastDataStreamSinkOperatorX;
    std::string name_suffix() override;

private:
    std::shared_ptr<pipeline::MultiCastDataStreamer> _multi_cast_data_streamer;
};

class MultiCastDataStreamSinkOperatorX final
        : public DataSinkOperatorX<MultiCastDataStreamSinkLocalState> {
    using Base = DataSinkOperatorX<MultiCastDataStreamSinkLocalState>;

public:
    MultiCastDataStreamSinkOperatorX(int sink_id, std::vector<int>& sources,
                                     const int cast_sender_count, ObjectPool* pool,
                                     const TMultiCastDataStreamSink& sink,
                                     const RowDescriptor& row_desc)
            : Base(sink_id, -1, sources),
              _pool(pool),
              _row_desc(row_desc),
              _cast_sender_count(cast_sender_count),
              _sink(sink) {}
    ~MultiCastDataStreamSinkOperatorX() override = default;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    const RowDescriptor& row_desc() const override { return _row_desc; }

    std::shared_ptr<BasicSharedState> create_shared_state() const override;

    const TMultiCastDataStreamSink& sink_node() { return _sink; }

private:
    friend class MultiCastDataStreamSinkLocalState;
    ObjectPool* _pool;
    RowDescriptor _row_desc;
    const int _cast_sender_count;
    const TMultiCastDataStreamSink& _sink;
    friend class MultiCastDataStreamSinkLocalState;
};

} // namespace doris::pipeline
