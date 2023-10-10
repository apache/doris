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
#include "pipeline/pipeline_x/operator.h"
#include "vec/sink/multi_cast_data_stream_sink.h"

namespace doris::pipeline {

class MultiCastDataStreamSinkOperatorBuilder final
        : public DataSinkOperatorBuilder<vectorized::MultiCastDataStreamSink> {
public:
    MultiCastDataStreamSinkOperatorBuilder(int32_t id, DataSink* sink)
            : DataSinkOperatorBuilder(id, "MultiCastDataStreamSinkOperator", sink) {}

    OperatorPtr build_operator() override;
};

class MultiCastDataStreamSinkOperator final
        : public DataSinkOperator<MultiCastDataStreamSinkOperatorBuilder> {
public:
    MultiCastDataStreamSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink)
            : DataSinkOperator(operator_builder, sink) {}

    bool can_write() override { return true; }
};

class MultiCastDataStreamSinkOperatorX;
class MultiCastDataStreamSinkLocalState final
        : public PipelineXSinkLocalState<MultiCastDependency> {
    ENABLE_FACTORY_CREATOR(MultiCastDataStreamSinkLocalState);
    MultiCastDataStreamSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    friend class MultiCastDataStreamSinkOperatorX;
    friend class DataSinkOperatorX<MultiCastDataStreamSinkLocalState>;
    using Base = PipelineXSinkLocalState<MultiCastDependency>;
    using Parent = MultiCastDataStreamSinkOperatorX;
    std::string id_name() override;

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

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override {
        CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
        SCOPED_TIMER(local_state.profile()->total_time_counter());
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        if (in_block->rows() > 0 || source_state == SourceState::FINISHED) {
            COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
            auto st = local_state._shared_state->multi_cast_data_streamer->push(
                    state, in_block, source_state == SourceState::FINISHED);
            // TODO: improvement: if sink returned END_OF_FILE, pipeline task can be finished
            if (st.template is<ErrorCode::END_OF_FILE>()) {
                return Status::OK();
            }
            return st;
        }
        return Status::OK();
    }

    RowDescriptor& row_desc() override { return _row_desc; }

    std::shared_ptr<pipeline::MultiCastDataStreamer> create_multi_cast_data_streamer() {
        auto multi_cast_data_streamer = std::make_shared<pipeline::MultiCastDataStreamer>(
                _row_desc, _pool, _cast_sender_count);
        return multi_cast_data_streamer;
    }
    const TMultiCastDataStreamSink& sink_node() { return _sink; }

private:
    friend class MultiCastDataStreamSinkLocalState;
    ObjectPool* _pool;
    RowDescriptor _row_desc;
    int _cast_sender_count;
    const TMultiCastDataStreamSink& _sink;
    friend class MultiCastDataStreamSinkLocalState;
};

} // namespace doris::pipeline
