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

#include <stdint.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/runtime_filter_consumer.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

namespace pipeline {
class MultiCastDataStreamer;

class MultiCastDataStreamerSourceOperatorBuilder final : public OperatorBuilderBase {
public:
    MultiCastDataStreamerSourceOperatorBuilder(int32_t id, const int consumer_id,
                                               std::shared_ptr<MultiCastDataStreamer>&,
                                               const TDataStreamSink&);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;

    const RowDescriptor& row_desc() override;

private:
    const int _consumer_id;
    std::shared_ptr<MultiCastDataStreamer> _multi_cast_data_streamer;
    TDataStreamSink _t_data_stream_sink;
};

class MultiCastDataStreamerSourceOperator final : public OperatorBase,
                                                  public vectorized::RuntimeFilterConsumer {
public:
    MultiCastDataStreamerSourceOperator(OperatorBuilderBase* operator_builder,
                                        const int consumer_id,
                                        std::shared_ptr<MultiCastDataStreamer>& data_streamer,
                                        const TDataStreamSink& sink);

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    bool runtime_filters_are_ready_or_timeout() override;

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override {
        return Status::OK();
    }

    bool can_read() override;

    Status close(doris::RuntimeState* state) override;

    [[nodiscard]] RuntimeProfile* get_runtime_profile() const override;

private:
    const int _consumer_id;
    std::shared_ptr<MultiCastDataStreamer> _multi_cast_data_streamer;
    const TDataStreamSink _t_data_stream_sink;

    vectorized::VExprContextSPtrs _output_expr_contexts;
    vectorized::VExprContextSPtrs _conjuncts;
};

class MultiCastDataStreamerSourceOperatorX;

class MultiCastDataStreamSourceLocalState final : public PipelineXLocalState<MultiCastDependency> {
public:
    ENABLE_FACTORY_CREATOR(MultiCastDataStreamSourceLocalState);
    using Base = PipelineXLocalState<MultiCastDependency>;
    using Parent = MultiCastDataStreamerSourceOperatorX;
    MultiCastDataStreamSourceLocalState(RuntimeState* state, OperatorXBase* parent)
            : Base(state, parent) {};

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    friend class MultiCastDataStreamerSourceOperatorX;

private:
    vectorized::VExprContextSPtrs _output_expr_contexts;
};
class MultiCastDataStreamerSourceOperatorX final
        : public OperatorX<MultiCastDataStreamSourceLocalState> {
public:
    using Base = OperatorX<MultiCastDataStreamSourceLocalState>;
    MultiCastDataStreamerSourceOperatorX(const int consumer_id, ObjectPool* pool,
                                         const TDataStreamSink& sink,
                                         const RowDescriptor& row_descriptor, int id)
            : Base(pool, id),
              _consumer_id(consumer_id),
              _t_data_stream_sink(sink),
              _row_descriptor(row_descriptor) {};
    ~MultiCastDataStreamerSourceOperatorX() override = default;
    Dependency* wait_for_dependency(RuntimeState* state) override {
        CREATE_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state);
        return local_state._dependency->can_read(_consumer_id);
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Base::prepare(state));
        // RETURN_IF_ERROR(vectorized::RuntimeFilterConsumer::init(state));
        // init profile for runtime filter
        // RuntimeFilterConsumer::_init_profile(local_state._shared_state->_multi_cast_data_streamer->profile());
        if (_t_data_stream_sink.__isset.output_exprs) {
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_data_stream_sink.output_exprs,
                                                                 _output_expr_contexts));
            RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_expr_contexts, state, _row_desc()));
        }

        if (_t_data_stream_sink.__isset.conjuncts) {
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_data_stream_sink.conjuncts,
                                                                 _conjuncts));
            RETURN_IF_ERROR(vectorized::VExpr::prepare(_conjuncts, state, _row_desc()));
        }
        return Status::OK();
    }

    Status open(RuntimeState* state) override {
        RETURN_IF_ERROR(Base::open(state));
        if (_t_data_stream_sink.__isset.output_exprs) {
            RETURN_IF_ERROR(vectorized::VExpr::open(_output_expr_contexts, state));
        }
        if (_t_data_stream_sink.__isset.conjuncts) {
            RETURN_IF_ERROR(vectorized::VExpr::open(_conjuncts, state));
        }
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

private:
    friend class MultiCastDataStreamSourceLocalState;
    const int _consumer_id;
    const TDataStreamSink _t_data_stream_sink;
    vectorized::VExprContextSPtrs _output_expr_contexts;
    const RowDescriptor& _row_descriptor;
    const RowDescriptor& _row_desc() { return _row_descriptor; }
};

// sink operator

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
};

class MultiCastDataStreamSinkOperatorX final
        : public DataSinkOperatorX<MultiCastDataStreamSinkLocalState> {
    using Base = DataSinkOperatorX<MultiCastDataStreamSinkLocalState>;

public:
    friend class UnionSinkLocalState;
    MultiCastDataStreamSinkOperatorX(int sink_id, std::vector<int>& sources,
                                     const int cast_sender_count, ObjectPool* pool,
                                     const TMultiCastDataStreamSink& sink,
                                     const RowDescriptor& row_desc)
            : Base(sink_id, sources),
              _pool(pool),
              _row_desc(row_desc),
              _cast_sender_count(cast_sender_count) {}
    ~MultiCastDataStreamSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override { return Status::OK(); }

    Status open(doris::RuntimeState* state) override { return Status::OK(); };

    Status prepare(RuntimeState* state) override { return Status::OK(); }

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override {
        CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
        SCOPED_TIMER(local_state.profile()->total_time_counter());
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        if (in_block->rows() > 0 || source_state == SourceState::FINISHED) {
            COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
            auto st = local_state._shared_state->_multi_cast_data_streamer->push(
                    state, in_block, source_state == SourceState::FINISHED);
            // TODO: improvement: if sink returned END_OF_FILE, pipeline task can be finished
            if (st.template is<ErrorCode::END_OF_FILE>()) {
                return Status::OK();
            }
            return st;
        }
        return Status::OK();
    }

    std::shared_ptr<pipeline::MultiCastDataStreamer> multi_cast_data_streamer() {
        auto multi_cast_data_streamer = std::make_shared<pipeline::MultiCastDataStreamer>(
                _row_desc, _pool, _cast_sender_count);
        return multi_cast_data_streamer;
    }

    RowDescriptor& row_desc() override { return _row_desc; }

private:
    ObjectPool* _pool;
    RowDescriptor _row_desc;
    int _cast_sender_count;
    friend class MultiCastDataStreamSinkLocalState;
};

} // namespace pipeline
} // namespace doris