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

class MultiCastSourceDependency final : public Dependency {
public:
    using SharedState = MultiCastSharedState;
    MultiCastSourceDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "MultiCastSourceDependency", query_ctx) {}
    ~MultiCastSourceDependency() override = default;
};

class MultiCastDataStreamerSourceOperatorX;

class MultiCastDataStreamSourceLocalState final
        : public PipelineXLocalState<MultiCastSourceDependency>,
          public vectorized::RuntimeFilterConsumer {
public:
    ENABLE_FACTORY_CREATOR(MultiCastDataStreamSourceLocalState);
    using Base = PipelineXLocalState<MultiCastSourceDependency>;
    using Parent = MultiCastDataStreamerSourceOperatorX;
    MultiCastDataStreamSourceLocalState(RuntimeState* state, OperatorXBase* parent);
    Status init(RuntimeState* state, LocalStateInfo& info) override;

    Status open(RuntimeState* state) override {
        RETURN_IF_ERROR(Base::open(state));
        RETURN_IF_ERROR(_acquire_runtime_filter());
        return Status::OK();
    }

    friend class MultiCastDataStreamerSourceOperatorX;

    RuntimeFilterDependency* filterdependency() override { return _filter_dependency.get(); }

private:
    vectorized::VExprContextSPtrs _output_expr_contexts;
    std::shared_ptr<RuntimeFilterDependency> _filter_dependency;
};

class MultiCastDataStreamerSourceOperatorX final
        : public OperatorX<MultiCastDataStreamSourceLocalState> {
public:
    using Base = OperatorX<MultiCastDataStreamSourceLocalState>;
    MultiCastDataStreamerSourceOperatorX(const int consumer_id, ObjectPool* pool,
                                         const TDataStreamSink& sink,
                                         const RowDescriptor& row_descriptor, int operator_id)
            : Base(pool, -1, operator_id),
              _consumer_id(consumer_id),
              _t_data_stream_sink(sink),
              _row_descriptor(row_descriptor) {
        _op_name = "MULTI_CAST_DATA_STREAM_SOURCE_OPERATOR";
    };
    ~MultiCastDataStreamerSourceOperatorX() override = default;

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

    const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() override {
        return _t_data_stream_sink.runtime_filters;
    }

    int dest_id_from_sink() const { return _t_data_stream_sink.dest_node_id; }

private:
    friend class MultiCastDataStreamSourceLocalState;
    const int _consumer_id;
    const TDataStreamSink _t_data_stream_sink;
    vectorized::VExprContextSPtrs _output_expr_contexts;
    // FIXME: non-static data member '_row_descriptor' of 'MultiCastDataStreamerSourceOperatorX' shadows member inherited from type 'OperatorXBase'
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
    const RowDescriptor& _row_descriptor;
#ifdef __clang__
#pragma clang diagnostic pop
#endif
    const RowDescriptor& _row_desc() { return _row_descriptor; }
};

} // namespace pipeline
} // namespace doris
