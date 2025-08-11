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
#include "runtime_filter/runtime_filter_consumer_helper.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

namespace pipeline {
#include "common/compile_check_begin.h"
class MultiCastDataStreamer;
class MultiCastDataStreamerSourceOperatorX;

class MultiCastDataStreamSourceLocalState final
        : public PipelineXSpillLocalState<MultiCastSharedState> {
public:
    ENABLE_FACTORY_CREATOR(MultiCastDataStreamSourceLocalState);
    using Base = PipelineXSpillLocalState<MultiCastSharedState>;
    using Parent = MultiCastDataStreamerSourceOperatorX;
    MultiCastDataStreamSourceLocalState(RuntimeState* state, OperatorXBase* parent);
    Status init(RuntimeState* state, LocalStateInfo& info) override;

    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    friend class MultiCastDataStreamerSourceOperatorX;

    std::vector<Dependency*> execution_dependencies() override {
        if (_filter_dependencies.empty()) {
            return {};
        }
        std::vector<Dependency*> res;
        res.resize(_filter_dependencies.size());
        for (size_t i = 0; i < _filter_dependencies.size(); i++) {
            res[i] = _filter_dependencies[i].get();
        }
        return res;
    }

    std::vector<Dependency*> dependencies() const override;

private:
    friend class MultiCastDataStreamerSourceOperatorX;
    vectorized::VExprContextSPtrs _output_expr_contexts;
    std::vector<std::shared_ptr<Dependency>> _filter_dependencies;

    RuntimeProfile::Counter* _wait_for_rf_timer = nullptr;
    RuntimeProfile::Counter* _filter_timer = nullptr;
    RuntimeProfile::Counter* _get_data_timer = nullptr;
    RuntimeProfile::Counter* _materialize_data_timer = nullptr;

    RuntimeFilterConsumerHelper _helper;
};

class MultiCastDataStreamerSourceOperatorX final
        : public OperatorX<MultiCastDataStreamSourceLocalState> {
public:
    using Base = OperatorX<MultiCastDataStreamSourceLocalState>;
    MultiCastDataStreamerSourceOperatorX(const int node_id, const int consumer_id, ObjectPool* pool,
                                         const TDataStreamSink& sink,
                                         const RowDescriptor& row_descriptor, int operator_id)
            : Base(pool, /*node_id=*/node_id, operator_id),
              _consumer_id(consumer_id),
              _t_data_stream_sink(sink),
              _multi_cast_output_row_descriptor(row_descriptor) {
        _op_name = "MULTI_CAST_DATA_STREAM_SOURCE_OPERATOR";
    };
    ~MultiCastDataStreamerSourceOperatorX() override = default;

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Base::prepare(state));
        // init profile for runtime filter
        if (_t_data_stream_sink.__isset.output_exprs) {
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_data_stream_sink.output_exprs,
                                                                 _output_expr_contexts));
            RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_expr_contexts, state,
                                                       _multi_cast_output_row_descriptor));
            RETURN_IF_ERROR(vectorized::VExpr::open(_output_expr_contexts, state));
        }
        if (_t_data_stream_sink.__isset.conjuncts) {
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_data_stream_sink.conjuncts,
                                                                 conjuncts()));
            RETURN_IF_ERROR(vectorized::VExpr::prepare(conjuncts(), state,
                                                       _multi_cast_output_row_descriptor));
            RETURN_IF_ERROR(vectorized::VExpr::open(conjuncts(), state));
        }
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

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
    const RowDescriptor& _multi_cast_output_row_descriptor;
};

} // namespace pipeline
} // namespace doris
#include "common/compile_check_end.h"