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

#include "common/status.h"
#include "operator.h"
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
    TDataStreamSink _t_data_stream_sink;

    vectorized::VExprContextSPtrs _output_expr_contexts;
    vectorized::VExprContextSPtrs _conjuncts;
};

} // namespace pipeline
} // namespace doris