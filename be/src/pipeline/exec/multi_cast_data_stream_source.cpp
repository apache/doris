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

#include "multi_cast_data_stream_source.h"

#include <functional>

#include "common/status.h"
#include "pipeline/exec/multi_cast_data_streamer.h"
#include "pipeline/exec/operator.h"
#include "vec/core/block.h"

namespace doris::pipeline {

MultiCastDataStreamerSourceOperatorBuilder::MultiCastDataStreamerSourceOperatorBuilder(
        int32_t id, const int consumer_id, std::shared_ptr<MultiCastDataStreamer>& data_streamer)
        : OperatorBuilderBase(id, "MultiCastDataStreamerSourceOperator"),
          _consumer_id(consumer_id),
          _multi_cast_data_streamer(data_streamer) {};

OperatorPtr MultiCastDataStreamerSourceOperatorBuilder::build_operator() {
    return std::make_shared<MultiCastDataStreamerSourceOperator>(this, _consumer_id,
                                                                 _multi_cast_data_streamer);
}

const RowDescriptor& MultiCastDataStreamerSourceOperatorBuilder::row_desc() {
    return _multi_cast_data_streamer->row_desc();
}

MultiCastDataStreamerSourceOperator::MultiCastDataStreamerSourceOperator(
        OperatorBuilderBase* operator_builder, const int consumer_id,
        std::shared_ptr<MultiCastDataStreamer>& data_streamer)
        : OperatorBase(operator_builder),
          _consumer_id(consumer_id),
          _multi_cast_data_streamer(data_streamer) {};

bool MultiCastDataStreamerSourceOperator::can_read() {
    return _multi_cast_data_streamer->can_read(_consumer_id);
}

Status MultiCastDataStreamerSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                                      SourceState& source_state) {
    bool eos = false;
    _multi_cast_data_streamer->pull(_consumer_id, block, &eos);
    if (eos) {
        source_state = SourceState::FINISHED;
    }
    return Status::OK();
}

Status MultiCastDataStreamerSourceOperator::close(doris::RuntimeState* state) {
    _multi_cast_data_streamer->close_sender(_consumer_id);
    return OperatorBase::close(state);
}

RuntimeProfile* MultiCastDataStreamerSourceOperator::get_runtime_profile() const {
    return _multi_cast_data_streamer->profile();
}

} // namespace doris::pipeline
