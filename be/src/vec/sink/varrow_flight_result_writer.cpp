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

#include "vec/sink/varrow_flight_result_writer.h"

#include "runtime/buffer_control_block.h"
#include "runtime/runtime_state.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

VArrowFlightResultWriter::VArrowFlightResultWriter(
        BufferControlBlock* sinker, const VExprContextSPtrs& output_vexpr_ctxs,
        RuntimeProfile* parent_profile, const std::shared_ptr<arrow::Schema>& arrow_schema)
        : _sinker(sinker),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _parent_profile(parent_profile),
          _arrow_schema(arrow_schema) {}

Status VArrowFlightResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }
    _is_dry_run = state->query_options().dry_run_query;
    _timezone_obj = state->timezone_obj();
    return Status::OK();
}

void VArrowFlightResultWriter::_init_profile() {
    _append_row_batch_timer = ADD_TIMER(_parent_profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultSendTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
    _bytes_sent_counter = ADD_COUNTER(_parent_profile, "BytesSent", TUnit::BYTES);
}

Status VArrowFlightResultWriter::write(RuntimeState* state, Block& input_block) {
    SCOPED_TIMER(_append_row_batch_timer);
    Status status = Status::OK();
    if (UNLIKELY(input_block.rows() == 0)) {
        return status;
    }

    // Exec vectorized expr here to speed up, block.rows() == 0 means expr exec
    // failed, just return the error status
    Block block;
    RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(_output_vexpr_ctxs,
                                                                       input_block, &block));

    // convert one batch
    std::shared_ptr<arrow::RecordBatch> result;
    auto num_rows = block.rows();
    // arrow::RecordBatch without `nbytes()` in C++
    uint64_t bytes_sent = block.bytes();
    {
        SCOPED_TIMER(_convert_tuple_timer);
        RETURN_IF_ERROR(convert_to_arrow_batch(block, _arrow_schema, arrow::default_memory_pool(),
                                               &result, _timezone_obj));
    }
    {
        SCOPED_TIMER(_result_send_timer);
        // If this is a dry run task, no need to send data block
        if (!_is_dry_run) {
            status = _sinker->add_arrow_batch(state, result);
        }
        if (status.ok()) {
            _written_rows += num_rows;
            if (!_is_dry_run) {
                _bytes_sent += bytes_sent;
            }
        } else {
            LOG(WARNING) << "append result batch to sink failed.";
        }
    }
    return status;
}

Status VArrowFlightResultWriter::close(Status st) {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    COUNTER_UPDATE(_bytes_sent_counter, _bytes_sent);
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
