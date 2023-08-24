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

#include "vec/sink/vmemory_scratch_sink.h"

#include <arrow/type_fwd.h>
#include <gen_cpp/Types_types.h>

#include <sstream>

#include "common/object_pool.h"
#include "runtime/exec_env.h"
#include "runtime/record_batch_queue.h"
#include "runtime/runtime_state.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace arrow {
class RecordBatch;
} // namespace arrow

namespace doris {
class RowDescriptor;
class TExpr;
class TMemoryScratchSink;
} // namespace doris

namespace doris::vectorized {

MemoryScratchSink::MemoryScratchSink(const RowDescriptor& row_desc,
                                     const std::vector<TExpr>& t_output_expr)
        : _row_desc(row_desc), _t_output_expr(t_output_expr) {
    _name = "VMemoryScratchSink";
}

Status MemoryScratchSink::_prepare_vexpr(RuntimeState* state) {
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    // generate the arrow schema
    RETURN_IF_ERROR(convert_to_arrow_schema(_row_desc, &_arrow_schema));
    return Status::OK();
}

Status MemoryScratchSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // prepare output_expr
    RETURN_IF_ERROR(_prepare_vexpr(state));
    // create queue
    TUniqueId fragment_instance_id = state->fragment_instance_id();
    state->exec_env()->result_queue_mgr()->create_queue(fragment_instance_id, &_queue);
    std::stringstream title;
    title << "VMemoryScratchSink (frag_id=" << fragment_instance_id << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));

    return Status::OK();
}

Status MemoryScratchSink::send(RuntimeState* state, Block* input_block, bool eos) {
    if (nullptr == input_block || 0 == input_block->rows()) {
        return Status::OK();
    }
    std::shared_ptr<arrow::RecordBatch> result;
    // Exec vectorized expr here to speed up, block.rows() == 0 means expr exec
    // failed, just return the error status
    Block block;
    RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(_output_vexpr_ctxs,
                                                                       *input_block, &block));
    RETURN_IF_ERROR(
            convert_to_arrow_batch(block, _arrow_schema, arrow::default_memory_pool(), &result));
    _queue->blocking_put(result);
    return Status::OK();
}

Status MemoryScratchSink::open(RuntimeState* state) {
    return VExpr::open(_output_vexpr_ctxs, state);
}

Status MemoryScratchSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    // put sentinel
    if (_queue != nullptr) {
        _queue->blocking_put(nullptr);
    }
    return DataSink::close(state, exec_status);
}

} // namespace doris::vectorized
