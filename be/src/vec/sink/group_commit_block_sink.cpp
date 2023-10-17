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

#include "vec/sink/group_commit_block_sink.h"

#include "runtime/group_commit_mgr.h"
#include "runtime/runtime_state.h"
#include "util/doris_metrics.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/vtablet_finder.h"
#include "vec/sink/vtablet_sink.h"

namespace doris {

namespace stream_load {

GroupCommitBlockSink::GroupCommitBlockSink(ObjectPool* pool, const RowDescriptor& row_desc,
                                           const std::vector<TExpr>& texprs, Status* status)
        : DataSink(row_desc) {
    // From the thrift expressions create the real exprs.
    *status = vectorized::VExpr::create_expr_trees(texprs, _output_vexpr_ctxs);
    _name = "GroupCommitBlockSink";
}

GroupCommitBlockSink::~GroupCommitBlockSink() = default;

Status GroupCommitBlockSink::init(const TDataSink& t_sink) {
    DCHECK(t_sink.__isset.olap_table_sink);
    auto& table_sink = t_sink.olap_table_sink;
    _tuple_desc_id = table_sink.tuple_id;
    _schema.reset(new OlapTableSchemaParam());
    RETURN_IF_ERROR(_schema->init(table_sink.schema));
    return Status::OK();
}

Status GroupCommitBlockSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    _state = state;

    // profile must add to state's object pool
    _profile = state->obj_pool()->add(new RuntimeProfile("OlapTableSink"));
    _mem_tracker =
            std::make_shared<MemTracker>("OlapTableSink:" + std::to_string(state->load_job_id()));
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    // get table's tuple descriptor
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_desc_id);
    if (_output_tuple_desc == nullptr) {
        LOG(WARNING) << "unknown destination tuple descriptor, id=" << _tuple_desc_id;
        return Status::InternalError("unknown destination tuple descriptor");
    }

    _block_convertor = std::make_unique<vectorized::OlapTableBlockConvertor>(_output_tuple_desc);
    _block_convertor->init_autoinc_info(_schema->db_id(), _schema->table_id(),
                                        _state->batch_size());
    // Prepare the exprs to run.
    return vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc);
}

Status GroupCommitBlockSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    return vectorized::VExpr::open(_output_vexpr_ctxs, state);
}

Status GroupCommitBlockSink::send(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    Status status = Status::OK();
    auto rows = input_block->rows();
    auto bytes = input_block->bytes();
    if (UNLIKELY(rows == 0)) {
        return status;
    }
    SCOPED_TIMER(_profile->total_time_counter());
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_total(rows);
    state->update_num_bytes_load_total(bytes);
    DorisMetrics::instance()->load_rows->increment(rows);
    DorisMetrics::instance()->load_bytes->increment(bytes);

    std::shared_ptr<vectorized::Block> block;
    bool has_filtered_rows = false;
    RETURN_IF_ERROR(_block_convertor->validate_and_convert_block(
            state, input_block, block, _output_vexpr_ctxs, rows, has_filtered_rows));
    block->swap(*input_block);
    return Status::OK();
}

} // namespace stream_load
} // namespace doris
