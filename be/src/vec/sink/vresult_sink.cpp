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

#include "vec/sink/vresult_sink.h"

#include <fmt/format.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <time.h>

#include <new>

#include "common/config.h"
#include "common/consts.h"
#include "common/object_pool.h"
#include "exec/rowid_fetcher.h"
#include "gutil/port.h"
#include "runtime/buffer_control_block.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "util/arrow/row_batch.h"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/varrow_flight_result_writer.h"
#include "vec/sink/vmysql_result_writer.h"
#include "vec/sink/writer/vfile_result_writer.h"

namespace doris {
class QueryStatistics;
class RowDescriptor;
class TExpr;

namespace vectorized {
class Block;

VResultSink::VResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_output_expr,
                         const TResultSink& sink, int buffer_size)
        : DataSink(row_desc), _t_output_expr(t_output_expr), _buf_size(buffer_size) {
    if (!sink.__isset.type || sink.type == TResultSinkType::MYSQL_PROTOCAL) {
        _sink_type = TResultSinkType::MYSQL_PROTOCAL;
    } else {
        _sink_type = sink.type;
    }
    _fetch_option = sink.fetch_option;
    _name = "ResultSink";
}

VResultSink::~VResultSink() = default;

Status VResultSink::prepare_exprs(RuntimeState* state) {
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    if (_fetch_option.use_two_phase_fetch) {
        for (auto& expr_ctx : _output_vexpr_ctxs) {
            // Must materialize if it a slot, or the slot column id will be -1
            expr_ctx->set_force_materialize_slot();
        }
    }
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    return Status::OK();
}

Status VResultSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    auto fragment_instance_id = state->fragment_instance_id();
    auto title = fmt::format("VDataBufferSender (dst_fragment_instance_id={:x}-{:x})",
                             fragment_instance_id.hi, fragment_instance_id.lo);
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title));
    // prepare output_expr
    RETURN_IF_ERROR(prepare_exprs(state));

    // create sender
    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
            state->fragment_instance_id(), _buf_size, &_sender, state->enable_pipeline_exec(),
            state->execution_timeout()));

    // create writer based on sink type
    switch (_sink_type) {
    case TResultSinkType::MYSQL_PROTOCAL:
        _writer.reset(new (std::nothrow)
                              VMysqlResultWriter(_sender.get(), _output_vexpr_ctxs, _profile));
        break;
    case TResultSinkType::ARROW_FLIGHT_PROTOCAL: {
        std::shared_ptr<arrow::Schema> arrow_schema;
        RETURN_IF_ERROR(convert_expr_ctxs_arrow_schema(_output_vexpr_ctxs, &arrow_schema));
        state->exec_env()->result_mgr()->register_arrow_schema(state->fragment_instance_id(),
                                                               arrow_schema);
        _writer.reset(new (std::nothrow) VArrowFlightResultWriter(_sender.get(), _output_vexpr_ctxs,
                                                                  _profile, arrow_schema));
        break;
    }
    default:
        return Status::InternalError("Unknown result sink type");
    }

    RETURN_IF_ERROR(_writer->init(state));
    return Status::OK();
}

Status VResultSink::open(RuntimeState* state) {
    return VExpr::open(_output_vexpr_ctxs, state);
}

Status VResultSink::second_phase_fetch_data(RuntimeState* state, Block* final_block) {
    auto row_id_col = final_block->get_by_position(final_block->columns() - 1);
    CHECK(row_id_col.name == BeConsts::ROWID_COL);
    auto tuple_desc = _row_desc.tuple_descriptors()[0];
    FetchOption fetch_option;
    fetch_option.desc = tuple_desc;
    fetch_option.t_fetch_opt = _fetch_option;
    fetch_option.runtime_state = state;
    RowIDFetcher id_fetcher(fetch_option);
    RETURN_IF_ERROR(id_fetcher.init());
    RETURN_IF_ERROR(id_fetcher.fetch(row_id_col.column, final_block));
    return Status::OK();
}

Status VResultSink::send(RuntimeState* state, Block* block, bool eos) {
    if (_fetch_option.use_two_phase_fetch && block->rows() > 0) {
        DCHECK(_sink_type == TResultSinkType::MYSQL_PROTOCAL);
        RETURN_IF_ERROR(second_phase_fetch_data(state, block));
    }
    RETURN_IF_ERROR(_writer->append_block(*block));
    if (_fetch_option.use_two_phase_fetch) {
        // Block structure may be changed by calling _second_phase_fetch_data().
        // So we should clear block in case of unmatched columns
        block->clear();
    }
    return Status::OK();
}

Status VResultSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    Status final_status = exec_status;

    if (_writer) {
        // close the writer
        Status st = _writer->close();
        if (!st.ok() && exec_status.ok()) {
            // close file writer failed, should return this error to client
            final_status = st;
        }
    }

    // close sender, this is normal path end
    if (_sender) {
        if (_writer) {
            _sender->update_num_written_rows(_writer->get_written_rows());
        }
        _sender->update_max_peak_memory_bytes();
        static_cast<void>(_sender->close(final_status));
    }
    static_cast<void>(state->exec_env()->result_mgr()->cancel_at_time(
            time(nullptr) + config::result_buffer_cancelled_interval_time,
            state->fragment_instance_id()));
    return DataSink::close(state, exec_status);
}

void VResultSink::set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
    _sender->set_query_statistics(statistics);
}

} // namespace vectorized
} // namespace doris
