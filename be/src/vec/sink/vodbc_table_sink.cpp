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

#include "vec/sink/vodbc_table_sink.h"

#include <sstream>

#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/materialize_block.h"
#include "vec/sink/vtable_sink.h"

namespace doris {
namespace vectorized {

VOdbcTableSink::VOdbcTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                               const std::vector<TExpr>& t_exprs)
        : VTableSink(pool, row_desc, t_exprs) {
    _name = "VOdbcTableSink";
}

Status VOdbcTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(VTableSink::init(t_sink));
    const TOdbcTableSink& t_odbc_sink = t_sink.odbc_table_sink;
    _odbc_param.connect_string = t_odbc_sink.connect_string;
    _table_name = t_odbc_sink.table;
    _use_transaction = t_odbc_sink.use_transaction;
    return Status::OK();
}

Status VOdbcTableSink::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOdbcTableSink::open");
    RETURN_IF_ERROR(VTableSink::open(state));

    // create writer
    _writer.reset(new ODBCConnector(_odbc_param));
    RETURN_IF_ERROR(_writer->open(state));
    if (_use_transaction) {
        RETURN_IF_ERROR(_writer->begin_trans());
    }
    RETURN_IF_ERROR(_writer->init_to_write(_profile));
    return Status::OK();
}

Status VOdbcTableSink::send(RuntimeState* state, Block* block) {
    INIT_AND_SCOPE_SEND_SPAN(state->get_tracer(), _send_span, "VOdbcTableSink::send");
    Status status = Status::OK();
    if (block == nullptr || block->rows() == 0) {
        return status;
    }

    auto output_block = vectorized::VExprContext::get_output_block_after_execute_exprs(
            _output_vexpr_ctxs, *block, status);
    materialize_block_inplace(output_block);

    uint32_t start_send_row = 0;
    uint32_t num_row_sent = 0;
    while (start_send_row < output_block.rows()) {
        RETURN_IF_ERROR(_writer->append(_table_name, &output_block, _output_vexpr_ctxs,
                                        start_send_row, &num_row_sent));
        start_send_row += num_row_sent;
        num_row_sent = 0;
    }

    return Status::OK();
}

Status VOdbcTableSink::close(RuntimeState* state, Status exec_status) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VOdbcTableSink::close");
    RETURN_IF_ERROR(VTableSink::close(state, exec_status));
    if (exec_status.ok() && _use_transaction) {
        RETURN_IF_ERROR(_writer->finish_trans());
    }
    return DataSink::close(state, exec_status);
}
} // namespace vectorized
} // namespace doris