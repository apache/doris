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

#include "runtime/odbc_table_sink.h"

#include <sstream>

#include "exprs/expr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/debug_util.h"

namespace doris {

OdbcTableSink::OdbcTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                             const std::vector<TExpr>& t_exprs)
        : _pool(pool), _row_desc(row_desc), _t_output_expr(t_exprs) {
    _name = "OOBC_TABLE_SINK";
}

OdbcTableSink::~OdbcTableSink() = default;

Status OdbcTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(DataSink::init(t_sink));
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, _t_output_expr, &_output_expr_ctxs));

    const TOdbcTableSink& t_odbc_sink = t_sink.odbc_table_sink;

    _odbc_param.connect_string = t_odbc_sink.connect_string;
    _odbc_param.output_expr_ctxs = _output_expr_ctxs;
    _odbc_tbl = t_odbc_sink.table;
    _use_transaction = t_odbc_sink.use_transaction;

    return Status::OK();
}

Status OdbcTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state, _row_desc, _expr_mem_tracker));
    std::stringstream title;
    title << "ODBC_TABLE_SINK (frag_id=" << state->fragment_instance_id() << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status OdbcTableSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    // create writer
    _writer.reset(new ODBCConnector(_odbc_param));
    RETURN_IF_ERROR(_writer->open());
    if (_use_transaction) {
        RETURN_IF_ERROR(_writer->begin_trans());
    }
    RETURN_IF_ERROR(_writer->init_to_write(_profile));
    return Status::OK();
}

Status OdbcTableSink::send(RuntimeState* state, RowBatch* batch) {
    if (batch == nullptr || batch->num_rows() == 0) {
        return Status::OK();
    }
    uint32_t start_send_row = 0;
    uint32_t num_row_sent = 0;
    while (start_send_row < batch->num_rows()) {
        auto status = _writer->append(_odbc_tbl, batch, start_send_row, &num_row_sent);
        if (UNLIKELY(!status.ok())) return status;
        start_send_row += num_row_sent;
        num_row_sent = 0;
    }
    return Status::OK();
}

Status OdbcTableSink::close(RuntimeState* state, Status exec_status) {
    Expr::close(_output_expr_ctxs, state);
    if (exec_status.ok() && _use_transaction) {
        RETURN_IF_ERROR(_writer->finish_trans());
    }
    return Status::OK();
}

} // namespace doris
