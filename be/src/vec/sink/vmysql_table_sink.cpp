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

#include "vec/sink/vmysql_table_sink.h"

#include <sstream>

#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vexpr.h"

namespace doris {
namespace vectorized {
VMysqlTableSink::VMysqlTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                                 const std::vector<TExpr>& t_exprs)
        : _pool(pool),
          _row_desc(row_desc),
          _t_output_expr(t_exprs),
          _mem_tracker(MemTracker::CreateTracker(-1, "VMysqlTableSink")) {
    _name = "VMysqlTableSink";
}

VMysqlTableSink::~VMysqlTableSink() {}

Status VMysqlTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(DataSink::init(t_sink));
    const TMysqlTableSink& t_mysql_sink = t_sink.mysql_table_sink;

    _conn_info.host = t_mysql_sink.host;
    _conn_info.port = t_mysql_sink.port;
    _conn_info.user = t_mysql_sink.user;
    _conn_info.passwd = t_mysql_sink.passwd;
    _conn_info.db = t_mysql_sink.db;
    _mysql_tbl = t_mysql_sink.table;

    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, _t_output_expr, &_output_expr_ctxs));
    return Status::OK();
}

Status VMysqlTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::prepare(_output_expr_ctxs, state, _row_desc, _mem_tracker));
    std::stringstream title;
    title << "VMysqlTableSink (frag_id=" << state->fragment_instance_id() << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status VMysqlTableSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::open(_output_expr_ctxs, state));
    // create writer
    _writer = state->obj_pool()->add(new VMysqlTableWriter(_output_expr_ctxs));
    RETURN_IF_ERROR(_writer->open(_conn_info, _mysql_tbl));
    return Status::OK();
}

Status VMysqlTableSink::send(RuntimeState* state, RowBatch* batch) {
    return Status::NotSupported(
            "Not Implemented VMysqlTableSink::send(RuntimeState* state, RowBatch* batch)");
}

Status VMysqlTableSink::send(RuntimeState* state, Block* block) {
    return _writer->append(block);
}

Status VMysqlTableSink::close(RuntimeState* state, Status exec_status) {
    VExpr::close(_output_expr_ctxs, state);
    return Status::OK();
}
} // namespace vectorized
} // namespace doris
