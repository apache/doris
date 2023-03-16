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

#include "runtime/runtime_state.h"
#include "vec/sink/vtable_sink.h"

namespace doris {
namespace vectorized {
VMysqlTableSink::VMysqlTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                                 const std::vector<TExpr>& t_exprs)
        : VTableSink(pool, row_desc, t_exprs) {
    _name = "VMysqlTableSink";
}

Status VMysqlTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(VTableSink::init(t_sink));
    const TMysqlTableSink& t_mysql_sink = t_sink.mysql_table_sink;
    _conn_info.host = t_mysql_sink.host;
    _conn_info.port = t_mysql_sink.port;
    _conn_info.user = t_mysql_sink.user;
    _conn_info.passwd = t_mysql_sink.passwd;
    _conn_info.db = t_mysql_sink.db;
    _table_name = t_mysql_sink.table;
    _conn_info.charset = t_mysql_sink.charset;
    return Status::OK();
}

Status VMysqlTableSink::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VMysqlTableSink::open");
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VTableSink::open(state));
    // create writer
    _writer.reset(new VMysqlTableWriter(_output_vexpr_ctxs));
    RETURN_IF_ERROR(_writer->open(_conn_info, _table_name));
    return Status::OK();
}

Status VMysqlTableSink::send(RuntimeState* state, Block* block, bool eos) {
    INIT_AND_SCOPE_SEND_SPAN(state->get_tracer(), _send_span, "VMysqlTableSink::send");
    return _writer->append(block);
}

Status VMysqlTableSink::close(RuntimeState* state, Status exec_status) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VMysqlTableSink::close");
    RETURN_IF_ERROR(VTableSink::close(state, exec_status));
    return Status::OK();
}
} // namespace vectorized
} // namespace doris
