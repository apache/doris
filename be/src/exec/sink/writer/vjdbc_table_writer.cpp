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

#include "vjdbc_table_writer.h"

#include <gen_cpp/DataSinks_types.h>
#include <stdint.h>

#include <sstream>

#include "common/logging.h"
#include "core/block/block.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"
#include "util/jdbc_utils.h"

namespace doris {

std::map<std::string, std::string> VJdbcTableWriter::_build_writer_params(const TDataSink& t_sink) {
    const TJdbcTableSink& t_jdbc_sink = t_sink.jdbc_table_sink;
    std::map<std::string, std::string> params;

    params["jdbc_url"] = t_jdbc_sink.jdbc_table.jdbc_url;
    params["jdbc_user"] = t_jdbc_sink.jdbc_table.jdbc_user;
    params["jdbc_password"] = t_jdbc_sink.jdbc_table.jdbc_password;
    params["jdbc_driver_class"] = t_jdbc_sink.jdbc_table.jdbc_driver_class;
    // Resolve jdbc_driver_url to absolute file:// URL
    std::string driver_url;
    auto resolve_st =
            JdbcUtils::resolve_driver_url(t_jdbc_sink.jdbc_table.jdbc_driver_url, &driver_url);
    if (!resolve_st.ok()) {
        LOG(WARNING) << "Failed to resolve JDBC driver URL: " << resolve_st.to_string();
        driver_url = t_jdbc_sink.jdbc_table.jdbc_driver_url;
    }
    params["jdbc_driver_url"] = driver_url;

    params["jdbc_driver_checksum"] = t_jdbc_sink.jdbc_table.jdbc_driver_checksum;
    // The unified JNI writer needs the dialect to preserve instant semantics in parameter binds.
    params["table_type"] = to_string(t_jdbc_sink.table_type);
    params["insert_sql"] = t_jdbc_sink.insert_sql;
    params["use_transaction"] = t_jdbc_sink.use_transaction ? "true" : "false";
    params["catalog_id"] = std::to_string(t_jdbc_sink.jdbc_table.catalog_id);
    params["connection_pool_min_size"] =
            std::to_string(t_jdbc_sink.jdbc_table.connection_pool_min_size);
    params["connection_pool_max_size"] =
            std::to_string(t_jdbc_sink.jdbc_table.connection_pool_max_size);
    params["connection_pool_max_wait_time"] =
            std::to_string(t_jdbc_sink.jdbc_table.connection_pool_max_wait_time);
    params["connection_pool_max_life_time"] =
            std::to_string(t_jdbc_sink.jdbc_table.connection_pool_max_life_time);
    params["connection_pool_keep_alive"] =
            t_jdbc_sink.jdbc_table.connection_pool_keep_alive ? "true" : "false";

    return params;
}

VJdbcTableWriter::VJdbcTableWriter(const TDataSink& t_sink,
                                   const VExprContextSPtrs& output_expr_ctxs,
                                   std::shared_ptr<Dependency> dep,
                                   std::shared_ptr<Dependency> fin_dep)
        : AsyncResultWriter(output_expr_ctxs, dep, fin_dep),
          _writer_params(_build_writer_params(t_sink)),
          _use_transaction(t_sink.jdbc_table_sink.use_transaction) {}

Status VJdbcTableWriter::open(RuntimeState* state, RuntimeProfile* operator_profile) {
    _state = state;
    _writer = std::make_unique<VJniFormatTransformer>(state, _vec_output_expr_ctxs,
                                                      Jni::plugin::JDBC_WRITER, _writer_params);
    return _writer->open();
}

Status VJdbcTableWriter::write(RuntimeState* state, Block& block) {
    Block output_block;
    RETURN_IF_ERROR(_projection_block(block, &output_block));

    if (output_block.rows() == 0) {
        return Status::OK();
    }

    return _writer->write(output_block);
}

Status VJdbcTableWriter::finish(RuntimeState* state) {
    // AsyncResultWriter invokes finish only after all queued writes completed successfully.
    // Defer the commit decision until close so a cancellation after finish still causes rollback.
    _successful_eos = true;
    return Status::OK();
}

Status VJdbcTableWriter::_transaction_status(Status status) {
    // A write error remains primary even if cancellation follows it.
    if (!status.ok()) {
        return status;
    }
    status = get_writer_status();
    // Query cancellation can reach RuntimeState before the sink calls force_close().
    if (status.ok() && _state->is_cancelled()) {
        status = _state->cancel_reason();
    }
    return status;
}

Status VJdbcTableWriter::close(Status s) {
    // Open may fail before the transformer is allocated; there is then nothing to release.
    if (!_writer) {
        return s;
    }
    // Non-transactional writers retain their existing close behavior.
    if (!_use_transaction) {
        return _writer->close();
    }
    s = _transaction_status(s);
    // This is the commit decision point. A later cancellation cannot undo a remote commit.
    Status close_status = _writer->close(_successful_eos && s.ok());
    // Cleanup must run even after failure, but must not hide the original execution error.
    if (!s.ok()) {
        if (!close_status.ok()) {
            LOG(WARNING) << "JDBC writer cleanup also failed: " << close_status;
        }
        return s;
    }
    return close_status;
}

} // namespace doris
