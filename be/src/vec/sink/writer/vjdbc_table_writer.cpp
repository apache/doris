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
#include "runtime/runtime_state.h"
#include "util/jdbc_utils.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

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
                                   std::shared_ptr<pipeline::Dependency> dep,
                                   std::shared_ptr<pipeline::Dependency> fin_dep)
        : AsyncResultWriter(output_expr_ctxs, dep, fin_dep),
          _writer_params(_build_writer_params(t_sink)),
          _use_transaction(t_sink.jdbc_table_sink.use_transaction) {}

Status VJdbcTableWriter::open(RuntimeState* state, RuntimeProfile* operator_profile) {
    _writer = std::make_unique<VJniFormatTransformer>(
            state, _vec_output_expr_ctxs, "org/apache/doris/jdbc/JdbcJniWriter", _writer_params);
    return _writer->open();
}

Status VJdbcTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    Block output_block;
    RETURN_IF_ERROR(_projection_block(block, &output_block));

    if (output_block.rows() == 0) {
        return Status::OK();
    }

    return _writer->write(output_block);
}

Status VJdbcTableWriter::finish(RuntimeState* state) {
    if (!_use_transaction || !_writer) {
        return Status::OK();
    }

    // Call commitTrans on the Java JdbcJniWriter via JNI
    // VJniFormatTransformer manages the JNI writer object, so we use get_statistics
    // to check for errors. The actual commit is done as part of close() on the Java side
    // when use_transaction is true.
    // TODO: Add explicit commitTrans JNI call support to VJniFormatTransformer
    return Status::OK();
}

Status VJdbcTableWriter::close(Status s) {
    if (_writer) {
        return _writer->close();
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
