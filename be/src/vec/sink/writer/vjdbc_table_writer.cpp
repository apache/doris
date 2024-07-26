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

#include "util/binary_cast.hpp"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

JdbcConnectorParam VJdbcTableWriter::create_connect_param(const doris::TDataSink& t_sink) {
    const TJdbcTableSink& t_jdbc_sink = t_sink.jdbc_table_sink;

    JdbcConnectorParam jdbc_param;

    jdbc_param.catalog_id = t_jdbc_sink.jdbc_table.catalog_id;
    jdbc_param.jdbc_url = t_jdbc_sink.jdbc_table.jdbc_url;
    jdbc_param.user = t_jdbc_sink.jdbc_table.jdbc_user;
    jdbc_param.passwd = t_jdbc_sink.jdbc_table.jdbc_password;
    jdbc_param.driver_class = t_jdbc_sink.jdbc_table.jdbc_driver_class;
    jdbc_param.driver_path = t_jdbc_sink.jdbc_table.jdbc_driver_url;
    jdbc_param.driver_checksum = t_jdbc_sink.jdbc_table.jdbc_driver_checksum;
    jdbc_param.resource_name = t_jdbc_sink.jdbc_table.jdbc_resource_name;
    jdbc_param.table_type = t_jdbc_sink.table_type;
    jdbc_param.query_string = t_jdbc_sink.insert_sql;
    jdbc_param.table_name = t_jdbc_sink.jdbc_table.jdbc_table_name;
    jdbc_param.use_transaction = t_jdbc_sink.use_transaction;
    jdbc_param.connection_pool_min_size = t_jdbc_sink.jdbc_table.connection_pool_min_size;
    jdbc_param.connection_pool_max_size = t_jdbc_sink.jdbc_table.connection_pool_max_size;
    jdbc_param.connection_pool_max_wait_time = t_jdbc_sink.jdbc_table.connection_pool_max_wait_time;
    jdbc_param.connection_pool_max_life_time = t_jdbc_sink.jdbc_table.connection_pool_max_life_time;
    jdbc_param.connection_pool_keep_alive = t_jdbc_sink.jdbc_table.connection_pool_keep_alive;

    return jdbc_param;
}

VJdbcTableWriter::VJdbcTableWriter(const TDataSink& t_sink,
                                   const VExprContextSPtrs& output_expr_ctxs)
        : AsyncResultWriter(output_expr_ctxs), JdbcConnector(create_connect_param(t_sink)) {}

Status VJdbcTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    Block output_block;
    RETURN_IF_ERROR(_projection_block(block, &output_block));
    auto num_rows = output_block.rows();

    uint32_t start_send_row = 0;
    uint32_t num_row_sent = 0;
    while (start_send_row < num_rows) {
        RETURN_IF_ERROR(append(&output_block, _vec_output_expr_ctxs, start_send_row, &num_row_sent,
                               _conn_param.table_type));
        start_send_row += num_row_sent;
        num_row_sent = 0;
    }

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
