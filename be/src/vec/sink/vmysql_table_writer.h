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

#pragma once

#include <fmt/format.h>
#include <mysql/mysql.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/mysql_table_writer.h"

namespace doris {
namespace vectorized {

class VExprContext;
class Block;
class VMysqlTableWriter {
public:
    VMysqlTableWriter(const std::vector<vectorized::VExprContext*>& output_exprs);
    ~VMysqlTableWriter();

    // connect to mysql server
    Status open(const MysqlConnInfo& conn_info, const std::string& tbl);

    Status begin_trans() { return Status::OK(); }

    Status append(vectorized::Block* block);

    Status abort_tarns() { return Status::OK(); }

    Status finish_tarns() { return Status::OK(); }

private:
    Status insert_row(vectorized::Block& block, size_t row);
    const std::vector<vectorized::VExprContext*>& _vec_output_expr_ctxs;
    fmt::memory_buffer _insert_stmt_buffer;
    std::string _mysql_tbl;
    MYSQL* _mysql_conn;
};
} // namespace vectorized
} // namespace doris
