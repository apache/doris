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

#ifndef DORIS_BE_RUNTIME_MYSQL_TABLE_WRITER_H
#define DORIS_BE_RUNTIME_MYSQL_TABLE_WRITER_H
#include <mysql/mysql.h>

#include <string>
#include <vector>

#include "common/status.h"

namespace doris {

struct MysqlConnInfo {
    std::string host;
    std::string user;
    std::string passwd;
    std::string db;
    int port;

    std::string debug_string() const;
};

class RowBatch;
class TupleRow;
class ExprContext;

class MysqlTableWriter {
public:
    MysqlTableWriter(const std::vector<ExprContext*>& output_exprs);
    ~MysqlTableWriter();

    // connect to mysql server
    Status open(const MysqlConnInfo& conn_info, const std::string& tbl);

    Status begin_trans() { return Status::OK(); }

    Status append(RowBatch* batch);

    Status abort_tarns() { return Status::OK(); }

    Status finish_tarns() { return Status::OK(); }

private:
    Status insert_row(TupleRow* row);

    const std::vector<ExprContext*>& _output_expr_ctxs;
    std::string _mysql_tbl;
    MYSQL* _mysql_conn;
};

} // namespace doris

#endif
