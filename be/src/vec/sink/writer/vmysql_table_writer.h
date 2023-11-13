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
#include <stddef.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {
namespace vectorized {

struct MysqlConnInfo {
    std::string host;
    std::string user;
    std::string passwd;
    std::string db;
    std::string table_name;
    int port;
    std::string charset;

    std::string debug_string() const;
};

class Block;

class VMysqlTableWriter final : public AsyncResultWriter {
public:
    VMysqlTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs);

    // connect to mysql server
    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status append_block(vectorized::Block& block) override;

    Status close(Status) override;

private:
    Status _insert_row(vectorized::Block& block, size_t row);
    MysqlConnInfo _conn_info;
    fmt::memory_buffer _insert_stmt_buffer;
    MYSQL* _mysql_conn;
};
} // namespace vectorized
} // namespace doris
