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

#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/odbc_connector.h"
#include "vec/sink/vmysql_table_writer.h"

namespace doris {

class RowDescriptor;
class TExpr;
class RuntimeState;
class RuntimeProfile;
namespace vectorized {

// This class is a sinker, which put input data to odbc table
class VOdbcTableSink : public DataSink {
public:
    VOdbcTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                   const std::vector<TExpr>& t_exprs);

    Status init(const TDataSink& thrift_sink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send(RuntimeState* state, RowBatch* batch) override;

    Status send(RuntimeState* state, vectorized::Block* block) override;
    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    Status close(RuntimeState* state, Status exec_status) override;

    RuntimeProfile* profile() override { return _profile; }

private:
    // owned by RuntimeState
    ObjectPool* _pool;
    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;

    std::vector<VExprContext*> _output_expr_ctxs;

    RuntimeProfile* _profile;

    ODBCConnectorParam _odbc_param;
    std::string _odbc_tbl;
    std::unique_ptr<ODBCConnector> _writer;
    // whether use transaction
    bool _use_transaction;
};
} // namespace vectorized
} // namespace doris
