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

#ifndef DORIS_BE_RUNTIME_ODBC_TABLE_SINK_H
#define DORIS_BE_RUNTIME_ODBC_TABLE_SINK_H

#include <vector>

#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/odbc_connector.h"

namespace doris {

class RowDescriptor;
class TExpr;
class TOdbcTableSink;
class RuntimeState;
class RuntimeProfile;
class ExprContext;

//This class is a sinker, which put input data to odbc table
class OdbcTableSink : public DataSink {
public:
    OdbcTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                  const std::vector<TExpr>& t_exprs);

    virtual ~OdbcTableSink();

    virtual Status init(const TDataSink& thrift_sink);

    virtual Status prepare(RuntimeState* state);

    virtual Status open(RuntimeState* state);

    // send data in 'batch' to this backend stream mgr
    // Blocks until all rows in batch are placed in the buffer
    virtual Status send(RuntimeState* state, RowBatch* batch);

    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    virtual Status close(RuntimeState* state, Status exec_status);

    virtual RuntimeProfile* profile() { return _profile; }

private:
    ObjectPool* _pool;
    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;

    std::vector<ExprContext*> _output_expr_ctxs;
    ODBCConnectorParam _odbc_param;
    std::string _odbc_tbl;
    std::unique_ptr<ODBCConnector> _writer;
    // whether use transaction
    bool _use_transaction;

    RuntimeProfile* _profile;
};

} // namespace doris

#endif
