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
#include <sqltypes.h>

#include "common/status.h"
#include "exec/table_connector.h"

namespace doris {
struct ODBCConnectorParam {
    std::string connect_string;

    // only use in query
    std::string query_string;
    const TupleDescriptor* tuple_desc;

    // only use in write
    std::vector<ExprContext*> output_expr_ctxs;
};

// Because the DataBinding have the mem alloc, so
// this class should not be copyable
struct DataBinding {
    SQLSMALLINT target_type;
    SQLINTEGER buffer_length;
    SQLLEN strlen_or_ind;
    SQLPOINTER target_value_ptr;

    DataBinding() = default;

    ~DataBinding() { free(target_value_ptr); }
    DataBinding(const DataBinding&) = delete;
    DataBinding& operator=(const DataBinding&) = delete;
};

// ODBC Connector for scan data from ODBC
class ODBCConnector : public TableConnector {
public:
    explicit ODBCConnector(const ODBCConnectorParam& param);
    ~ODBCConnector() override;

    Status open(RuntimeState* state, bool read = false) override;
    // query for ODBC table
    Status query() override;

    Status get_next_row(bool* eos);

    Status exec_write_sql(const std::u16string& insert_stmt,
                          const fmt::memory_buffer& insert_stmt_buffer) override;

    // use in ODBC transaction
    Status begin_trans() override; // should be call after connect and before query or init_to_write
    Status abort_trans() override; // should be call after transaction abort
    Status finish_trans() override; // should be call after transaction commit

    const DataBinding& get_column_data(int i) const { return *_columns_data.at(i).get(); }
    Status init_to_write(RuntimeProfile* profile);

private:
    static Status error_status(const std::string& prefix, const std::string& error_msg);

    static std::string handle_diagnostic_record(SQLHANDLE hHandle, SQLSMALLINT hType,
                                                RETCODE RetCode);

    std::string _connect_string;
    SQLSMALLINT _field_num;
    SQLHENV _env;
    SQLHDBC _dbc;
    SQLHSTMT _stmt;
    std::vector<std::unique_ptr<DataBinding>> _columns_data;
};

} // namespace doris
