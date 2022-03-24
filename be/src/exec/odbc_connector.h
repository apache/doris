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

#ifndef DORIS_BE_SRC_QUERY_EXEC_ODBC_CONNECTOR_H
#define DORIS_BE_SRC_QUERY_EXEC_ODBC_CONNECTOR_H

#include <fmt/format.h>
#include <sql.h>

#include <boost/format.hpp>
#include <cstdlib>
#include <string>
#include <vector>

#include "common/status.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"

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
class ODBCConnector {
public:
    explicit ODBCConnector(const ODBCConnectorParam& param);
    ~ODBCConnector();

    Status open();
    // query for ODBC table
    Status query();
    Status get_next_row(bool* eos);

    // write for ODBC table
    Status init_to_write(RuntimeProfile* profile);
    Status append(const std::string& table_name, RowBatch* batch, uint32_t start_send_row,
                  uint32_t* num_row_sent);

    // use in ODBC transaction
    Status begin_trans();  // should be call after connect and before query or init_to_write
    Status abort_trans();  // should be call after transaction abort
    Status finish_trans(); // should be call after transaction commit

    const DataBinding& get_column_data(int i) const { return *_columns_data.at(i).get(); }

private:
    void _init_profile(RuntimeProfile*);

    static Status error_status(const std::string& prefix, const std::string& error_msg);

    static std::string handle_diagnostic_record(SQLHANDLE hHandle, SQLSMALLINT hType,
                                                RETCODE RetCode);

    std::string _connect_string;
    // only use in query
    std::string _sql_str;
    const TupleDescriptor* _tuple_desc;

    // only use in write
    const std::vector<ExprContext*> _output_expr_ctxs;
    fmt::memory_buffer _insert_stmt_buffer;

    // profile use in write
    // tuple convert timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;

    bool _is_open;
    bool _is_in_transaction;

    SQLSMALLINT _field_num;

    SQLHENV _env;
    SQLHDBC _dbc;
    SQLHSTMT _stmt;

    std::vector<std::unique_ptr<DataBinding>> _columns_data;
};

} // namespace doris

#endif