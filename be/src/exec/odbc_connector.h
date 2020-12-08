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

#include <sql.h>

#include <boost/format.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <cstdlib>
#include <string>
#include <vector>

#include "exprs/expr_context.h"
#include "runtime/row_batch.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"

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
struct DataBinding : public boost::noncopyable {
    SQLSMALLINT target_type;
    SQLINTEGER buffer_length;
    SQLLEN strlen_or_ind;
    SQLPOINTER target_value_ptr;

    DataBinding() = default;

    ~DataBinding() { free(target_value_ptr); }
};

// ODBC Connector for scan data from ODBC
class ODBCConnector {
public:
    ODBCConnector(const ODBCConnectorParam& param);
    ~ODBCConnector();

    Status open();
    // query for ODBC table
    Status query();
    Status get_next_row(bool* eos);

    // write for ODBC table
    Status init_to_write();
    Status append(const std::string& table_name, RowBatch* batch);

    // use in ODBC transaction
    Status begin_trans();  // should be call after connect and before query or init_to_write
    Status abort_trans();  // should be call after transaction abort
    Status finish_trans(); // should be call after transaction commit

    const DataBinding& get_column_data(int i) const { return _columns_data.at(i); }
private:
    Status insert_row(const string& table_name, TupleRow* row);

    static Status error_status(const std::string& prefix, const std::string& error_msg);

    static std::string handle_diagnostic_record(SQLHANDLE hHandle, SQLSMALLINT hType,
                                                RETCODE RetCode);

    std::string _connect_string;
    // only use in query
    std::string _sql_str;
    const TupleDescriptor* _tuple_desc;

    // only use in write
    const std::vector<ExprContext*> _output_expr_ctxs;

    bool _is_open;
    bool _is_in_transaction;


    SQLSMALLINT _field_num;
    uint64_t _row_count;

    SQLHENV _env;
    SQLHDBC _dbc;
    SQLHSTMT _stmt;

    boost::ptr_vector<DataBinding> _columns_data;
};

} // namespace doris

#endif