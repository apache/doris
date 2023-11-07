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

#include "exec/odbc_connector.h"

#include <glog/logging.h>
#include <sql.h>
#include <sqlext.h>
#include <wchar.h>

#include <ostream>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class RuntimeState;
} // namespace doris

#define ODBC_DISPOSE(h, ht, x, op)                                                        \
    {                                                                                     \
        auto rc = x;                                                                      \
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {                           \
            return error_status(fmt::to_string(op), handle_diagnostic_record(h, ht, rc)); \
        }                                                                                 \
        if (rc == SQL_ERROR) {                                                            \
            auto err_msg = std::string("Error in") + fmt::to_string(op);                  \
            return Status::InternalError(err_msg.c_str());                                \
        }                                                                                 \
    }

namespace doris {

ODBCConnector::ODBCConnector(const ODBCConnectorParam& param)
        : TableConnector(param.tuple_desc, param.use_transaction, param.table_name,
                         param.query_string),
          _connect_string(param.connect_string),
          _field_num(0),
          _env(nullptr),
          _dbc(nullptr),
          _stmt(nullptr) {}

Status ODBCConnector::close(Status) {
    // do not commit transaction, roll back
    if (_is_in_transaction) {
        RETURN_IF_ERROR(abort_trans());
    }

    if (_stmt != nullptr) {
        SQLFreeHandle(SQL_HANDLE_STMT, _stmt);
    }

    if (_dbc != nullptr) {
        SQLDisconnect(_dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, _dbc);
    }

    if (_env != nullptr) {
        SQLFreeHandle(SQL_HANDLE_ENV, _env);
    }

    return Status::OK();
}

Status ODBCConnector::append(vectorized::Block* block,
                             const vectorized::VExprContextSPtrs& output_vexpr_ctxs,
                             uint32_t start_send_row, uint32_t* num_rows_sent,
                             TOdbcTableType::type table_type) {
    _insert_stmt_buffer.clear();
    std::u16string insert_stmt;
    SCOPED_TIMER(_convert_tuple_timer);
    fmt::format_to(_insert_stmt_buffer, "INSERT INTO {} VALUES (", _table_name);

    int num_rows = block->rows();
    int num_columns = block->columns();
    for (int i = start_send_row; i < num_rows; ++i) {
        (*num_rows_sent)++;

        // Construct insert statement of odbc/jdbc table
        for (int j = 0; j < num_columns; ++j) {
            if (j != 0) {
                fmt::format_to(_insert_stmt_buffer, "{}", ", ");
            }
            auto& column_ptr = block->get_by_position(j).column;
            auto& type_ptr = block->get_by_position(j).type;
            RETURN_IF_ERROR(convert_column_data(
                    column_ptr, type_ptr, output_vexpr_ctxs[j]->root()->type(), i, table_type));
        }

        if (i < num_rows - 1 && _insert_stmt_buffer.size() < INSERT_BUFFER_SIZE) {
            fmt::format_to(_insert_stmt_buffer, "{}", "),(");
        } else {
            // batch exhausted or _insert_stmt_buffer is full, need to do real insert stmt
            fmt::format_to(_insert_stmt_buffer, "{}", ")");
            break;
        }
    }
    // Translate utf8 string to utf16 to use unicode encoding
    insert_stmt = utf8_to_u16string(_insert_stmt_buffer.data(),
                                    _insert_stmt_buffer.data() + _insert_stmt_buffer.size());

    RETURN_IF_ERROR(exec_write_sql(insert_stmt, _insert_stmt_buffer));
    COUNTER_UPDATE(_sent_rows_counter, *num_rows_sent);
    return Status::OK();
}

Status ODBCConnector::open(RuntimeState* state, bool read) {
    if (_is_open) {
        LOG(INFO) << "this scanner already opened";
        return Status::OK();
    }

    // Allocate an environment
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &_env) != SQL_SUCCESS) {
        return Status::InternalError("alloc env failed");
    }
    // We want ODBC 3 support
    ODBC_DISPOSE(_env, SQL_HANDLE_ENV,
                 SQLSetEnvAttr(_env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0),
                 "set env attr");
    // Allocate a connection handle
    ODBC_DISPOSE(_env, SQL_HANDLE_ENV, SQLAllocHandle(SQL_HANDLE_DBC, _env, &_dbc), "alloc dbc");
    // Set connect timeout
    int64_t timeout = config::external_table_connect_timeout_sec;
    SQLSetConnectAttr(_dbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)timeout, 0);
    SQLSetConnectAttr(_dbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)timeout, 0);
    // Connect to the Database
    ODBC_DISPOSE(_dbc, SQL_HANDLE_DBC,
                 SQLDriverConnect(_dbc, nullptr, (SQLCHAR*)_connect_string.c_str(), SQL_NTS,
                                  nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT),
                 "driver connect");

    LOG(INFO) << "connect success:" << _connect_string.substr(0, _connect_string.find("Pwd="));
    _is_open = true;
    static_cast<void>(begin_trans());

    return Status::OK();
}

Status ODBCConnector::query() {
    if (!_is_open) {
        return Status::InternalError("Query before open.");
    }

    // Allocate a statement handle
    ODBC_DISPOSE(_dbc, SQL_HANDLE_DBC, SQLAllocHandle(SQL_HANDLE_STMT, _dbc, &_stmt),
                 "alloc statement");

    // Translate utf8 string to utf16 to use unicode encoding
    auto wquery = utf8_to_u16string(_sql_str.c_str(), _sql_str.c_str() + _sql_str.length());
    ODBC_DISPOSE(_stmt, SQL_HANDLE_STMT,
                 SQLExecDirectW(_stmt, (SQLWCHAR*)(wquery.c_str()), SQL_NTS), "exec direct");

    // How many columns are there */
    ODBC_DISPOSE(_stmt, SQL_HANDLE_STMT, SQLNumResultCols(_stmt, &_field_num), "count num column");

    LOG(INFO) << "execute success:" << _sql_str << " column count:" << _field_num;

    // check materialize num equal _field_num
    int materialize_num = 0;
    for (int i = 0; i < _tuple_desc->slots().size(); ++i) {
        if (_tuple_desc->slots()[i]->is_materialized()) {
            materialize_num++;
        }
    }
    if (_field_num != materialize_num) {
        return Status::InternalError("input and output not equal.");
    }

    // allocate memory for the binding
    for (int i = 0; i < _field_num; i++) {
        DataBinding* column_data = new DataBinding;
        column_data->target_type = SQL_C_CHAR;
        auto type = _tuple_desc->slots()[i]->type().type;
        column_data->buffer_length = (type == TYPE_HLL || type == TYPE_CHAR ||
                                      type == TYPE_VARCHAR || type == TYPE_STRING)
                                             ? big_column_size_buffer
                                             : small_column_size_buffer;
        column_data->target_value_ptr = malloc(sizeof(char) * column_data->buffer_length);
        _columns_data.emplace_back(column_data);
    }

    // setup the binding
    for (int i = 0; i < _field_num; i++) {
        ODBC_DISPOSE(_stmt, SQL_HANDLE_STMT,
                     SQLBindCol(_stmt, (SQLUSMALLINT)i + 1, _columns_data[i]->target_type,
                                _columns_data[i]->target_value_ptr, _columns_data[i]->buffer_length,
                                &(_columns_data[i]->strlen_or_ind)),
                     "bind col");
    }

    return Status::OK();
}

Status ODBCConnector::get_next_row(bool* eos) {
    if (!_is_open) {
        return Status::InternalError("GetNextRow before open.");
    }

    auto ret = SQLFetch(_stmt);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return Status::OK();
    } else if (ret != SQL_NO_DATA_FOUND) {
        return error_status("result fetch", handle_diagnostic_record(_stmt, SQL_HANDLE_STMT, ret));
    }

    *eos = true;
    return Status::OK();
}

Status ODBCConnector::init_to_write(doris::RuntimeProfile* profile) {
    if (!_is_open) {
        return Status::InternalError("Init before open.");
    }

    init_profile(profile);
    // Allocate a statement handle
    ODBC_DISPOSE(_dbc, SQL_HANDLE_DBC, SQLAllocHandle(SQL_HANDLE_STMT, _dbc, &_stmt),
                 "alloc statement");

    return Status::OK();
}

Status ODBCConnector::exec_write_sql(const std::u16string& insert_stmt,
                                     const fmt::memory_buffer& insert_stmt_buffer) {
    SCOPED_TIMER(_result_send_timer);
    ODBC_DISPOSE(_stmt, SQL_HANDLE_STMT,
                 SQLExecDirectW(_stmt, (SQLWCHAR*)(insert_stmt.c_str()), SQL_NTS),
                 insert_stmt_buffer.data());
    return Status::OK();
}

Status ODBCConnector::begin_trans() {
    if (_use_tranaction) {
        ODBC_DISPOSE(_dbc, SQL_HANDLE_DBC,
                     SQLSetConnectAttr(_dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF,
                                       SQL_IS_UINTEGER),
                     "Begin transcation");
        _is_in_transaction = true;
    }

    return Status::OK();
}

Status ODBCConnector::abort_trans() {
    if (!_is_in_transaction) {
        return Status::InternalError("Abort transaction before begin trans.");
    }

    ODBC_DISPOSE(_dbc, SQL_HANDLE_DBC, SQLEndTran(SQL_HANDLE_DBC, _dbc, SQL_ROLLBACK),
                 "Abort transcation");

    _is_in_transaction = false;

    return Status::OK();
}

Status ODBCConnector::finish_trans() {
    if (_use_tranaction && _is_in_transaction) {
        ODBC_DISPOSE(_dbc, SQL_HANDLE_DBC, SQLEndTran(SQL_HANDLE_DBC, _dbc, SQL_COMMIT),
                     "commit transcation");
        _is_in_transaction = false;
    }

    return Status::OK();
}

Status ODBCConnector::error_status(const std::string& prefix, const std::string& error_msg) {
    std::stringstream msg;
    msg << prefix << " Error: " << error_msg;
    LOG(WARNING) << msg.str();
    return Status::InternalError(msg.str());
}

// handle_diagnostic_record : use SQLGetDiagRec to get the display error/warning information
//
// Parameters:
//      hHandle     ODBC handle
//      hType       Type of handle (HANDLE_STMT, HANDLE_ENV, HANDLE_DBC)
//      RetCode     Return code of failing command
std::string ODBCConnector::handle_diagnostic_record(SQLHANDLE hHandle, SQLSMALLINT hType,
                                                    RETCODE RetCode) {
    SQLSMALLINT rec = 0;
    SQLINTEGER error;
    CHAR message[1000];
    CHAR state[SQL_SQLSTATE_SIZE + 1];

    if (RetCode == SQL_INVALID_HANDLE) {
        return "Invalid handle!";
    }

    std::string diagnostic_msg;

    while (SQLGetDiagRec(hType, hHandle, ++rec, (SQLCHAR*)(state), &error,
                         reinterpret_cast<SQLCHAR*>(message),
                         (SQLSMALLINT)(sizeof(message) / sizeof(WCHAR)),
                         (SQLSMALLINT*)nullptr) == SQL_SUCCESS) {
        // Hide data truncated..
        if (wcsncmp(reinterpret_cast<const wchar_t*>(state), L"01004", 5)) {
            diagnostic_msg += fmt::format("{} {} ({})", state, message, error);
        }
    }

    return diagnostic_msg;
}

} // namespace doris
