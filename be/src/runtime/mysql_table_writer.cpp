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

#include "runtime/mysql_table_writer.h"

#include <mysql/mysql.h>

#include <sstream>

#include "exprs/expr_context.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/types.h"

namespace doris {

std::string MysqlConnInfo::debug_string() const {
    std::stringstream ss;

    ss << "(host=" << host << ",port=" << port << ",user=" << user << ",db=" << db
       << ",passwd=" << passwd << ")";
    return ss.str();
}

MysqlTableWriter::MysqlTableWriter(const std::vector<ExprContext*>& output_expr_ctxs)
        : _output_expr_ctxs(output_expr_ctxs) {}

MysqlTableWriter::~MysqlTableWriter() {
    if (_mysql_conn) {
        mysql_close(_mysql_conn);
    }
}

Status MysqlTableWriter::open(const MysqlConnInfo& conn_info, const std::string& tbl) {
    _mysql_conn = mysql_init(nullptr);
    if (_mysql_conn == nullptr) {
        return Status::InternalError("Call mysql_init failed.");
    }

    MYSQL* res = mysql_real_connect(_mysql_conn, conn_info.host.c_str(), conn_info.user.c_str(),
                                    conn_info.passwd.c_str(), conn_info.db.c_str(), conn_info.port,
                                    nullptr, // unix socket
                                    0);      // flags
    if (res == nullptr) {
        std::stringstream ss;
        ss << "mysql_real_connect failed because " << mysql_error(_mysql_conn);
        return Status::InternalError(ss.str());
    }

    // set character
    if (mysql_set_character_set(_mysql_conn, "utf8")) {
        std::stringstream ss;
        ss << "mysql_set_character_set failed because " << mysql_error(_mysql_conn);
        return Status::InternalError(ss.str());
    }

    _mysql_tbl = tbl;

    return Status::OK();
}

Status MysqlTableWriter::insert_row(TupleRow* row) {
    std::stringstream ss;

    // Construct Insert statement of mysql
    ss << "INSERT INTO `" << _mysql_tbl << "` VALUES (";
    int num_columns = _output_expr_ctxs.size();
    for (int i = 0; i < num_columns; ++i) {
        if (i != 0) {
            ss << ", ";
        }
        void* item = _output_expr_ctxs[i]->get_value(row);
        if (item == nullptr) {
            ss << "NULL";
            continue;
        }
        switch (_output_expr_ctxs[i]->root()->type().type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            ss << (int)*static_cast<int8_t*>(item);
            break;
        case TYPE_SMALLINT:
            ss << *static_cast<int16_t*>(item);
            break;
        case TYPE_INT:
            ss << *static_cast<int32_t*>(item);
            break;
        case TYPE_BIGINT:
            ss << *static_cast<int64_t*>(item);
            break;
        case TYPE_FLOAT:
            ss << *static_cast<float*>(item);
            break;
        case TYPE_DOUBLE:
            ss << *static_cast<double*>(item);
            break;
        case TYPE_DATE:
        case TYPE_DATETIME: {
            char buf[64];
            const DateTimeValue* time_val = (const DateTimeValue*)(item);
            time_val->to_string(buf);
            ss << "\'" << buf << "\'";
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            const StringValue* string_val = (const StringValue*)(item);

            if (string_val->ptr == nullptr) {
                if (string_val->len == 0) {
                    ss << "\'\'";
                } else {
                    ss << "NULL";
                }
            } else {
                char* buf = new char[2 * string_val->len + 1];
                mysql_real_escape_string(_mysql_conn, buf, string_val->ptr, string_val->len);
                ss << "\'" << buf << "\'";
                delete[] buf;
            }
            break;
        }

        case TYPE_DECIMALV2: {
            const DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(item)->value);
            std::string decimal_str;
            int output_scale = _output_expr_ctxs[i]->root()->output_scale();
            decimal_str = decimal_val.to_string(output_scale);
            ss << decimal_str;
            break;
        }

        default: {
            std::stringstream err_ss;
            err_ss << "can't convert this type to mysql type. type = "
                   << _output_expr_ctxs[i]->root()->type();
            return Status::InternalError(err_ss.str());
        }
        }
    }
    ss << ")";

    // Insert this to MySQL server
    std::string insert_stmt = ss.str();
    LOG(INFO) << insert_stmt;
    if (mysql_real_query(_mysql_conn, insert_stmt.c_str(), insert_stmt.length())) {
        std::stringstream err_ss;
        err_ss << "Insert to mysql server(" << mysql_get_host_info(_mysql_conn)
               << ") failed, because: " << mysql_error(_mysql_conn);
        return Status::InternalError(err_ss.str());
    }

    return Status::OK();
}

Status MysqlTableWriter::append(RowBatch* batch) {
    if (batch == nullptr || batch->num_rows() == 0) {
        return Status::OK();
    }

    int num_rows = batch->num_rows();
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(insert_row(batch->get_row(i)));
    }

    return Status::OK();
}

} // namespace doris
