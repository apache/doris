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

#include "vec/sink/vmysql_table_writer.h"

#include <mysql/mysql.h>

#include <sstream>

#include "exprs/expr.h"
#include "util/types.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/materialize_block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

VMysqlTableWriter::VMysqlTableWriter(const std::vector<vectorized::VExprContext*>& output_expr_ctxs)
        : _vec_output_expr_ctxs(output_expr_ctxs) {}

VMysqlTableWriter::~VMysqlTableWriter() {
    if (_mysql_conn) {
        mysql_close(_mysql_conn);
    }
}

Status VMysqlTableWriter::open(const MysqlConnInfo& conn_info, const std::string& tbl) {
    _mysql_conn = mysql_init(nullptr);
    if (_mysql_conn == nullptr) {
        return Status::InternalError("Call mysql_init failed.");
    }

    MYSQL* res = mysql_real_connect(_mysql_conn, conn_info.host.c_str(), conn_info.user.c_str(),
                                    conn_info.passwd.c_str(), conn_info.db.c_str(), conn_info.port,
                                    nullptr, // unix socket
                                    0);      // flags
    if (res == nullptr) {
        fmt::memory_buffer err_ss;
        fmt::format_to(err_ss, "mysql_real_connect failed because : {}.", mysql_error(_mysql_conn));
        return Status::InternalError(fmt::to_string(err_ss.data()));
    }

    // set character
    if (mysql_set_character_set(_mysql_conn, "utf8")) {
        fmt::memory_buffer err_ss;
        fmt::format_to(err_ss, "mysql_set_character_set failed because : {}.",
                       mysql_error(_mysql_conn));
        return Status::InternalError(fmt::to_string(err_ss.data()));
    }

    _mysql_tbl = tbl;

    return Status::OK();
}

Status VMysqlTableWriter::append(vectorized::Block* block) {
    Status status = Status::OK();
    if (block == nullptr || block->rows() == 0) {
        return status;
    }

    auto output_block = vectorized::VExprContext::get_output_block_after_execute_exprs(
            _vec_output_expr_ctxs, *block, status);

    auto num_rows = output_block.rows();
    if (UNLIKELY(num_rows == 0)) {
        return status;
    }
    materialize_block_inplace(output_block);
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(insert_row(output_block, i));
    }
    return Status::OK();
}

Status VMysqlTableWriter::insert_row(vectorized::Block& block, size_t row) {
    _insert_stmt_buffer.clear();
    fmt::format_to(_insert_stmt_buffer, "INSERT INTO {} VALUES (", _mysql_tbl);
    int num_columns = _vec_output_expr_ctxs.size();

    for (int i = 0; i < num_columns; ++i) {
        auto& column_ptr = block.get_by_position(i).column;
        auto& type_ptr = block.get_by_position(i).type;

        if (i != 0) {
            fmt::format_to(_insert_stmt_buffer, "{}", ", ");
        }

        vectorized::ColumnPtr column;
        if (type_ptr->is_nullable()) {
            column = assert_cast<const vectorized::ColumnNullable&>(*column_ptr)
                             .get_nested_column_ptr();
            if (column_ptr->is_null_at(row)) {
                fmt::format_to(_insert_stmt_buffer, "{}", "NULL");
                continue;
            }
        } else {
            column = column_ptr;
        }

        switch (_vec_output_expr_ctxs[i]->root()->result_type()) {
        case TYPE_BOOLEAN: {
            auto& data = assert_cast<const vectorized::ColumnUInt8&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_TINYINT: {
            auto& data = assert_cast<const vectorized::ColumnInt8&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_SMALLINT: {
            auto& data = assert_cast<const vectorized::ColumnInt16&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_INT: {
            auto& data = assert_cast<const vectorized::ColumnInt32&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_BIGINT: {
            auto& data = assert_cast<const vectorized::ColumnInt64&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_FLOAT: {
            auto& data = assert_cast<const vectorized::ColumnFloat32&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_DOUBLE: {
            auto& data = assert_cast<const vectorized::ColumnFloat64&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }

        case TYPE_STRING:
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            const auto& string_val =
                    assert_cast<const vectorized::ColumnString&>(*column).get_data_at(row);
            DCHECK(string_val.data != nullptr);
            std::unique_ptr<char[]> buf(new char[2 * string_val.size + 1]);
            mysql_real_escape_string(_mysql_conn, buf.get(), string_val.data, string_val.size);
            fmt::format_to(_insert_stmt_buffer, "'{}'", buf.get());

            break;
        }
        case TYPE_DECIMALV2: {
            DecimalV2Value value =
                    (DecimalV2Value)
                            assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>&>(
                                    *column)
                                    .get_data()[row];
            fmt::format_to(_insert_stmt_buffer, "{}", value.to_string());
            break;
        }
        case TYPE_DATE:
        case TYPE_DATETIME: {
            int64_t int_val = assert_cast<const vectorized::ColumnInt64&>(*column).get_data()[row];
            vectorized::VecDateTimeValue value =
                    binary_cast<int64_t, doris::vectorized::VecDateTimeValue>(int_val);

            char buf[64];
            char* pos = value.to_string(buf);
            std::string str(buf, pos - buf - 1);
            fmt::format_to(_insert_stmt_buffer, "'{}'", str);
            break;
        }
        default: {
            fmt::memory_buffer err_out;
            fmt::format_to(err_out, "can't convert this type to mysql type. type = {}",
                           _vec_output_expr_ctxs[i]->root()->type().type);
            return Status::InternalError(fmt::to_string(err_out));
        }
        }
    }

    fmt::format_to(_insert_stmt_buffer, "{}", ")");

    // Insert this to MySQL server
    if (mysql_real_query(_mysql_conn, _insert_stmt_buffer.data(), _insert_stmt_buffer.size())) {
        fmt::memory_buffer err_ss;
        fmt::format_to(err_ss, "Insert to mysql server({}) failed, because: {}.",
                       mysql_get_host_info(_mysql_conn), mysql_error(_mysql_conn));
        return Status::InternalError(fmt::to_string(err_ss));
    }

    return Status::OK();
}
} // namespace vectorized
} // namespace doris
