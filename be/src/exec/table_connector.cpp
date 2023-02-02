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

#include "exec/table_connector.h"

#include <gen_cpp/Types_types.h>

#include <codecvt>

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/mysql_global.h"
#include "vec/columns/column_array.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {

// Default max buffer size use in insert to: 50MB, normally a batch is smaller than the size
static constexpr uint32_t INSERT_BUFFER_SIZE = 1024l * 1024 * 50;

TableConnector::TableConnector(const TupleDescriptor* tuple_desc, const std::string& sql_str)
        : _is_open(false), _is_in_transaction(false), _tuple_desc(tuple_desc), _sql_str(sql_str) {}

void TableConnector::init_profile(doris::RuntimeProfile* profile) {
    _convert_tuple_timer = ADD_TIMER(profile, "TupleConvertTime");
    _result_send_timer = ADD_TIMER(profile, "ResultSendTime");
    _sent_rows_counter = ADD_COUNTER(profile, "NumSentRows", TUnit::UNIT);
}

std::u16string TableConnector::utf8_to_u16string(const char* first, const char* last) {
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> utf8_utf16_cvt;
    return utf8_utf16_cvt.from_bytes(first, last);
}

Status TableConnector::append(const std::string& table_name, vectorized::Block* block,
                              const std::vector<vectorized::VExprContext*>& output_vexpr_ctxs,
                              uint32_t start_send_row, uint32_t* num_rows_sent,
                              TOdbcTableType::type table_type) {
    _insert_stmt_buffer.clear();
    std::u16string insert_stmt;
    if (table_type == TOdbcTableType::ORACLE) {
        SCOPED_TIMER(_convert_tuple_timer);
        oracle_type_append(table_name, block, output_vexpr_ctxs, start_send_row, num_rows_sent,
                           table_type);
        // Translate utf8 string to utf16 to use unicode encoding
        insert_stmt = utf8_to_u16string(_insert_stmt_buffer.data(),
                                        _insert_stmt_buffer.data() + _insert_stmt_buffer.size());
    } else {
        SCOPED_TIMER(_convert_tuple_timer);
        fmt::format_to(_insert_stmt_buffer, "INSERT INTO {} VALUES (", table_name);

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
    }
    RETURN_IF_ERROR(exec_write_sql(insert_stmt, _insert_stmt_buffer));
    COUNTER_UPDATE(_sent_rows_counter, *num_rows_sent);
    return Status::OK();
}

Status TableConnector::oracle_type_append(
        const std::string& table_name, vectorized::Block* block,
        const std::vector<vectorized::VExprContext*>& output_vexpr_ctxs, uint32_t start_send_row,
        uint32_t* num_rows_sent, TOdbcTableType::type table_type) {
    fmt::format_to(_insert_stmt_buffer, "INSERT ALL ");
    int num_rows = block->rows();
    int num_columns = block->columns();
    for (int i = start_send_row; i < num_rows; ++i) {
        (*num_rows_sent)++;
        fmt::format_to(_insert_stmt_buffer, "INTO {} VALUES (", table_name);
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
            fmt::format_to(_insert_stmt_buffer, "{}", ") ");
        } else {
            // batch exhausted or _insert_stmt_buffer is full, need to do real insert stmt
            fmt::format_to(_insert_stmt_buffer, "{}", ") SELECT 1 FROM DUAL");
            break;
        }
    }
    return Status::OK();
}

Status TableConnector::convert_column_data(const vectorized::ColumnPtr& column_ptr,
                                           const vectorized::DataTypePtr& type_ptr,
                                           const TypeDescriptor& type, int row,
                                           TOdbcTableType::type table_type) {
    auto extra_convert_func = [&](const std::string_view& str, const bool& is_date) -> void {
        if (table_type != TOdbcTableType::ORACLE) {
            fmt::format_to(_insert_stmt_buffer, "\"{}\"", str);
        } else {
            //if is ORACLE and date type, insert into need convert
            if (is_date) {
                fmt::format_to(_insert_stmt_buffer, "to_date('{}','yyyy-mm-dd')", str);
            } else {
                fmt::format_to(_insert_stmt_buffer, "to_date('{}','yyyy-mm-dd hh24:mi:ss')", str);
            }
        }
    };
    const vectorized::IColumn* column = column_ptr;
    if (type_ptr->is_nullable()) {
        auto nullable_column = assert_cast<const vectorized::ColumnNullable*>(column_ptr.get());
        if (nullable_column->is_null_at(row)) {
            fmt::format_to(_insert_stmt_buffer, "{}", "NULL");
            return Status::OK();
        }
        column = nullable_column->get_nested_column_ptr().get();
    } else {
        column = column_ptr;
    }
    auto [item, size] = column->get_data_at(row);
    switch (type.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        fmt::format_to(_insert_stmt_buffer, "{}", *reinterpret_cast<const int8_t*>(item));
        break;
    case TYPE_SMALLINT:
        fmt::format_to(_insert_stmt_buffer, "{}", *reinterpret_cast<const int16_t*>(item));
        break;
    case TYPE_INT:
        fmt::format_to(_insert_stmt_buffer, "{}", *reinterpret_cast<const int32_t*>(item));
        break;
    case TYPE_BIGINT:
        fmt::format_to(_insert_stmt_buffer, "{}", *reinterpret_cast<const int64_t*>(item));
        break;
    case TYPE_FLOAT:
        fmt::format_to(_insert_stmt_buffer, "{}", *reinterpret_cast<const float*>(item));
        break;
    case TYPE_DOUBLE:
        fmt::format_to(_insert_stmt_buffer, "{}", *reinterpret_cast<const double*>(item));
        break;
    case TYPE_DATE: {
        vectorized::VecDateTimeValue value =
                binary_cast<int64_t, doris::vectorized::VecDateTimeValue>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        std::string_view str(buf, pos - buf - 1);
        extra_convert_func(str, true);
        break;
    }
    case TYPE_DATETIME: {
        vectorized::VecDateTimeValue value =
                binary_cast<int64_t, doris::vectorized::VecDateTimeValue>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        std::string_view str(buf, pos - buf - 1);
        extra_convert_func(str, false);
        break;
    }
    case TYPE_DATEV2: {
        vectorized::DateV2Value<vectorized::DateV2ValueType> value =
                binary_cast<uint32_t, doris::vectorized::DateV2Value<vectorized::DateV2ValueType>>(
                        *(int32_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        std::string str(buf, pos - buf - 1);
        extra_convert_func(str, true);
        break;
    }
    case TYPE_DATETIMEV2: {
        vectorized::DateV2Value<vectorized::DateTimeV2ValueType> value =
                binary_cast<uint64_t,
                            doris::vectorized::DateV2Value<vectorized::DateTimeV2ValueType>>(
                        *(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf, type.scale);
        std::string str(buf, pos - buf - 1);
        extra_convert_func(str, false);
        break;
    }
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        // TODO(zhangstar333): check array data type of postgresql
        // for oracle/pg database string must be '
        if (table_type == TOdbcTableType::ORACLE || table_type == TOdbcTableType::POSTGRESQL) {
            fmt::format_to(_insert_stmt_buffer, "'{}'", fmt::basic_string_view(item, size));
        } else {
            fmt::format_to(_insert_stmt_buffer, "\"{}\"", fmt::basic_string_view(item, size));
        }
        break;
    }
    case TYPE_ARRAY: {
        auto& arr_nested = reinterpret_cast<const vectorized::ColumnArray*>(column)->get_data_ptr();
        auto& arr_offset = reinterpret_cast<const vectorized::ColumnArray*>(column)->get_offsets();
        auto array_type = remove_nullable(type_ptr);
        auto nested_type =
                reinterpret_cast<const vectorized::DataTypeArray&>(*array_type).get_nested_type();

        //for doris、CK insert into --->  []
        //for PG        insert into ---> '{}'
        if (table_type == TOdbcTableType::POSTGRESQL) {
            fmt::format_to(_insert_stmt_buffer, "{}", "'{");
        } else if (table_type == TOdbcTableType::CLICKHOUSE ||
                   table_type == TOdbcTableType::MYSQL) {
            fmt::format_to(_insert_stmt_buffer, "{}", "[");
        }
        bool first_value = true;
        for (auto idx = arr_offset[row - 1]; idx < arr_offset[row]; ++idx) {
            if (first_value == false) {
                fmt::format_to(_insert_stmt_buffer, "{}", ", ");
            }
            if (arr_nested->is_null_at(idx)) {
                fmt::format_to(_insert_stmt_buffer, "{}", "NULL");
            } else {
                RETURN_IF_ERROR(convert_column_data(arr_nested, nested_type, type.children[0], idx,
                                                    table_type));
            }
            first_value = false;
        }
        if (table_type == TOdbcTableType::POSTGRESQL) {
            fmt::format_to(_insert_stmt_buffer, "{}", "}'");
        } else if (table_type == TOdbcTableType::CLICKHOUSE ||
                   table_type == TOdbcTableType::MYSQL) {
            fmt::format_to(_insert_stmt_buffer, "{}", "]");
        }
        break;
    }
    case TYPE_DECIMALV2: {
        DecimalV2Value value = *(DecimalV2Value*)(item);
        fmt::format_to(_insert_stmt_buffer, "{}", value.to_string());
        break;
    }
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I: {
        auto val = type_ptr->to_string(*column, row);
        fmt::format_to(_insert_stmt_buffer, "{}", val);
        break;
    }
    case TYPE_LARGEINT: {
        fmt::format_to(_insert_stmt_buffer, "{}", *reinterpret_cast<const __int128*>(item));
        break;
    }
    default: {
        return Status::InternalError("can't convert this type to mysql type. type = {}",
                                     type.debug_string());
    }
    }
    return Status::OK();
}
} // namespace doris
