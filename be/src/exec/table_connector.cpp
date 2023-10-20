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

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <iconv.h>

#include <memory>
#include <string_view>
#include <type_traits>

#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class TupleDescriptor;

TableConnector::TableConnector(const TupleDescriptor* tuple_desc, bool use_transaction,
                               std::string_view table_name, const std::string& sql_str)
        : _is_open(false),
          _use_tranaction(use_transaction),
          _is_in_transaction(false),
          _table_name(table_name),
          _tuple_desc(tuple_desc),
          _sql_str(sql_str) {}

void TableConnector::init_profile(doris::RuntimeProfile* profile) {
    _convert_tuple_timer = ADD_TIMER(profile, "TupleConvertTime");
    _result_send_timer = ADD_TIMER(profile, "ResultSendTime");
    _sent_rows_counter = ADD_COUNTER(profile, "NumSentRows", TUnit::UNIT);
}

std::u16string TableConnector::utf8_to_u16string(const char* first, const char* last) {
    auto deleter = [](auto convertor) {
        if (convertor == reinterpret_cast<decltype(convertor)>(-1)) {
            return;
        }
        iconv_close(convertor);
    };
    std::unique_ptr<std::remove_pointer_t<iconv_t>, decltype(deleter)> convertor(
            iconv_open("UTF-16LE", "UTF-8"), deleter);

    char* in = const_cast<char*>(first);
    size_t inbytesleft = last - first;

    char16_t buffer[1024];
    char* out = reinterpret_cast<char*>(&buffer[0]);
    size_t outbytesleft = sizeof(buffer);

    std::u16string result;
    while (inbytesleft > 0) {
        if (iconv(convertor.get(), &in, &inbytesleft, &out, &outbytesleft)) {
            if (errno == E2BIG) {
                result += std::u16string_view(buffer,
                                              (sizeof(buffer) - outbytesleft) / sizeof(char16_t));
                out = reinterpret_cast<char*>(&buffer[0]);
                outbytesleft = sizeof(buffer);
            } else {
                LOG(WARNING) << fmt::format("Failed to convert the UTF-8 string {} to UTF-16LE",
                                            std::string(first, last));
                return result;
            }
        }
    }
    result += std::u16string_view(buffer, (sizeof(buffer) - outbytesleft) / sizeof(char16_t));
    return result;
}

Status TableConnector::convert_column_data(const vectorized::ColumnPtr& column_ptr,
                                           const vectorized::DataTypePtr& type_ptr,
                                           const TypeDescriptor& type, int row,
                                           TOdbcTableType::type table_type) {
    auto extra_convert_func = [&](const std::string_view& str, const bool& is_date) -> void {
        if (table_type == TOdbcTableType::ORACLE || table_type == TOdbcTableType::SAP_HANA) {
            //if is ORACLE and date type, insert into need convert
            if (is_date) {
                fmt::format_to(_insert_stmt_buffer, "to_date('{}','yyyy-mm-dd')", str);
            } else {
                fmt::format_to(_insert_stmt_buffer, "to_date('{}','yyyy-mm-dd hh24:mi:ss')", str);
            }
        } else if (table_type == TOdbcTableType::POSTGRESQL) {
            fmt::format_to(_insert_stmt_buffer, "'{}'::date", str);
        } else if (table_type == TOdbcTableType::SQLSERVER) {
            // Values in sqlserver should be enclosed by single quotes
            fmt::format_to(_insert_stmt_buffer, "'{}'", str);
        } else {
            fmt::format_to(_insert_stmt_buffer, "\"{}\"", str);
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
        if (table_type == TOdbcTableType::SAP_HANA) {
            fmt::format_to(_insert_stmt_buffer, "{}", *reinterpret_cast<const bool*>(item));
        } else {
            fmt::format_to(_insert_stmt_buffer, "{}", *reinterpret_cast<const int8_t*>(item));
        }
        break;
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
        VecDateTimeValue value = binary_cast<int64_t, doris::VecDateTimeValue>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        std::string_view str(buf, pos - buf - 1);
        extra_convert_func(str, true);
        break;
    }
    case TYPE_DATETIME: {
        VecDateTimeValue value = binary_cast<int64_t, doris::VecDateTimeValue>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        std::string_view str(buf, pos - buf - 1);
        extra_convert_func(str, false);
        break;
    }
    case TYPE_DATEV2: {
        DateV2Value<DateV2ValueType> value =
                binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(int32_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        std::string str(buf, pos - buf - 1);
        extra_convert_func(str, true);
        break;
    }
    case TYPE_DATETIMEV2: {
        DateV2Value<DateTimeV2ValueType> value =
                binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf, type.scale);
        std::string str(buf, pos - buf - 1);
        extra_convert_func(str, false);
        break;
    }
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        // for oracle/pg database string must be '
        if (table_type == TOdbcTableType::ORACLE || table_type == TOdbcTableType::POSTGRESQL ||
            table_type == TOdbcTableType::SAP_HANA || table_type == TOdbcTableType::MYSQL ||
            table_type == TOdbcTableType::CLICKHOUSE || table_type == TOdbcTableType::SQLSERVER) {
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
        //for PG        insert into ---> ARRAY[]
        if (table_type == TOdbcTableType::POSTGRESQL) {
            fmt::format_to(_insert_stmt_buffer, "{}", "ARRAY[");
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
        fmt::format_to(_insert_stmt_buffer, "{}", "]");
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
        auto decimal_type = remove_nullable(type_ptr);
        auto val = decimal_type->to_string(*column, row);
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
