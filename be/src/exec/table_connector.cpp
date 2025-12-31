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
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
#include "common/compile_check_begin.h"

class TupleDescriptor;

TableConnector::TableConnector(const TupleDescriptor* tuple_desc, bool use_transaction,
                               std::string_view table_name, const std::string& sql_str)
        : _is_open(false),
          _use_tranaction(use_transaction),
          _is_in_transaction(false),
          _table_name(table_name),
          _tuple_desc(tuple_desc),
          _sql_str(sql_str) {}

void TableConnector::init_profile(doris::RuntimeProfile* operator_profile) {
    RuntimeProfile* custom_profile = operator_profile->get_child("CustomCounters");
    _convert_tuple_timer = ADD_TIMER(custom_profile, "TupleConvertTime");
    _result_send_timer = ADD_TIMER(custom_profile, "ResultSendTime");
    _sent_rows_counter = ADD_COUNTER(custom_profile, "NumSentRows", TUnit::UNIT);
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
    // The `first` variable remains unmodified; `const_cast` is used to adapt to third-party functions.
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

#include "common/compile_check_end.h"
} // namespace doris
