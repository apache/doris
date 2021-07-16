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

#include "runtime/datetime_value.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct StrToDate {
    static constexpr auto name = "str_to_date";
    using ReturnType = DataTypeDateTime;
    using ColumnType = ColumnVector<Int128>;

    static void vector_vector(const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, ColumnType::Container& res,
                              NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1] - 1;

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1] - 1;

            auto& ts_val = *reinterpret_cast<DateTimeValue*>(&res[i]);
            if (!ts_val.from_date_format_str(r_raw_str, r_str_size, l_raw_str, l_str_size)) {
                null_map[i] = 1;
            }
            ts_val.to_datetime();
        }
    }
};

using FunctionStrToDate = FunctionBinaryStringOperateToNullType<StrToDate>;

void register_function_str_to_date(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStrToDate>();
}

} // namespace doris::vectorized