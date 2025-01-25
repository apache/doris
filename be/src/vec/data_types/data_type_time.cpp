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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeDateTime.cpp
// and modified by Doris

#include "vec/data_types/data_type_time.h"

#include <gen_cpp/data.pb.h>

#include <cctype>
#include <typeinfo>
#include <utility>

#include "common/status.h"
#include "util/date_func.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

void DataTypeTimeV2::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->mutable_decimal_param()->set_scale(_scale);
}

bool DataTypeTimeV2::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

size_t DataTypeTimeV2::number_length() const {
    //59:59:59:000000
    return 14;
}
void DataTypeTimeV2::push_number(ColumnString::Chars& chars, const Float64& num) const {
    auto timev2_str = timev2_to_buffer_from_double(num, _scale);
    chars.insert(timev2_str.begin(), timev2_str.end());
}

std::string DataTypeTimeV2::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto value = assert_cast<const ColumnFloat64&>(*ptr).get_element(row_num);
    return timev2_to_buffer_from_double(value, _scale);
}

std::string DataTypeTimeV2::to_string(double value) const {
    return timev2_to_buffer_from_double(value, _scale);
}
void DataTypeTimeV2::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    std::string value = to_string(column, row_num);
    ostr.write(value.data(), value.size());
}

MutableColumnPtr DataTypeTimeV2::create_column() const {
    return DataTypeNumberBase<Float64>::create_column();
}

Field DataTypeTimeV2::get_field(const TExprNode& node) const {
    const static int64_t max_time = (int64_t)3020399 * 1000 * 1000;
    const int32_t scale = node.type.types.empty() ? -1 : node.type.types.front().scalar_type.scale;
    auto len = cast_set<size_t>(node.time_literal.value.size());
    const char* p = node.time_literal.value.c_str();
    const char* end = p + len;
    bool valid {true};
    double value {};
    int v[4] = {};
    int part {}, sign {1};
    if (*p == '-') {
        sign = -1;
        p++;
    }
    while (p != end) {
        if (isdigit(*p)) {
            v[part] *= 10;
            v[part] += *p - '0';
        } else {
            part++;
        }
        if (part == 3) {
            if (scale == 0) {
                break;
            }
            const char* t = ++p;
            for (int i = 0; i < 7; i++) {
                v[3] *= 10;
                if (i >= scale || t == end) {
                    continue;
                }
                if (!isdigit(*t)) {
                    valid = false;
                    break;
                }
                v[3] += *t - '0';
                t++;
            }
            break;
        }
        ++p;
    }
    if (v[1] > 59 || v[2] > 59) {
        valid = false;
    }
    value = (((v[0] * 60.0) + v[1]) * 60 + v[2]) * 1000000 + v[3];
    if (valid) {
        return (value > max_time ? max_time : value) * sign;
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Invalid value: {} for type TimeV2", node.time_literal.value);
    }
}
} // namespace doris::vectorized
