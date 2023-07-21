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

#include <typeinfo>
#include <utility>

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

bool DataTypeTime::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

std::string DataTypeTime::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto value = assert_cast<const ColumnFloat64&>(*ptr).get_element(row_num);
    return time_to_buffer_from_double(value);
}

void DataTypeTime::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    std::string value = to_string(column, row_num);
    ostr.write(value.data(), value.size());
}

void DataTypeTimeV2::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->mutable_decimal_param()->set_scale(_scale);
}

MutableColumnPtr DataTypeTime::create_column() const {
    return DataTypeNumberBase<Float64>::create_column();
}

bool DataTypeTimeV2::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

std::string DataTypeTimeV2::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto value = assert_cast<const ColumnFloat64&>(*ptr).get_element(row_num);
    return timev2_to_buffer_from_double(value, _scale);
}

void DataTypeTimeV2::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    std::string value = to_string(column, row_num);
    ostr.write(value.data(), value.size());
}

MutableColumnPtr DataTypeTimeV2::create_column() const {
    return DataTypeNumberBase<Float64>::create_column();
}
} // namespace doris::vectorized
