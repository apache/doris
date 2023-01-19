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

#include "util/date_func.h"
#include "vec/columns/columns_number.h"

namespace doris::vectorized {

bool DataTypeTime::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

std::string DataTypeTime::to_string(const IColumn& column, size_t row_num) const {
    Float64 float_val =
            assert_cast<const ColumnFloat64&>(*column.convert_to_full_column_if_const().get())
                    .get_data()[row_num];
    return time_to_buffer_from_double(float_val);
}

void DataTypeTime::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    Float64 float_val =
            assert_cast<const ColumnFloat64&>(*column.convert_to_full_column_if_const().get())
                    .get_data()[row_num];
    std::string time_val = time_to_buffer_from_double(float_val);
    // DateTime to_string the end is /0
    ostr.write(time_val.data(), time_val.size());
}

MutableColumnPtr DataTypeTime::create_column() const {
    return DataTypeNumberBase<Float64>::create_column();
}

} // namespace doris::vectorized
