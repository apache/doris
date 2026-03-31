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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeDate.cpp
// and modified by Doris

#include "core/data_type/data_type_date.h"

#include <typeinfo>
#include <utility>

#include "core/assert_cast.h"
#include "core/binary_cast.hpp"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_date_or_datetime_impl.hpp"
#include "util/io_helper.h"

namespace doris {
bool DataTypeDate::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

void DataTypeDate::cast_to_date(VecDateTimeValue& value) {
    value.cast_to_date();
}

MutableColumnPtr DataTypeDate::create_column() const {
    return DataTypeNumberBase<PrimitiveType::TYPE_DATE>::create_column();
}

Field DataTypeDate::get_field(const TExprNode& node) const {
    VecDateTimeValue value;
    CastParameters params;
    if (CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                      DatelikeTargetType::DATE>(
                {node.date_literal.value.c_str(), node.date_literal.value.size()}, value, nullptr,
                params)) {
        value.cast_to_date();
        return Field::create_field<TYPE_DATE>(std::move(value));
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Invalid value: {} for type Date", node.date_literal.value);
    }
}

} // namespace doris
