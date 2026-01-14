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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeDateTime.h
// and modified by Doris

#include "vec/data_types/data_type_timestamptz.h"

#include "vec/functions/cast/cast_to_timestamptz.h"

namespace doris::vectorized {
Field DataTypeTimeStampTz::get_field(const TExprNode& node) const {
    TimestampTzValue res;
    CastParameters params {.status = Status::OK(), .is_strict = true};

    if (!CastToTimstampTz::from_string(
                {node.date_literal.value.c_str(), node.date_literal.value.size()}, res, params,
                nullptr, _scale)) [[unlikely]] {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Invalid value: {} for type TimeStampTz({})",
                               node.date_literal.value, _scale);
    } else {
        return Field::create_field<TYPE_TIMESTAMPTZ>(res);
    }
}

} // namespace doris::vectorized