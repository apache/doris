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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/FieldVisitors.h
// and modified by Doris

#pragma once

#include "common/exception.h"
#include "common/status.h"
#include "vec/common/demangle.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/field.h"

namespace doris::vectorized {

/** StaticVisitor (and its descendants) - class with overloaded operator() for all types of fields.
  * You could call visitor for field using function 'apply_visitor'.
  * Also "binary visitor" is supported - its operator() takes two arguments.
  */
template <typename R = void>
struct StaticVisitor {
    using ResultType = R;
};

/// F is template parameter, to allow universal reference for field, that is useful for const and non-const values.
template <typename Visitor, typename F>
typename std::decay_t<Visitor>::ResultType apply_visitor(Visitor&& visitor, F&& field) {
    switch (field.get_type()) {
    case PrimitiveType::TYPE_NULL:
        return visitor(field.template get<Null>());
    case PrimitiveType::TYPE_DATETIMEV2:
        return visitor(field.template get<UInt64>());
    case PrimitiveType::TYPE_LARGEINT:
        return visitor(field.template get<Int128>());
    case PrimitiveType::TYPE_DATETIME:
    case PrimitiveType::TYPE_DATE:
    case PrimitiveType::TYPE_BIGINT:
        return visitor(field.template get<Int64>());
    case PrimitiveType::TYPE_DOUBLE:
        return visitor(field.template get<Float64>());
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        return visitor(field.template get<String>());
    case PrimitiveType::TYPE_ARRAY:
        return visitor(field.template get<Array>());
    case PrimitiveType::TYPE_STRUCT:
        return visitor(field.template get<Tuple>());
    case PrimitiveType::TYPE_VARIANT:
        return visitor(field.template get<VariantMap>());
    case PrimitiveType::TYPE_DECIMAL32:
        return visitor(field.template get<DecimalField<Decimal32>>());
    case PrimitiveType::TYPE_DECIMAL64:
        return visitor(field.template get<DecimalField<Decimal64>>());
    case PrimitiveType::TYPE_DECIMALV2:
        return visitor(field.template get<DecimalField<Decimal128V2>>());
    case PrimitiveType::TYPE_DECIMAL128I:
        return visitor(field.template get<DecimalField<Decimal128V3>>());
    case PrimitiveType::TYPE_DECIMAL256:
        return visitor(field.template get<DecimalField<Decimal256>>());
    case PrimitiveType::TYPE_JSONB:
        return visitor(field.template get<JsonbField>());
    default:
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Bad type of Field {}",
                               static_cast<int>(field.get_type()));
        __builtin_unreachable();
        return {};
    }
}

} // namespace doris::vectorized
