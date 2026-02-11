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
#include "runtime/primitive_type.h"
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
        return visitor.template apply<PrimitiveType::TYPE_NULL>(field.template get<TYPE_NULL>());
    case PrimitiveType::TYPE_DATETIMEV2:
        return visitor.template apply<PrimitiveType::TYPE_DATETIMEV2>(
                field.template get<TYPE_DATETIMEV2>());
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        return visitor.template apply<PrimitiveType::TYPE_TIMESTAMPTZ>(
                field.template get<TYPE_TIMESTAMPTZ>());
    case PrimitiveType::TYPE_LARGEINT:
        return visitor.template apply<PrimitiveType::TYPE_LARGEINT>(
                field.template get<TYPE_LARGEINT>());
    case PrimitiveType::TYPE_DATETIME:
        return visitor.template apply<PrimitiveType::TYPE_DATETIME>(
                field.template get<TYPE_DATETIME>());
    case PrimitiveType::TYPE_DATE:
        return visitor.template apply<PrimitiveType::TYPE_DATE>(field.template get<TYPE_DATE>());
    case PrimitiveType::TYPE_DATEV2:
        return visitor.template apply<PrimitiveType::TYPE_DATEV2>(
                field.template get<TYPE_DATEV2>());
    case PrimitiveType::TYPE_BOOLEAN:
        return visitor.template apply<PrimitiveType::TYPE_BOOLEAN>(
                field.template get<TYPE_BOOLEAN>());
    case PrimitiveType::TYPE_TINYINT:
        return visitor.template apply<PrimitiveType::TYPE_TINYINT>(
                field.template get<TYPE_TINYINT>());
    case PrimitiveType::TYPE_SMALLINT:
        return visitor.template apply<PrimitiveType::TYPE_SMALLINT>(
                field.template get<TYPE_SMALLINT>());
    case PrimitiveType::TYPE_INT:
        return visitor.template apply<PrimitiveType::TYPE_INT>(field.template get<TYPE_INT>());
    case PrimitiveType::TYPE_BIGINT:
        return visitor.template apply<PrimitiveType::TYPE_BIGINT>(
                field.template get<TYPE_BIGINT>());
    case PrimitiveType::TYPE_UINT32:
        return visitor.template apply<PrimitiveType::TYPE_UINT32>(
                field.template get<TYPE_UINT32>());
    case PrimitiveType::TYPE_UINT64:
        return visitor.template apply<PrimitiveType::TYPE_UINT64>(
                field.template get<TYPE_UINT64>());
    case PrimitiveType::TYPE_FLOAT:
        return visitor.template apply<PrimitiveType::TYPE_FLOAT>(field.template get<TYPE_FLOAT>());
    case PrimitiveType::TYPE_TIMEV2:
        return visitor.template apply<PrimitiveType::TYPE_TIMEV2>(
                field.template get<TYPE_TIMEV2>());
    case PrimitiveType::TYPE_DOUBLE:
        return visitor.template apply<PrimitiveType::TYPE_DOUBLE>(
                field.template get<TYPE_DOUBLE>());
    case PrimitiveType::TYPE_IPV4:
        return visitor.template apply<PrimitiveType::TYPE_IPV4>(field.template get<TYPE_IPV4>());
    case PrimitiveType::TYPE_IPV6:
        return visitor.template apply<PrimitiveType::TYPE_IPV6>(field.template get<TYPE_IPV6>());
    case PrimitiveType::TYPE_STRING:
        return visitor.template apply<PrimitiveType::TYPE_STRING>(
                field.template get<TYPE_STRING>());
    case PrimitiveType::TYPE_CHAR:
        return visitor.template apply<PrimitiveType::TYPE_CHAR>(field.template get<TYPE_CHAR>());
    case PrimitiveType::TYPE_VARCHAR:
        return visitor.template apply<PrimitiveType::TYPE_VARCHAR>(
                field.template get<TYPE_VARCHAR>());
    case PrimitiveType::TYPE_VARBINARY:
        return visitor.template apply<PrimitiveType::TYPE_VARBINARY>(
                field.template get<TYPE_VARBINARY>());
    case PrimitiveType::TYPE_ARRAY:
        return visitor.template apply<PrimitiveType::TYPE_ARRAY>(field.template get<TYPE_ARRAY>());
    case PrimitiveType::TYPE_STRUCT:
        return visitor.template apply<PrimitiveType::TYPE_STRUCT>(
                field.template get<TYPE_STRUCT>());
    case PrimitiveType::TYPE_MAP:
        return visitor.template apply<PrimitiveType::TYPE_MAP>(field.template get<TYPE_MAP>());
    case PrimitiveType::TYPE_VARIANT:
        return visitor.template apply<PrimitiveType::TYPE_VARIANT>(
                field.template get<TYPE_VARIANT>());
    case PrimitiveType::TYPE_DECIMAL32:
        return visitor.template apply<PrimitiveType::TYPE_DECIMAL32>(
                field.template get<TYPE_DECIMAL32>());
    case PrimitiveType::TYPE_DECIMAL64:
        return visitor.template apply<PrimitiveType::TYPE_DECIMAL64>(
                field.template get<TYPE_DECIMAL64>());
    case PrimitiveType::TYPE_DECIMALV2:
        return visitor.template apply<PrimitiveType::TYPE_DECIMALV2>(
                field.template get<TYPE_DECIMALV2>());
    case PrimitiveType::TYPE_DECIMAL128I:
        return visitor.template apply<PrimitiveType::TYPE_DECIMAL128I>(
                field.template get<TYPE_DECIMAL128I>());
    case PrimitiveType::TYPE_DECIMAL256:
        return visitor.template apply<PrimitiveType::TYPE_DECIMAL256>(
                field.template get<TYPE_DECIMAL256>());
    case PrimitiveType::TYPE_BITMAP:
        return visitor.template apply<PrimitiveType::TYPE_BITMAP>(
                field.template get<TYPE_BITMAP>());
    case PrimitiveType::TYPE_HLL:
        return visitor.template apply<PrimitiveType::TYPE_HLL>(field.template get<TYPE_HLL>());
    case PrimitiveType::TYPE_QUANTILE_STATE:
        return visitor.template apply<PrimitiveType::TYPE_QUANTILE_STATE>(
                field.template get<TYPE_QUANTILE_STATE>());
    case PrimitiveType::TYPE_JSONB:
        return visitor.template apply<PrimitiveType::TYPE_JSONB>(field.template get<TYPE_JSONB>());
    default:
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Bad type of Field {}",
                               static_cast<int>(field.get_type()));
        return {};
    }
}

} // namespace doris::vectorized
