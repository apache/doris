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
        return visitor.template apply<PrimitiveType::TYPE_NULL>(
                field.template get<typename PrimitiveTypeTraits<TYPE_NULL>::CppType>());
    case PrimitiveType::TYPE_DATETIMEV2:
        return visitor.template apply<PrimitiveType::TYPE_DATETIMEV2>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType>());
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        return visitor.template apply<PrimitiveType::TYPE_TIMESTAMPTZ>(
                field.template get<typename PrimitiveTypeTraits<TYPE_TIMESTAMPTZ>::CppType>());
    case PrimitiveType::TYPE_LARGEINT:
        return visitor.template apply<PrimitiveType::TYPE_LARGEINT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_LARGEINT>::CppType>());
    case PrimitiveType::TYPE_DATETIME:
        return visitor.template apply<PrimitiveType::TYPE_DATETIME>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType>());
    case PrimitiveType::TYPE_DATE:
        return visitor.template apply<PrimitiveType::TYPE_DATE>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DATE>::CppType>());
    case PrimitiveType::TYPE_BOOLEAN:
        return visitor.template apply<PrimitiveType::TYPE_BOOLEAN>(
                field.template get<typename PrimitiveTypeTraits<TYPE_BOOLEAN>::CppType>());
    case PrimitiveType::TYPE_TINYINT:
        return visitor.template apply<PrimitiveType::TYPE_TINYINT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_TINYINT>::CppType>());
    case PrimitiveType::TYPE_SMALLINT:
        return visitor.template apply<PrimitiveType::TYPE_SMALLINT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_SMALLINT>::CppType>());
    case PrimitiveType::TYPE_INT:
        return visitor.template apply<PrimitiveType::TYPE_INT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_INT>::CppType>());
    case PrimitiveType::TYPE_BIGINT:
        return visitor.template apply<PrimitiveType::TYPE_BIGINT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_BIGINT>::CppType>());
    case PrimitiveType::TYPE_FLOAT:
        return visitor.template apply<PrimitiveType::TYPE_FLOAT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_FLOAT>::CppType>());
    case PrimitiveType::TYPE_DOUBLE:
        return visitor.template apply<PrimitiveType::TYPE_DOUBLE>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DOUBLE>::CppType>());
    case PrimitiveType::TYPE_STRING:
        return visitor.template apply<PrimitiveType::TYPE_STRING>(
                field.template get<typename PrimitiveTypeTraits<TYPE_STRING>::CppType>());
    case PrimitiveType::TYPE_CHAR:
        return visitor.template apply<PrimitiveType::TYPE_CHAR>(
                field.template get<typename PrimitiveTypeTraits<TYPE_CHAR>::CppType>());
    case PrimitiveType::TYPE_VARCHAR:
        return visitor.template apply<PrimitiveType::TYPE_VARCHAR>(
                field.template get<typename PrimitiveTypeTraits<TYPE_VARCHAR>::CppType>());
    case PrimitiveType::TYPE_ARRAY:
        return visitor.template apply<PrimitiveType::TYPE_ARRAY>(
                field.template get<typename PrimitiveTypeTraits<TYPE_ARRAY>::CppType>());
    case PrimitiveType::TYPE_STRUCT:
        return visitor.template apply<PrimitiveType::TYPE_STRUCT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_STRUCT>::CppType>());
    case PrimitiveType::TYPE_VARIANT:
        return visitor.template apply<PrimitiveType::TYPE_VARIANT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_VARIANT>::CppType>());
    case PrimitiveType::TYPE_DECIMAL32:
        return visitor.template apply<PrimitiveType::TYPE_DECIMAL32>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DECIMAL32>::CppType>());
    case PrimitiveType::TYPE_DECIMAL64:
        return visitor.template apply<PrimitiveType::TYPE_DECIMAL64>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DECIMAL64>::CppType>());
    case PrimitiveType::TYPE_DECIMALV2:
        return visitor.template apply<PrimitiveType::TYPE_DECIMALV2>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType>());
    case PrimitiveType::TYPE_DECIMAL128I:
        return visitor.template apply<PrimitiveType::TYPE_DECIMAL128I>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::CppType>());
    case PrimitiveType::TYPE_DECIMAL256:
        return visitor.template apply<PrimitiveType::TYPE_DECIMAL256>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DECIMAL256>::CppType>());
    case PrimitiveType::TYPE_JSONB:
        return visitor.template apply<PrimitiveType::TYPE_JSONB>(
                field.template get<typename PrimitiveTypeTraits<TYPE_JSONB>::CppType>());
    default:
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Bad type of Field {}",
                               static_cast<int>(field.get_type()));
        return {};
    }
}

} // namespace doris::vectorized
