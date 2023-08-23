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
    case Field::Types::Null:
        return visitor(field.template get<Null>());
    case Field::Types::UInt64:
        return visitor(field.template get<UInt64>());
    case Field::Types::UInt128:
        return visitor(field.template get<UInt128>());
    case Field::Types::Int64:
        return visitor(field.template get<Int64>());
    case Field::Types::Float64:
        return visitor(field.template get<Float64>());
    case Field::Types::String:
        return visitor(field.template get<String>());
    case Field::Types::Array:
        return visitor(field.template get<Array>());
    case Field::Types::Tuple:
        return visitor(field.template get<Tuple>());
    case Field::Types::Decimal32:
        return visitor(field.template get<DecimalField<Decimal32>>());
    case Field::Types::Decimal64:
        return visitor(field.template get<DecimalField<Decimal64>>());
    case Field::Types::Decimal128:
        return visitor(field.template get<DecimalField<Decimal128>>());
    case Field::Types::Decimal128I:
        return visitor(field.template get<DecimalField<Decimal128I>>());
    case Field::Types::AggregateFunctionState:
        return visitor(field.template get<AggregateFunctionStateData>());

    default:
        LOG(FATAL) << "Bad type of Field";
        return {};
    }
}

template <typename Visitor, typename F1, typename F2>
typename std::decay_t<Visitor>::ResultType apply_visitor(Visitor&& visitor, F1&& field1,
                                                         F2&& field2) {
    switch (field1.getType()) {
    case Field::Types::Null:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<Null>(), std::forward<F2>(field2));
    case Field::Types::UInt64:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<UInt64>(), std::forward<F2>(field2));
    case Field::Types::UInt128:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<UInt128>(), std::forward<F2>(field2));
    case Field::Types::Int64:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<Int64>(), std::forward<F2>(field2));
    case Field::Types::Float64:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<Float64>(), std::forward<F2>(field2));
    case Field::Types::String:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<String>(), std::forward<F2>(field2));
    case Field::Types::Array:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<Array>(), std::forward<F2>(field2));
    case Field::Types::Tuple:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<Tuple>(), std::forward<F2>(field2));
    case Field::Types::Decimal32:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<DecimalField<Decimal32>>(),
                                         std::forward<F2>(field2));
    case Field::Types::Decimal64:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<DecimalField<Decimal64>>(),
                                         std::forward<F2>(field2));
    case Field::Types::Decimal128:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<DecimalField<Decimal128>>(),
                                         std::forward<F2>(field2));
    case Field::Types::Decimal128I:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<DecimalField<Decimal128I>>(),
                                         std::forward<F2>(field2));
    case Field::Types::AggregateFunctionState:
        return apply_binary_visitor_impl(std::forward<Visitor>(visitor),
                                         field1.template get<AggregateFunctionStateData>(),
                                         std::forward<F2>(field2));

    default:
        LOG(FATAL) << "Bad type of Field";
        return {};
    }
}

} // namespace doris::vectorized
