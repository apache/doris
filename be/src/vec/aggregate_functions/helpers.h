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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/Helpers.h
// and modified by Doris

#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_null.h"
#include "vec/data_types/data_type.h"

// TODO: Should we support decimal in numeric types?
#define FOR_NUMERIC_TYPES(M) \
    M(UInt8)                 \
    M(UInt16)                \
    M(UInt32)                \
    M(UInt64)                \
    M(Int8)                  \
    M(Int16)                 \
    M(Int32)                 \
    M(Int64)                 \
    M(Int128)                \
    M(Float32)               \
    M(Float64)

#define FOR_DECIMAL_TYPES(M) \
    M(Decimal32)             \
    M(Decimal64)             \
    M(Decimal128)            \
    M(Decimal128I)

namespace doris::vectorized {

/** Create an aggregate function with a numeric type in the template parameter, depending on the type of the argument.
  */
template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction* create_with_numeric_type(const IDataType& argument_type,
                                                    TArgs&&... args) {
    WhichDataType which(argument_type);
#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<TYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction* create_with_numeric_type_null(const DataTypes& argument_types,
                                                         TArgs&&... args) {
    WhichDataType which(argument_types[0]);
#define DISPATCH(TYPE)                                                                      \
    if (which.idx == TypeIndex::TYPE)                                                       \
        return new AggregateFunctionNullUnaryInline<AggregateFunctionTemplate<TYPE>, true>( \
                new AggregateFunctionTemplate<TYPE>(std::forward<TArgs>(args)...),          \
                argument_types);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, bool> class AggregateFunctionTemplate, bool bool_param,
          typename... TArgs>
static IAggregateFunction* create_with_numeric_type(const IDataType& argument_type,
                                                    TArgs&&... args) {
    WhichDataType which(argument_type);
#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<TYPE, bool_param>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data,
          typename... TArgs>
static IAggregateFunction* create_with_numeric_type(const IDataType& argument_type,
                                                    TArgs&&... args) {
    WhichDataType which(argument_type);
#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<TYPE, Data>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate,
          template <typename> class Data, typename... TArgs>
static IAggregateFunction* create_with_numeric_type(const IDataType& argument_type,
                                                    TArgs&&... args) {
    WhichDataType which(argument_type);
#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<TYPE, Data<TYPE>>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
          typename... TArgs>
static IAggregateFunction* create_with_numeric_type(const IDataType& argument_type,
                                                    TArgs&&... args) {
    WhichDataType which(argument_type);
#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<Data<TYPE>>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction* create_with_decimal_type(const IDataType& argument_type,
                                                    TArgs&&... args) {
    WhichDataType which(argument_type);
#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<TYPE>(std::forward<TArgs>(args)...);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction* create_with_decimal_type_null(const DataTypes& argument_types,
                                                         TArgs&&... args) {
    WhichDataType which(argument_types[0]);
#define DISPATCH(TYPE)                                                                      \
    if (which.idx == TypeIndex::TYPE)                                                       \
        return new AggregateFunctionNullUnaryInline<AggregateFunctionTemplate<TYPE>, true>( \
                new AggregateFunctionTemplate<TYPE>(std::forward<TArgs>(args)...),          \
                argument_types);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data,
          typename... TArgs>
static IAggregateFunction* create_with_decimal_type(const IDataType& argument_type,
                                                    TArgs&&... args) {
    WhichDataType which(argument_type);
#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<TYPE, Data>(std::forward<TArgs>(args)...);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

/** For template with two arguments.
  */
template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate,
          typename... TArgs>
static IAggregateFunction* create_with_two_numeric_types_second(const IDataType& second_type,
                                                                TArgs&&... args) {
    WhichDataType which(second_type);
#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<FirstType, TYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction* create_with_two_numeric_types(const IDataType& first_type,
                                                         const IDataType& second_type,
                                                         TArgs&&... args) {
    WhichDataType which(first_type);
#define DISPATCH(TYPE)                                                                \
    if (which.idx == TypeIndex::TYPE)                                                 \
        return create_with_two_numeric_types_second<TYPE, AggregateFunctionTemplate>( \
                second_type, std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

} // namespace  doris::vectorized
