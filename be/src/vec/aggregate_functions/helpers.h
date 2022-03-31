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
#include "vec/data_types/data_type.h"

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
    if (which.idx == TypeIndex::Enum8) {
        return new AggregateFunctionTemplate<Int8>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Enum16) {
        return new AggregateFunctionTemplate<Int16>(std::forward<TArgs>(args)...);
    }
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
    if (which.idx == TypeIndex::Enum8) {
        return new AggregateFunctionTemplate<Int8, bool_param>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Enum16) {
        return new AggregateFunctionTemplate<Int16, bool_param>(std::forward<TArgs>(args)...);
    }
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
    if (which.idx == TypeIndex::Enum8) {
        return new AggregateFunctionTemplate<Int8, Data>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Enum16) {
        return new AggregateFunctionTemplate<Int16, Data>(std::forward<TArgs>(args)...);
    }
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
    if (which.idx == TypeIndex::Enum8) {
        return new AggregateFunctionTemplate<Int8, Data<Int8>>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Enum16) {
        return new AggregateFunctionTemplate<Int16, Data<Int16>>(std::forward<TArgs>(args)...);
    }
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
    // if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Data<Int8>>(std::forward<TArgs>(args)...);
    // if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Data<Int16>>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate,
          template <typename> class Data, typename... TArgs>
static IAggregateFunction* create_with_unsigned_integer_type(const IDataType& argument_type,
                                                             TArgs&&... args) {
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::UInt8) {
        return new AggregateFunctionTemplate<UInt8, Data<UInt8>>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::UInt16) {
        return new AggregateFunctionTemplate<UInt16, Data<UInt16>>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::UInt32) {
        return new AggregateFunctionTemplate<UInt32, Data<UInt32>>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::UInt64) {
        return new AggregateFunctionTemplate<UInt64, Data<UInt64>>(std::forward<TArgs>(args)...);
    }
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction* create_with_numeric_based_type(const IDataType& argument_type,
                                                          TArgs&&... args) {
    IAggregateFunction* f = create_with_numeric_type<AggregateFunctionTemplate>(
            argument_type, std::forward<TArgs>(args)...);
    if (f) {
        return f;
    }

    /// expects that DataTypeDate based on UInt16, DataTypeDateTime based on UInt32 and UUID based on UInt128
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) {
        return new AggregateFunctionTemplate<UInt16>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::DateTime) {
        return new AggregateFunctionTemplate<UInt32>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::UUID) {
        return new AggregateFunctionTemplate<UInt128>(std::forward<TArgs>(args)...);
    }
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction* create_with_decimal_type(const IDataType& argument_type,
                                                    TArgs&&... args) {
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Decimal32) {
        return new AggregateFunctionTemplate<Decimal32>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Decimal64) {
        return new AggregateFunctionTemplate<Decimal64>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Decimal128) {
        return new AggregateFunctionTemplate<Decimal128>(std::forward<TArgs>(args)...);
    }
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data,
          typename... TArgs>
static IAggregateFunction* create_with_decimal_type(const IDataType& argument_type,
                                                    TArgs&&... args) {
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Decimal32) {
        return new AggregateFunctionTemplate<Decimal32, Data>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Decimal64) {
        return new AggregateFunctionTemplate<Decimal64, Data>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Decimal128) {
        return new AggregateFunctionTemplate<Decimal128, Data>(std::forward<TArgs>(args)...);
    }
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
    if (which.idx == TypeIndex::Enum8) {
        return new AggregateFunctionTemplate<FirstType, Int8>(std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Enum16) {
        return new AggregateFunctionTemplate<FirstType, Int16>(std::forward<TArgs>(args)...);
    }
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
    if (which.idx == TypeIndex::Enum8) {
        return create_with_two_numeric_types_second<Int8, AggregateFunctionTemplate>(
                second_type, std::forward<TArgs>(args)...);
    }
    if (which.idx == TypeIndex::Enum16) {
        return create_with_two_numeric_types_second<Int16, AggregateFunctionTemplate>(
                second_type, std::forward<TArgs>(args)...);
    }
    return nullptr;
}

} // namespace  doris::vectorized
