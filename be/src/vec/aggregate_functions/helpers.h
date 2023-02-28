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
#include "vec/utils/template_helpers.hpp"

// TODO: Should we support decimal in numeric types?
#define FOR_INTEGER_TYPES(M) \
    M(UInt8)                 \
    M(Int8)                  \
    M(Int16)                 \
    M(Int32)                 \
    M(Int64)                 \
    M(Int128)

#define FOR_FLOAT_TYPES(M) \
    M(Float32)             \
    M(Float64)

#define FOR_NUMERIC_TYPES(M) \
    FOR_INTEGER_TYPES(M)     \
    FOR_FLOAT_TYPES(M)

#define FOR_DECIMAL_TYPES(M) \
    M(Decimal32)             \
    M(Decimal64)             \
    M(Decimal128)            \
    M(Decimal128I)

namespace doris::vectorized {

/** Create an aggregate function with a numeric type in the template parameter, depending on the type of the argument.
  */
template <template <typename> class AggregateFunctionTemplate, typename Type>
struct BuilderDirect {
    using T = AggregateFunctionTemplate<Type>;
};
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
          typename Type>
struct BuilderData {
    using T = AggregateFunctionTemplate<Data<Type>>;
};
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
          template <typename> class Impl, typename Type>
struct BuilderDataImpl {
    using T = AggregateFunctionTemplate<Data<Impl<Type>>>;
};
template <template <typename, typename> class AggregateFunctionTemplate,
          template <typename> class Data, typename Type>
struct BuilderDirectAndData {
    using T = AggregateFunctionTemplate<Type, Data<Type>>;
};

template <template <typename> class AggregateFunctionTemplate>
struct CurryDirect {
    template <typename Type>
    using Builder = BuilderDirect<AggregateFunctionTemplate, Type>;
};
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
struct CurryData {
    template <typename Type>
    using Builder = BuilderData<AggregateFunctionTemplate, Data, Type>;
};
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
          template <typename> class Impl>
struct CurryDataImpl {
    template <typename Type>
    using Builder = BuilderDataImpl<AggregateFunctionTemplate, Data, Impl, Type>;
};
template <template <typename, typename> class AggregateFunctionTemplate,
          template <typename> class Data>
struct CurryDirectAndData {
    template <typename Type>
    using Builder = BuilderDirectAndData<AggregateFunctionTemplate, Data, Type>;
};

template <bool allow_integer, bool allow_float, bool allow_decimal, int define_index = 0>
struct creator_with_type_base {
    template <typename Class, typename... TArgs>
    static IAggregateFunction* create_base(const bool result_is_nullable,
                                           const DataTypes& argument_types, TArgs&&... args) {
        WhichDataType which(remove_nullable(argument_types[define_index]));
#define DISPATCH(TYPE)                                                                            \
    if (which.idx == TypeIndex::TYPE) {                                                           \
        using T = typename Class::template Builder<TYPE>::T;                                      \
        if (have_nullable(argument_types)) {                                                      \
            IAggregateFunction* result = nullptr;                                                 \
            if (argument_types.size() > 1) {                                                      \
                std::visit(                                                                       \
                        [&](auto result_is_nullable) {                                            \
                            result = new AggregateFunctionNullVariadicInline<T,                   \
                                                                             result_is_nullable>( \
                                    new T(std::forward<TArgs>(args)...,                           \
                                          remove_nullable(argument_types)),                       \
                                    argument_types);                                              \
                        },                                                                        \
                        make_bool_variant(result_is_nullable));                                   \
            } else {                                                                              \
                std::visit(                                                                       \
                        [&](auto result_is_nullable) {                                            \
                            result = new AggregateFunctionNullUnaryInline<T, result_is_nullable>( \
                                    new T(std::forward<TArgs>(args)...,                           \
                                          remove_nullable(argument_types)),                       \
                                    argument_types);                                              \
                        },                                                                        \
                        make_bool_variant(result_is_nullable));                                   \
            }                                                                                     \
            return result;                                                                        \
        } else {                                                                                  \
            return new T(std::forward<TArgs>(args)..., argument_types);                           \
        }                                                                                         \
    }

        if constexpr (allow_integer) {
            FOR_INTEGER_TYPES(DISPATCH);
        }
        if constexpr (allow_float) {
            FOR_FLOAT_TYPES(DISPATCH);
        }
        if constexpr (allow_decimal) {
            FOR_DECIMAL_TYPES(DISPATCH);
        }
#undef DISPATCH
        return nullptr;
    }

    template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
    static IAggregateFunction* create(TArgs&&... args) {
        return create_base<CurryDirect<AggregateFunctionTemplate>>(std::forward<TArgs>(args)...);
    }

    template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
              typename... TArgs>
    static IAggregateFunction* create(TArgs&&... args) {
        return create_base<CurryData<AggregateFunctionTemplate, Data>>(
                std::forward<TArgs>(args)...);
    }

    template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
              template <typename> class Impl, typename... TArgs>
    static IAggregateFunction* create(TArgs&&... args) {
        return create_base<CurryDataImpl<AggregateFunctionTemplate, Data, Impl>>(
                std::forward<TArgs>(args)...);
    }

    template <template <typename, typename> class AggregateFunctionTemplate,
              template <typename> class Data, typename... TArgs>
    static IAggregateFunction* create(TArgs&&... args) {
        return create_base<CurryDirectAndData<AggregateFunctionTemplate, Data>>(
                std::forward<TArgs>(args)...);
    }
};

using creator_with_integer_type = creator_with_type_base<true, false, false>;
using creator_with_numeric_type = creator_with_type_base<true, true, false>;
using creator_with_decimal_type = creator_with_type_base<false, false, true>;
using creator_with_type = creator_with_type_base<true, true, true>;

struct creator_without_type {
    template <typename AggregateFunctionTemplate, typename... TArgs>
    static IAggregateFunction* create(const bool result_is_nullable,
                                      const DataTypes& argument_types, TArgs&&... args) {
        if (have_nullable(argument_types)) {
            IAggregateFunction* result = nullptr;
            if (argument_types.size() > 1) {
                std::visit(
                        [&](auto result_is_nullable) {
                            result = new AggregateFunctionNullVariadicInline<
                                    AggregateFunctionTemplate, result_is_nullable>(
                                    new AggregateFunctionTemplate(std::forward<TArgs>(args)...,
                                                                  remove_nullable(argument_types)),
                                    argument_types);
                        },
                        make_bool_variant(result_is_nullable));
            } else {
                std::visit(
                        [&](auto result_is_nullable) {
                            result = new AggregateFunctionNullUnaryInline<AggregateFunctionTemplate,
                                                                          result_is_nullable>(
                                    new AggregateFunctionTemplate(std::forward<TArgs>(args)...,
                                                                  remove_nullable(argument_types)),
                                    argument_types);
                        },
                        make_bool_variant(result_is_nullable));
            }
            return result;
        } else {
            return new AggregateFunctionTemplate(std::forward<TArgs>(args)..., argument_types);
        }
    }
};
} // namespace  doris::vectorized
