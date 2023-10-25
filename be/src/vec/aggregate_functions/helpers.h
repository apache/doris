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
    M(Decimal128I)           \
    M(Decimal256)

/** If the serialized type is not the default type(string),
 * aggregation function need to override these functions:
 * 1. serialize_to_column
 * 2. streaming_agg_serialize_to_column
 * 3. deserialize_and_merge_vec
 * 4. deserialize_and_merge_vec_selected
 * 5. serialize_without_key_to_column
 * 6. deserialize_and_merge_from_column
 */
#define CHECK_AGG_FUNCTION_SERIALIZED_TYPE(FunctionTemplate)                                       \
    do {                                                                                           \
        constexpr bool _is_new_serialized_type =                                                   \
                !std::is_same_v<decltype(&FunctionTemplate::get_serialized_type),                  \
                                decltype(&IAggregateFunction::get_serialized_type)>;               \
        if constexpr (_is_new_serialized_type) {                                                   \
            static_assert(!std::is_same_v<decltype(&FunctionTemplate::serialize_to_column),        \
                                          decltype(&IAggregateFunctionHelper<                      \
                                                   FunctionTemplate>::serialize_to_column)>,       \
                          "need to override serialize_to_column");                                 \
            static_assert(                                                                         \
                    !std::is_same_v<                                                               \
                            decltype(&FunctionTemplate::streaming_agg_serialize_to_column),        \
                            decltype(&IAggregateFunction::streaming_agg_serialize_to_column)>,     \
                    "need to override "                                                            \
                    "streaming_agg_serialize_to_column");                                          \
            static_assert(!std::is_same_v<decltype(&FunctionTemplate::deserialize_and_merge_vec),  \
                                          decltype(&IAggregateFunctionHelper<                      \
                                                   FunctionTemplate>::deserialize_and_merge_vec)>, \
                          "need to override deserialize_and_merge_vec");                           \
            static_assert(                                                                         \
                    !std::is_same_v<                                                               \
                            decltype(&FunctionTemplate::deserialize_and_merge_vec_selected),       \
                            decltype(&IAggregateFunctionHelper<                                    \
                                     FunctionTemplate>::deserialize_and_merge_vec_selected)>,      \
                    "need to override "                                                            \
                    "deserialize_and_merge_vec_selected");                                         \
            static_assert(                                                                         \
                    !std::is_same_v<decltype(&FunctionTemplate::serialize_without_key_to_column),  \
                                    decltype(&IAggregateFunctionHelper<                            \
                                             FunctionTemplate>::serialize_without_key_to_column)>, \
                    "need to override serialize_without_key_to_column");                           \
            static_assert(!std::is_same_v<                                                         \
                                  decltype(&FunctionTemplate::deserialize_and_merge_from_column),  \
                                  decltype(&IAggregateFunctionHelper<                              \
                                           FunctionTemplate>::deserialize_and_merge_from_column)>, \
                          "need to override "                                                      \
                          "deserialize_and_merge_from_column");                                    \
        }                                                                                          \
    } while (false)

namespace doris::vectorized {

struct creator_without_type {
    template <bool multi_arguments, bool f, typename T>
    using NullableT = std::conditional_t<multi_arguments, AggregateFunctionNullVariadicInline<T, f>,
                                         AggregateFunctionNullUnaryInline<T, f>>;

    template <typename AggregateFunctionTemplate>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable) {
        CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
        return create<AggregateFunctionTemplate>(argument_types, result_is_nullable);
    }

    template <typename AggregateFunctionTemplate, typename... TArgs>
    static AggregateFunctionPtr create(const DataTypes& argument_types,
                                       const bool result_is_nullable, TArgs&&... args) {
        IAggregateFunction* result(new AggregateFunctionTemplate(std::forward<TArgs>(args)...,
                                                                 remove_nullable(argument_types)));
        if (have_nullable(argument_types)) {
            std::visit(
                    [&](auto multi_arguments, auto result_is_nullable) {
                        result = new NullableT<multi_arguments, result_is_nullable,
                                               AggregateFunctionTemplate>(result, argument_types);
                    },
                    make_bool_variant(argument_types.size() > 1),
                    make_bool_variant(result_is_nullable));
        }

        CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
        return AggregateFunctionPtr(result);
    }

    /// AggregateFunctionTemplate will handle the nullable arguments, no need to use
    /// AggregateFunctionNullVariadicInline/AggregateFunctionNullUnaryInline
    template <typename AggregateFunctionTemplate, typename... TArgs>
    static AggregateFunctionPtr create_ignore_nullable(const DataTypes& argument_types,
                                                       const bool /*result_is_nullable*/,
                                                       TArgs&&... args) {
        IAggregateFunction* result(
                new AggregateFunctionTemplate(std::forward<TArgs>(args)..., argument_types));
        CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
        return AggregateFunctionPtr(result);
    }
};

template <template <typename> class AggregateFunctionTemplate>
struct CurryDirect {
    template <typename Type>
    using T = AggregateFunctionTemplate<Type>;
};
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
struct CurryData {
    template <typename Type>
    using T = AggregateFunctionTemplate<Data<Type>>;
};
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
          template <typename> class Impl>
struct CurryDataImpl {
    template <typename Type>
    using T = AggregateFunctionTemplate<Data<Impl<Type>>>;
};
template <template <typename, typename> class AggregateFunctionTemplate,
          template <typename> class Data>
struct CurryDirectAndData {
    template <typename Type>
    using T = AggregateFunctionTemplate<Type, Data<Type>>;
};

template <bool allow_integer, bool allow_float, bool allow_decimal, int define_index = 0>
struct creator_with_type_base {
    template <typename Class, typename... TArgs>
    static AggregateFunctionPtr create_base(const DataTypes& argument_types,
                                            const bool result_is_nullable, TArgs&&... args) {
        WhichDataType which(remove_nullable(argument_types[define_index]));
#define DISPATCH(TYPE)                                                             \
    if (which.idx == TypeIndex::TYPE) {                                            \
        return creator_without_type::create<typename Class::template T<TYPE>>(     \
                argument_types, result_is_nullable, std::forward<TArgs>(args)...); \
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

    template <template <typename> class AggregateFunctionTemplate>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable) {
        return create_base<CurryDirect<AggregateFunctionTemplate>>(argument_types,
                                                                   result_is_nullable);
    }

    template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
    static AggregateFunctionPtr create(TArgs&&... args) {
        return create_base<CurryDirect<AggregateFunctionTemplate>>(std::forward<TArgs>(args)...);
    }

    template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable) {
        return create_base<CurryData<AggregateFunctionTemplate, Data>>(argument_types,
                                                                       result_is_nullable);
    }

    template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
              typename... TArgs>
    static AggregateFunctionPtr create(TArgs&&... args) {
        return create_base<CurryData<AggregateFunctionTemplate, Data>>(
                std::forward<TArgs>(args)...);
    }

    template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
              template <typename> class Impl>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable) {
        return create_base<CurryDataImpl<AggregateFunctionTemplate, Data, Impl>>(
                argument_types, result_is_nullable);
    }

    template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
              template <typename> class Impl, typename... TArgs>
    static AggregateFunctionPtr create(TArgs&&... args) {
        return create_base<CurryDataImpl<AggregateFunctionTemplate, Data, Impl>>(
                std::forward<TArgs>(args)...);
    }

    template <template <typename, typename> class AggregateFunctionTemplate,
              template <typename> class Data>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable) {
        return create_base<CurryDirectAndData<AggregateFunctionTemplate, Data>>(argument_types,
                                                                                result_is_nullable);
    }

    template <template <typename, typename> class AggregateFunctionTemplate,
              template <typename> class Data, typename... TArgs>
    static AggregateFunctionPtr create(TArgs&&... args) {
        return create_base<CurryDirectAndData<AggregateFunctionTemplate, Data>>(
                std::forward<TArgs>(args)...);
    }
};

using creator_with_integer_type = creator_with_type_base<true, false, false>;
using creator_with_numeric_type = creator_with_type_base<true, true, false>;
using creator_with_decimal_type = creator_with_type_base<false, false, true>;
using creator_with_type = creator_with_type_base<true, true, true>;

} // namespace  doris::vectorized
