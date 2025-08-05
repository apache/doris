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
#include "common/compile_check_begin.h"

struct creator_without_type {
    template <bool multi_arguments, bool f, typename T>
    using NullableT = std::conditional_t<multi_arguments, AggregateFunctionNullVariadicInline<T, f>,
                                         AggregateFunctionNullUnaryInline<T, f>>;

    template <typename AggregateFunctionTemplate>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable,
                                        const AggregateFunctionAttr& attr) {
        CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
        return create<AggregateFunctionTemplate>(argument_types, result_is_nullable, attr);
    }

    template <typename AggregateFunctionTemplate, typename... TArgs>
    static AggregateFunctionPtr create(const DataTypes& argument_types_,
                                       const bool result_is_nullable,
                                       const AggregateFunctionAttr& attr, TArgs&&... args) {
        std::unique_ptr<IAggregateFunction> result(std::make_unique<AggregateFunctionTemplate>(
                std::forward<TArgs>(args)..., remove_nullable(argument_types_)));
        if (have_nullable(argument_types_)) {
            std::visit(
                    [&](auto multi_arguments, auto result_is_nullable) {
                        result.reset(new NullableT<multi_arguments, result_is_nullable,
                                                   AggregateFunctionTemplate>(
                                result.release(), argument_types_, attr.is_window_function));
                    },
                    make_bool_variant(argument_types_.size() > 1),
                    make_bool_variant(result_is_nullable));
        }

        CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
        return AggregateFunctionPtr(result.release());
    }

    template <typename AggregateFunctionTemplate, typename... TArgs>
    static AggregateFunctionPtr create_multi_arguments(const DataTypes& argument_types_,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr,
                                                       TArgs&&... args) {
        std::unique_ptr<IAggregateFunction> result(std::make_unique<AggregateFunctionTemplate>(
                std::forward<TArgs>(args)..., remove_nullable(argument_types_)));
        if (have_nullable(argument_types_)) {
            std::visit(
                    [&](auto result_is_nullable) {
                        result.reset(
                                new NullableT<true, result_is_nullable, AggregateFunctionTemplate>(
                                        result.release(), argument_types_,
                                        attr.is_window_function));
                    },
                    make_bool_variant(result_is_nullable));
        }
        CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
        return AggregateFunctionPtr(result.release());
    }

    template <typename AggregateFunctionTemplate, typename... TArgs>
    static AggregateFunctionPtr create_unary_arguments(const DataTypes& argument_types_,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr,
                                                       TArgs&&... args) {
        std::unique_ptr<IAggregateFunction> result(std::make_unique<AggregateFunctionTemplate>(
                std::forward<TArgs>(args)..., remove_nullable(argument_types_)));
        if (have_nullable(argument_types_)) {
            std::visit(
                    [&](auto result_is_nullable) {
                        result.reset(
                                new NullableT<false, result_is_nullable, AggregateFunctionTemplate>(
                                        result.release(), argument_types_,
                                        attr.is_window_function));
                    },
                    make_bool_variant(result_is_nullable));
        }
        CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
        return AggregateFunctionPtr(result.release());
    }

    /// AggregateFunctionTemplate will handle the nullable arguments, no need to use
    /// AggregateFunctionNullVariadicInline/AggregateFunctionNullUnaryInline
    template <typename AggregateFunctionTemplate, typename... TArgs>
    static AggregateFunctionPtr create_ignore_nullable(const DataTypes& argument_types_,
                                                       const bool /*result_is_nullable*/,
                                                       const AggregateFunctionAttr& /*attr*/,
                                                       TArgs&&... args) {
        std::unique_ptr<IAggregateFunction> result = std::make_unique<AggregateFunctionTemplate>(
                std::forward<TArgs>(args)..., argument_types_);
        CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
        return AggregateFunctionPtr(result.release());
    }
};

template <template <PrimitiveType> class AggregateFunctionTemplate>
struct CurryDirect {
    template <PrimitiveType type>
    using T = AggregateFunctionTemplate<type>;
};
template <template <typename> class AggregateFunctionTemplate, template <PrimitiveType> class Data>
struct CurryData {
    template <PrimitiveType Type>
    using T = AggregateFunctionTemplate<Data<Type>>;
};
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
          template <PrimitiveType> class Impl>
struct CurryDataImpl {
    template <PrimitiveType Type>
    using T = AggregateFunctionTemplate<Data<Impl<Type>>>;
};
template <template <PrimitiveType, typename> class AggregateFunctionTemplate,
          template <PrimitiveType> class Data>
struct CurryDirectAndData {
    template <PrimitiveType Type>
    using T = AggregateFunctionTemplate<Type, Data<Type>>;
};

template <bool allow_integer, bool allow_float, bool allow_decimal, bool allow_stringlike,
          bool allow_datelike, bool allow_ip, int define_index = 0>
struct creator_with_type_base {
    template <typename Class, typename... TArgs>
    static AggregateFunctionPtr create_base(const DataTypes& argument_types,
                                            const bool result_is_nullable,
                                            const AggregateFunctionAttr& attr, TArgs&&... args) {
        auto create = [&]<PrimitiveType Ptype>() {
            return creator_without_type::create<typename Class::template T<Ptype>>(
                    argument_types, result_is_nullable, attr, std::forward<TArgs>(args)...);
        };
        if constexpr (allow_integer) {
            switch (argument_types[define_index]->get_primitive_type()) {
            case PrimitiveType::TYPE_BOOLEAN:
                return create.template operator()<PrimitiveType::TYPE_BOOLEAN>();
            case PrimitiveType::TYPE_TINYINT:
                return create.template operator()<PrimitiveType::TYPE_TINYINT>();
            case PrimitiveType::TYPE_SMALLINT:
                return create.template operator()<PrimitiveType::TYPE_SMALLINT>();
            case PrimitiveType::TYPE_INT:
                return create.template operator()<PrimitiveType::TYPE_INT>();
            case PrimitiveType::TYPE_BIGINT:
                return create.template operator()<PrimitiveType::TYPE_BIGINT>();
            case PrimitiveType::TYPE_LARGEINT:
                return create.template operator()<PrimitiveType::TYPE_LARGEINT>();
            default:
                break;
            }
        }
        if constexpr (allow_float) {
            switch (argument_types[define_index]->get_primitive_type()) {
            case PrimitiveType::TYPE_FLOAT:
                return create.template operator()<PrimitiveType::TYPE_FLOAT>();
            case PrimitiveType::TYPE_DOUBLE:
                return create.template operator()<PrimitiveType::TYPE_DOUBLE>();
            default:
                break;
            }
        }
        if constexpr (allow_decimal) {
            switch (argument_types[define_index]->get_primitive_type()) {
            case PrimitiveType::TYPE_DECIMAL32:
                return create.template operator()<PrimitiveType::TYPE_DECIMAL32>();
            case PrimitiveType::TYPE_DECIMAL64:
                return create.template operator()<PrimitiveType::TYPE_DECIMAL64>();
            case PrimitiveType::TYPE_DECIMALV2:
                return create.template operator()<PrimitiveType::TYPE_DECIMALV2>();
            case PrimitiveType::TYPE_DECIMAL128I:
                return create.template operator()<PrimitiveType::TYPE_DECIMAL128I>();
            case PrimitiveType::TYPE_DECIMAL256:
                return create.template operator()<PrimitiveType::TYPE_DECIMAL256>();
            default:
                break;
            }
        }

        if constexpr (allow_stringlike) {
            switch (argument_types[define_index]->get_primitive_type()) {
            case PrimitiveType::TYPE_CHAR:
            case PrimitiveType::TYPE_VARCHAR:
            case PrimitiveType::TYPE_STRING:
            case PrimitiveType::TYPE_JSONB:
                return create.template operator()<PrimitiveType::TYPE_VARCHAR>();
            default:
                break;
            }
        }

        if constexpr (allow_datelike) {
            switch (argument_types[define_index]->get_primitive_type()) {
            case PrimitiveType::TYPE_DATE:
                return create.template operator()<PrimitiveType::TYPE_DATE>();
            case PrimitiveType::TYPE_DATETIME:
                return create.template operator()<PrimitiveType::TYPE_DATETIME>();
            case PrimitiveType::TYPE_DATEV2:
                return create.template operator()<PrimitiveType::TYPE_DATEV2>();
            case PrimitiveType::TYPE_DATETIMEV2:
                return create.template operator()<PrimitiveType::TYPE_DATETIMEV2>();
            default:
                break;
            }
        }

        if constexpr (allow_ip) {
            switch (argument_types[define_index]->get_primitive_type()) {
            case PrimitiveType::TYPE_IPV4:
                return create.template operator()<PrimitiveType::TYPE_IPV4>();
            case PrimitiveType::TYPE_IPV6:
                return create.template operator()<PrimitiveType::TYPE_IPV6>();
            default:
                break;
            }
        }

        return nullptr;
    }

    template <template <PrimitiveType> class AggregateFunctionTemplate>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable,
                                        const AggregateFunctionAttr& attr) {
        return create_base<CurryDirect<AggregateFunctionTemplate>>(argument_types,
                                                                   result_is_nullable, attr);
    }

    template <template <PrimitiveType> class AggregateFunctionTemplate, typename... TArgs>
    static AggregateFunctionPtr create(TArgs&&... args) {
        return create_base<CurryDirect<AggregateFunctionTemplate>>(std::forward<TArgs>(args)...);
    }

    template <template <typename> class AggregateFunctionTemplate,
              template <PrimitiveType> class Data>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable,
                                        const AggregateFunctionAttr& attr) {
        return create_base<CurryData<AggregateFunctionTemplate, Data>>(argument_types,
                                                                       result_is_nullable, attr);
    }

    template <template <typename> class AggregateFunctionTemplate,
              template <PrimitiveType> class Data, typename... TArgs>
    static AggregateFunctionPtr create(TArgs&&... args) {
        return create_base<CurryData<AggregateFunctionTemplate, Data>>(
                std::forward<TArgs>(args)...);
    }

    template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
              template <PrimitiveType> class Impl>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable,
                                        const AggregateFunctionAttr& attr) {
        return create_base<CurryDataImpl<AggregateFunctionTemplate, Data, Impl>>(
                argument_types, result_is_nullable, attr);
    }

    template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
              template <PrimitiveType> class Impl, typename... TArgs>
    static AggregateFunctionPtr create(TArgs&&... args) {
        return create_base<CurryDataImpl<AggregateFunctionTemplate, Data, Impl>>(
                std::forward<TArgs>(args)...);
    }

    template <template <PrimitiveType, typename> class AggregateFunctionTemplate,
              template <PrimitiveType> class Data>
    static AggregateFunctionPtr creator(const std::string& name, const DataTypes& argument_types,
                                        const bool result_is_nullable,
                                        const AggregateFunctionAttr& attr) {
        return create_base<CurryDirectAndData<AggregateFunctionTemplate, Data>>(
                argument_types, result_is_nullable, attr);
    }

    template <template <PrimitiveType, typename> class AggregateFunctionTemplate,
              template <PrimitiveType> class Data, typename... TArgs>
    static AggregateFunctionPtr create(TArgs&&... args) {
        return create_base<CurryDirectAndData<AggregateFunctionTemplate, Data>>(
                std::forward<TArgs>(args)...);
    }
};
template <int define_index>
using creator_with_integer_type_with_index =
        creator_with_type_base<true, false, false, false, false, false, define_index>;
using creator_with_integer_type = creator_with_integer_type_with_index<0>;
using creator_with_numeric_type = creator_with_type_base<true, true, false, false, false, false>;
using creator_with_decimal_type = creator_with_type_base<false, false, true, false, false, false>;
using creator_with_type = creator_with_type_base<true, true, true, false, false, false>;
using creator_with_any = creator_with_type_base<true, true, true, true, true, true>;

} // namespace  doris::vectorized

#include "common/compile_check_end.h"
