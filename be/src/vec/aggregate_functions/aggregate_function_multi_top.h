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

#pragma once

#include "vec/core/types.h"

namespace doris::vectorized {

class AggregateFunctionMultiTop {
public:
    static inline constexpr UInt64 TOP_K_MAX_SIZE = 0xFFFFFF;
};

template <typename NestFunction, bool result_is_nullable>
class AggregateFunctionNullVariadicInline;

#define REGISTER_AGGREGATE_FUNCTION(Name) \
    template <typename T>                 \
    struct is_aggregate_function_##Name : std::is_base_of<AggregateFunctionMultiTop, T> {}

REGISTER_AGGREGATE_FUNCTION(MultiTopN);
REGISTER_AGGREGATE_FUNCTION(MultiTopSum);

template <typename T>
struct is_aggregate_function_multi_top
        : std::bool_constant<is_aggregate_function_MultiTopN<T>::value ||
                             is_aggregate_function_MultiTopSum<T>::value> {};

template <typename T>
struct is_aggregate_function_multi_top_with_null_variadic_inline : std::false_type {};

template <typename NestFunction, bool result_is_nullable>
struct is_aggregate_function_multi_top_with_null_variadic_inline<
        AggregateFunctionNullVariadicInline<NestFunction, result_is_nullable>>
        : is_aggregate_function_multi_top<NestFunction> {};

} // namespace doris::vectorized
