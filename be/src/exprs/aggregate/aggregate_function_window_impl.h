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

#include <string>
#include <variant>

#include "core/data_type/data_type.h"
#include "exec/common/template_helpers.hpp"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/aggregate_function_window.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

// All window-path Data types (LeadLagData, FirstLastData, NthValueData) no longer have
// ColVecType template parameter. Direct instantiation with only (result_is_nullable, arg_is_nullable).
// ARG_IGNORE_NULL is a compile-time bool for first_value/last_value ignore-null variants.
#define CREATE_WINDOW_FUNCTION_DIRECT(CREATE_FUNCTION_NAME, FUNCTION_DATA, FUNCTION_IMPL,     \
                                      ARG_IGNORE_NULL)                                        \
    AggregateFunctionPtr CREATE_FUNCTION_NAME(                                                \
            const std::string& name, const DataTypes& argument_types,                         \
            const DataTypePtr& result_type, const bool result_is_nullable,                    \
            const AggregateFunctionAttr& attr) {                                              \
        const bool arg_is_nullable = argument_types[0]->is_nullable();                        \
        AggregateFunctionPtr res = nullptr;                                                   \
                                                                                              \
        std::visit(                                                                           \
                [&](auto result_is_nullable, auto arg_is_nullable) {                          \
                    res = std::make_shared<WindowFunctionData<                                \
                            FUNCTION_IMPL<FUNCTION_DATA<result_is_nullable, arg_is_nullable>, \
                                          ARG_IGNORE_NULL>>>(argument_types);                 \
                },                                                                            \
                make_bool_variant(result_is_nullable), make_bool_variant(arg_is_nullable));   \
        if (!res) {                                                                           \
            LOG(WARNING) << " failed in  create_aggregate_function_" << name                  \
                         << " and type is: " << argument_types[0]->get_name();                \
        }                                                                                     \
        return res;                                                                           \
    }

} // namespace doris
