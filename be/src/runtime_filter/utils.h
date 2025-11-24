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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/internal_service.pb.h>

#include "runtime/large_int_value.h"
#include "runtime/types.h"
#include "runtime_filter/runtime_filter_definitions.h"
#include "vec/core/extended_types.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
#include "common/compile_check_begin.h"

template <typename T>
auto get_convertor() {
    if constexpr (std::is_same_v<T, bool>) {
        return [](PColumnValue* value, const T& data) { value->set_boolval(data); };
    } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                         std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t> ||
                         std::is_same_v<T, vectorized::Decimal32>) {
        return [](PColumnValue* value, const T& data) { value->set_intval(data); };
    } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, vectorized::Decimal64>) {
        return [](PColumnValue* value, const T& data) { value->set_longval(data); };
    } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
        return [](PColumnValue* value, const T& data) { value->set_doubleval(data); };
    } else if constexpr (std::is_same_v<T, int128_t> || std::is_same_v<T, uint128_t> ||
                         std::is_same_v<T, vectorized::Decimal128V3>) {
        return [](PColumnValue* value, const T& data) {
            value->set_stringval(LargeIntValue::to_string(data));
        };
    } else if constexpr (std::is_same_v<T, vectorized::Decimal256>) {
        return [](PColumnValue* value, const T& data) {
            value->set_stringval(wide::to_string(wide::Int256(data)));
        };
    } else if constexpr (std::is_same_v<T, std::string>) {
        return [](PColumnValue* value, const T& data) { value->set_stringval(data); };
    } else if constexpr (std::is_same_v<T, StringRef> ||
                         std::is_same_v<T, vectorized::Decimal128V2> ||
                         std::is_same_v<T, DecimalV2Value>) {
        return [](PColumnValue* value, const T& data) { value->set_stringval(data.to_string()); };
    } else if constexpr (std::is_same_v<T, VecDateTimeValue>) {
        return [](PColumnValue* value, const T& data) {
            char convert_buffer[30];
            data.to_string(convert_buffer);
            value->set_stringval(convert_buffer);
        };
    } else if constexpr (std::is_same_v<T, DateV2Value<DateV2ValueType>>) {
        return [](PColumnValue* value, const T& data) {
            value->set_intval(data.to_date_int_val());
        };
    } else if constexpr (std::is_same_v<T, DateV2Value<DateTimeV2ValueType>>) {
        return [](PColumnValue* value, const T& data) {
            value->set_longval(data.to_date_int_val());
        };
    } else {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "runtime filter data convertor meet invalid type {}", typeid(T).name());
        return [](PColumnValue* value, const T& data) {};
    }
}

std::string filter_type_to_string(RuntimeFilterType type);

RuntimeFilterType get_runtime_filter_type(const TRuntimeFilterDesc* desc);

// PFilterType -> RuntimeFilterType
RuntimeFilterType get_type(int filter_type);
// RuntimeFilterType -> PFilterType
PFilterType get_type(RuntimeFilterType type);

Status create_literal(const vectorized::DataTypePtr& type, const void* data,
                      vectorized::VExprSPtr& expr);

Status create_vbin_predicate(const vectorized::DataTypePtr& type, TExprOpcode::type opcode,
                             vectorized::VExprSPtr& expr, TExprNode* tnode, bool contain_null);

template <typename T>
std::string states_to_string(std::vector<typename T::State> assumed_states) {
    std::vector<std::string> strs;
    for (auto state : assumed_states) {
        strs.push_back(T::to_string(state));
    }
    return fmt::format("[{}]", fmt::join(strs, ", "));
}

template <typename T>
bool check_state_impl(typename T::State real_state, std::vector<typename T::State> assumed_states) {
    bool matched = false;
    for (auto state : assumed_states) {
        if (real_state == state) {
            matched = true;
            break;
        }
    }
    return matched;
}

#include "common/compile_check_end.h"
} // namespace doris
