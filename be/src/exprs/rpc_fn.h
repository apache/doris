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

#include <memory>
#include <string>

#include "common/status.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/function_service.pb.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

class RPCFn {
public:
    enum AggregationStep {
        INIT = 0,
        UPDATE = 1,
        MERGE = 2,
        REMOVE = 3,
        SERIALIZE = 4,
        GET_VALUE = 5,
        FINALIZE = 6,
        INVALID = 999,
    };

    RPCFn(RuntimeState* state, const TFunction& fn, int fn_ctx_id, bool is_agg);
    RPCFn(const TFunction& fn, bool is_agg);
    RPCFn(RuntimeState* state, const TFunction& fn, AggregationStep step, bool is_agg);
    ~RPCFn() {}
    template <typename T>
    T call(ExprContext* context, TupleRow* row, const std::vector<Expr*>& exprs);
    Status vec_call(FunctionContext* context, vectorized::Block& block,
                    const std::vector<size_t>& arguments, size_t result, size_t input_rows_count);
    bool avliable() { return _client != nullptr; }

private:
    Status call_internal(ExprContext* context, TupleRow* row, PFunctionCallResponse* response,
                         const std::vector<Expr*>& exprs);
    void cancel(const std::string& msg);

    std::shared_ptr<PFunctionService_Stub> _client;
    RuntimeState* _state;
    std::string _function_name;
    std::string _server_addr;
    std::string _signature;
    TFunction _fn;
    int _fn_ctx_id;
    bool _is_agg;
    AggregationStep _step = AggregationStep::INVALID;
};

template <typename T>
T RPCFn::call(ExprContext* context, TupleRow* row, const std::vector<Expr*>& exprs) {
    PFunctionCallResponse response;
    Status st = call_internal(context, row, &response, exprs);
    WARN_IF_ERROR(st, "call rpc udf error");
    if (!st.ok() || (response.result(0).has_null() && response.result(0).null_map(0))) {
        return T::null();
    }
    T res_val;
    // TODO(yangzhg) deal with udtf and udaf
    const PValues& result = response.result(0);
    if constexpr (std::is_same_v<T, TinyIntVal>) {
        DCHECK(result.type().id() == PGenericType::INT8);
        res_val.val = static_cast<int8_t>(result.int32_value(0));
    } else if constexpr (std::is_same_v<T, SmallIntVal>) {
        DCHECK(result.type().id() == PGenericType::INT16);
        res_val.val = static_cast<int16_t>(result.int32_value(0));
    } else if constexpr (std::is_same_v<T, IntVal>) {
        DCHECK(result.type().id() == PGenericType::INT32);
        res_val.val = result.int32_value(0);
    } else if constexpr (std::is_same_v<T, BigIntVal>) {
        DCHECK(result.type().id() == PGenericType::INT64);
        res_val.val = result.int64_value(0);
    } else if constexpr (std::is_same_v<T, FloatVal>) {
        DCHECK(result.type().id() == PGenericType::FLOAT);
        res_val.val = result.float_value(0);
    } else if constexpr (std::is_same_v<T, DoubleVal>) {
        DCHECK(result.type().id() == PGenericType::DOUBLE);
        res_val.val = result.double_value(0);
    } else if constexpr (std::is_same_v<T, StringVal>) {
        DCHECK(result.type().id() == PGenericType::STRING);
        auto* fn_ctx = context->fn_context(_fn_ctx_id);
        StringVal val(fn_ctx, result.string_value(0).size());
        res_val = val.copy_from(fn_ctx,
                                reinterpret_cast<const uint8_t*>(result.string_value(0).c_str()),
                                result.string_value(0).size());
    } else if constexpr (std::is_same_v<T, LargeIntVal>) {
        DCHECK(result.type().id() == PGenericType::INT128);
        memcpy(&(res_val.val), result.bytes_value(0).data(), sizeof(__int128_t));
    } else if constexpr (std::is_same_v<T, DateTimeVal>) {
        DCHECK(result.type().id() == PGenericType::DATE ||
               result.type().id() == PGenericType::DATETIME);
        DateTimeValue value;
        value.set_time(result.datetime_value(0).year(), result.datetime_value(0).month(),
                       result.datetime_value(0).day(), result.datetime_value(0).hour(),
                       result.datetime_value(0).minute(), result.datetime_value(0).second(),
                       result.datetime_value(0).microsecond());
        if (result.type().id() == PGenericType::DATE) {
            value.set_type(TimeType::TIME_DATE);
        } else if (result.type().id() == PGenericType::DATETIME) {
            if (result.datetime_value(0).has_year()) {
                value.set_type(TimeType::TIME_DATETIME);
            } else
                value.set_type(TimeType::TIME_TIME);
        }
        value.to_datetime_val(&res_val);
    } else if constexpr (std::is_same_v<T, DecimalV2Val>) {
        DCHECK(result.type().id() == PGenericType::DECIMAL128);
        memcpy(&(res_val.val), result.bytes_value(0).data(), sizeof(__int128_t));
    }
    return res_val;
}
} // namespace doris