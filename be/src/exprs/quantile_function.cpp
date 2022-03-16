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

#include "exprs/quantile_function.h"

#include "exprs/anyval_util.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "util/quantile_state.h"
#include "util/slice.h"
#include "util/string_parser.hpp"

namespace doris {

using doris_udf::DoubleVal;
using doris_udf::StringVal;
using doris_udf::FloatVal;

void QuantileStateFunctions::init() {}

void QuantileStateFunctions::quantile_state_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(QuantileState<double>);
    dst->ptr = (uint8_t*)new QuantileState<double>();
}

void QuantileStateFunctions::quantile_percent_prepare(FunctionContext* ctx,
                                                      FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    if (!ctx->is_arg_constant(1)) {
        std::stringstream ss;
        ss << "quantile_percent function's second arg must be constant.";
        ctx->set_error(ss.str().c_str());
        return;
    }
    float percentile_value = reinterpret_cast<const FloatVal*>(ctx->get_constant_arg(1))->val;
    if (percentile_value > 1 || percentile_value < 0) {
        std::stringstream error_msg;
        error_msg << "The percentile must between 0 and 1, but input is:"
                  << std::to_string(percentile_value);
        ctx->set_error(error_msg.str().c_str());
        return;
    }
}

void QuantileStateFunctions::to_quantile_state_prepare(FunctionContext* ctx,
                                                       FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    if (!ctx->is_arg_constant(1)) {
        // use default value, just return is ok.
        return;
    }
    float compression = reinterpret_cast<const FloatVal*>(ctx->get_constant_arg(1))->val;
    if (compression > QUANTILE_STATE_COMPRESSION_MAX ||
        compression < QUANTILE_STATE_COMPRESSION_MIN) {
        std::stringstream error_msg;
        error_msg << "The compression of to_quantile_state must between "
                  << QUANTILE_STATE_COMPRESSION_MIN << " and " << QUANTILE_STATE_COMPRESSION_MAX
                  << std::endl
                  << "but input is:" << std::to_string(compression);
        ctx->set_error(error_msg.str().c_str());
        return;
    }
}

static StringVal serialize(FunctionContext* ctx, QuantileState<double>* value) {
    StringVal result(ctx, value->get_serialized_size());
    value->serialize(result.ptr);
    return result;
}

StringVal QuantileStateFunctions::to_quantile_state(FunctionContext* ctx, const StringVal& src) {
    QuantileState<double> quantile_state;
    quantile_state.set_compression(QUANTILE_STATE_COMPRESSION_MIN);
    const AnyVal* digest_compression = ctx->get_constant_arg(1);
    if (digest_compression != nullptr) {
        // compression will be between 2048 and 10000, promised by `to_quantile_state_prepare`
        float compression = reinterpret_cast<const FloatVal*>(digest_compression)->val;
        quantile_state.set_compression(compression);
    }

    if (!src.is_null) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        double double_value = StringParser::string_to_float<double>(
                reinterpret_cast<char*>(src.ptr), src.len, &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            std::stringstream error_msg;
            error_msg << "The input: " << std::string(reinterpret_cast<char*>(src.ptr), src.len)
                      << " is not valid, to_quantile_state only support bigint value from 0 to "
                         "18446744073709551615 currently";
            ctx->set_error(error_msg.str().c_str());
            return StringVal::null();
        }
        quantile_state.add_value(double_value);
    }
    return serialize(ctx, &quantile_state);
}

void QuantileStateFunctions::quantile_union(FunctionContext* ctx, const StringVal& src,
                                            StringVal* dst) {
    if (src.is_null) {
        return;
    }
    auto dst_quantile = reinterpret_cast<QuantileState<double>*>(dst->ptr);
    if (src.len == 0) {
        dst_quantile->merge(*reinterpret_cast<QuantileState<double>*>(src.ptr));
    } else {
        QuantileState<double> state(Slice(src.ptr, src.len));
        dst_quantile->merge(state);
    }
}

DoubleVal QuantileStateFunctions::quantile_percent(FunctionContext* ctx, StringVal& src) {
    const AnyVal* percentile = ctx->get_constant_arg(1);
    if (percentile != nullptr) {
        // percentile_value will be between 0 and 1, promised by `quantile_percent_prepare`
        float percentile_value = reinterpret_cast<const FloatVal*>(percentile)->val;
        if (src.len == 0) {
            auto quantile_state = reinterpret_cast<QuantileState<double>*>(src.ptr);
            return {static_cast<double>(quantile_state->get_value_by_percentile(percentile_value))};
        } else {
            QuantileState<double> quantile_state(Slice(src.ptr, src.len));
            return {static_cast<double>(quantile_state.get_value_by_percentile(percentile_value))};
        }

    } else {
        std::stringstream error_msg;
        error_msg << "quantile_percent function's second argument must be constant. eg: "
                     "quantile_percent(col, 0.95)";
        ctx->set_error(error_msg.str().c_str());
    }
    return DoubleVal::null();
}

StringVal QuantileStateFunctions::quantile_state_serialize(FunctionContext* ctx,
                                                           const StringVal& src) {
    if (src.is_null) {
        return src;
    }
    auto tmp_ptr = reinterpret_cast<QuantileState<double>*>(src.ptr);
    StringVal result = serialize(ctx, tmp_ptr);
    delete tmp_ptr;
    return result;
}

} // namespace doris