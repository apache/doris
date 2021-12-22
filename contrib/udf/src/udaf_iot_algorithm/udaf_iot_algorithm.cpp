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

#include "udaf_iot_algorithm.h"

namespace doris_udf {

    struct TsKeyValueState {
        BigIntVal ts;
        DoubleVal value;
    };

// ---------------------------------------------------------------------------
// This is a function of the iot data processing algorithm.
// ---------------------------------------------------------------------------
    void IotFunctions::init_iot_functions(FunctionContext *context, StringVal *dst) {
        dst->is_null = true;
    }

    void IotFunctions::iot_first_update(FunctionContext *context,
                                        const BigIntVal &ts,
                                        const DoubleVal &value,
                                        StringVal *dst) {
        if (ts.is_null || value.is_null) {
            return;
        }
        if (dst->is_null) {
            int str_len = sizeof(TsKeyValueState);
            dst->is_null = false;
            dst->ptr = context->allocate(str_len);
            dst->len = str_len;
            auto *dst_tk = reinterpret_cast<TsKeyValueState *>(dst->ptr);
            dst_tk->ts = ts;
            dst_tk->value = value;
        } else {
            auto *dst_tk = reinterpret_cast<TsKeyValueState *>(dst->ptr);
            BigIntVal dst_tk_ts = dst_tk->ts;
            if (ts.val < dst_tk_ts.val) {
                dst_tk->ts = ts;
                dst_tk->value = value;
            }
        }
    }

    void IotFunctions::iot_last_update(FunctionContext *context,
                                       const BigIntVal &ts,
                                       const DoubleVal &value,
                                       StringVal *dst) {
        if (ts.is_null || value.is_null) {
            return;
        }
        if (dst->is_null) {
            int str_len = sizeof(TsKeyValueState);
            dst->is_null = false;
            dst->ptr = context->allocate(str_len);
            dst->len = str_len;
            auto *dst_tk = reinterpret_cast<TsKeyValueState *>(dst->ptr);
            dst_tk->ts = ts;
            dst_tk->value = value;
        } else {
            auto *dst_tk = reinterpret_cast<TsKeyValueState *>(dst->ptr);
            BigIntVal dst_tk_ts = dst_tk->ts;
            if (ts.val > dst_tk_ts.val) {
                dst_tk->ts = ts;
                dst_tk->value = value;
            }
        }
    }

    void IotFunctions::iot_first_merge(FunctionContext *context,
                                       const StringVal &src,
                                       StringVal *dst) {
        const TsKeyValueState *src_struct = reinterpret_cast<const TsKeyValueState *>(src.ptr);
        if (src.is_null) {
            return;
        }
        if (dst->is_null) {
            int str_len = sizeof(TsKeyValueState);
            dst->is_null = false;
            dst->ptr = context->allocate(str_len);
            dst->len = str_len;
            auto *dst_tk = reinterpret_cast<TsKeyValueState *>(dst->ptr);
            dst_tk->ts = src_struct->ts;
            dst_tk->value = src_struct->value;
        } else {
            auto *dst_tk = reinterpret_cast<TsKeyValueState *>(dst->ptr);
            BigIntVal src_struct_ts = src_struct->ts;
            BigIntVal dst_tk_ts = dst_tk->ts;
            if (src_struct_ts.val < dst_tk_ts.val) {
                dst_tk->ts = src_struct->ts;
                dst_tk->value = src_struct->value;
            }
        }
    }

    void IotFunctions::iot_last_merge(FunctionContext *context,
                                      const StringVal &src,
                                      StringVal *dst) {
        const TsKeyValueState *src_struct = reinterpret_cast<const TsKeyValueState *>(src.ptr);
        if (src.is_null) {
            return;
        }
        if (dst->is_null) {
            int str_len = sizeof(TsKeyValueState);
            dst->is_null = false;
            dst->ptr = context->allocate(str_len);
            dst->len = str_len;
            auto *dst_tk = reinterpret_cast<TsKeyValueState *>(dst->ptr);
            dst_tk->ts = src_struct->ts;
            dst_tk->value = src_struct->value;
        } else {
            auto *dst_tk = reinterpret_cast<TsKeyValueState *>(dst->ptr);
            BigIntVal src_struct_ts = src_struct->ts;
            BigIntVal dst_tk_ts = dst_tk->ts;
            if (src_struct_ts.val > dst_tk_ts.val) {
                dst_tk->ts = src_struct->ts;
                dst_tk->value = src_struct->value;
            }
        }
    }

    StringVal IotFunctions::serialize(FunctionContext *ctx, const StringVal &src) {
        StringVal result(ctx, src.len);
        memcpy(result.ptr, src.ptr, src.len);
        delete (TsKeyValueState *) src.ptr;
        return result;
    }

    DoubleVal IotFunctions::finalize(FunctionContext *context,
                                     const StringVal &src) {
        if (src.is_null) {
            return DoubleVal::null();
        }
        const TsKeyValueState *src_struct = reinterpret_cast<const TsKeyValueState *>(src.ptr);
        DoubleVal result = src_struct->value;
        context->free(src.ptr);
        return Double();
    }
}
