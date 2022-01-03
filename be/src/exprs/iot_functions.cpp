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

#include "exprs/iot_functions.h"

#include "common/logging.h"

namespace doris {

struct IoTFirstLastState {
    BigIntVal ts;
    DoubleVal val;

    IoTFirstLastState(uint8_t* ptr) {
        memcpy(&ts.val, ptr, sizeof(int64_t));
        memcpy(&val.val, ptr + sizeof(int64_t), sizeof(double));
    }

    void serialize(StringVal* result) {
        uint8_t* ptr = result->ptr;
        memcpy(ptr, &(ts.val), sizeof(int64_t));
        ptr += sizeof(int64_t);
        memcpy(ptr, &(val.val), sizeof(double));
    }
};

void IoTFunctions::init() {}

void IoTFunctions::_init_iot_first_last_state(FunctionContext* context,
        const BigIntVal& ts, const DoubleVal& val,
        StringVal* dst) {
    size_t str_len = sizeof(IoTFirstLastState);
    dst->is_null = false;
    dst->ptr = context->allocate(str_len);
    dst->len = str_len;
    auto *dst_data = reinterpret_cast<IoTFirstLastState*>(dst->ptr);
    dst_data->ts = ts;
    dst_data->val = val; 
}

void IoTFunctions::iot_first_update(FunctionContext* context, const BigIntVal& ts, const DoubleVal& val,
        StringVal* dst) {
    if (ts.is_null || val.is_null) {
        return;
    }

    if (dst->is_null) {
        // first update
        _init_iot_first_last_state(context, ts, val, dst);
    } else {
        auto* dst_data = reinterpret_cast<IoTFirstLastState*>(dst->ptr);
        if (val.val < dst_data->val.val) {
            dst_data->ts = ts;
            dst_data->val = val; 
        }
    }
}

void IoTFunctions::iot_last_update(FunctionContext* context, const BigIntVal& ts, const DoubleVal& val,
        StringVal* dst) {
    if (ts.is_null || val.is_null) {
        return;
    }

    if (dst->is_null) {
        // first update
        _init_iot_first_last_state(context, ts, val, dst);
    } else {
        auto* dst_data = reinterpret_cast<IoTFirstLastState*>(dst->ptr);
        if (val.val > dst_data->val.val) {
            dst_data->ts = ts;
            dst_data->val = val; 
        }
    }
}

void IoTFunctions::iot_first_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    IoTFirstLastState src_data(src.ptr);
    if (dst->is_null) {
        // first update
        _init_iot_first_last_state(ctx, src_data.ts, src_data.val, dst);
    } else {
        auto* dst_data = reinterpret_cast<IoTFirstLastState*>(dst->ptr);
        if (src_data.ts.val < dst_data->ts.val) {
            dst_data->ts = src_data.ts;
            dst_data->val = src_data.val;
        }
    }
}

void IoTFunctions::iot_last_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    IoTFirstLastState src_data(src.ptr);
    if (dst->is_null) {
        // first update
        _init_iot_first_last_state(ctx, src_data.ts, src_data.val, dst);
    } else {
        auto* dst_data = reinterpret_cast<IoTFirstLastState*>(dst->ptr);
        if (src_data.ts.val > dst_data->ts.val) {
            dst_data->ts = src_data.ts;
            dst_data->val = src_data.val;
        }
    }
}

StringVal IoTFunctions::iot_first_last_serialize(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return src;
    }

    auto* src_data = reinterpret_cast<IoTFirstLastState*>(src.ptr);
    StringVal result(ctx, sizeof(IoTFirstLastState));
    src_data->serialize(&result);
    ctx->free(src.ptr);
    return result;
}

DoubleVal IoTFunctions::iot_first_last_finalize(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return DoubleVal::null();
    }

    auto* src_data = reinterpret_cast<IoTFirstLastState*>(src.ptr);
    return src_data->val;
}

} // namespace doris
