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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/anyval-util.cc
// and modified by Doris

#include "exprs/anyval_util.h"

#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {
using doris_udf::BooleanVal;
using doris_udf::TinyIntVal;
using doris_udf::SmallIntVal;
using doris_udf::IntVal;
using doris_udf::BigIntVal;
using doris_udf::LargeIntVal;
using doris_udf::FloatVal;
using doris_udf::DoubleVal;
using doris_udf::DecimalV2Val;
using doris_udf::DateTimeVal;
using doris_udf::StringVal;
using doris_udf::AnyVal;

Status allocate_any_val(RuntimeState* state, MemPool* pool, const TypeDescriptor& type,
                        const std::string& mem_limit_exceeded_msg, AnyVal** result) {
    const int anyval_size = AnyValUtil::any_val_size(type);
    const int anyval_alignment = AnyValUtil::any_val_alignment(type);
    Status rst;
    *result = reinterpret_cast<AnyVal*>(
            pool->try_allocate_aligned(anyval_size, anyval_alignment, &rst));
    if (*result == nullptr) {
        RETURN_LIMIT_EXCEEDED(pool->mem_tracker(), state, mem_limit_exceeded_msg, anyval_size, rst);
    }
    memset(static_cast<void*>(*result), 0, anyval_size);
    return Status::OK();
}

AnyVal* create_any_val(ObjectPool* pool, const TypeDescriptor& type) {
    switch (type.type) {
    case TYPE_NULL:
        return pool->add(new AnyVal);

    case TYPE_BOOLEAN:
        return pool->add(new BooleanVal);

    case TYPE_TINYINT:
        return pool->add(new TinyIntVal);

    case TYPE_SMALLINT:
        return pool->add(new SmallIntVal);

    case TYPE_INT:
        return pool->add(new IntVal);

    case TYPE_BIGINT:
        return pool->add(new BigIntVal);

    case TYPE_LARGEINT:
        return pool->add(new LargeIntVal);

    case TYPE_FLOAT:
        return pool->add(new FloatVal);

    case TYPE_TIME:
    case TYPE_DOUBLE:
        return pool->add(new DoubleVal);

    case TYPE_CHAR:
    case TYPE_HLL:
    case TYPE_VARCHAR:
    case TYPE_OBJECT:
    case TYPE_QUANTILE_STATE:
    case TYPE_STRING:
        return pool->add(new StringVal);

    case TYPE_DECIMALV2:
        return pool->add(new DecimalV2Val);

    case TYPE_DATE:
        return pool->add(new DateTimeVal);

    case TYPE_DATETIME:
        return pool->add(new DateTimeVal);

    case TYPE_ARRAY:
        return pool->add(new CollectionVal);

    default:
        DCHECK(false) << "Unsupported type: " << type.type;
        return nullptr;
    }
}

FunctionContext::TypeDesc AnyValUtil::column_type_to_type_desc(const TypeDescriptor& type) {
    FunctionContext::TypeDesc out;
    switch (type.type) {
    case TYPE_BOOLEAN:
        out.type = FunctionContext::TYPE_BOOLEAN;
        break;
    case TYPE_TINYINT:
        out.type = FunctionContext::TYPE_TINYINT;
        break;
    case TYPE_SMALLINT:
        out.type = FunctionContext::TYPE_SMALLINT;
        break;
    case TYPE_INT:
        out.type = FunctionContext::TYPE_INT;
        break;
    case TYPE_BIGINT:
        out.type = FunctionContext::TYPE_BIGINT;
        break;
    case TYPE_LARGEINT:
        out.type = FunctionContext::TYPE_LARGEINT;
        break;
    case TYPE_FLOAT:
        out.type = FunctionContext::TYPE_FLOAT;
        break;
    case TYPE_TIME:
    case TYPE_DOUBLE:
        out.type = FunctionContext::TYPE_DOUBLE;
        break;
    case TYPE_DATE:
        out.type = FunctionContext::TYPE_DATE;
        break;
    case TYPE_DATETIME:
        out.type = FunctionContext::TYPE_DATETIME;
        break;
    case TYPE_VARCHAR:
        out.type = FunctionContext::TYPE_VARCHAR;
        out.len = type.len;
        break;
    case TYPE_HLL:
        out.type = FunctionContext::TYPE_HLL;
        out.len = type.len;
        break;
    case TYPE_OBJECT:
        out.type = FunctionContext::TYPE_OBJECT;
        // FIXME(cmy): is this fallthrough meaningful?
    case TYPE_QUANTILE_STATE:
        out.type = FunctionContext::TYPE_QUANTILE_STATE;
        break;
    case TYPE_CHAR:
        out.type = FunctionContext::TYPE_CHAR;
        out.len = type.len;
        break;
    case TYPE_DECIMALV2:
        out.type = FunctionContext::TYPE_DECIMALV2;
        // out.precision = type.precision;
        // out.scale = type.scale;
        break;
    case TYPE_NULL:
        out.type = FunctionContext::TYPE_NULL;
        break;
    case TYPE_ARRAY:
        out.type = FunctionContext::TYPE_ARRAY;
        for (const auto& t : type.children) {
            out.children.push_back(column_type_to_type_desc(t));
        }
        break;
    case TYPE_STRING:
        out.type = FunctionContext::TYPE_STRING;
        out.len = type.len;
        break;
    default:
        DCHECK(false) << "Unknown type: " << type;
    }
    return out;
}

} // namespace doris
