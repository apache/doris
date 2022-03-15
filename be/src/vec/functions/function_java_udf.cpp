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

#include "vec/functions/function_java_udf.h"

#include <fmt/format.h>

#include <memory>
#include <sstream>

#include "runtime/exec_env.h"
#include "runtime/user_function_cache.h"

#include "util/jni-util.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

const char* EXECUTOR_CLASS = "org/apache/doris/udf/UdfExecutor";
const char* EXECUTOR_CTOR_SIGNATURE ="([B)V";
const char* EXECUTOR_EVALUATE_SIGNATURE = "()V";
const char* EXECUTOR_CLOSE_SIGNATURE = "()V";

namespace doris::vectorized {
JavaFunctionCall::JavaFunctionCall(const TFunction& fn,
                     const DataTypes& argument_types, const DataTypePtr& return_type)
        : fn_(fn),
          _argument_types(argument_types),
          _return_type(return_type) {}

Status JavaFunctionCall::prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    DCHECK(executor_cl_ == NULL) << "Init() already called!";
    JNIEnv* env = JniUtil::GetJNIEnv();
    if (env == NULL) return Status::InternalError("Failed to get/create JVM");
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, EXECUTOR_CLASS, &executor_cl_));
    executor_ctor_id_ = env->GetMethodID(
            executor_cl_, "<init>", EXECUTOR_CTOR_SIGNATURE);
    RETURN_ERROR_IF_EXC(env);
    executor_evaluate_id_ = env->GetMethodID(
            executor_cl_, "evaluate", EXECUTOR_EVALUATE_SIGNATURE);
    RETURN_ERROR_IF_EXC(env);
    executor_close_id_ = env->GetMethodID(
            executor_cl_, "close", EXECUTOR_CLOSE_SIGNATURE);
    RETURN_ERROR_IF_EXC(env);

    int64_t input_values_buffer_ptr = (int64_t) new int64_t[_argument_types.size()];
    int64_t input_nulls_buffer_ptr = (int64_t) new int64_t[_argument_types.size()];
    int64_t input_byte_offsets_ptr = (int64_t) new int64_t[_argument_types.size()];

    int64_t output_buffer = (int64_t) malloc(sizeof(int64_t));
    int64_t output_null = (int64_t) malloc(sizeof(int64_t));
    int64_t batch_size = (int64_t) malloc(sizeof(int32_t));
    JniContext* jni_ctx = new JniContext(input_values_buffer_ptr, input_nulls_buffer_ptr,
                                         input_byte_offsets_ptr, output_buffer, output_null,
                                         batch_size);
    context->set_function_state(FunctionContext::THREAD_LOCAL, jni_ctx);

    jni_ctx->hdfs_location = fn_.hdfs_location.c_str();
    jni_ctx->scalar_fn_symbol = fn_.scalar_fn.symbol.c_str();

    // Add a scoped cleanup jni reference object. This cleans up local refs made below.
    JniLocalFrame jni_frame;
    {
        std::string local_location;
        auto function_cache = UserFunctionCache::instance();
        RETURN_IF_ERROR(function_cache->get_jarpath(fn_.id, fn_.hdfs_location, fn_.checksum, &local_location));
        TJavaUdfExecutorCtorParams ctor_params;
        ctor_params.fn = fn_;
        ctor_params.location = local_location;
        ctor_params.input_byte_offsets = jni_ctx->input_byte_offsets_ptr;
        ctor_params.input_buffer_ptrs = jni_ctx->input_values_buffer_ptr;
        ctor_params.input_nulls_ptrs = jni_ctx->input_nulls_buffer_ptr;
        ctor_params.output_buffer_ptr = jni_ctx->output_value_buffer;
        ctor_params.output_null_ptr = jni_ctx->output_null_value;
        ctor_params.batch_size_ptr = jni_ctx->batch_size_ptr;

        jbyteArray ctor_params_bytes;

        // Pushed frame will be popped when jni_frame goes out-of-scope.
        RETURN_IF_ERROR(jni_frame.push(env));

        RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));
        jni_ctx->executor = env->NewObject(executor_cl_, executor_ctor_id_, ctor_params_bytes);
    }
    RETURN_ERROR_IF_EXC(env);
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jni_ctx->executor, &jni_ctx->executor));

    return Status::OK();
}

Status JavaFunctionCall::execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t num_rows, bool dry_run) {
    JNIEnv* env = JniUtil::GetJNIEnv();
    JniContext* jni_ctx = reinterpret_cast<JniContext*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    for (size_t col_idx : arguments) {
        ColumnWithTypeAndName& column = block.get_by_position(col_idx);
        auto col = column.column->convert_to_full_column_if_const();
        auto data_col = col;
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*col)) {
            data_col = nullable->get_nested_column_ptr();
            auto null_col = check_and_get_column<ColumnVector<UInt8>>(nullable->get_null_map_column_ptr());
            ((int64_t*) jni_ctx->input_nulls_buffer_ptr)[col_idx] = reinterpret_cast<int64_t>(null_col->get_data().data());
        }
        ((int64_t*) jni_ctx->input_values_buffer_ptr)[col_idx] = reinterpret_cast<int64_t>(data_col->get_raw_data().data);
        if (!data_col->values_have_fixed_size()) {
            DCHECK(false) << "Java UDF doesn't support type " << column.type->get_name() << " now!";
        }
    }

    auto data_type = block.get_data_type(result);
    if (data_type->is_nullable()) {
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(data_type);
        auto data_col = null_type->get_nested_type()->create_column();
        auto null_col = ColumnUInt8::create(data_col->size(), 0);
        null_col->reserve(num_rows);
        null_col->resize(num_rows);
        data_col->reserve(num_rows);
        data_col->resize(num_rows);

        *((int64_t*) jni_ctx->output_null_value) =
                reinterpret_cast<int64_t>(null_col->get_data().data());
        *((int64_t*) jni_ctx->output_value_buffer) = reinterpret_cast<int64_t>(data_col->get_raw_data().data);
        if (!data_col->values_have_fixed_size()) {
            DCHECK(false) << "Java UDF doesn't support type " << data_type->get_name() << " now!";
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(data_col), std::move(null_col)));
    } else {
        auto data_col = data_type->create_column();
        data_col->reserve(num_rows);
        data_col->resize(num_rows);

        *((int64_t*) jni_ctx->output_value_buffer) = reinterpret_cast<int64_t>(data_col->get_raw_data().data);
        if (!data_col->values_have_fixed_size()) {
            DCHECK(false) << "Java UDF doesn't support type " << data_type->get_name() << " now!";
        }
        block.replace_by_position(result, std::move(data_col));
    }
    *((int32_t*) jni_ctx->batch_size_ptr) = num_rows;
    // Using this version of Call has the lowest overhead. This eliminates the
    // vtable lookup and setting up return stacks.
    env->CallNonvirtualVoidMethodA(
            jni_ctx->executor, executor_cl_, executor_evaluate_id_, nullptr);
    Status status = JniUtil::GetJniExceptionMsg(env);
    if (!status.ok()) {
        if (!jni_ctx->warning_logged) {
            jni_ctx->warning_logged = true;
        }
        return status;
    }
    return Status::OK();
}

Status JavaFunctionCall::close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    JniContext* jni_ctx = reinterpret_cast<JniContext*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (jni_ctx != NULL) {
        JNIEnv* env = JniUtil::GetJNIEnv();
        if (jni_ctx->executor != NULL) {
            env->CallNonvirtualVoidMethodA(
                    jni_ctx->executor, executor_cl_, executor_close_id_, NULL);
            Status s = JniUtil::GetJniExceptionMsg(env);
            if (!s.ok()) LOG(WARNING) << s.get_error_msg();
            env->DeleteGlobalRef(jni_ctx->executor);
        }
        if (LIKELY((int64*) jni_ctx->input_values_buffer_ptr != NULL)) {
            delete[] ((int64*) jni_ctx->input_values_buffer_ptr);
        }
        if (LIKELY((int64*) jni_ctx->input_nulls_buffer_ptr != NULL)) {
            delete[] ((int64*) jni_ctx->input_nulls_buffer_ptr);
        }
        if (LIKELY((int64*) jni_ctx->input_byte_offsets_ptr != NULL)) {
            delete[] ((int64*) jni_ctx->input_byte_offsets_ptr);
        }
        if (LIKELY((int64*) jni_ctx->output_value_buffer != NULL)) {
            free((int64*) jni_ctx->output_value_buffer);
        }
        if (LIKELY((int64*) jni_ctx->output_null_value != NULL)) {
            free((int64*) jni_ctx->output_null_value);
        }
        if (LIKELY((int64*) jni_ctx->batch_size_ptr != NULL)) {
            free((int32*) jni_ctx->batch_size_ptr);
        }
        delete jni_ctx;
        context->set_function_state(FunctionContext::THREAD_LOCAL, nullptr);
    }
    return Status::OK();
}
} // namespace doris::vectorized
