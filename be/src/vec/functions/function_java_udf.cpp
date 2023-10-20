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

#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gutil/strings/substitute.h"
#include "jni.h"
#include "jni_md.h"
#include "runtime/user_function_cache.h"
#include "util/jni-util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/jni_connector.h"

const char* EXECUTOR_CLASS = "org/apache/doris/udf/UdfExecutor";
const char* EXECUTOR_CTOR_SIGNATURE = "([B)V";
const char* EXECUTOR_EVALUATE_SIGNATURE = "(Ljava/util/Map;Ljava/util/Map;)J";
const char* EXECUTOR_CLOSE_SIGNATURE = "()V";

namespace doris::vectorized {
JavaFunctionCall::JavaFunctionCall(const TFunction& fn, const DataTypes& argument_types,
                                   const DataTypePtr& return_type)
        : fn_(fn), _argument_types(argument_types), _return_type(return_type) {}

Status JavaFunctionCall::open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    if (env == nullptr) {
        return Status::InternalError("Failed to get/create JVM");
    }
    if (scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
        std::shared_ptr<JniEnv> jni_env = std::make_shared<JniEnv>();
        RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, EXECUTOR_CLASS, &jni_env->executor_cl));
        jni_env->executor_ctor_id =
                env->GetMethodID(jni_env->executor_cl, "<init>", EXECUTOR_CTOR_SIGNATURE);
        RETURN_ERROR_IF_EXC(env);
        jni_env->executor_evaluate_id =
                env->GetMethodID(jni_env->executor_cl, "evaluate", EXECUTOR_EVALUATE_SIGNATURE);
        jni_env->executor_close_id =
                env->GetMethodID(jni_env->executor_cl, "close", EXECUTOR_CLOSE_SIGNATURE);
        RETURN_ERROR_IF_EXC(env);
        context->set_function_state(FunctionContext::FRAGMENT_LOCAL, jni_env);
    }

    if (scope == FunctionContext::FunctionStateScope::THREAD_LOCAL) {
        JniEnv* jni_env = reinterpret_cast<JniEnv*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        std::shared_ptr<JniContext> jni_ctx = std::make_shared<JniContext>(
                _argument_types.size(), jni_env->executor_cl, jni_env->executor_close_id);
        context->set_function_state(FunctionContext::THREAD_LOCAL, jni_ctx);

        // Add a scoped cleanup jni reference object. This cleans up local refs made below.
        JniLocalFrame jni_frame;
        {
            std::string local_location;
            auto function_cache = UserFunctionCache::instance();
            RETURN_IF_ERROR(function_cache->get_jarpath(fn_.id, fn_.hdfs_location, fn_.checksum,
                                                        &local_location));
            TJavaUdfExecutorCtorParams ctor_params;
            ctor_params.__set_fn(fn_);
            ctor_params.__set_location(local_location);
            jbyteArray ctor_params_bytes;

            // Pushed frame will be popped when jni_frame goes out-of-scope.
            RETURN_IF_ERROR(jni_frame.push(env));

            RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));

            jni_ctx->executor = env->NewObject(jni_env->executor_cl, jni_env->executor_ctor_id,
                                               ctor_params_bytes);

            jbyte* pBytes = env->GetByteArrayElements(ctor_params_bytes, nullptr);
            env->ReleaseByteArrayElements(ctor_params_bytes, pBytes, JNI_ABORT);
            env->DeleteLocalRef(ctor_params_bytes);
        }
        RETURN_ERROR_IF_EXC(env);
        RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jni_ctx->executor, &jni_ctx->executor));
    }
    return Status::OK();
}

Status JavaFunctionCall::execute_impl(FunctionContext* context, Block& block,
                                      const ColumnNumbers& arguments, size_t result,
                                      size_t num_rows) const {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    JniContext* jni_ctx = reinterpret_cast<JniContext*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    JniEnv* jni_env =
            reinterpret_cast<JniEnv*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    std::unique_ptr<long[]> input_table;
    RETURN_IF_ERROR(JniConnector::to_java_table(&block, num_rows, arguments, input_table));
    auto input_table_schema = JniConnector::parse_table_schema(&block, arguments, true);
    std::map<String, String> input_params = {
            {"meta_address", std::to_string((long)input_table.get())},
            {"required_fields", input_table_schema.first},
            {"columns_types", input_table_schema.second}};
    jobject input_map = JniUtil::convert_to_java_map(env, input_params);
    auto output_table_schema = JniConnector::parse_table_schema(&block, {result}, true);
    std::string output_nullable =
            block.get_by_position(result).type->is_nullable() ? "true" : "false";
    std::map<String, String> output_params = {{"is_nullable", output_nullable},
                                              {"required_fields", output_table_schema.first},
                                              {"columns_types", output_table_schema.second}};
    jobject output_map = JniUtil::convert_to_java_map(env, output_params);
    long output_address = env->CallLongMethod(jni_ctx->executor, jni_env->executor_evaluate_id,
                                              input_map, output_map);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    env->DeleteLocalRef(input_map);
    env->DeleteLocalRef(output_map);

    return JniConnector::fill_block(&block, {result}, output_address);
}

Status JavaFunctionCall::close(FunctionContext* context,
                               FunctionContext::FunctionStateScope scope) {
    JniContext* jni_ctx = reinterpret_cast<JniContext*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    // JNIContext own some resource and its release method depend on JavaFunctionCall
    // has to release the resource before JavaFunctionCall is deconstructed.
    if (jni_ctx) {
        jni_ctx->close();
    }
    return Status::OK();
}
} // namespace doris::vectorized
