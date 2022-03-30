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

#ifdef LIBJVM
#include <jni.h>

#include "gen_cpp/Exprs_types.h"
#include "util/jni-util.h"
#include "vec/functions/function.h"

namespace doris {

namespace vectorized {
class JavaFunctionCall : public IFunctionBase {
public:
    JavaFunctionCall(const TFunction& fn, const DataTypes& argument_types,
                     const DataTypePtr& return_type);

    static FunctionBasePtr create(const TFunction& fn,
                                  const ColumnsWithTypeAndName& argument_types,
                                  const DataTypePtr& return_type) {
        DataTypes data_types(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i) {
            data_types[i] = argument_types[i].type;
        }
        return std::make_shared<JavaFunctionCall>(fn, data_types, return_type);
    }

    /// Get the main function name.
    String get_name() const override { return fn_.name.function_name; };

    const DataTypes& get_argument_types() const override { return _argument_types; };
    const DataTypePtr& get_return_type() const override { return _return_type; };

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& sample_block,
                                const ColumnNumbers& arguments, size_t result) const override {
        return nullptr;
    }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   size_t result, size_t input_rows_count, bool dry_run = false) override;

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    bool is_deterministic() const override { return false; }

    bool is_deterministic_in_scope_of_query() const override { return false; }

private:
    const TFunction& fn_;
    const DataTypes _argument_types;
    const DataTypePtr _return_type;

    /// Global class reference to the UdfExecutor Java class and related method IDs. Set in
    /// Init(). These have the lifetime of the process (i.e. 'executor_cl_' is never freed).
    jclass executor_cl_;
    jmethodID executor_ctor_id_;
    jmethodID executor_evaluate_id_;
    jmethodID executor_close_id_;

    struct JniContext {
        JavaFunctionCall* parent = nullptr;

        jobject executor = nullptr;

        int64_t input_values_buffer_ptr;
        int64_t input_nulls_buffer_ptr;
        int64_t input_byte_offsets_ptr;
        int64_t output_value_buffer;
        int64_t output_null_value;
        int64_t batch_size_ptr;

        JniContext(int64_t num_args, JavaFunctionCall* parent):
                  parent(parent) {
            input_values_buffer_ptr = (int64_t) new int64_t[num_args];
            input_nulls_buffer_ptr = (int64_t) new int64_t[num_args];
            input_byte_offsets_ptr = (int64_t) new int64_t[num_args];

            output_value_buffer = (int64_t) malloc(sizeof(int64_t));
            output_null_value = (int64_t) malloc(sizeof(int64_t));
            batch_size_ptr = (int64_t) malloc(sizeof(int32_t));
        }

        ~JniContext() {
            VLOG_DEBUG << "Free resources for JniContext";
            JNIEnv* env;
            Status status;
            RETURN_IF_STATUS_ERROR(status, JniUtil::GetJNIEnv(&env));
            env->CallNonvirtualVoidMethodA(
                    executor, parent->executor_cl_, parent->executor_close_id_, NULL);
            Status s = JniUtil::GetJniExceptionMsg(env);
            if (!s.ok()) LOG(WARNING) << s.get_error_msg();
            env->DeleteGlobalRef(executor);
            delete[] ((int64*) input_values_buffer_ptr);
            delete[] ((int64*) input_nulls_buffer_ptr);
            delete[] ((int64*) input_byte_offsets_ptr);
            free((int64*) output_value_buffer);
            free((int64*) output_null_value);
            free((int32*) batch_size_ptr);
        }

        /// These functions are cross-compiled to IR and used by codegen.
        static void SetInputNullsBufferElement(
                JniContext* jni_ctx, int index, uint8_t value);
        static uint8_t* GetInputValuesBufferAtOffset(JniContext* jni_ctx, int offset);
    };
};

} // namespace vectorized
} // namespace doris
#endif
