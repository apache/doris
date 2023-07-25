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

#include <gen_cpp/Types_types.h>
#include <jni.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <mutex>
#include <ostream>

#include "common/logging.h"
#include "common/status.h"
#include "udf/udf.h"
#include "util/jni-util.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"

namespace doris {

namespace vectorized {
class JavaFunctionCall : public IFunctionBase {
public:
    JavaFunctionCall(const TFunction& fn, const DataTypes& argument_types,
                     const DataTypePtr& return_type);

    static FunctionBasePtr create(const TFunction& fn, const ColumnsWithTypeAndName& argument_types,
                                  const DataTypePtr& return_type) {
        DataTypes data_types(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i) {
            data_types[i] = argument_types[i].type;
        }
        return std::make_shared<JavaFunctionCall>(fn, data_types, return_type);
    }

    /// Get the main function name.
    String get_name() const override { return fn_.name.function_name; }

    const DataTypes& get_argument_types() const override { return _argument_types; }
    const DataTypePtr& get_return_type() const override { return _return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& sample_block,
                                const ColumnNumbers& arguments, size_t result) const override {
        return nullptr;
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   size_t result, size_t input_rows_count, bool dry_run = false) override;

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    bool is_deterministic() const override { return false; }

    bool is_deterministic_in_scope_of_query() const override { return false; }

    bool is_use_default_implementation_for_constants() const override { return true; }

private:
    const TFunction& fn_;
    const DataTypes _argument_types;
    const DataTypePtr _return_type;

    struct IntermediateState {
        size_t buffer_size;
        size_t row_idx;

        IntermediateState() : buffer_size(0), row_idx(0) {}
    };

    struct JniEnv {
        /// Global class reference to the UdfExecutor Java class and related method IDs. Set in
        /// Init(). These have the lifetime of the process (i.e. 'executor_cl_' is never freed).
        jclass executor_cl;
        jmethodID executor_ctor_id;
        jmethodID executor_evaluate_id;
        jmethodID executor_convert_basic_argument_id;
        jmethodID executor_convert_array_argument_id;
        jmethodID executor_convert_map_argument_id;
        jmethodID executor_result_basic_batch_id;
        jmethodID executor_result_array_batch_id;
        jmethodID executor_close_id;
    };

    struct JniContext {
        // Do not save parent directly, because parent is in VExpr, but jni context is in FunctionContext
        // The deconstruct sequence is not determined, it will core.
        // JniContext's lifecycle should same with function context, not related with expr
        jclass executor_cl_;
        jmethodID executor_close_id_;
        jobject executor = nullptr;
        bool is_closed = false;

        std::unique_ptr<int64_t[]> input_values_buffer_ptr;
        std::unique_ptr<int64_t[]> input_nulls_buffer_ptr;
        std::unique_ptr<int64_t[]> input_offsets_ptrs;
        //used for array type nested column null map, because array nested column must be nullable
        std::unique_ptr<int64_t[]> input_array_nulls_buffer_ptr;
        //used for array type of nested string column offset, not the array column offset
        std::unique_ptr<int64_t[]> input_array_string_offsets_ptrs;
        std::unique_ptr<int64_t> output_value_buffer;
        std::unique_ptr<int64_t> output_null_value;
        std::unique_ptr<int64_t> output_offsets_ptr;
        //used for array type nested column null map
        std::unique_ptr<int64_t> output_array_null_ptr;
        //used for array type of nested string column offset
        std::unique_ptr<int64_t> output_array_string_offsets_ptr;
        std::unique_ptr<int32_t> batch_size_ptr;
        // intermediate_state includes two parts: reserved / used buffer size and rows
        std::unique_ptr<IntermediateState> output_intermediate_state_ptr;

        JniContext(int64_t num_args, jclass executor_cl, jmethodID executor_close_id)
                : executor_cl_(executor_cl),
                  executor_close_id_(executor_close_id),
                  input_values_buffer_ptr(new int64_t[num_args]),
                  input_nulls_buffer_ptr(new int64_t[num_args]),
                  input_offsets_ptrs(new int64_t[num_args]),
                  input_array_nulls_buffer_ptr(new int64_t[num_args]),
                  input_array_string_offsets_ptrs(new int64_t[num_args]),
                  output_value_buffer(new int64_t()),
                  output_null_value(new int64_t()),
                  output_offsets_ptr(new int64_t()),
                  output_array_null_ptr(new int64_t()),
                  output_array_string_offsets_ptr(new int64_t()),
                  batch_size_ptr(new int32_t()),
                  output_intermediate_state_ptr(new IntermediateState()) {}

        void close() {
            if (is_closed) {
                return;
            }
            VLOG_DEBUG << "Free resources for JniContext";
            JNIEnv* env;
            Status status = JniUtil::GetJNIEnv(&env);
            if (!status.ok()) {
                LOG(WARNING) << "errors while get jni env " << status;
                return;
            }
            env->CallNonvirtualVoidMethodA(executor, executor_cl_, executor_close_id_, NULL);
            Status s = JniUtil::GetJniExceptionMsg(env);
            if (!s.ok()) LOG(WARNING) << s;
            env->DeleteGlobalRef(executor);
            is_closed = true;
        }

        /// These functions are cross-compiled to IR and used by codegen.
        static void SetInputNullsBufferElement(JniContext* jni_ctx, int index, uint8_t value);
        static uint8_t* GetInputValuesBufferAtOffset(JniContext* jni_ctx, int offset);
    };
};

} // namespace vectorized
} // namespace doris
