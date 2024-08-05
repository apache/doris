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

#include "common/status.h"
#include "jni.h"
#include "util/jni-util.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/functions/array/function_array_utils.h"

namespace doris::vectorized {

class UDFTableFunction final : public TableFunction {
    ENABLE_FACTORY_CREATOR(UDFTableFunction);

public:
    UDFTableFunction(const TFunction& t_fn);
    ~UDFTableFunction() override = default;

    Status open() override;
    Status process_init(Block* block, RuntimeState* state) override;
    void process_row(size_t row_idx) override;
    void process_close() override;
    void get_same_many_values(MutableColumnPtr& column, int length) override;
    int get_value(MutableColumnPtr& column, int max_step) override;
    Status close() override {
        if (_jni_ctx) {
            RETURN_IF_ERROR(_jni_ctx->close());
        }
        return TableFunction::close();
    }

private:
    struct JniContext {
        // Do not save parent directly, because parent is in VExpr, but jni context is in FunctionContext
        // The deconstruct sequence is not determined, it will core.
        // JniContext's lifecycle should same with function context, not related with expr
        jclass executor_cl;
        jmethodID executor_ctor_id;
        jmethodID executor_evaluate_id;
        jmethodID executor_close_id;
        jobject executor = nullptr;
        bool is_closed = false;
        bool open_successes = false;

        JniContext() = default;

        Status close() {
            if (!open_successes) {
                LOG_WARNING("maybe open failed, need check the reason");
                return Status::OK(); //maybe open failed, so can't call some jni
            }
            if (is_closed) {
                return Status::OK();
            }
            JNIEnv* env = nullptr;
            Status status = JniUtil::GetJNIEnv(&env);
            if (!status.ok() || env == nullptr) {
                LOG(WARNING) << "errors while get jni env " << status;
                return status;
            }
            env->CallNonvirtualVoidMethodA(executor, executor_cl, executor_close_id, nullptr);
            env->DeleteGlobalRef(executor);
            env->DeleteGlobalRef(executor_cl);
            RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
            is_closed = true;
            return Status::OK();
        }
    };

    const TFunction& _t_fn;
    std::shared_ptr<JniContext> _jni_ctx = nullptr;
    DataTypePtr _return_type = nullptr;
    ColumnPtr _array_result_column = nullptr;
    ColumnArrayExecutionData _array_column_detail;
    size_t _result_column_idx = 0; // _array_result_column pos in block
    size_t _array_offset = 0;      // start offset of array[row_idx]
};

} // namespace doris::vectorized
