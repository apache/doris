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
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "exprs/function/array/function_array_utils.h"
#include "exprs/table_function/table_function.h"
#include "jni.h"
#include "util/jni-util.h"

namespace doris {

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

        Jni::GlobalClass executor_cl;
        Jni::MethodId executor_evaluate_id;
        Jni::MethodId executor_close_id;
        Jni::GlobalObject executor;
        bool is_closed = false;
        bool open_successes = false;

        JniContext() = default;

        Status close() {
            if (is_closed) {
                return Status::OK();
            }
            // Not gated on open_successes: _open_udf() creates the Java executor before it
            // resolves any method id, so a failure in between leaves an executor that only this
            // can close. What has to be bound is the close id itself, which _open_udf() resolves
            // first for exactly this reason.
            if (executor.uninitialized() || executor_cl.uninitialized() ||
                executor_close_id.uninitialized()) {
                if (!open_successes) {
                    LOG_WARNING("maybe open failed, need check the reason");
                }
                return Status::OK();
            }
            JNIEnv* env = nullptr;
            Status status = Jni::Env::Get(&env);
            if (!status.ok() || env == nullptr) {
                LOG(WARNING) << "errors while get jni env " << status;
                return status;
            }
            // Before the call, not after: a close that threw halfway is still a close, and the
            // executor it was closing must not be handed to a second one. Same order as the
            // scalar path in function_java_udf.h and the UDAF path.
            is_closed = true;
            return executor.call_nonvirtual_void_method(env, executor_cl, executor_close_id).call();
        }
    };

    const TFunction& _t_fn;
    std::shared_ptr<JniContext> _jni_ctx = nullptr;
    DataTypePtr _return_type = nullptr;
    ColumnPtr _array_result_column = nullptr;
    ColumnArrayExecutionData _array_column_detail;
    uint32_t _result_column_idx = 0; // _array_result_column pos in block
    size_t _array_offset = 0;        // start offset of array[row_idx]
};

} // namespace doris
