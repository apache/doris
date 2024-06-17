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

#include "vec/exprs/table_function/udf_table_function.h"

#include <glog/logging.h>

#include "runtime/user_function_cache.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/jni_connector.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
const char* EXECUTOR_CLASS = "org/apache/doris/udf/UdfExecutor";
const char* EXECUTOR_CTOR_SIGNATURE = "([B)V";
const char* EXECUTOR_EVALUATE_SIGNATURE = "(Ljava/util/Map;Ljava/util/Map;)J";
const char* EXECUTOR_CLOSE_SIGNATURE = "()V";
UDFTableFunction::UDFTableFunction(const TFunction& t_fn) : TableFunction(), _t_fn(t_fn) {
    _fn_name = _t_fn.name.function_name;
    _return_type = DataTypeFactory::instance().create_data_type(
            TypeDescriptor::from_thrift(t_fn.ret_type));
    // as the java-utdf function in java code is eg: ArrayList<String>
    // so we need a array column to save the execute result, and make_nullable could help deal with nullmap
    _return_type = make_nullable(std::make_shared<DataTypeArray>(make_nullable(_return_type)));
}

Status UDFTableFunction::open() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    if (env == nullptr) {
        return Status::InternalError("Failed to get/create JVM");
    }
    _jni_ctx = std::make_shared<JniContext>();
    // Add a scoped cleanup jni reference object. This cleans up local refs made below.
    JniLocalFrame jni_frame;
    {
        std::string local_location;
        auto* function_cache = UserFunctionCache::instance();
        TJavaUdfExecutorCtorParams ctor_params;
        ctor_params.__set_fn(_t_fn);
        if (!_t_fn.hdfs_location.empty() && !_t_fn.checksum.empty()) {
            // get jar path if both file path location and checksum are null
            RETURN_IF_ERROR(function_cache->get_jarpath(_t_fn.id, _t_fn.hdfs_location,
                                                        _t_fn.checksum, &local_location));
            ctor_params.__set_location(local_location);
        }
        jbyteArray ctor_params_bytes;
        // Pushed frame will be popped when jni_frame goes out-of-scope.
        RETURN_IF_ERROR(jni_frame.push(env));
        RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));
        RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, EXECUTOR_CLASS, &_jni_ctx->executor_cl));
        _jni_ctx->executor_ctor_id =
                env->GetMethodID(_jni_ctx->executor_cl, "<init>", EXECUTOR_CTOR_SIGNATURE);
        _jni_ctx->executor_evaluate_id =
                env->GetMethodID(_jni_ctx->executor_cl, "evaluate", EXECUTOR_EVALUATE_SIGNATURE);
        _jni_ctx->executor_close_id =
                env->GetMethodID(_jni_ctx->executor_cl, "close", EXECUTOR_CLOSE_SIGNATURE);
        _jni_ctx->executor = env->NewObject(_jni_ctx->executor_cl, _jni_ctx->executor_ctor_id,
                                            ctor_params_bytes);
        jbyte* pBytes = env->GetByteArrayElements(ctor_params_bytes, nullptr);
        env->ReleaseByteArrayElements(ctor_params_bytes, pBytes, JNI_ABORT);
        env->DeleteLocalRef(ctor_params_bytes);
    }
    RETURN_ERROR_IF_EXC(env);
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, _jni_ctx->executor, &_jni_ctx->executor));
    _jni_ctx->open_successes = true;
    return Status::OK();
}

Status UDFTableFunction::process_init(Block* block, RuntimeState* state) {
    auto child_size = _expr_context->root()->children().size();
    std::vector<size_t> child_column_idxs;
    child_column_idxs.resize(child_size);
    for (int i = 0; i < child_size; ++i) {
        int result_id = -1;
        RETURN_IF_ERROR(_expr_context->root()->children()[i]->execute(_expr_context.get(), block,
                                                                      &result_id));
        DCHECK_NE(result_id, -1);
        child_column_idxs[i] = result_id;
    }
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    std::unique_ptr<long[]> input_table;
    RETURN_IF_ERROR(
            JniConnector::to_java_table(block, block->rows(), child_column_idxs, input_table));
    auto input_table_schema = JniConnector::parse_table_schema(block, child_column_idxs, true);
    std::map<String, String> input_params = {
            {"meta_address", std::to_string((long)input_table.get())},
            {"required_fields", input_table_schema.first},
            {"columns_types", input_table_schema.second}};

    jobject input_map = JniUtil::convert_to_java_map(env, input_params);
    _array_result_column = _return_type->create_column();
    _result_column_idx = block->columns();
    block->insert({_array_result_column, _return_type, "res"});
    auto output_table_schema = JniConnector::parse_table_schema(block, {_result_column_idx}, true);
    std::string output_nullable = _return_type->is_nullable() ? "true" : "false";
    std::map<String, String> output_params = {{"is_nullable", output_nullable},
                                              {"required_fields", output_table_schema.first},
                                              {"columns_types", output_table_schema.second}};

    jobject output_map = JniUtil::convert_to_java_map(env, output_params);
    DCHECK(_jni_ctx != nullptr);
    DCHECK(_jni_ctx->executor != nullptr);
    long output_address = env->CallLongMethod(_jni_ctx->executor, _jni_ctx->executor_evaluate_id,
                                              input_map, output_map);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    env->DeleteLocalRef(input_map);
    env->DeleteLocalRef(output_map);
    RETURN_IF_ERROR(JniConnector::fill_block(block, {_result_column_idx}, output_address));
    block->erase(_result_column_idx);
    if (!extract_column_array_info(*_array_result_column, _array_column_detail)) {
        return Status::NotSupported("column type {} not supported now",
                                    block->get_by_position(_result_column_idx).column->get_name());
    }
    return Status::OK();
}

void UDFTableFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);
    if (!_array_column_detail.array_nullmap_data ||
        !_array_column_detail.array_nullmap_data[row_idx]) {
        _array_offset = (*_array_column_detail.offsets_ptr)[row_idx - 1];
        _cur_size = (*_array_column_detail.offsets_ptr)[row_idx] - _array_offset;
    }
    // so when it's NULL of row_idx, will not update _cur_size
    // it's will be _cur_size == 0, and means current_empty.
    // if the fn is outer, will be continue insert_default
    // if the fn is not outer function, will be not insert any value.
}

void UDFTableFunction::process_close() {
    _array_result_column = nullptr;
    _array_column_detail.reset();
    _array_offset = 0;
}

void UDFTableFunction::get_same_many_values(MutableColumnPtr& column, int length) {
    size_t pos = _array_offset + _cur_offset;
    if (current_empty() || (_array_column_detail.nested_nullmap_data &&
                            _array_column_detail.nested_nullmap_data[pos])) {
        column->insert_many_defaults(length);
    } else {
        if (_is_nullable) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
            auto nested_column = nullable_column->get_nested_column_ptr();
            auto nullmap_column = nullable_column->get_null_map_column_ptr();
            nested_column->insert_many_from(*_array_column_detail.nested_col, pos, length);
            assert_cast<ColumnUInt8*>(nullmap_column.get())->insert_many_defaults(length);
        } else {
            column->insert_many_from(*_array_column_detail.nested_col, pos, length);
        }
    }
}

int UDFTableFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
    size_t pos = _array_offset + _cur_offset;
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        if (_is_nullable) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
            auto nested_column = nullable_column->get_nested_column_ptr();
            auto* nullmap_column =
                    assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
            nested_column->insert_range_from(*_array_column_detail.nested_col, pos, max_step);
            size_t old_size = nullmap_column->size();
            nullmap_column->resize(old_size + max_step);
            memcpy(nullmap_column->get_data().data() + old_size,
                   _array_column_detail.nested_nullmap_data + pos * sizeof(UInt8),
                   max_step * sizeof(UInt8));
        } else {
            column->insert_range_from(*_array_column_detail.nested_col, pos, max_step);
        }
    }
    forward(max_step);
    return max_step;
}
} // namespace doris::vectorized
