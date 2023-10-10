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

const char* EXECUTOR_CLASS = "org/apache/doris/udf/UdfExecutor";
const char* EXECUTOR_CTOR_SIGNATURE = "([B)V";
const char* EXECUTOR_EVALUATE_SIGNATURE = "()V";
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
        jni_env->executor_evaluate_id = env->GetMethodID(
                jni_env->executor_cl, "evaluate", "(I[Ljava/lang/Object;)[Ljava/lang/Object;");

        jni_env->executor_convert_basic_argument_id = env->GetMethodID(
                jni_env->executor_cl, "convertBasicArguments", "(IZIJJJ)[Ljava/lang/Object;");
        jni_env->executor_convert_array_argument_id = env->GetMethodID(
                jni_env->executor_cl, "convertArrayArguments", "(IZIJJJJJ)[Ljava/lang/Object;");
        jni_env->executor_convert_map_argument_id = env->GetMethodID(
                jni_env->executor_cl, "convertMapArguments", "(IZIJJJJJJJJ)[Ljava/lang/Object;");
        jni_env->executor_result_basic_batch_id = env->GetMethodID(
                jni_env->executor_cl, "copyBatchBasicResult", "(ZI[Ljava/lang/Object;JJJ)V");
        jni_env->executor_result_array_batch_id = env->GetMethodID(
                jni_env->executor_cl, "copyBatchArrayResult", "(ZI[Ljava/lang/Object;JJJJJ)V");
        jni_env->executor_result_map_batch_id = env->GetMethodID(
                jni_env->executor_cl, "copyBatchMapResult", "(ZI[Ljava/lang/Object;JJJJJJJJ)V");
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
    int arg_size = arguments.size();
    ColumnPtr data_cols[arg_size];
    ColumnPtr null_cols[arg_size];
    jclass obj_class = env->FindClass("[Ljava/lang/Object;");
    jclass arraylist_class = env->FindClass("Ljava/util/ArrayList;");
    jclass hashmap_class = env->FindClass("Ljava/util/HashMap;");
    jobjectArray arg_objects = env->NewObjectArray(arg_size, obj_class, nullptr);
    int64_t nullmap_address = 0;
    for (size_t arg_idx = 0; arg_idx < arg_size; ++arg_idx) {
        bool arg_column_nullable = false;
        // get argument column and type
        ColumnWithTypeAndName& column = block.get_by_position(arguments[arg_idx]);
        auto column_type = column.type;
        data_cols[arg_idx] = column.column->convert_to_full_column_if_const();

        // check type
        DCHECK(_argument_types[arg_idx]->equals(*column_type))
                << " input column's type is " + column_type->get_name()
                << " does not equal to required type " << _argument_types[arg_idx]->get_name();

        // get argument null map and nested column
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*data_cols[arg_idx])) {
            arg_column_nullable = true;
            column_type = remove_nullable(column_type);
            null_cols[arg_idx] = nullable->get_null_map_column_ptr();
            data_cols[arg_idx] = nullable->get_nested_column_ptr();
            nullmap_address = reinterpret_cast<int64_t>(
                    check_and_get_column<ColumnVector<UInt8>>(null_cols[arg_idx])
                            ->get_data()
                            .data());
        }

        // convert argument column data into java type
        jobjectArray arr_obj = nullptr;
        if (data_cols[arg_idx]->is_numeric() || data_cols[arg_idx]->is_column_decimal()) {
            arr_obj = (jobjectArray)env->CallNonvirtualObjectMethod(
                    jni_ctx->executor, jni_env->executor_cl,
                    jni_env->executor_convert_basic_argument_id, arg_idx, arg_column_nullable,
                    num_rows, nullmap_address,
                    reinterpret_cast<int64_t>(data_cols[arg_idx]->get_raw_data().data), 0);
        } else if (data_cols[arg_idx]->is_column_string()) {
            const ColumnString* str_col =
                    assert_cast<const ColumnString*>(data_cols[arg_idx].get());
            arr_obj = (jobjectArray)env->CallNonvirtualObjectMethod(
                    jni_ctx->executor, jni_env->executor_cl,
                    jni_env->executor_convert_basic_argument_id, arg_idx, arg_column_nullable,
                    num_rows, nullmap_address,
                    reinterpret_cast<int64_t>(str_col->get_chars().data()),
                    reinterpret_cast<int64_t>(str_col->get_offsets().data()));
        } else if (data_cols[arg_idx]->is_column_array()) {
            const ColumnArray* array_col =
                    assert_cast<const ColumnArray*>(data_cols[arg_idx].get());
            const ColumnNullable& array_nested_nullable =
                    assert_cast<const ColumnNullable&>(array_col->get_data());
            auto data_column_null_map = array_nested_nullable.get_null_map_column_ptr();
            auto data_column = array_nested_nullable.get_nested_column_ptr();
            auto offset_address =
                    reinterpret_cast<int64_t>(array_col->get_offsets_column().get_raw_data().data);
            auto nested_nullmap_address = reinterpret_cast<int64_t>(
                    check_and_get_column<ColumnVector<UInt8>>(data_column_null_map)
                            ->get_data()
                            .data());
            int64_t nested_data_address = 0, nested_offset_address = 0;
            // array type need pass address: [nullmap_address], offset_address, nested_nullmap_address, nested_data_address/nested_char_address,nested_offset_address
            if (data_column->is_column_string()) {
                const ColumnString* col = assert_cast<const ColumnString*>(data_column.get());
                nested_data_address = reinterpret_cast<int64_t>(col->get_chars().data());
                nested_offset_address = reinterpret_cast<int64_t>(col->get_offsets().data());
            } else {
                nested_data_address = reinterpret_cast<int64_t>(data_column->get_raw_data().data);
            }
            arr_obj = (jobjectArray)env->CallNonvirtualObjectMethod(
                    jni_ctx->executor, jni_env->executor_cl,
                    jni_env->executor_convert_array_argument_id, arg_idx, arg_column_nullable,
                    num_rows, nullmap_address, offset_address, nested_nullmap_address,
                    nested_data_address, nested_offset_address);
        } else if (data_cols[arg_idx]->is_column_map()) {
            const ColumnMap* map_col = assert_cast<const ColumnMap*>(data_cols[arg_idx].get());
            auto offset_address =
                    reinterpret_cast<int64_t>(map_col->get_offsets_column().get_raw_data().data);
            const ColumnNullable& map_key_column_nullable =
                    assert_cast<const ColumnNullable&>(map_col->get_keys());
            auto key_data_column_null_map = map_key_column_nullable.get_null_map_column_ptr();
            auto key_data_column = map_key_column_nullable.get_nested_column_ptr();

            auto key_nested_nullmap_address = reinterpret_cast<int64_t>(
                    check_and_get_column<ColumnVector<UInt8>>(key_data_column_null_map)
                            ->get_data()
                            .data());
            int64_t key_nested_data_address = 0, key_nested_offset_address = 0;
            if (key_data_column->is_column_string()) {
                const ColumnString* col = assert_cast<const ColumnString*>(key_data_column.get());
                key_nested_data_address = reinterpret_cast<int64_t>(col->get_chars().data());
                key_nested_offset_address = reinterpret_cast<int64_t>(col->get_offsets().data());
            } else {
                key_nested_data_address =
                        reinterpret_cast<int64_t>(key_data_column->get_raw_data().data);
            }

            const ColumnNullable& map_value_column_nullable =
                    assert_cast<const ColumnNullable&>(map_col->get_values());
            auto value_data_column_null_map = map_value_column_nullable.get_null_map_column_ptr();
            auto value_data_column = map_value_column_nullable.get_nested_column_ptr();
            auto value_nested_nullmap_address = reinterpret_cast<int64_t>(
                    check_and_get_column<ColumnVector<UInt8>>(value_data_column_null_map)
                            ->get_data()
                            .data());
            int64_t value_nested_data_address = 0, value_nested_offset_address = 0;
            if (value_data_column->is_column_string()) {
                const ColumnString* col = assert_cast<const ColumnString*>(value_data_column.get());
                value_nested_data_address = reinterpret_cast<int64_t>(col->get_chars().data());
                value_nested_offset_address = reinterpret_cast<int64_t>(col->get_offsets().data());
            } else {
                value_nested_data_address =
                        reinterpret_cast<int64_t>(value_data_column->get_raw_data().data);
            }
            arr_obj = (jobjectArray)env->CallNonvirtualObjectMethod(
                    jni_ctx->executor, jni_env->executor_cl,
                    jni_env->executor_convert_map_argument_id, arg_idx, arg_column_nullable,
                    num_rows, nullmap_address, offset_address, key_nested_nullmap_address,
                    key_nested_data_address, key_nested_offset_address,
                    value_nested_nullmap_address, value_nested_data_address,
                    value_nested_offset_address);
        } else {
            return Status::InvalidArgument(
                    strings::Substitute("Java UDF doesn't support type $0 now !",
                                        _argument_types[arg_idx]->get_name()));
        }

        env->SetObjectArrayElement(arg_objects, arg_idx, arr_obj);
        env->DeleteLocalRef(arr_obj);
    }
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));

    // evaluate with argument object
    jobjectArray result_obj = (jobjectArray)env->CallNonvirtualObjectMethod(
            jni_ctx->executor, jni_env->executor_cl, jni_env->executor_evaluate_id, num_rows,
            arg_objects);
    env->DeleteLocalRef(arg_objects);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));

    auto return_type = block.get_data_type(result);
    bool result_nullable = return_type->is_nullable();
    ColumnUInt8::MutablePtr null_col = nullptr;
    if (result_nullable) {
        return_type = remove_nullable(return_type);
        null_col = ColumnUInt8::create(num_rows, 0);
        memset(null_col->get_data().data(), 0, num_rows);
        nullmap_address = reinterpret_cast<int64_t>(null_col->get_data().data());
    }
    auto res_col = return_type->create_column();
    res_col->resize(num_rows);

    //could resize for column firstly, copy batch result into column
    if (res_col->is_numeric() || res_col->is_column_decimal()) {
        env->CallNonvirtualVoidMethod(jni_ctx->executor, jni_env->executor_cl,
                                      jni_env->executor_result_basic_batch_id, result_nullable,
                                      num_rows, result_obj, nullmap_address,
                                      reinterpret_cast<int64_t>(res_col->get_raw_data().data), 0);
    } else if (res_col->is_column_string()) {
        const ColumnString* str_col = assert_cast<const ColumnString*>(res_col.get());
        ColumnString::Chars& chars = const_cast<ColumnString::Chars&>(str_col->get_chars());
        ColumnString::Offsets& offsets = const_cast<ColumnString::Offsets&>(str_col->get_offsets());

        env->CallNonvirtualVoidMethod(
                jni_ctx->executor, jni_env->executor_cl, jni_env->executor_result_basic_batch_id,
                result_nullable, num_rows, result_obj, nullmap_address,
                reinterpret_cast<int64_t>(&chars), reinterpret_cast<int64_t>(offsets.data()));
    } else if (res_col->is_column_array()) {
        ColumnArray* array_col = assert_cast<ColumnArray*>(res_col.get());
        ColumnNullable& array_nested_nullable = assert_cast<ColumnNullable&>(array_col->get_data());
        auto data_column_null_map = array_nested_nullable.get_null_map_column_ptr();
        auto data_column = array_nested_nullable.get_nested_column_ptr();
        auto& offset_column = array_col->get_offsets_column();
        auto offset_address = reinterpret_cast<int64_t>(offset_column.get_raw_data().data);
        auto& null_map_data =
                assert_cast<ColumnVector<UInt8>*>(data_column_null_map.get())->get_data();
        auto nested_nullmap_address = reinterpret_cast<int64_t>(null_map_data.data());
        jmethodID list_size = env->GetMethodID(arraylist_class, "size", "()I");
        int element_size = 0; // get all element size in num_rows of array column
        for (int i = 0; i < num_rows; ++i) {
            jobject obj = env->GetObjectArrayElement(result_obj, i);
            if (obj == nullptr) {
                continue;
            }
            element_size = element_size + env->CallIntMethod(obj, list_size);
            env->DeleteLocalRef(obj);
        }
        array_nested_nullable.resize(element_size);
        memset(null_map_data.data(), 0, element_size);
        int64_t nested_data_address = 0, nested_offset_address = 0;
        // array type need pass address: [nullmap_address], offset_address, nested_nullmap_address, nested_data_address/nested_char_address,nested_offset_address
        if (data_column->is_column_string()) {
            ColumnString* str_col = assert_cast<ColumnString*>(data_column.get());
            ColumnString::Chars& chars = assert_cast<ColumnString::Chars&>(str_col->get_chars());
            ColumnString::Offsets& offsets =
                    assert_cast<ColumnString::Offsets&>(str_col->get_offsets());
            nested_data_address = reinterpret_cast<int64_t>(&chars);
            nested_offset_address = reinterpret_cast<int64_t>(offsets.data());
        } else {
            nested_data_address = reinterpret_cast<int64_t>(data_column->get_raw_data().data);
        }
        env->CallNonvirtualVoidMethod(
                jni_ctx->executor, jni_env->executor_cl, jni_env->executor_result_array_batch_id,
                result_nullable, num_rows, result_obj, nullmap_address, offset_address,
                nested_nullmap_address, nested_data_address, nested_offset_address);
    } else if (res_col->is_column_map()) {
        ColumnMap* map_col = assert_cast<ColumnMap*>(res_col.get());
        auto& offset_column = map_col->get_offsets_column();
        auto offset_address = reinterpret_cast<int64_t>(offset_column.get_raw_data().data);
        ColumnNullable& map_key_column_nullable = assert_cast<ColumnNullable&>(map_col->get_keys());
        auto key_data_column_null_map = map_key_column_nullable.get_null_map_column_ptr();
        auto key_data_column = map_key_column_nullable.get_nested_column_ptr();
        auto& key_null_map_data =
                assert_cast<ColumnVector<UInt8>*>(key_data_column_null_map.get())->get_data();
        auto key_nested_nullmap_address = reinterpret_cast<int64_t>(key_null_map_data.data());
        ColumnNullable& map_value_column_nullable =
                assert_cast<ColumnNullable&>(map_col->get_values());
        auto value_data_column_null_map = map_value_column_nullable.get_null_map_column_ptr();
        auto value_data_column = map_value_column_nullable.get_nested_column_ptr();
        auto& value_null_map_data =
                assert_cast<ColumnVector<UInt8>*>(value_data_column_null_map.get())->get_data();
        auto value_nested_nullmap_address = reinterpret_cast<int64_t>(value_null_map_data.data());
        jmethodID map_size = env->GetMethodID(hashmap_class, "size", "()I");
        int element_size = 0; // get all element size in num_rows of map column
        for (int i = 0; i < num_rows; ++i) {
            jobject obj = env->GetObjectArrayElement(result_obj, i);
            if (obj == nullptr) {
                continue;
            }
            element_size = element_size + env->CallIntMethod(obj, map_size);
            env->DeleteLocalRef(obj);
        }
        map_key_column_nullable.resize(element_size);
        memset(key_null_map_data.data(), 0, element_size);
        map_value_column_nullable.resize(element_size);
        memset(value_null_map_data.data(), 0, element_size);
        int64_t key_nested_data_address = 0, key_nested_offset_address = 0;
        if (key_data_column->is_column_string()) {
            ColumnString* str_col = assert_cast<ColumnString*>(key_data_column.get());
            ColumnString::Chars& chars = assert_cast<ColumnString::Chars&>(str_col->get_chars());
            ColumnString::Offsets& offsets =
                    assert_cast<ColumnString::Offsets&>(str_col->get_offsets());
            key_nested_data_address = reinterpret_cast<int64_t>(&chars);
            key_nested_offset_address = reinterpret_cast<int64_t>(offsets.data());
        } else {
            key_nested_data_address =
                    reinterpret_cast<int64_t>(key_data_column->get_raw_data().data);
        }
        int64_t value_nested_data_address = 0, value_nested_offset_address = 0;
        if (value_data_column->is_column_string()) {
            ColumnString* str_col = assert_cast<ColumnString*>(value_data_column.get());
            ColumnString::Chars& chars = assert_cast<ColumnString::Chars&>(str_col->get_chars());
            ColumnString::Offsets& offsets =
                    assert_cast<ColumnString::Offsets&>(str_col->get_offsets());
            value_nested_data_address = reinterpret_cast<int64_t>(&chars);
            value_nested_offset_address = reinterpret_cast<int64_t>(offsets.data());
        } else {
            value_nested_data_address =
                    reinterpret_cast<int64_t>(value_data_column->get_raw_data().data);
        }
        env->CallNonvirtualVoidMethod(jni_ctx->executor, jni_env->executor_cl,
                                      jni_env->executor_result_map_batch_id, result_nullable,
                                      num_rows, result_obj, nullmap_address, offset_address,
                                      key_nested_nullmap_address, key_nested_data_address,
                                      key_nested_offset_address, value_nested_nullmap_address,
                                      value_nested_data_address, value_nested_offset_address);
    } else {
        return Status::InvalidArgument(strings::Substitute(
                "Java UDF doesn't support return type $0 now !", return_type->get_name()));
    }
    env->DeleteLocalRef(result_obj);
    env->DeleteLocalRef(obj_class);
    env->DeleteLocalRef(arraylist_class);
    env->DeleteLocalRef(hashmap_class);
    if (result_nullable) {
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res_col), std::move(null_col)));
    } else {
        block.replace_by_position(result, std::move(res_col));
    }
    return JniUtil::GetJniExceptionMsg(env);
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
