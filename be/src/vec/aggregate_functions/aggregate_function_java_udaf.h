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

#include <jni.h>
#include <unistd.h>

#include <cstdint>
#include <memory>

#include "common/compiler_util.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "runtime/user_function_cache.h"
#include "util/jni-util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

const char* UDAF_EXECUTOR_CLASS = "org/apache/doris/udf/UdafExecutor";
const char* UDAF_EXECUTOR_CTOR_SIGNATURE = "([B)V";
const char* UDAF_EXECUTOR_CLOSE_SIGNATURE = "()V";
const char* UDAF_EXECUTOR_DESTROY_SIGNATURE = "()V";
const char* UDAF_EXECUTOR_ADD_SIGNATURE = "(ZJJ)V";
const char* UDAF_EXECUTOR_SERIALIZE_SIGNATURE = "(J)[B";
const char* UDAF_EXECUTOR_MERGE_SIGNATURE = "(J[B)V";
const char* UDAF_EXECUTOR_RESULT_SIGNATURE = "(JJ)Z";
const char* UDAF_EXECUTOR_RESET_SIGNATURE = "(J)V";
// Calling Java method about those signature means: "(argument-types)return-type"
// https://www.iitk.ac.in/esc101/05Aug/tutorial/native1.1/implementing/method.html

struct AggregateJavaUdafData {
public:
    AggregateJavaUdafData() = default;
    AggregateJavaUdafData(int64_t num_args) { argument_size = num_args; }

    ~AggregateJavaUdafData() {
        JNIEnv* env;
        Status status;
        RETURN_IF_STATUS_ERROR(status, JniUtil::GetJNIEnv(&env));
        env->CallNonvirtualVoidMethod(executor_obj, executor_cl, executor_close_id);
        RETURN_IF_STATUS_ERROR(status, JniUtil::GetJniExceptionMsg(env));
        env->DeleteGlobalRef(executor_cl);
        env->DeleteGlobalRef(executor_obj);
    }

    Status init_udaf(const TFunction& fn, const std::string& local_location) {
        JNIEnv* env = nullptr;
        RETURN_NOT_OK_STATUS_WITH_WARN(JniUtil::GetJNIEnv(&env), "Java-Udaf init_udaf function");
        RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, UDAF_EXECUTOR_CLASS, &executor_cl));
        RETURN_NOT_OK_STATUS_WITH_WARN(register_func_id(env),
                                       "Java-Udaf register_func_id function");

        // Add a scoped cleanup jni reference object. This cleans up local refs made below.
        JniLocalFrame jni_frame;
        {
            TJavaUdfExecutorCtorParams ctor_params;
            ctor_params.__set_fn(fn);
            ctor_params.__set_location(local_location);

            jbyteArray ctor_params_bytes;

            // Pushed frame will be popped when jni_frame goes out-of-scope.
            RETURN_IF_ERROR(jni_frame.push(env));
            RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));
            executor_obj = env->NewObject(executor_cl, executor_ctor_id, ctor_params_bytes);

            jbyte* pBytes = env->GetByteArrayElements(ctor_params_bytes, nullptr);
            env->ReleaseByteArrayElements(ctor_params_bytes, pBytes, JNI_ABORT);
            env->DeleteLocalRef(ctor_params_bytes);
        }
        RETURN_ERROR_IF_EXC(env);
        RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, executor_obj, &executor_obj));
        return Status::OK();
    }

    Status add(int64_t places_address, bool is_single_place, const IColumn** columns,
               int row_num_start, int row_num_end, const DataTypes& argument_types,
               int place_offset) {
        JNIEnv* env = nullptr;
        RETURN_NOT_OK_STATUS_WITH_WARN(JniUtil::GetJNIEnv(&env), "Java-Udaf add function");
        jclass obj_class = env->FindClass("[Ljava/lang/Object;");
        jobjectArray arg_objects = env->NewObjectArray(argument_size, obj_class, nullptr);
        int64_t nullmap_address = 0;

        for (int arg_idx = 0; arg_idx < argument_size; ++arg_idx) {
            bool arg_column_nullable = false;
            auto data_col = columns[arg_idx];
            if (auto* nullable = check_and_get_column<const ColumnNullable>(*columns[arg_idx])) {
                arg_column_nullable = true;
                auto null_col = nullable->get_null_map_column_ptr();
                data_col = nullable->get_nested_column_ptr();
                nullmap_address = reinterpret_cast<int64_t>(
                        check_and_get_column<ColumnVector<UInt8>>(null_col)->get_data().data());
            }
            // convert argument column data into java type
            jobjectArray arr_obj = nullptr;
            if (data_col->is_numeric() || data_col->is_column_decimal()) {
                arr_obj = (jobjectArray)env->CallObjectMethod(
                        executor_obj, executor_convert_basic_argument_id, arg_idx,
                        arg_column_nullable, row_num_start, row_num_end, nullmap_address,
                        reinterpret_cast<int64_t>(data_col->get_raw_data().data), 0);
            } else if (data_col->is_column_string()) {
                const ColumnString* str_col = assert_cast<const ColumnString*>(data_col);
                arr_obj = (jobjectArray)env->CallObjectMethod(
                        executor_obj, executor_convert_basic_argument_id, arg_idx,
                        arg_column_nullable, row_num_start, row_num_end, nullmap_address,
                        reinterpret_cast<int64_t>(str_col->get_chars().data()),
                        reinterpret_cast<int64_t>(str_col->get_offsets().data()));
            } else if (data_col->is_column_array()) {
                const ColumnArray* array_col = assert_cast<const ColumnArray*>(data_col);
                const ColumnNullable& array_nested_nullable =
                        assert_cast<const ColumnNullable&>(array_col->get_data());
                auto data_column_null_map = array_nested_nullable.get_null_map_column_ptr();
                auto data_column = array_nested_nullable.get_nested_column_ptr();
                auto offset_address = reinterpret_cast<int64_t>(
                        array_col->get_offsets_column().get_raw_data().data);
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
                    nested_data_address =
                            reinterpret_cast<int64_t>(data_column->get_raw_data().data);
                }
                arr_obj = (jobjectArray)env->CallObjectMethod(
                        executor_obj, executor_convert_array_argument_id, arg_idx,
                        arg_column_nullable, row_num_start, row_num_end, nullmap_address,
                        offset_address, nested_nullmap_address, nested_data_address,
                        nested_offset_address);
            } else if (data_col->is_column_map()) {
                const ColumnMap* map_col = assert_cast<const ColumnMap*>(data_col);
                auto offset_address = reinterpret_cast<int64_t>(
                        map_col->get_offsets_column().get_raw_data().data);
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
                    const ColumnString* col =
                            assert_cast<const ColumnString*>(key_data_column.get());
                    key_nested_data_address = reinterpret_cast<int64_t>(col->get_chars().data());
                    key_nested_offset_address =
                            reinterpret_cast<int64_t>(col->get_offsets().data());
                } else {
                    key_nested_data_address =
                            reinterpret_cast<int64_t>(key_data_column->get_raw_data().data);
                }

                const ColumnNullable& map_value_column_nullable =
                        assert_cast<const ColumnNullable&>(map_col->get_values());
                auto value_data_column_null_map =
                        map_value_column_nullable.get_null_map_column_ptr();
                auto value_data_column = map_value_column_nullable.get_nested_column_ptr();
                auto value_nested_nullmap_address = reinterpret_cast<int64_t>(
                        check_and_get_column<ColumnVector<UInt8>>(value_data_column_null_map)
                                ->get_data()
                                .data());
                int64_t value_nested_data_address = 0, value_nested_offset_address = 0;
                if (value_data_column->is_column_string()) {
                    const ColumnString* col =
                            assert_cast<const ColumnString*>(value_data_column.get());
                    value_nested_data_address = reinterpret_cast<int64_t>(col->get_chars().data());
                    value_nested_offset_address =
                            reinterpret_cast<int64_t>(col->get_offsets().data());
                } else {
                    value_nested_data_address =
                            reinterpret_cast<int64_t>(value_data_column->get_raw_data().data);
                }
                arr_obj = (jobjectArray)env->CallObjectMethod(
                        executor_obj, executor_convert_map_argument_id, arg_idx,
                        arg_column_nullable, row_num_start, row_num_end, nullmap_address,
                        offset_address, key_nested_nullmap_address, key_nested_data_address,
                        key_nested_offset_address, value_nested_nullmap_address,
                        value_nested_data_address, value_nested_offset_address);
            } else {
                return Status::InvalidArgument(
                        strings::Substitute("Java UDAF doesn't support type is $0 now !",
                                            argument_types[arg_idx]->get_name()));
            }
            env->SetObjectArrayElement(arg_objects, arg_idx, arr_obj);
            env->DeleteLocalRef(arr_obj);
        }
        RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
        // invoke add batch
        env->CallObjectMethod(executor_obj, executor_add_batch_id, is_single_place, row_num_start,
                              row_num_end, places_address, place_offset, arg_objects);
        env->DeleteLocalRef(arg_objects);
        env->DeleteLocalRef(obj_class);
        return JniUtil::GetJniExceptionMsg(env);
    }

    Status merge(const AggregateJavaUdafData& rhs, int64_t place) {
        JNIEnv* env = nullptr;
        RETURN_NOT_OK_STATUS_WITH_WARN(JniUtil::GetJNIEnv(&env), "Java-Udaf merge function");
        serialize_data = rhs.serialize_data;
        long len = serialize_data.length();
        jbyteArray arr = env->NewByteArray(len);
        env->SetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte*>(serialize_data.data()));
        env->CallNonvirtualVoidMethod(executor_obj, executor_cl, executor_merge_id, place, arr);
        jbyte* pBytes = env->GetByteArrayElements(arr, nullptr);
        env->ReleaseByteArrayElements(arr, pBytes, JNI_ABORT);
        env->DeleteLocalRef(arr);
        return JniUtil::GetJniExceptionMsg(env);
    }

    Status write(BufferWritable& buf, int64_t place) {
        JNIEnv* env = nullptr;
        RETURN_NOT_OK_STATUS_WITH_WARN(JniUtil::GetJNIEnv(&env), "Java-Udaf write function");
        // TODO: Here get a byte[] from FE serialize, and then allocate the same length bytes to
        // save it in BE, Because i'm not sure there is a way to use the byte[] not allocate again.
        jbyteArray arr = (jbyteArray)(env->CallNonvirtualObjectMethod(
                executor_obj, executor_cl, executor_serialize_id, place));
        RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
        int len = env->GetArrayLength(arr);
        serialize_data.resize(len);
        env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte*>(serialize_data.data()));
        write_binary(serialize_data, buf);
        jbyte* pBytes = env->GetByteArrayElements(arr, nullptr);
        env->ReleaseByteArrayElements(arr, pBytes, JNI_ABORT);
        env->DeleteLocalRef(arr);
        return JniUtil::GetJniExceptionMsg(env);
    }

    Status reset(int64_t place) {
        JNIEnv* env = nullptr;
        RETURN_NOT_OK_STATUS_WITH_WARN(JniUtil::GetJNIEnv(&env), "Java-Udaf reset function");
        env->CallNonvirtualVoidMethod(executor_obj, executor_cl, executor_reset_id, place);
        return JniUtil::GetJniExceptionMsg(env);
    }

    void read(BufferReadable& buf) { read_binary(serialize_data, buf); }

    Status destroy() {
        JNIEnv* env = nullptr;
        RETURN_NOT_OK_STATUS_WITH_WARN(JniUtil::GetJNIEnv(&env), "Java-Udaf destroy function");
        env->CallNonvirtualVoidMethod(executor_obj, executor_cl, executor_destroy_id);
        return JniUtil::GetJniExceptionMsg(env);
    }

    Status get(IColumn& to, const DataTypePtr& result_type, int64_t place) const {
        to.insert_default();
        JNIEnv* env = nullptr;
        RETURN_NOT_OK_STATUS_WITH_WARN(JniUtil::GetJNIEnv(&env), "Java-Udaf get value function");
        int64_t nullmap_address = 0;
        if (result_type->is_nullable()) {
            auto& nullable = assert_cast<ColumnNullable&>(to);
            nullmap_address =
                    reinterpret_cast<int64_t>(nullable.get_null_map_column().get_raw_data().data);
            auto& data_col = nullable.get_nested_column();
            RETURN_IF_ERROR(get_result(to, result_type, place, env, data_col, nullmap_address));
        } else {
            nullmap_address = -1;
            auto& data_col = to;
            RETURN_IF_ERROR(get_result(to, result_type, place, env, data_col, nullmap_address));
        }
        return JniUtil::GetJniExceptionMsg(env);
    }

private:
    Status get_result(IColumn& to, const DataTypePtr& return_type, int64_t place, JNIEnv* env,
                      IColumn& data_col, int64_t nullmap_address) const {
        jobject result_obj = env->CallNonvirtualObjectMethod(executor_obj, executor_cl,
                                                             executor_get_value_id, place);
        bool result_nullable = return_type->is_nullable();
        if (data_col.is_column_string()) {
            const ColumnString* str_col = check_and_get_column<ColumnString>(data_col);
            ColumnString::Chars& chars = const_cast<ColumnString::Chars&>(str_col->get_chars());
            ColumnString::Offsets& offsets =
                    const_cast<ColumnString::Offsets&>(str_col->get_offsets());
            int increase_buffer_size = 0;
            int64_t buffer_size = JniUtil::IncreaseReservedBufferSize(increase_buffer_size);
            chars.resize(buffer_size);
            env->CallNonvirtualVoidMethod(
                    executor_obj, executor_cl, executor_copy_basic_result_id, result_obj,
                    to.size() - 1, nullmap_address, reinterpret_cast<int64_t>(chars.data()),
                    reinterpret_cast<int64_t>(&chars), reinterpret_cast<int64_t>(offsets.data()));
        } else if (data_col.is_numeric() || data_col.is_column_decimal()) {
            env->CallNonvirtualVoidMethod(executor_obj, executor_cl, executor_copy_basic_result_id,
                                          result_obj, to.size() - 1, nullmap_address,
                                          reinterpret_cast<int64_t>(data_col.get_raw_data().data),
                                          0, 0);
        } else if (data_col.is_column_array()) {
            jclass arraylist_class = env->FindClass("Ljava/util/ArrayList;");
            ColumnArray* array_col = assert_cast<ColumnArray*>(&data_col);
            ColumnNullable& array_nested_nullable =
                    assert_cast<ColumnNullable&>(array_col->get_data());
            auto data_column_null_map = array_nested_nullable.get_null_map_column_ptr();
            auto data_column = array_nested_nullable.get_nested_column_ptr();
            auto& offset_column = array_col->get_offsets_column();
            auto offset_address = reinterpret_cast<int64_t>(offset_column.get_raw_data().data);
            auto& null_map_data =
                    assert_cast<ColumnVector<UInt8>*>(data_column_null_map.get())->get_data();
            auto nested_nullmap_address = reinterpret_cast<int64_t>(null_map_data.data());
            jmethodID list_size = env->GetMethodID(arraylist_class, "size", "()I");

            size_t has_put_element_size = array_col->get_offsets().back();
            size_t arrar_list_size = env->CallIntMethod(result_obj, list_size);
            size_t element_size = has_put_element_size + arrar_list_size;
            array_nested_nullable.resize(element_size);
            memset(null_map_data.data() + has_put_element_size, 0, arrar_list_size);
            int64_t nested_data_address = 0, nested_offset_address = 0;
            if (data_column->is_column_string()) {
                ColumnString* str_col = assert_cast<ColumnString*>(data_column.get());
                ColumnString::Chars& chars =
                        assert_cast<ColumnString::Chars&>(str_col->get_chars());
                ColumnString::Offsets& offsets =
                        assert_cast<ColumnString::Offsets&>(str_col->get_offsets());
                nested_data_address = reinterpret_cast<int64_t>(&chars);
                nested_offset_address = reinterpret_cast<int64_t>(offsets.data());
            } else {
                nested_data_address = reinterpret_cast<int64_t>(data_column->get_raw_data().data);
            }
            int row = to.size() - 1;
            env->CallNonvirtualVoidMethod(executor_obj, executor_cl, executor_copy_array_result_id,
                                          has_put_element_size, result_nullable, row, result_obj,
                                          nullmap_address, offset_address, nested_nullmap_address,
                                          nested_data_address, nested_offset_address);
            env->DeleteLocalRef(arraylist_class);
        } else if (data_col.is_column_map()) {
            jclass hashmap_class = env->FindClass("Ljava/util/HashMap;");
            ColumnMap* map_col = assert_cast<ColumnMap*>(&data_col);
            auto& offset_column = map_col->get_offsets_column();
            auto offset_address = reinterpret_cast<int64_t>(offset_column.get_raw_data().data);
            ColumnNullable& map_key_column_nullable =
                    assert_cast<ColumnNullable&>(map_col->get_keys());
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
            auto value_nested_nullmap_address =
                    reinterpret_cast<int64_t>(value_null_map_data.data());
            jmethodID map_size = env->GetMethodID(hashmap_class, "size", "()I");
            size_t has_put_element_size = map_col->get_offsets().back();
            size_t hashmap_size = env->CallIntMethod(result_obj, map_size);
            size_t element_size = has_put_element_size + hashmap_size;
            map_key_column_nullable.resize(element_size);
            memset(key_null_map_data.data() + has_put_element_size, 0, hashmap_size);
            map_value_column_nullable.resize(element_size);
            memset(value_null_map_data.data() + has_put_element_size, 0, hashmap_size);

            int64_t key_nested_data_address = 0, key_nested_offset_address = 0;
            if (key_data_column->is_column_string()) {
                ColumnString* str_col = assert_cast<ColumnString*>(key_data_column.get());
                ColumnString::Chars& chars =
                        assert_cast<ColumnString::Chars&>(str_col->get_chars());
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
                ColumnString::Chars& chars =
                        assert_cast<ColumnString::Chars&>(str_col->get_chars());
                ColumnString::Offsets& offsets =
                        assert_cast<ColumnString::Offsets&>(str_col->get_offsets());
                value_nested_data_address = reinterpret_cast<int64_t>(&chars);
                value_nested_offset_address = reinterpret_cast<int64_t>(offsets.data());
            } else {
                value_nested_data_address =
                        reinterpret_cast<int64_t>(value_data_column->get_raw_data().data);
            }
            int row = to.size() - 1;
            env->CallNonvirtualVoidMethod(executor_obj, executor_cl, executor_copy_map_result_id,
                                          has_put_element_size, result_nullable, row, result_obj,
                                          nullmap_address, offset_address,
                                          key_nested_nullmap_address, key_nested_data_address,
                                          key_nested_offset_address, value_nested_nullmap_address,
                                          value_nested_data_address, value_nested_offset_address);
            env->DeleteLocalRef(hashmap_class);
        } else {
            return Status::InvalidArgument(strings::Substitute(
                    "Java UDAF doesn't support return type is $0 now !", return_type->get_name()));
        }
        return Status::OK();
    }

    Status register_func_id(JNIEnv* env) {
        auto register_id = [&](const char* func_name, const char* func_sign, jmethodID& func_id) {
            func_id = env->GetMethodID(executor_cl, func_name, func_sign);
            Status s = JniUtil::GetJniExceptionMsg(env);
            if (!s.ok()) {
                return Status::InternalError(strings::Substitute(
                        "Java-Udaf register_func_id meet error and error is $0", s.to_string()));
            }
            return s;
        };
        RETURN_IF_ERROR(register_id("<init>", UDAF_EXECUTOR_CTOR_SIGNATURE, executor_ctor_id));
        RETURN_IF_ERROR(register_id("reset", UDAF_EXECUTOR_RESET_SIGNATURE, executor_reset_id));
        RETURN_IF_ERROR(register_id("close", UDAF_EXECUTOR_CLOSE_SIGNATURE, executor_close_id));
        RETURN_IF_ERROR(register_id("merge", UDAF_EXECUTOR_MERGE_SIGNATURE, executor_merge_id));
        RETURN_IF_ERROR(
                register_id("serialize", UDAF_EXECUTOR_SERIALIZE_SIGNATURE, executor_serialize_id));
        RETURN_IF_ERROR(register_id("getValue", "(J)Ljava/lang/Object;", executor_get_value_id));
        RETURN_IF_ERROR(
                register_id("destroy", UDAF_EXECUTOR_DESTROY_SIGNATURE, executor_destroy_id));
        RETURN_IF_ERROR(register_id("convertBasicArguments", "(IZIIJJJ)[Ljava/lang/Object;",
                                    executor_convert_basic_argument_id));
        RETURN_IF_ERROR(register_id("convertArrayArguments", "(IZIIJJJJJ)[Ljava/lang/Object;",
                                    executor_convert_array_argument_id));
        RETURN_IF_ERROR(register_id("convertMapArguments", "(IZIIJJJJJJJJ)[Ljava/lang/Object;",
                                    executor_convert_map_argument_id));

        RETURN_IF_ERROR(register_id("copyTupleBasicResult", "(Ljava/lang/Object;IJJJJ)V",
                                    executor_copy_basic_result_id));

        RETURN_IF_ERROR(register_id("copyTupleArrayResult", "(JZILjava/lang/Object;JJJJJ)V",
                                    executor_copy_array_result_id));

        RETURN_IF_ERROR(register_id("copyTupleMapResult", "(JZILjava/lang/Object;JJJJJJJJ)V",
                                    executor_copy_map_result_id));

        RETURN_IF_ERROR(
                register_id("addBatch", "(ZIIJI[Ljava/lang/Object;)V", executor_add_batch_id));
        return Status::OK();
    }

private:
    // TODO: too many variables are hold, it's causing a lot of memory waste
    // it's time to refactor it.
    jclass executor_cl;
    jobject executor_obj;
    jmethodID executor_ctor_id;

    jmethodID executor_add_batch_id;
    jmethodID executor_merge_id;
    jmethodID executor_serialize_id;
    jmethodID executor_get_value_id;
    jmethodID executor_reset_id;
    jmethodID executor_close_id;
    jmethodID executor_destroy_id;
    jmethodID executor_convert_basic_argument_id;
    jmethodID executor_convert_array_argument_id;
    jmethodID executor_convert_map_argument_id;
    jmethodID executor_copy_basic_result_id;
    jmethodID executor_copy_array_result_id;
    jmethodID executor_copy_map_result_id;
    int argument_size = 0;
    std::string serialize_data;
};

class AggregateJavaUdaf final
        : public IAggregateFunctionDataHelper<AggregateJavaUdafData, AggregateJavaUdaf> {
public:
    ENABLE_FACTORY_CREATOR(AggregateJavaUdaf);
    AggregateJavaUdaf(const TFunction& fn, const DataTypes& argument_types,
                      const DataTypePtr& return_type)
            : IAggregateFunctionDataHelper(argument_types),
              _fn(fn),
              _return_type(return_type),
              _first_created(true),
              _exec_place(nullptr) {}
    ~AggregateJavaUdaf() override = default;

    static AggregateFunctionPtr create(const TFunction& fn, const DataTypes& argument_types,
                                       const DataTypePtr& return_type) {
        return std::make_shared<AggregateJavaUdaf>(fn, argument_types, return_type);
    }
    //Note: The condition is added because maybe the BE can't find java-udaf impl jar
    //So need to check as soon as possible, before call Data function
    Status check_udaf(const TFunction& fn) {
        auto function_cache = UserFunctionCache::instance();
        return function_cache->get_jarpath(fn.id, fn.hdfs_location, fn.checksum, &_local_location);
    }

    void create(AggregateDataPtr __restrict place) const override {
        if (_first_created) {
            new (place) Data(argument_types.size());
            Status status = Status::OK();
            SAFE_CREATE(RETURN_IF_STATUS_ERROR(status,
                                               this->data(place).init_udaf(_fn, _local_location)),
                        {
                            static_cast<void>(this->data(place).destroy());
                            this->data(place).~Data();
                        });
            _first_created = false;
            _exec_place = place;
        }
    }

    // To avoid multiple times JNI call, Here will destroy all data at once
    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        if (place == _exec_place) {
            static_cast<void>(this->data(_exec_place).destroy());
            this->data(_exec_place).~Data();
            _first_created = true;
        }
    }

    String get_name() const override { return _fn.name.function_name; }

    DataTypePtr get_return_type() const override { return _return_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        int64_t places_address = reinterpret_cast<int64_t>(place);
        Status st = this->data(_exec_place)
                            .add(places_address, true, columns, row_num, row_num + 1,
                                 argument_types, 0);
        if (UNLIKELY(!st.ok())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
        }
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena* /*arena*/, bool /*agg_many*/) const override {
        int64_t places_address = reinterpret_cast<int64_t>(places);
        Status st = this->data(_exec_place)
                            .add(places_address, false, columns, 0, batch_size, argument_types,
                                 place_offset);
        if (UNLIKELY(!st.ok())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* /*arena*/) const override {
        int64_t places_address = reinterpret_cast<int64_t>(place);
        Status st = this->data(_exec_place)
                            .add(places_address, true, columns, 0, batch_size, argument_types, 0);
        if (UNLIKELY(!st.ok())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
        }
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);
        int64_t places_address = reinterpret_cast<int64_t>(place);
        Status st = this->data(_exec_place)
                            .add(places_address, true, columns, frame_start, frame_end,
                                 argument_types, 0);
        if (UNLIKELY(!st.ok())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
        }
    }

    void reset(AggregateDataPtr place) const override {
        Status st = this->data(_exec_place).reset(reinterpret_cast<int64_t>(place));
        if (UNLIKELY(!st.ok())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        Status st =
                this->data(_exec_place).merge(this->data(rhs), reinterpret_cast<int64_t>(place));
        if (UNLIKELY(!st.ok())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        Status st = this->data(const_cast<AggregateDataPtr&>(_exec_place))
                            .write(buf, reinterpret_cast<int64_t>(place));
        if (UNLIKELY(!st.ok())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
        }
    }

    // during merge-finalized phase, for deserialize and merge firstly,
    // will call create --- deserialize --- merge --- destory for each rows ,
    // so need doing new (place), to create Data and read to buf, then call merge ,
    // and during destory about deserialize, because haven't done init_udaf,
    // so it's can't call ~Data, only to change _destory_deserialize flag.
    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        new (place) Data(argument_types.size());
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        Status st = this->data(_exec_place).get(to, _return_type, reinterpret_cast<int64_t>(place));
        if (UNLIKELY(!st.ok())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
        }
    }

private:
    TFunction _fn;
    DataTypePtr _return_type;
    mutable bool _first_created;
    mutable AggregateDataPtr _exec_place;
    std::string _local_location;
};

} // namespace doris::vectorized
