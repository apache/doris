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

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "runtime/user_function_cache.h"
#include "util/jni-util.h"
#include "vec/aggregate_functions/aggregate_function.h"
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
// Calling Java method about those signature means: "(argument-types)return-type"
// https://www.iitk.ac.in/esc101/05Aug/tutorial/native1.1/implementing/method.html

struct AggregateJavaUdafData {
public:
    AggregateJavaUdafData() = default;
    AggregateJavaUdafData(int64_t num_args) {
        argument_size = num_args;
        input_values_buffer_ptr.reset(new int64_t[num_args]);
        input_nulls_buffer_ptr.reset(new int64_t[num_args]);
        input_offsets_ptrs.reset(new int64_t[num_args]);
        input_place_ptrs.reset(new int64_t);
        output_value_buffer.reset(new int64_t);
        output_null_value.reset(new int64_t);
        output_offsets_ptr.reset(new int64_t);
        output_intermediate_state_ptr.reset(new int64_t);
    }

    ~AggregateJavaUdafData() {
        JNIEnv* env;
        Status status;
        RETURN_IF_STATUS_ERROR(status, JniUtil::GetJNIEnv(&env));
        env->CallNonvirtualVoidMethod(executor_obj, executor_cl, executor_close_id);
        RETURN_IF_STATUS_ERROR(status, JniUtil::GetJniExceptionMsg(env));
        env->DeleteGlobalRef(executor_cl);
        env->DeleteGlobalRef(executor_obj);
    }

    Status init_udaf(const TFunction& fn) {
        JNIEnv* env = nullptr;
        RETURN_NOT_OK_STATUS_WITH_WARN(JniUtil::GetJNIEnv(&env), "Java-Udaf init_udaf function");
        RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, UDAF_EXECUTOR_CLASS, &executor_cl));
        RETURN_NOT_OK_STATUS_WITH_WARN(register_func_id(env),
                                       "Java-Udaf register_func_id function");

        // Add a scoped cleanup jni reference object. This cleans up local refs made below.
        JniLocalFrame jni_frame;
        {
            std::string local_location;
            auto function_cache = UserFunctionCache::instance();
            RETURN_IF_ERROR(function_cache->get_jarpath(fn.id, fn.hdfs_location, fn.checksum,
                                                        &local_location));
            TJavaUdfExecutorCtorParams ctor_params;
            ctor_params.__set_fn(fn);
            ctor_params.__set_location(local_location);
            ctor_params.__set_input_offsets_ptrs((int64_t)input_offsets_ptrs.get());
            ctor_params.__set_input_buffer_ptrs((int64_t)input_values_buffer_ptr.get());
            ctor_params.__set_input_nulls_ptrs((int64_t)input_nulls_buffer_ptr.get());
            ctor_params.__set_output_buffer_ptr((int64_t)output_value_buffer.get());
            ctor_params.__set_input_places_ptr((int64_t)input_place_ptrs.get());

            ctor_params.__set_output_null_ptr((int64_t)output_null_value.get());
            ctor_params.__set_output_offsets_ptr((int64_t)output_offsets_ptr.get());
            ctor_params.__set_output_intermediate_state_ptr(
                    (int64_t)output_intermediate_state_ptr.get());

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

    Status add(const int64_t places_address[], bool is_single_place, const IColumn** columns,
               size_t row_num_start, size_t row_num_end, const DataTypes& argument_types) {
        JNIEnv* env = nullptr;
        RETURN_NOT_OK_STATUS_WITH_WARN(JniUtil::GetJNIEnv(&env), "Java-Udaf add function");
        for (int arg_idx = 0; arg_idx < argument_size; ++arg_idx) {
            auto data_col = columns[arg_idx];
            if (auto* nullable = check_and_get_column<const ColumnNullable>(*columns[arg_idx])) {
                data_col = nullable->get_nested_column_ptr();
                auto null_col = check_and_get_column<ColumnVector<UInt8>>(
                        nullable->get_null_map_column_ptr());
                input_nulls_buffer_ptr.get()[arg_idx] =
                        reinterpret_cast<int64_t>(null_col->get_data().data());
            } else {
                input_nulls_buffer_ptr.get()[arg_idx] = -1;
            }
            if (data_col->is_column_string()) {
                const ColumnString* str_col = check_and_get_column<ColumnString>(data_col);
                input_values_buffer_ptr.get()[arg_idx] =
                        reinterpret_cast<int64_t>(str_col->get_chars().data());
                input_offsets_ptrs.get()[arg_idx] =
                        reinterpret_cast<int64_t>(str_col->get_offsets().data());
            } else if (data_col->is_numeric() || data_col->is_column_decimal()) {
                input_values_buffer_ptr.get()[arg_idx] =
                        reinterpret_cast<int64_t>(data_col->get_raw_data().data);
            } else {
                return Status::InvalidArgument(
                        strings::Substitute("Java UDAF doesn't support type is $0 now !",
                                            argument_types[arg_idx]->get_name()));
            }
        }
        *input_place_ptrs = reinterpret_cast<int64_t>(places_address);
        env->CallNonvirtualVoidMethod(executor_obj, executor_cl, executor_add_id, is_single_place,
                                      row_num_start, row_num_end);
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
        int len = env->GetArrayLength(arr);
        serialize_data.resize(len);
        env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte*>(serialize_data.data()));
        write_binary(serialize_data, buf);
        jbyte* pBytes = env->GetByteArrayElements(arr, nullptr);
        env->ReleaseByteArrayElements(arr, pBytes, JNI_ABORT);
        env->DeleteLocalRef(arr);
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
        if (result_type->is_nullable()) {
            auto& nullable = assert_cast<ColumnNullable&>(to);
            *output_null_value =
                    reinterpret_cast<int64_t>(nullable.get_null_map_column().get_raw_data().data);
            auto& data_col = nullable.get_nested_column();

#ifndef EVALUATE_JAVA_UDAF
#define EVALUATE_JAVA_UDAF                                                                         \
    if (data_col.is_column_string()) {                                                             \
        const ColumnString* str_col = check_and_get_column<ColumnString>(data_col);                \
        ColumnString::Chars& chars = const_cast<ColumnString::Chars&>(str_col->get_chars());       \
        ColumnString::Offsets& offsets =                                                           \
                const_cast<ColumnString::Offsets&>(str_col->get_offsets());                        \
        int increase_buffer_size = 0;                                                              \
        int32_t buffer_size = JniUtil::IncreaseReservedBufferSize(increase_buffer_size);           \
        chars.resize(buffer_size);                                                                 \
        *output_value_buffer = reinterpret_cast<int64_t>(chars.data());                            \
        *output_offsets_ptr = reinterpret_cast<int64_t>(offsets.data());                           \
        *output_intermediate_state_ptr = chars.size();                                             \
        jboolean res = env->CallNonvirtualBooleanMethod(executor_obj, executor_cl,                 \
                                                        executor_result_id, to.size() - 1, place); \
        while (res != JNI_TRUE) {                                                                  \
            increase_buffer_size++;                                                                \
            int32_t buffer_size = JniUtil::IncreaseReservedBufferSize(increase_buffer_size);       \
            chars.resize(buffer_size);                                                             \
            *output_value_buffer = reinterpret_cast<int64_t>(chars.data());                        \
            *output_intermediate_state_ptr = chars.size();                                         \
            res = env->CallNonvirtualBooleanMethod(executor_obj, executor_cl, executor_result_id,  \
                                                   to.size() - 1, place);                          \
        }                                                                                          \
    } else if (data_col.is_numeric() || data_col.is_column_decimal()) {                            \
        *output_value_buffer = reinterpret_cast<int64_t>(data_col.get_raw_data().data);            \
        env->CallNonvirtualBooleanMethod(executor_obj, executor_cl, executor_result_id,            \
                                         to.size() - 1, place);                                    \
    } else {                                                                                       \
        return Status::InvalidArgument(strings::Substitute(                                        \
                "Java UDAF doesn't support return type is $0 now !", result_type->get_name()));    \
    }
#endif
            EVALUATE_JAVA_UDAF;
        } else {
            *output_null_value = -1;
            auto& data_col = to;
            EVALUATE_JAVA_UDAF;
        }
        return JniUtil::GetJniExceptionMsg(env);
    }

private:
    Status register_func_id(JNIEnv* env) {
        auto register_id = [&](const char* func_name, const char* func_sign, jmethodID& func_id) {
            func_id = env->GetMethodID(executor_cl, func_name, func_sign);
            Status s = JniUtil::GetJniExceptionMsg(env);
            if (!s.ok()) {
                return Status::InternalError(
                        strings::Substitute("Java-Udaf register_func_id meet error and error is $0",
                                            s.get_error_msg()));
            }
            return s;
        };

        RETURN_IF_ERROR(register_id("<init>", UDAF_EXECUTOR_CTOR_SIGNATURE, executor_ctor_id));
        RETURN_IF_ERROR(register_id("add", UDAF_EXECUTOR_ADD_SIGNATURE, executor_add_id));
        RETURN_IF_ERROR(register_id("close", UDAF_EXECUTOR_CLOSE_SIGNATURE, executor_close_id));
        RETURN_IF_ERROR(register_id("merge", UDAF_EXECUTOR_MERGE_SIGNATURE, executor_merge_id));
        RETURN_IF_ERROR(
                register_id("serialize", UDAF_EXECUTOR_SERIALIZE_SIGNATURE, executor_serialize_id));
        RETURN_IF_ERROR(
                register_id("getValue", UDAF_EXECUTOR_RESULT_SIGNATURE, executor_result_id));
        RETURN_IF_ERROR(
                register_id("destroy", UDAF_EXECUTOR_DESTROY_SIGNATURE, executor_destroy_id));
        return Status::OK();
    }

private:
    // TODO: too many variables are hold, it's causing a lot of memory waste
    // it's time to refactor it.
    jclass executor_cl;
    jobject executor_obj;
    jmethodID executor_ctor_id;

    jmethodID executor_add_id;
    jmethodID executor_merge_id;
    jmethodID executor_serialize_id;
    jmethodID executor_result_id;
    jmethodID executor_close_id;
    jmethodID executor_destroy_id;

    std::unique_ptr<int64_t[]> input_values_buffer_ptr;
    std::unique_ptr<int64_t[]> input_nulls_buffer_ptr;
    std::unique_ptr<int64_t[]> input_offsets_ptrs;
    std::unique_ptr<int64_t> input_place_ptrs;
    std::unique_ptr<int64_t> output_value_buffer;
    std::unique_ptr<int64_t> output_null_value;
    std::unique_ptr<int64_t> output_offsets_ptr;
    std::unique_ptr<int64_t> output_intermediate_state_ptr;

    int argument_size = 0;
    std::string serialize_data;
};

class AggregateJavaUdaf final
        : public IAggregateFunctionDataHelper<AggregateJavaUdafData, AggregateJavaUdaf> {
public:
    AggregateJavaUdaf(const TFunction& fn, const DataTypes& argument_types, const Array& parameters,
                      const DataTypePtr& return_type)
            : IAggregateFunctionDataHelper(argument_types, parameters),
              _fn(fn),
              _return_type(return_type),
              _first_created(true),
              _exec_place(nullptr) {}
    ~AggregateJavaUdaf() = default;

    static AggregateFunctionPtr create(const TFunction& fn, const DataTypes& argument_types,
                                       const Array& parameters, const DataTypePtr& return_type) {
        return std::make_shared<AggregateJavaUdaf>(fn, argument_types, parameters, return_type);
    }
    //Note: The condition is added because maybe the BE can't find java-udaf impl jar
    //So need to check as soon as possible, before call Data function
    Status check_udaf(const TFunction& fn) {
        auto function_cache = UserFunctionCache::instance();
        return function_cache->check_jar(fn.id, fn.hdfs_location, fn.checksum);
    }

    void create(AggregateDataPtr __restrict place) const override {
        if (_first_created) {
            new (place) Data(argument_types.size());
            Status status = Status::OK();
            RETURN_IF_STATUS_ERROR(status, this->data(place).init_udaf(_fn));
            _first_created = false;
            _exec_place = place;
        }
    }

    // To avoid multiple times JNI call, Here will destroy all data at once
    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        if (place == _exec_place) {
            this->data(_exec_place).destroy();
            this->data(_exec_place).~Data();
        }
    }

    String get_name() const override { return _fn.name.function_name; }

    DataTypePtr get_return_type() const override { return _return_type; }

    void add(AggregateDataPtr __restrict /*place*/, const IColumn** /*columns*/, size_t /*row_num*/,
             Arena*) const override {
        LOG(WARNING) << " shouldn't going add function, there maybe some error about function "
                     << _fn.name.function_name;
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t /*place_offset*/,
                   const IColumn** columns, Arena* /*arena*/, bool /*agg_many*/) const override {
        int64_t places_address[batch_size];
        for (size_t i = 0; i < batch_size; ++i) {
            places_address[i] = reinterpret_cast<int64_t>(places[i]);
        }
        this->data(_exec_place).add(places_address, false, columns, 0, batch_size, argument_types);
    }

    // TODO: Here we calling method by jni, And if we get a thrown from FE,
    // But can't let user known the error, only return directly and output error to log file.
    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* /*arena*/) const override {
        int64_t places_address[1];
        places_address[0] = reinterpret_cast<int64_t>(place);
        this->data(_exec_place).add(places_address, true, columns, 0, batch_size, argument_types);
    }

    // TODO: reset function should be implement also in struct data
    void reset(AggregateDataPtr /*place*/) const override {
        LOG(WARNING) << " shouldn't going reset function, there maybe some error about function "
                     << _fn.name.function_name;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(_exec_place).merge(this->data(rhs), reinterpret_cast<int64_t>(place));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(const_cast<AggregateDataPtr&>(_exec_place))
                .write(buf, reinterpret_cast<int64_t>(place));
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
        this->data(_exec_place).get(to, _return_type, reinterpret_cast<int64_t>(place));
    }

private:
    TFunction _fn;
    DataTypePtr _return_type;
    mutable bool _first_created;
    mutable AggregateDataPtr _exec_place;
};

} // namespace doris::vectorized
