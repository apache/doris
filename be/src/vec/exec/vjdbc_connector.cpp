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

#include "vec/exec/vjdbc_connector.h"
#ifdef LIBJVM
#include "exec/table_connector.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "jni.h"
#include "runtime/user_function_cache.h"
#include "util/jni-util.h"
#include "vec/columns/column_nullable.h"
#include "vec/exprs/vexpr.h"

namespace doris {
namespace vectorized {
const char* JDBC_EXECUTOR_CLASS = "org/apache/doris/udf/JdbcExecutor";
const char* JDBC_EXECUTOR_CTOR_SIGNATURE = "([B)V";
const char* JDBC_EXECUTOR_QUERYSQL_SIGNATURE = "(Ljava/lang/String;)I";
const char* JDBC_EXECUTOR_HAS_NEXT_SIGNATURE = "()Z";
const char* JDBC_EXECUTOR_GET_BLOCK_SIGNATURE = "(I)Ljava/util/List;";
const char* JDBC_EXECUTOR_CLOSE_SIGNATURE = "()V";
const char* JDBC_EXECUTOR_CONVERT_DATE_SIGNATURE = "(Ljava/lang/Object;)J";
const char* JDBC_EXECUTOR_CONVERT_DATETIME_SIGNATURE = "(Ljava/lang/Object;)J";
const char* JDBC_EXECUTOR_TRANSACTION_SIGNATURE = "()V";

JdbcConnector::JdbcConnector(const JdbcConnectorParam& param)
        : TableConnector(param.tuple_desc, param.query_string), _conn_param(param) {}

JdbcConnector::~JdbcConnector() {
    if (!_is_open) {
        return;
    }
    if (_is_in_transaction) {
        abort_trans();
    }
    JNIEnv* env;
    Status status;
    RETURN_IF_STATUS_ERROR(status, JniUtil::GetJNIEnv(&env));
    env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_close_id);
    RETURN_IF_STATUS_ERROR(status, JniUtil::GetJniExceptionMsg(env));
    env->DeleteGlobalRef(_executor_obj);
}

Status JdbcConnector::open() {
    if (_is_open) {
        LOG(INFO) << "this scanner of jdbc already opened";
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, JDBC_EXECUTOR_CLASS, &_executor_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/util/List", &_executor_list_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Object", &_executor_object_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Boolean", &_executor_uint8_t_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Byte", &_executor_int8_t_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Short", &_executor_int16_t_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Integer", &_executor_int32_t_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Long", &_executor_int64_t_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Float", &_executor_float_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Float", &_executor_double_clazz));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/String", &_executor_string_clazz));
    RETURN_IF_ERROR(_register_func_id(env));

    // Add a scoped cleanup jni reference object. This cleans up local refs made below.
    JniLocalFrame jni_frame;
    {
        std::string local_location;
        std::hash<std::string> hash_str;
        auto function_cache = UserFunctionCache::instance();
        RETURN_IF_ERROR(function_cache->get_jarpath(
                std::abs((int64_t)hash_str(_conn_param.resource_name)), _conn_param.driver_path,
                _conn_param.driver_checksum, &local_location));
        TJdbcExecutorCtorParams ctor_params;
        ctor_params.__set_jar_location_path(local_location);
        ctor_params.__set_jdbc_url(_conn_param.jdbc_url);
        ctor_params.__set_jdbc_user(_conn_param.user);
        ctor_params.__set_jdbc_password(_conn_param.passwd);
        ctor_params.__set_jdbc_driver_class(_conn_param.driver_class);

        jbyteArray ctor_params_bytes;
        // Pushed frame will be popped when jni_frame goes out-of-scope.
        RETURN_IF_ERROR(jni_frame.push(env));
        RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));
        _executor_obj = env->NewObject(_executor_clazz, _executor_ctor_id, ctor_params_bytes);
    }
    RETURN_ERROR_IF_EXC(env);
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, _executor_obj, &_executor_obj));
    _is_open = true;
    return Status::OK();
}

Status JdbcConnector::query() {
    if (!_is_open) {
        return Status::InternalError("Query before open of JdbcConnector.");
    }
    // check materialize num equal
    int materialize_num = 0;
    for (int i = 0; i < _tuple_desc->slots().size(); ++i) {
        if (_tuple_desc->slots()[i]->is_materialized()) {
            materialize_num++;
        }
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    jstring query_sql = env->NewStringUTF(_sql_str.c_str());
    jint colunm_count = env->CallNonvirtualIntMethod(_executor_obj, _executor_clazz,
                                                     _executor_query_id, query_sql);
    env->DeleteLocalRef(query_sql);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));

    if (colunm_count != materialize_num) {
        return Status::InternalError("input and output column num not equal of jdbc query.");
    }
    return Status::OK();
}

Status JdbcConnector::get_next(bool* eos, std::vector<MutableColumnPtr>& columns, int batch_size) {
    if (!_is_open) {
        return Status::InternalError("get_next before open of jdbc connector.");
    }
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    jboolean has_next =
            env->CallNonvirtualBooleanMethod(_executor_obj, _executor_clazz, _executor_has_next_id);
    if (has_next != JNI_TRUE) {
        *eos = true;
        return Status::OK();
    }

    jobject block_obj = env->CallNonvirtualObjectMethod(_executor_obj, _executor_clazz,
                                                        _executor_get_blocks_id, batch_size);

    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));

    auto column_size = _tuple_desc->slots().size();
    for (int column_index = 0, materialized_column_index = 0; column_index < column_size;
         ++column_index) {
        auto slot_desc = _tuple_desc->slots()[column_index];
        // because the fe planner filter the non_materialize column
        if (!slot_desc->is_materialized()) {
            continue;
        }
        jobject column_data =
                env->CallObjectMethod(block_obj, _executor_get_list_id, materialized_column_index);
        jint num_rows = env->CallIntMethod(column_data, _executor_get_list_size_id);

        for (int row = 0; row < num_rows; ++row) {
            jobject cur_data = env->CallObjectMethod(column_data, _executor_get_list_id, row);
            _convert_column_data(env, cur_data, slot_desc, columns[column_index].get());
        }

        materialized_column_index++;
    }
    return JniUtil::GetJniExceptionMsg(env);
}

Status JdbcConnector::_register_func_id(JNIEnv* env) {
    auto register_id = [&](jclass clazz, const char* func_name, const char* func_sign,
                           jmethodID& func_id) {
        func_id = env->GetMethodID(clazz, func_name, func_sign);
        Status s = JniUtil::GetJniExceptionMsg(env);
        if (!s.ok()) {
            return Status::InternalError(strings::Substitute(
                    "Jdbc connector _register_func_id meet error and error is $0",
                    s.get_error_msg()));
        }
        return s;
    };

    RETURN_IF_ERROR(register_id(_executor_clazz, "<init>", JDBC_EXECUTOR_CTOR_SIGNATURE,
                                _executor_ctor_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "querySQL", JDBC_EXECUTOR_QUERYSQL_SIGNATURE,
                                _executor_query_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "close", JDBC_EXECUTOR_CLOSE_SIGNATURE,
                                _executor_close_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "hasNext", JDBC_EXECUTOR_HAS_NEXT_SIGNATURE,
                                _executor_has_next_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "getBlock", JDBC_EXECUTOR_GET_BLOCK_SIGNATURE,
                                _executor_get_blocks_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "convertDateToLong",
                                JDBC_EXECUTOR_CONVERT_DATE_SIGNATURE, _executor_convert_date_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "convertDateTimeToLong",
                                JDBC_EXECUTOR_CONVERT_DATETIME_SIGNATURE,
                                _executor_convert_datetime_id));
    RETURN_IF_ERROR(register_id(_executor_list_clazz, "get", "(I)Ljava/lang/Object;",
                                _executor_get_list_id));
    RETURN_IF_ERROR(register_id(_executor_list_clazz, "size", "()I", _executor_get_list_size_id));
    RETURN_IF_ERROR(register_id(_executor_string_clazz, "getBytes", "(Ljava/lang/String;)[B",
                                _get_bytes_id));
    RETURN_IF_ERROR(
            register_id(_executor_object_clazz, "toString", "()Ljava/lang/String;", _to_string_id));

    RETURN_IF_ERROR(register_id(_executor_clazz, "openTrans", JDBC_EXECUTOR_TRANSACTION_SIGNATURE,
                                _executor_begin_trans_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "commitTrans", JDBC_EXECUTOR_TRANSACTION_SIGNATURE,
                                _executor_finish_trans_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "rollbackTrans",
                                JDBC_EXECUTOR_TRANSACTION_SIGNATURE, _executor_abort_trans_id));
    return Status::OK();
}

Status JdbcConnector::_convert_column_data(JNIEnv* env, jobject jobj,
                                           const SlotDescriptor* slot_desc,
                                           vectorized::IColumn* column_ptr) {
    vectorized::IColumn* col_ptr = column_ptr;
    if (true == slot_desc->is_nullable()) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
        if (jobj == nullptr) {
            nullable_column->insert_data(nullptr, 0);
            return Status::OK();
        } else {
            nullable_column->get_null_map_data().push_back(0);
            col_ptr = &nullable_column->get_nested_column();
        }
    }

    switch (slot_desc->type().type) {
    case TYPE_BOOLEAN: {
        uint8_t num = _jobject_to_uint8_t(env, jobj);
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt8>*>(col_ptr)->insert_value(
                (uint8_t)num);
        break;
    }
    case TYPE_TINYINT: {
        int8_t num = _jobject_to_int8_t(env, jobj);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int8>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_SMALLINT: {
        int16_t num = _jobject_to_int16_t(env, jobj);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int16>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_INT: {
        int32_t num = _jobject_to_int32_t(env, jobj);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_BIGINT: {
        int64_t num = _jobject_to_int64_t(env, jobj);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_FLOAT: {
        float num = _jobject_to_float(env, jobj);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float32>*>(col_ptr)->insert_value(
                num);
        break;
    }
    case TYPE_DOUBLE: {
        double num = _jobject_to_double(env, jobj);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float64>*>(col_ptr)->insert_value(
                num);
        break;
    }

    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        std::string data = _jobject_to_string(env, jobj);
        reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(data.c_str(),
                                                                          data.length());
        break;
    }

    case TYPE_DATE: {
        int64_t num = _jobject_to_date(env, jobj);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_DATETIME: {
        int64_t num = _jobject_to_datetime(env, jobj);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_DECIMALV2: {
        std::string data = _jobject_to_string(env, jobj);
        DecimalV2Value decimal_slot;
        decimal_slot.parse_from_str(data.c_str(), data.length());
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)->insert_value(
                decimal_slot.value());
        break;
    }
    default: {
        std::string error_msg =
                fmt::format("Fail to convert jdbc value to {} on column: {}.",
                            slot_desc->type().debug_string(), slot_desc->col_name());
        return Status::InternalError(std::string(error_msg));
    }
    }
    return Status::OK();
}

Status JdbcConnector::exec_write_sql(const std::u16string& insert_stmt,
                                     const fmt::memory_buffer& insert_stmt_buffer) {
    SCOPED_TIMER(_result_send_timer);
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    jstring query_sql = env->NewString((const jchar*)insert_stmt.c_str(), insert_stmt.size());
    env->CallNonvirtualIntMethod(_executor_obj, _executor_clazz, _executor_query_id, query_sql);
    env->DeleteLocalRef(query_sql);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    return Status::OK();
}

std::string JdbcConnector::_jobject_to_string(JNIEnv* env, jobject jobj) {
    jobject jstr = env->CallObjectMethod(jobj, _to_string_id);
    const jbyteArray stringJbytes =
            (jbyteArray)env->CallObjectMethod(jstr, _get_bytes_id, env->NewStringUTF("UTF-8"));
    size_t length = (size_t)env->GetArrayLength(stringJbytes);
    jbyte* pBytes = env->GetByteArrayElements(stringJbytes, nullptr);
    std::string str = std::string((char*)pBytes, length);
    env->ReleaseByteArrayElements(stringJbytes, pBytes, JNI_ABORT);
    env->DeleteLocalRef(stringJbytes);
    return str;
}

int64_t JdbcConnector::_jobject_to_date(JNIEnv* env, jobject jobj) {
    return env->CallNonvirtualLongMethod(_executor_obj, _executor_clazz, _executor_convert_date_id,
                                         jobj);
}

int64_t JdbcConnector::_jobject_to_datetime(JNIEnv* env, jobject jobj) {
    return env->CallNonvirtualLongMethod(_executor_obj, _executor_clazz,
                                         _executor_convert_datetime_id, jobj);
}

Status JdbcConnector::begin_trans() {
    if (!_is_open) {
        return Status::InternalError("Begin transaction before open.");
    }
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_begin_trans_id);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    _is_in_transaction = true;
    return Status::OK();
}

Status JdbcConnector::abort_trans() {
    if (!_is_in_transaction) {
        return Status::InternalError("Abort transaction before begin trans.");
    }
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_abort_trans_id);
    return JniUtil::GetJniExceptionMsg(env);
}

Status JdbcConnector::finish_trans() {
    if (!_is_in_transaction) {
        return Status::InternalError("Abort transaction before begin trans.");
    }
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_finish_trans_id);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    _is_in_transaction = false;
    return Status::OK();
}

#define FUNC_IMPL_TO_CONVERT_DATA(cpp_return_type, java_type, sig, java_return_type)          \
    cpp_return_type JdbcConnector::_jobject_to_##cpp_return_type(JNIEnv* env, jobject jobj) { \
        jmethodID method_id_##cpp_return_type = env->GetMethodID(                             \
                _executor_##cpp_return_type##_clazz, #java_type "Value", "()" #sig);          \
        return env->Call##java_return_type##Method(jobj, method_id_##cpp_return_type);        \
    }

FUNC_IMPL_TO_CONVERT_DATA(uint8_t, boolean, Z, Boolean)
FUNC_IMPL_TO_CONVERT_DATA(int8_t, byte, B, Byte)
FUNC_IMPL_TO_CONVERT_DATA(int16_t, short, S, Short)
FUNC_IMPL_TO_CONVERT_DATA(int32_t, int, I, Int)
FUNC_IMPL_TO_CONVERT_DATA(int64_t, long, J, Long)
FUNC_IMPL_TO_CONVERT_DATA(float, float, F, Float)
FUNC_IMPL_TO_CONVERT_DATA(double, double, D, Double)

} // namespace vectorized
} // namespace doris

#endif