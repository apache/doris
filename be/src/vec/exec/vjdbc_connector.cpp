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

#include "common/status.h"
#include "exec/table_connector.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "jni.h"
#include "runtime/define_primitive_type.h"
#include "runtime/user_function_cache.h"
#include "util/jni-util.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
namespace vectorized {
const char* JDBC_EXECUTOR_CLASS = "org/apache/doris/udf/JdbcExecutor";
const char* JDBC_EXECUTOR_CTOR_SIGNATURE = "([B)V";
const char* JDBC_EXECUTOR_WRITE_SIGNATURE = "(Ljava/lang/String;)I";
const char* JDBC_EXECUTOR_HAS_NEXT_SIGNATURE = "()Z";
const char* JDBC_EXECUTOR_GET_BLOCK_SIGNATURE = "(I)Ljava/util/List;";
const char* JDBC_EXECUTOR_GET_TYPES_SIGNATURE = "()Ljava/util/List;";
const char* JDBC_EXECUTOR_GET_ARR_LIST_SIGNATURE = "(Ljava/lang/Object;)Ljava/util/List;";
const char* JDBC_EXECUTOR_GET_ARR_TYPE_SIGNATURE = "()I";
const char* JDBC_EXECUTOR_CLOSE_SIGNATURE = "()V";
const char* JDBC_EXECUTOR_CONVERT_DATE_SIGNATURE = "(Ljava/lang/Object;Z)J";
const char* JDBC_EXECUTOR_CONVERT_DATETIME_SIGNATURE = "(Ljava/lang/Object;Z)J";
const char* JDBC_EXECUTOR_TRANSACTION_SIGNATURE = "()V";

JdbcConnector::JdbcConnector(const JdbcConnectorParam& param)
        : TableConnector(param.tuple_desc, param.query_string),
          _conn_param(param),
          _closed(false) {}

JdbcConnector::~JdbcConnector() {
    if (!_closed) {
        close();
    }
}

#define GET_BASIC_JAVA_CLAZZ(JAVA_TYPE, CPP_TYPE) \
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, JAVA_TYPE, &_executor_##CPP_TYPE##_clazz));

#define DELETE_BASIC_JAVA_CLAZZ_REF(CPP_TYPE) env->DeleteGlobalRef(_executor_##CPP_TYPE##_clazz);

Status JdbcConnector::close() {
    _closed = true;
    if (!_is_open) {
        return Status::OK();
    }
    if (_is_in_transaction) {
        RETURN_IF_ERROR(abort_trans());
    }
    JNIEnv* env;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    env->DeleteGlobalRef(_executor_clazz);
    DELETE_BASIC_JAVA_CLAZZ_REF(object)
    DELETE_BASIC_JAVA_CLAZZ_REF(uint8_t)
    DELETE_BASIC_JAVA_CLAZZ_REF(int8_t)
    DELETE_BASIC_JAVA_CLAZZ_REF(int16_t)
    DELETE_BASIC_JAVA_CLAZZ_REF(int32_t)
    DELETE_BASIC_JAVA_CLAZZ_REF(int64_t)
    DELETE_BASIC_JAVA_CLAZZ_REF(float)
    DELETE_BASIC_JAVA_CLAZZ_REF(double)
    DELETE_BASIC_JAVA_CLAZZ_REF(string)
    DELETE_BASIC_JAVA_CLAZZ_REF(list)
#undef DELETE_BASIC_JAVA_CLAZZ_REF
    env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_close_id);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    env->DeleteGlobalRef(_executor_obj);
    return Status::OK();
}

Status JdbcConnector::open(RuntimeState* state, bool read) {
    if (_is_open) {
        LOG(INFO) << "this scanner of jdbc already opened";
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, JDBC_EXECUTOR_CLASS, &_executor_clazz));

    GET_BASIC_JAVA_CLAZZ("java/util/List", list)
    GET_BASIC_JAVA_CLAZZ("java/lang/Object", object)
    GET_BASIC_JAVA_CLAZZ("java/lang/Boolean", uint8_t)
    GET_BASIC_JAVA_CLAZZ("java/lang/Byte", int8_t)
    GET_BASIC_JAVA_CLAZZ("java/lang/Short", int16_t)
    GET_BASIC_JAVA_CLAZZ("java/lang/Integer", int32_t)
    GET_BASIC_JAVA_CLAZZ("java/lang/Long", int64_t)
    GET_BASIC_JAVA_CLAZZ("java/lang/Float", float)
    GET_BASIC_JAVA_CLAZZ("java/lang/Float", double)
    GET_BASIC_JAVA_CLAZZ("java/lang/String", string)

#undef GET_BASIC_JAVA_CLAZZ
    RETURN_IF_ERROR(_register_func_id(env));

    // Add a scoped cleanup jni reference object. This cleans up local refs made below.
    JniLocalFrame jni_frame;
    {
        std::string local_location;
        std::hash<std::string> hash_str;
        auto function_cache = UserFunctionCache::instance();
        if (_conn_param.resource_name.empty()) {
            // for jdbcExternalTable, _conn_param.resource_name == ""
            // so, we use _conn_param.driver_path as key of jarpath
            RETURN_IF_ERROR(function_cache->get_jarpath(
                    std::abs((int64_t)hash_str(_conn_param.driver_path)), _conn_param.driver_path,
                    _conn_param.driver_checksum, &local_location));
        } else {
            RETURN_IF_ERROR(function_cache->get_jarpath(
                    std::abs((int64_t)hash_str(_conn_param.resource_name)), _conn_param.driver_path,
                    _conn_param.driver_checksum, &local_location));
        }

        TJdbcExecutorCtorParams ctor_params;
        ctor_params.__set_statement(_sql_str);
        ctor_params.__set_jdbc_url(_conn_param.jdbc_url);
        ctor_params.__set_jdbc_user(_conn_param.user);
        ctor_params.__set_jdbc_password(_conn_param.passwd);
        ctor_params.__set_jdbc_driver_class(_conn_param.driver_class);
        ctor_params.__set_driver_path(local_location);
        ctor_params.__set_batch_size(read ? state->batch_size() : 0);
        ctor_params.__set_op(read ? TJdbcOperation::READ : TJdbcOperation::WRITE);

        jbyteArray ctor_params_bytes;
        // Pushed frame will be popped when jni_frame goes out-of-scope.
        RETURN_IF_ERROR(jni_frame.push(env));
        RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));
        _executor_obj = env->NewObject(_executor_clazz, _executor_ctor_id, ctor_params_bytes);

        jbyte* pBytes = env->GetByteArrayElements(ctor_params_bytes, nullptr);
        env->ReleaseByteArrayElements(ctor_params_bytes, pBytes, JNI_ABORT);
        env->DeleteLocalRef(ctor_params_bytes);
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
    jint colunm_count =
            env->CallNonvirtualIntMethod(_executor_obj, _executor_clazz, _executor_read_id);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));

    if (colunm_count != materialize_num) {
        return Status::InternalError("input and output column num not equal of jdbc query.");
    }
    LOG(INFO) << "JdbcConnector::query has exec success: " << _sql_str;
    RETURN_IF_ERROR(_check_column_type());
    return Status::OK();
}

Status JdbcConnector::_check_column_type() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    jobject type_lists =
            env->CallNonvirtualObjectMethod(_executor_obj, _executor_clazz, _executor_get_types_id);
    auto column_size = _tuple_desc->slots().size();
    for (int column_index = 0, materialized_column_index = 0; column_index < column_size;
         ++column_index) {
        auto slot_desc = _tuple_desc->slots()[column_index];
        if (!slot_desc->is_materialized()) {
            continue;
        }
        jobject column_type =
                env->CallObjectMethod(type_lists, _executor_get_list_id, materialized_column_index);

        const std::string& type_str = _jobject_to_string(env, column_type);
        RETURN_IF_ERROR(_check_type(slot_desc, type_str, column_index));
        env->DeleteLocalRef(column_type);
        materialized_column_index++;
    }
    env->DeleteLocalRef(type_lists);
    return JniUtil::GetJniExceptionMsg(env);
}
/* type mapping: https://doris.apache.org/zh-CN/docs/dev/ecosystem/external-table/jdbc-of-doris?_highlight=jdbc

Doris            MYSQL                      PostgreSQL                  Oracle                      SQLServer

BOOLEAN      java.lang.Boolean          java.lang.Boolean                                       java.lang.Boolean
TINYINT      java.lang.Integer                                                                  java.lang.Short    
SMALLINT     java.lang.Integer          java.lang.Integer           java.math.BigDecimal        java.lang.Short    
INT          java.lang.Integer          java.lang.Integer           java.math.BigDecimal        java.lang.Integer
BIGINT       java.lang.Long             java.lang.Long                                          java.lang.Long
LARGET       java.math.BigInteger
DECIMAL      java.math.BigDecimal       java.math.BigDecimal        java.math.BigDecimal        java.math.BigDecimal
VARCHAR      java.lang.String           java.lang.String            java.lang.String            java.lang.String
DOUBLE       java.lang.Double           java.lang.Double            java.lang.Double            java.lang.Double
FLOAT        java.lang.Float            java.lang.Float                                         java.lang.Float
DATE         java.sql.Date              java.sql.Date                                           java.sql.Date
DATETIME     java.sql.Timestamp         java.sql.Timestamp          java.sql.Timestamp          java.sql.Timestamp

NOTE: because oracle always use number(p,s) to create all numerical type, so it's java type maybe java.math.BigDecimal
*/

Status JdbcConnector::_check_type(SlotDescriptor* slot_desc, const std::string& type_str,
                                  int column_index) {
    const std::string error_msg = fmt::format(
            "Fail to convert jdbc type of {} to doris type {} on column: {}. You need to "
            "check this column type between external table and doris table.",
            type_str, slot_desc->type().debug_string(), slot_desc->col_name());
    switch (slot_desc->type().type) {
    case TYPE_BOOLEAN: {
        if (type_str != "java.lang.Boolean" && type_str != "java.math.BigDecimal") {
            return Status::InternalError(error_msg);
        }
        break;
    }
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT: {
        if (type_str != "java.lang.Short" && type_str != "java.lang.Integer" &&
            type_str != "java.math.BigDecimal") {
            return Status::InternalError(error_msg);
        }
        break;
    }
    case TYPE_BIGINT:
    case TYPE_LARGEINT: {
        if (type_str != "java.lang.Long" && type_str != "java.math.BigDecimal" &&
            type_str != "java.math.BigInteger") {
            return Status::InternalError(error_msg);
        }
        break;
    }
    case TYPE_FLOAT: {
        if (type_str != "java.lang.Float" && type_str != "java.math.BigDecimal") {
            return Status::InternalError(error_msg);
        }
        break;
    }
    case TYPE_DOUBLE: {
        if (type_str != "java.lang.Double" && type_str != "java.math.BigDecimal") {
            return Status::InternalError(error_msg);
        }
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        //now here break directly
        break;
    }
    case TYPE_DATE:
    case TYPE_DATEV2:
    case TYPE_TIMEV2:
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2: {
        if (type_str != "java.sql.Timestamp" && type_str != "java.time.LocalDateTime" &&
            type_str != "java.sql.Date") {
            return Status::InternalError(error_msg);
        }
        break;
    }
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I: {
        if (type_str != "java.math.BigDecimal") {
            return Status::InternalError(error_msg);
        }
        break;
    }
    case TYPE_ARRAY: {
        if (type_str != "java.sql.Array" && type_str != "java.lang.String") {
            return Status::InternalError(error_msg);
        }
        if (!slot_desc->type().children[0].children.empty()) {
            return Status::InternalError("Now doris not support nested array type in array {}.",
                                         slot_desc->type().debug_string());
        }
        // when type is array, except pd database, others use string cast array
        if (_conn_param.table_type != TOdbcTableType::POSTGRESQL) {
            _need_cast_array_type = true;
            _map_column_idx_to_cast_idx[column_index] = _input_array_string_types.size();
            if (slot_desc->is_nullable()) {
                _input_array_string_types.push_back(
                        make_nullable(std::make_shared<DataTypeString>()));
            } else {
                _input_array_string_types.push_back(std::make_shared<DataTypeString>());
            }
            str_array_cols.push_back(
                    _input_array_string_types[_map_column_idx_to_cast_idx[column_index]]
                            ->create_column());
        }
        break;
    }
    default: {
        return Status::InternalError(error_msg);
    }
    }
    return Status::OK();
}

Status JdbcConnector::get_next(bool* eos, std::vector<MutableColumnPtr>& columns, Block* block,
                               int batch_size) {
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
        const std::string& column_name = slot_desc->col_name();
        jobject column_data =
                env->CallObjectMethod(block_obj, _executor_get_list_id, materialized_column_index);
        jint num_rows = env->CallIntMethod(column_data, _executor_get_list_size_id);

        for (int row = 0; row < num_rows; ++row) {
            jobject cur_data = env->CallObjectMethod(column_data, _executor_get_list_id, row);
            RETURN_IF_ERROR(_convert_column_data(env, cur_data, slot_desc,
                                                 columns[column_index].get(), column_index,
                                                 column_name));
            env->DeleteLocalRef(cur_data);
        }
        env->DeleteLocalRef(column_data);
        //here need to cast string to array type
        if (_need_cast_array_type && slot_desc->type().is_array_type()) {
            _cast_string_to_array(slot_desc, block, column_index, num_rows);
        }
        materialized_column_index++;
    }
    // All Java objects returned by JNI functions are local references.
    env->DeleteLocalRef(block_obj);
    return JniUtil::GetJniExceptionMsg(env);
}

Status JdbcConnector::_register_func_id(JNIEnv* env) {
    auto register_id = [&](jclass clazz, const char* func_name, const char* func_sign,
                           jmethodID& func_id) {
        func_id = env->GetMethodID(clazz, func_name, func_sign);
        Status s = JniUtil::GetJniExceptionMsg(env);
        if (!s.ok()) {
            return Status::InternalError(strings::Substitute(
                    "Jdbc connector _register_func_id meet error and error is $0", s.to_string()));
        }
        return s;
    };

    RETURN_IF_ERROR(register_id(_executor_clazz, "<init>", JDBC_EXECUTOR_CTOR_SIGNATURE,
                                _executor_ctor_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "write", JDBC_EXECUTOR_WRITE_SIGNATURE,
                                _executor_write_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "read", "()I", _executor_read_id));
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
    RETURN_IF_ERROR(register_id(_executor_clazz, "getResultColumnTypeNames",
                                JDBC_EXECUTOR_GET_TYPES_SIGNATURE, _executor_get_types_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "getArrayColumnData",
                                JDBC_EXECUTOR_GET_ARR_LIST_SIGNATURE, _executor_get_arr_list_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "getBaseTypeInt",
                                JDBC_EXECUTOR_GET_ARR_TYPE_SIGNATURE, _executor_get_arr_type_id));

    return Status::OK();
}

Status JdbcConnector::_convert_column_data(JNIEnv* env, jobject jobj,
                                           const SlotDescriptor* slot_desc,
                                           vectorized::IColumn* column_ptr, int column_index,
                                           std::string_view column_name) {
    vectorized::IColumn* col_ptr = column_ptr;
    if (true == slot_desc->is_nullable()) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
        if (jobj == nullptr) {
            nullable_column->insert_data(nullptr, 0);
            if (_need_cast_array_type && slot_desc->type().type == TYPE_ARRAY) {
                reinterpret_cast<vectorized::ColumnNullable*>(
                        str_array_cols[_map_column_idx_to_cast_idx[column_index]].get())
                        ->insert_data(nullptr, 0);
            }
            return Status::OK();
        } else {
            nullable_column->get_null_map_data().push_back(0);
            col_ptr = &nullable_column->get_nested_column();
        }
    }
    RETURN_IF_ERROR(
            _insert_column_data(env, jobj, slot_desc->type(), col_ptr, column_index, column_name));
    return Status::OK();
}

Status JdbcConnector::_insert_column_data(JNIEnv* env, jobject jobj, const TypeDescriptor& type,
                                          vectorized::IColumn* col_ptr, int column_index,
                                          std::string_view column_name) {
    switch (type.type) {
#define M(TYPE, CPP_TYPE, COLUMN_TYPE)                              \
    case TYPE: {                                                    \
        CPP_TYPE num = _jobject_to_##CPP_TYPE(env, jobj);           \
        reinterpret_cast<COLUMN_TYPE*>(col_ptr)->insert_value(num); \
        break;                                                      \
    }
        M(TYPE_BOOLEAN, uint8_t, vectorized::ColumnVector<vectorized::UInt8>)
        M(TYPE_TINYINT, int8_t, vectorized::ColumnVector<vectorized::Int8>)
        M(TYPE_SMALLINT, int16_t, vectorized::ColumnVector<vectorized::Int16>)
        M(TYPE_INT, int32_t, vectorized::ColumnVector<vectorized::Int32>)
        M(TYPE_BIGINT, int64_t, vectorized::ColumnVector<vectorized::Int64>)
        M(TYPE_FLOAT, float, vectorized::ColumnVector<vectorized::Float32>)
        M(TYPE_DOUBLE, double, vectorized::ColumnVector<vectorized::Float64>)
#undef M
    case TYPE_CHAR: {
        std::string data = _jobject_to_string(env, jobj);
        // Now have test pg and oracle with char(100), if data='abc'
        // but read string data length is 100, so need trim extra spaces
        if ((_conn_param.table_type == TOdbcTableType::POSTGRESQL) ||
            (_conn_param.table_type == TOdbcTableType::ORACLE)) {
            data = data.erase(data.find_last_not_of(' ') + 1);
        }
        reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(data.c_str(),
                                                                          data.length());
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR: {
        std::string data = _jobject_to_string(env, jobj);
        reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(data.c_str(),
                                                                          data.length());
        break;
    }
    case TYPE_DATE: {
        int64_t num = _jobject_to_date(env, jobj, false);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_DATEV2: {
        int64_t num = _jobject_to_date(env, jobj, true);
        uint32_t num2 = static_cast<uint32_t>(num);
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt32>*>(col_ptr)->insert_value(
                num2);
        break;
    }
    case TYPE_DATETIME: {
        int64_t num = _jobject_to_datetime(env, jobj, false);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_DATETIMEV2: {
        int64_t num = _jobject_to_datetime(env, jobj, true);
        uint64_t num2 = static_cast<uint64_t>(num);
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt64>*>(col_ptr)->insert_value(
                num2);
        break;
    }
    case TYPE_LARGEINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        std::string data = _jobject_to_string(env, jobj);
        __int128 num =
                StringParser::string_to_int<__int128>(data.data(), data.size(), &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_DECIMALV2: {
        std::string data = _jobject_to_string(env, jobj);
        DecimalV2Value decimal_slot;
        decimal_slot.parse_from_str(data.c_str(), data.length());
        reinterpret_cast<vectorized::ColumnDecimal128*>(col_ptr)->insert_data(
                const_cast<const char*>(reinterpret_cast<char*>(&decimal_slot)), 0);
        break;
    }
    case TYPE_DECIMAL32: {
        std::string data = _jobject_to_string(env, jobj);
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        const Int32 decimal_slot = StringParser::string_to_decimal<Int32>(
                data.c_str(), data.length(), type.precision, type.scale, &result);
        reinterpret_cast<vectorized::ColumnDecimal32*>(col_ptr)->insert_data(
                reinterpret_cast<const char*>(&decimal_slot), 0);
        break;
    }
    case TYPE_DECIMAL64: {
        std::string data = _jobject_to_string(env, jobj);
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        const Int64 decimal_slot = StringParser::string_to_decimal<Int64>(
                data.c_str(), data.length(), type.precision, type.scale, &result);
        reinterpret_cast<vectorized::ColumnDecimal64*>(col_ptr)->insert_data(
                reinterpret_cast<const char*>(&decimal_slot), 0);
        break;
    }
    case TYPE_DECIMAL128I: {
        std::string data = _jobject_to_string(env, jobj);
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        const Int128 decimal_slot = StringParser::string_to_decimal<Int128>(
                data.c_str(), data.length(), type.precision, type.scale, &result);
        reinterpret_cast<vectorized::ColumnDecimal128I*>(col_ptr)->insert_data(
                reinterpret_cast<const char*>(&decimal_slot), 0);
        break;
    }
    case TYPE_ARRAY: {
        if (_need_cast_array_type) {
            // read array data is a big string: [1,2,3], need cast it by self
            std::string data = _jobject_to_string(env, jobj);
            str_array_cols[_map_column_idx_to_cast_idx[column_index]]->insert_data(data.c_str(),
                                                                                   data.length());
        } else {
            //POSTGRESQL read array is object[], so could get data by index
            jobject arr_lists = env->CallNonvirtualObjectMethod(_executor_obj, _executor_clazz,
                                                                _executor_get_arr_list_id, jobj);
            jint arr_type = env->CallNonvirtualIntMethod(_executor_obj, _executor_clazz,
                                                         _executor_get_arr_type_id);
            //here type check is maybe no needed，more checks affect performance
            if (_arr_jdbc_map[arr_type] != type.children[0].type) {
                const std::string& error_msg = fmt::format(
                        "Fail to convert jdbc value to array type of {} on column: {}, could check "
                        "this column type between external table and doris table. {}.{} ",
                        type.children[0].debug_string(), column_name, _arr_jdbc_map[arr_type],
                        arr_type);
                return Status::InternalError(std::string(error_msg));
            }
            jint num_rows = env->CallIntMethod(arr_lists, _executor_get_list_size_id);
            RETURN_IF_ERROR(_insert_arr_column_data(env, arr_lists, type.children[0], num_rows,
                                                    col_ptr, column_index, column_name));
            env->DeleteLocalRef(arr_lists);
        }
        break;
    }
    default: {
        const std::string& error_msg = fmt::format(
                "Fail to convert jdbc value to {} on column: {}, could check this column type "
                "between external table and doris table.",
                type.debug_string(), column_name);
        return Status::InternalError(std::string(error_msg));
    }
    }
    return Status::OK();
}

Status JdbcConnector::_insert_arr_column_data(JNIEnv* env, jobject arr_lists,
                                              const TypeDescriptor& type, int nums,
                                              vectorized::IColumn* arr_column_ptr, int column_index,
                                              std::string_view column_name) {
    auto& arr_nested = reinterpret_cast<vectorized::ColumnArray*>(arr_column_ptr)->get_data();
    vectorized::IColumn* col_ptr =
            reinterpret_cast<vectorized::ColumnNullable&>(arr_nested).get_nested_column_ptr();
    auto& nullmap_data =
            reinterpret_cast<vectorized::ColumnNullable&>(arr_nested).get_null_map_data();
    for (int i = 0; i < nums; ++i) {
        jobject cur_data = env->CallObjectMethod(arr_lists, _executor_get_list_id, i);
        if (cur_data == nullptr) {
            arr_nested.insert_default();
            continue;
        } else {
            nullmap_data.push_back(0);
        }
        RETURN_IF_ERROR(
                _insert_column_data(env, cur_data, type, col_ptr, column_index, column_name));
        env->DeleteLocalRef(cur_data);
    }
    auto old_size =
            reinterpret_cast<vectorized::ColumnArray*>(arr_column_ptr)->get_offsets().back();
    reinterpret_cast<vectorized::ColumnArray*>(arr_column_ptr)
            ->get_offsets()
            .push_back(nums + old_size);
    return Status::OK();
}

Status JdbcConnector::_cast_string_to_array(const SlotDescriptor* slot_desc, Block* block,
                                            int column_index, int rows) {
    DataTypePtr _target_data_type = slot_desc->get_data_type_ptr();
    std::string _target_data_type_name = DataTypeFactory::instance().get(_target_data_type);
    DataTypePtr _cast_param_data_type = std::make_shared<DataTypeString>();
    ColumnPtr _cast_param = _cast_param_data_type->create_column_const(1, _target_data_type_name);

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(
            std::move(str_array_cols[_map_column_idx_to_cast_idx[column_index]]),
            _input_array_string_types[_map_column_idx_to_cast_idx[column_index]],
            "java.sql.String");
    argument_template.emplace_back(_cast_param, _cast_param_data_type, _target_data_type_name);
    FunctionBasePtr func_cast = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, make_nullable(_target_data_type));

    Block cast_block(argument_template);
    int result_idx = cast_block.columns();
    cast_block.insert({nullptr, make_nullable(_target_data_type), "cast_result"});
    func_cast->execute(nullptr, cast_block, {0, 1}, result_idx, rows);

    auto res_col = cast_block.get_by_position(result_idx).column;
    if (_target_data_type->is_nullable()) {
        block->replace_by_position(column_index, res_col);
    } else {
        auto nested_ptr = reinterpret_cast<const vectorized::ColumnNullable*>(res_col.get())
                                  ->get_nested_column_ptr();
        block->replace_by_position(column_index, nested_ptr);
    }
    str_array_cols[_map_column_idx_to_cast_idx[column_index]] =
            _input_array_string_types[_map_column_idx_to_cast_idx[column_index]]->create_column();
    return Status::OK();
}

Status JdbcConnector::exec_write_sql(const std::u16string& insert_stmt,
                                     const fmt::memory_buffer& insert_stmt_buffer) {
    SCOPED_TIMER(_result_send_timer);
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    jstring query_sql = env->NewString((const jchar*)insert_stmt.c_str(), insert_stmt.size());
    env->CallNonvirtualIntMethod(_executor_obj, _executor_clazz, _executor_write_id, query_sql);
    env->DeleteLocalRef(query_sql);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    return Status::OK();
}

std::string JdbcConnector::_jobject_to_string(JNIEnv* env, jobject jobj) {
    jobject jstr = env->CallObjectMethod(jobj, _to_string_id);
    auto coding = env->NewStringUTF("UTF-8");
    const jbyteArray stringJbytes = (jbyteArray)env->CallObjectMethod(jstr, _get_bytes_id, coding);
    size_t length = (size_t)env->GetArrayLength(stringJbytes);
    jbyte* pBytes = env->GetByteArrayElements(stringJbytes, nullptr);
    std::string str = std::string((char*)pBytes, length);
    env->ReleaseByteArrayElements(stringJbytes, pBytes, JNI_ABORT);
    env->DeleteLocalRef(stringJbytes);
    env->DeleteLocalRef(jstr);
    env->DeleteLocalRef(coding);
    return str;
}

int64_t JdbcConnector::_jobject_to_date(JNIEnv* env, jobject jobj, bool is_date_v2) {
    return env->CallNonvirtualLongMethod(_executor_obj, _executor_clazz, _executor_convert_date_id,
                                         jobj, is_date_v2);
}

int64_t JdbcConnector::_jobject_to_datetime(JNIEnv* env, jobject jobj, bool is_datetime_v2) {
    return env->CallNonvirtualLongMethod(_executor_obj, _executor_clazz,
                                         _executor_convert_datetime_id, jobj, is_datetime_v2);
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
