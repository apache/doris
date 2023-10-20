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

#include <gen_cpp/Types_types.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath> // IWYU pragma: keep
#include <cstring>
#include <memory>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "exec/table_connector.h"
#include "gutil/strings/substitute.h"
#include "jni.h"
#include "jni_md.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "runtime/user_function_cache.h"
#include "util/jni-util.h"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exec/jni_connector.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
namespace vectorized {
const char* JDBC_EXECUTOR_CLASS = "org/apache/doris/jdbc/JdbcExecutor";
const char* JDBC_EXECUTOR_CTOR_SIGNATURE = "([B)V";
const char* JDBC_EXECUTOR_WRITE_SIGNATURE = "(Ljava/lang/String;)I";
const char* JDBC_EXECUTOR_STMT_WRITE_SIGNATURE = "(Ljava/util/Map;)I";
const char* JDBC_EXECUTOR_HAS_NEXT_SIGNATURE = "()Z";
const char* JDBC_EXECUTOR_GET_BLOCK_SIGNATURE = "(I)Ljava/util/List;";
const char* JDBC_EXECUTOR_GET_BLOCK_WITH_TYPES_SIGNATURE = "(ILjava/lang/Object;)Ljava/util/List;";
const char* JDBC_EXECUTOR_GET_TYPES_SIGNATURE = "()Ljava/util/List;";
const char* JDBC_EXECUTOR_CLOSE_SIGNATURE = "()V";
const char* JDBC_EXECUTOR_TRANSACTION_SIGNATURE = "()V";
const char* JDBC_EXECUTOR_COPY_BATCH_SIGNATURE = "(Ljava/lang/Object;ZIJJ)V";

JdbcConnector::JdbcConnector(const JdbcConnectorParam& param)
        : TableConnector(param.tuple_desc, param.use_transaction, param.table_name,
                         param.query_string),
          _conn_param(param),
          _closed(false) {}

JdbcConnector::~JdbcConnector() {
    if (!_closed) {
        static_cast<void>(close());
    }
}

#define GET_BASIC_JAVA_CLAZZ(JAVA_TYPE, CPP_TYPE) \
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, JAVA_TYPE, &_executor_##CPP_TYPE##_clazz));

#define DELETE_BASIC_JAVA_CLAZZ_REF(CPP_TYPE) env->DeleteGlobalRef(_executor_##CPP_TYPE##_clazz);

Status JdbcConnector::close(Status) {
    SCOPED_RAW_TIMER(&_jdbc_statistic._connector_close_timer);
    _closed = true;
    if (!_is_open) {
        return Status::OK();
    }
    if (_is_in_transaction) {
        static_cast<void>(abort_trans());
    }
    JNIEnv* env;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    env->DeleteGlobalRef(_executor_clazz);
    DELETE_BASIC_JAVA_CLAZZ_REF(object)
    DELETE_BASIC_JAVA_CLAZZ_REF(string)
    DELETE_BASIC_JAVA_CLAZZ_REF(list)
#undef DELETE_BASIC_JAVA_CLAZZ_REF
    env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_close_id);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    env->DeleteGlobalRef(_executor_obj);
    return Status::OK();
}

Status JdbcConnector::append(vectorized::Block* block,
                             const vectorized::VExprContextSPtrs& output_vexpr_ctxs,
                             uint32_t start_send_row, uint32_t* num_rows_sent,
                             TOdbcTableType::type table_type) {
    RETURN_IF_ERROR(exec_stmt_write(block, output_vexpr_ctxs, num_rows_sent));
    COUNTER_UPDATE(_sent_rows_counter, *num_rows_sent);
    return Status::OK();
}

Status JdbcConnector::open(RuntimeState* state, bool read) {
    if (_is_open) {
        LOG(INFO) << "this scanner of jdbc already opened";
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    RETURN_IF_ERROR(JniUtil::get_jni_scanner_class(env, JDBC_EXECUTOR_CLASS, &_executor_clazz));
    GET_BASIC_JAVA_CLAZZ("java/util/List", list)
    GET_BASIC_JAVA_CLAZZ("java/lang/Object", object)
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
            SCOPED_RAW_TIMER(&_jdbc_statistic._load_jar_timer);
            RETURN_IF_ERROR(function_cache->get_jarpath(
                    std::abs((int64_t)hash_str(_conn_param.driver_path)), _conn_param.driver_path,
                    _conn_param.driver_checksum, &local_location));
        } else {
            SCOPED_RAW_TIMER(&_jdbc_statistic._load_jar_timer);
            RETURN_IF_ERROR(function_cache->get_jarpath(
                    std::abs((int64_t)hash_str(_conn_param.resource_name)), _conn_param.driver_path,
                    _conn_param.driver_checksum, &local_location));
        }
        VLOG_QUERY << "driver local path = " << local_location;

        TJdbcExecutorCtorParams ctor_params;
        ctor_params.__set_statement(_sql_str);
        ctor_params.__set_jdbc_url(_conn_param.jdbc_url);
        ctor_params.__set_jdbc_user(_conn_param.user);
        ctor_params.__set_jdbc_password(_conn_param.passwd);
        ctor_params.__set_jdbc_driver_class(_conn_param.driver_class);
        ctor_params.__set_driver_path(local_location);
        ctor_params.__set_batch_size(read ? state->batch_size() : 0);
        ctor_params.__set_op(read ? TJdbcOperation::READ : TJdbcOperation::WRITE);
        ctor_params.__set_table_type(_conn_param.table_type);

        jbyteArray ctor_params_bytes;
        // Pushed frame will be popped when jni_frame goes out-of-scope.
        RETURN_IF_ERROR(jni_frame.push(env));
        RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));
        {
            SCOPED_RAW_TIMER(&_jdbc_statistic._init_connector_timer);
            _executor_obj = env->NewObject(_executor_clazz, _executor_ctor_id, ctor_params_bytes);
        }
        jbyte* pBytes = env->GetByteArrayElements(ctor_params_bytes, nullptr);
        env->ReleaseByteArrayElements(ctor_params_bytes, pBytes, JNI_ABORT);
        env->DeleteLocalRef(ctor_params_bytes);
    }
    RETURN_ERROR_IF_EXC(env);
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, _executor_obj, &_executor_obj));
    _is_open = true;
    static_cast<void>(begin_trans());

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
    {
        SCOPED_RAW_TIMER(&_jdbc_statistic._execte_read_timer);
        jint colunm_count =
                env->CallNonvirtualIntMethod(_executor_obj, _executor_clazz, _executor_read_id);
        if (auto status = JniUtil::GetJniExceptionMsg(env); !status) {
            return Status::InternalError("GetJniExceptionMsg meet error, query={}, msg={}",
                                         _conn_param.query_string, status.to_string());
        }
        if (colunm_count != materialize_num) {
            return Status::InternalError("input and output column num not equal of jdbc query.");
        }
    }

    LOG(INFO) << "JdbcConnector::query has exec success: " << _sql_str;
    if (_conn_param.table_type != TOdbcTableType::NEBULA) {
        RETURN_IF_ERROR(_check_column_type());
    }
    return Status::OK();
}

Status JdbcConnector::_check_column_type() {
    SCOPED_RAW_TIMER(&_jdbc_statistic._check_type_timer);
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
        if (type_str != "java.lang.Boolean" && type_str != "java.lang.Byte" &&
            type_str != "java.lang.Integer") {
            return Status::InternalError(error_msg);
        }
        break;
    }
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT: {
        if (type_str != "java.lang.Short" && type_str != "java.lang.Integer" &&
            type_str != "java.math.BigDecimal" && type_str != "java.lang.Byte" &&
            type_str != "com.clickhouse.data.value.UnsignedByte" &&
            type_str != "com.clickhouse.data.value.UnsignedShort" && type_str != "java.lang.Long") {
            return Status::InternalError(error_msg);
        }
        break;
    }
    case TYPE_BIGINT:
    case TYPE_LARGEINT: {
        if (type_str != "java.lang.Long" && type_str != "java.math.BigDecimal" &&
            type_str != "java.math.BigInteger" && type_str != "java.lang.String" &&
            type_str != "com.clickhouse.data.value.UnsignedInteger" &&
            type_str != "com.clickhouse.data.value.UnsignedLong") {
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
            type_str != "java.sql.Date" && type_str != "java.time.LocalDate" &&
            type_str != "oracle.sql.TIMESTAMP" && type_str != "java.time.OffsetDateTime") {
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
        if (type_str != "java.sql.Array" && type_str != "java.lang.String" &&
            type_str != "java.lang.Object") {
            return Status::InternalError(error_msg);
        }
        if (!slot_desc->type().children[0].children.empty()) {
            return Status::InternalError("Now doris not support nested array type in array {}.",
                                         slot_desc->type().debug_string());
        }
        _map_column_idx_to_cast_idx[column_index] = _input_array_string_types.size();
        if (slot_desc->is_nullable()) {
            _input_array_string_types.push_back(make_nullable(std::make_shared<DataTypeString>()));
        } else {
            _input_array_string_types.push_back(std::make_shared<DataTypeString>());
        }
        str_array_cols.push_back(
                _input_array_string_types[_map_column_idx_to_cast_idx[column_index]]
                        ->create_column());
        break;
    }
    case TYPE_JSONB: {
        if (type_str != "java.lang.String" && type_str != "org.postgresql.util.PGobject" && type_str != "java.lang.Object") {
            return Status::InternalError(error_msg);
        }

        _map_column_idx_to_cast_idx_json[column_index] = _input_json_string_types.size();
        if (slot_desc->is_nullable()) {
            _input_json_string_types.push_back(make_nullable(std::make_shared<DataTypeString>()));
        } else {
            _input_json_string_types.push_back(std::make_shared<DataTypeString>());
        }
        str_json_cols.push_back(
                _input_json_string_types[_map_column_idx_to_cast_idx_json[column_index]]
                        ->create_column());
        break;
    }
    case TYPE_HLL: {
        if (type_str != "java.lang.String") {
            return Status::InternalError(error_msg);
        }

        _map_column_idx_to_cast_idx_hll[column_index] = _input_hll_string_types.size();
        if (slot_desc->is_nullable()) {
            _input_hll_string_types.push_back(make_nullable(std::make_shared<DataTypeString>()));
        } else {
            _input_hll_string_types.push_back(std::make_shared<DataTypeString>());
        }

        str_hll_cols.push_back(
                _input_hll_string_types[_map_column_idx_to_cast_idx_hll[column_index]]
                        ->create_column());
        break;
    }
    case TYPE_OBJECT: {
        if (type_str != "java.lang.String") {
            return Status::InternalError(error_msg);
        }

        _map_column_idx_to_cast_idx_bitmap[column_index] = _input_bitmap_string_types.size();
        if (slot_desc->is_nullable()) {
            _input_bitmap_string_types.push_back(make_nullable(std::make_shared<DataTypeString>()));
        } else {
            _input_bitmap_string_types.push_back(std::make_shared<DataTypeString>());
        }

        str_bitmap_cols.push_back(
                _input_bitmap_string_types[_map_column_idx_to_cast_idx_bitmap[column_index]]
                        ->create_column());
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
    SCOPED_RAW_TIMER(&_jdbc_statistic._get_data_timer);
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    jboolean has_next =
            env->CallNonvirtualBooleanMethod(_executor_obj, _executor_clazz, _executor_has_next_id);
    if (has_next != JNI_TRUE) {
        *eos = true;
        return Status::OK();
    }

    jobject block_obj;
    // if contain HLL column, pass the column type to jni env
    if (_tuple_desc->has_hll_slot() || _tuple_desc->has_bitmap_slot()) {
        auto column_size = _tuple_desc->slots().size();
        // Find ArrayList and Integer
        jclass arrayListClass = env->FindClass("java/util/ArrayList");
        jclass integerClass = env->FindClass("java/lang/Integer");

        // Get method id of the constructor and the add in ArrayList
        jmethodID arrayListConstructor = env->GetMethodID(arrayListClass, "<init>", "()V");
        jmethodID arrayListAddMethod =
                env->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");

        // Create an ArrayList object
        jobject arrayListObject = env->NewObject(arrayListClass, arrayListConstructor);
        for (int column_index = 0; column_index < column_size; ++column_index) {
            auto slot_desc = _tuple_desc->slots()[column_index];
            if (slot_desc->type().is_hll_type() || slot_desc->type().is_bitmap_type()) {
                // Create an Integer object
                jobject integerObject = env->NewObject(
                        integerClass, env->GetMethodID(integerClass, "<init>", "(I)V"),
                        (int)slot_desc->type().type);
                // Add Integer into ArrayList
                env->CallBooleanMethod(arrayListObject, arrayListAddMethod, integerObject);

            } else {
                jobject integerObject = env->NewObject(
                        integerClass, env->GetMethodID(integerClass, "<init>", "(I)V"), 0);
                env->CallBooleanMethod(arrayListObject, arrayListAddMethod, integerObject);
            }
        }

        block_obj = env->CallNonvirtualObjectMethod(_executor_obj, _executor_clazz,
                                                    _executor_get_blocks_new_id, batch_size,
                                                    arrayListObject);
    } else {
        block_obj = env->CallNonvirtualObjectMethod(_executor_obj, _executor_clazz,
                                                    _executor_get_blocks_id, batch_size);
    }

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
        jint num_rows = env->CallNonvirtualIntMethod(_executor_obj, _executor_clazz,
                                                     _executor_block_rows_id);
        RETURN_IF_ERROR(_convert_batch_result_set(
                env, column_data, slot_desc, columns[column_index].get(), num_rows, column_index));
        env->DeleteLocalRef(column_data);
        //here need to cast string to array type
        if (slot_desc->type().is_array_type()) {
            static_cast<void>(_cast_string_to_array(slot_desc, block, column_index, num_rows));
        } else if (slot_desc->type().is_hll_type()) {
            static_cast<void>(_cast_string_to_hll(slot_desc, block, column_index, num_rows));
        } else if (slot_desc->type().is_json_type()) {
            static_cast<void>(_cast_string_to_json(slot_desc, block, column_index, num_rows));
        } else if (slot_desc->type().is_bitmap_type()) {
            static_cast<void>(_cast_string_to_bitmap(slot_desc, block, column_index, num_rows));
        }
        materialized_column_index++;
    }
    // All Java objects returned by JNI functions are local references.
    env->DeleteLocalRef(block_obj);
    return JniUtil::GetJniExceptionMsg(env);
}

Status JdbcConnector::_convert_batch_result_set(JNIEnv* env, jobject jcolumn_data,
                                                const SlotDescriptor* slot_desc,
                                                vectorized::IColumn* column_ptr, int num_rows,
                                                int column_index) {
    vectorized::IColumn* col_ptr = column_ptr;
    col_ptr->resize(num_rows);
    int64_t address[2] = {0, 0};
    bool column_is_nullable = slot_desc->is_nullable();
    if (column_is_nullable) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
        auto& null_map = nullable_column->get_null_map_data();
        memset(null_map.data(), 0, num_rows);
        address[0] = reinterpret_cast<int64_t>(null_map.data());
        col_ptr = &nullable_column->get_nested_column();
    }
    switch (slot_desc->type().type) {
    case TYPE_BOOLEAN: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_boolean_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_TINYINT: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_tinyint_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_SMALLINT: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_smallint_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_INT: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_int_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_BIGINT: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_bigint_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_LARGEINT: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_largeint_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_FLOAT: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_float_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_DOUBLE: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_double_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_CHAR: {
        bool need_trim_spaces = false;
        if ((_conn_param.table_type == TOdbcTableType::POSTGRESQL) ||
            (_conn_param.table_type == TOdbcTableType::ORACLE)) {
            need_trim_spaces = true;
        }
        auto column_string = reinterpret_cast<vectorized::ColumnString*>(col_ptr);
        address[1] = reinterpret_cast<int64_t>(column_string->get_offsets().data());
        auto chars_addres = reinterpret_cast<int64_t>(&column_string->get_chars());
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_char_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1], chars_addres, need_trim_spaces);
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR: {
        auto column_string = reinterpret_cast<vectorized::ColumnString*>(col_ptr);
        address[1] = reinterpret_cast<int64_t>(column_string->get_offsets().data());
        auto chars_addres = reinterpret_cast<int64_t>(&column_string->get_chars());
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_string_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1], chars_addres);
        break;
    }
    case TYPE_DATE: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_date_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_DATEV2: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_datev2_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_DATETIME: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_datetime_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1]);
        break;
    }
    case TYPE_DATETIMEV2: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz,
                                      _executor_get_datetimev2_result, jcolumn_data,
                                      column_is_nullable, num_rows, address[0], address[1]);
        break;
    }
    case TYPE_DECIMALV2: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz,
                                      _executor_get_decimalv2_result, jcolumn_data,
                                      column_is_nullable, num_rows, address[0], address[1]);
        break;
    }
    case TYPE_DECIMAL32: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(
                _executor_obj, _executor_clazz, _executor_get_decimal32_result, jcolumn_data,
                column_is_nullable, num_rows, address[0], address[1], slot_desc->type().scale);
        break;
    }
    case TYPE_DECIMAL64: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(
                _executor_obj, _executor_clazz, _executor_get_decimal64_result, jcolumn_data,
                column_is_nullable, num_rows, address[0], address[1], slot_desc->type().scale);
        break;
    }
    case TYPE_DECIMAL128I: {
        address[1] = reinterpret_cast<int64_t>(col_ptr->get_raw_data().data);
        env->CallNonvirtualVoidMethod(
                _executor_obj, _executor_clazz, _executor_get_decimal128_result, jcolumn_data,
                column_is_nullable, num_rows, address[0], address[1], slot_desc->type().scale);
        break;
    }
    case TYPE_ARRAY: {
        str_array_cols[_map_column_idx_to_cast_idx[column_index]]->resize(num_rows);
        if (column_is_nullable) {
            auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                    str_array_cols[_map_column_idx_to_cast_idx[column_index]].get());
            auto& null_map = nullable_column->get_null_map_data();
            memset(null_map.data(), 0, num_rows);
            address[0] = reinterpret_cast<int64_t>(null_map.data());
            col_ptr = &nullable_column->get_nested_column();
        } else {
            col_ptr = str_array_cols[_map_column_idx_to_cast_idx[column_index]].get();
        }
        auto column_string = reinterpret_cast<vectorized::ColumnString*>(col_ptr);
        address[1] = reinterpret_cast<int64_t>(column_string->get_offsets().data());
        auto chars_addres = reinterpret_cast<int64_t>(&column_string->get_chars());
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_array_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1], chars_addres);
        break;
    }
    case TYPE_JSONB: {
        str_json_cols[_map_column_idx_to_cast_idx_json[column_index]]->resize(num_rows);
        if (column_is_nullable) {
            auto* nullbale_column = reinterpret_cast<vectorized::ColumnNullable*>(
                    str_json_cols[_map_column_idx_to_cast_idx_json[column_index]].get());
            auto& null_map = nullbale_column->get_null_map_data();
            memset(null_map.data(), 0, num_rows);
            address[0] = reinterpret_cast<int64_t>(null_map.data());
            col_ptr = &nullbale_column->get_nested_column();
        } else {
            col_ptr = str_json_cols[_map_column_idx_to_cast_idx_json[column_index]].get();
        }
        auto column_string = reinterpret_cast<vectorized::ColumnString*>(col_ptr);
        address[1] = reinterpret_cast<int64_t>(column_string->get_offsets().data());
        auto chars_addres = reinterpret_cast<int64_t>(&column_string->get_chars());
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_json_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1], chars_addres);
        break;
    }
    case TYPE_HLL: {
        str_hll_cols[_map_column_idx_to_cast_idx_hll[column_index]]->resize(num_rows);
        if (column_is_nullable) {
            auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                    str_hll_cols[_map_column_idx_to_cast_idx_hll[column_index]].get());
            auto& null_map = nullable_column->get_null_map_data();
            memset(null_map.data(), 0, num_rows);
            address[0] = reinterpret_cast<int64_t>(null_map.data());
            col_ptr = &nullable_column->get_nested_column();
        } else {
            col_ptr = str_hll_cols[_map_column_idx_to_cast_idx_hll[column_index]].get();
        }
        auto column_string = reinterpret_cast<vectorized::ColumnString*>(col_ptr);
        address[1] = reinterpret_cast<int64_t>(column_string->get_offsets().data());
        auto chars_addres = reinterpret_cast<int64_t>(&column_string->get_chars());
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_hll_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1], chars_addres);
        break;
    }
    //BITMAP
    case TYPE_OBJECT: {
        str_bitmap_cols[_map_column_idx_to_cast_idx_bitmap[column_index]]->resize(num_rows);
        if (column_is_nullable) {
            auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                    str_bitmap_cols[_map_column_idx_to_cast_idx_bitmap[column_index]].get());
            auto& null_map = nullable_column->get_null_map_data();
            memset(null_map.data(), 0, num_rows);
            address[0] = reinterpret_cast<int64_t>(null_map.data());
            col_ptr = &nullable_column->get_nested_column();
        } else {
            col_ptr = str_bitmap_cols[_map_column_idx_to_cast_idx_bitmap[column_index]].get();
        }
        auto column_string = reinterpret_cast<vectorized::ColumnString*>(col_ptr);
        address[1] = reinterpret_cast<int64_t>(column_string->get_offsets().data());
        auto chars_addres = reinterpret_cast<int64_t>(&column_string->get_chars());
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_get_bitmap_result,
                                      jcolumn_data, column_is_nullable, num_rows, address[0],
                                      address[1], chars_addres);
        break;
    }
    default: {
        const std::string& error_msg =
                fmt::format("Fail to convert jdbc value to {} on column: {}",
                            slot_desc->type().debug_string(), slot_desc->col_name());
        return Status::InternalError(std::string(error_msg));
    }
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
                    "Jdbc connector _register_func_id meet error and error is $0", s.to_string()));
        }
        return s;
    };

    RETURN_IF_ERROR(register_id(_executor_clazz, "<init>", JDBC_EXECUTOR_CTOR_SIGNATURE,
                                _executor_ctor_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "write", JDBC_EXECUTOR_WRITE_SIGNATURE,
                                _executor_write_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "write", JDBC_EXECUTOR_STMT_WRITE_SIGNATURE,
                                _executor_stmt_write_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "read", "()I", _executor_read_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "close", JDBC_EXECUTOR_CLOSE_SIGNATURE,
                                _executor_close_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "hasNext", JDBC_EXECUTOR_HAS_NEXT_SIGNATURE,
                                _executor_has_next_id));
    RETURN_IF_ERROR(
            register_id(_executor_clazz, "getCurBlockRows", "()I", _executor_block_rows_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchBooleanResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_boolean_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchTinyIntResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_tinyint_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchSmallIntResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_smallint_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchIntResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_int_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchBigIntResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_bigint_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchLargeIntResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_largeint_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchFloatResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_float_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchDoubleResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_double_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchStringResult",
                                "(Ljava/lang/Object;ZIJJJ)V", _executor_get_string_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchArrayResult",
                                "(Ljava/lang/Object;ZIJJJ)V", _executor_get_array_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchHllResult", "(Ljava/lang/Object;ZIJJJ)V",
                                _executor_get_hll_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchBitMapResult",
                                "(Ljava/lang/Object;ZIJJJ)V", _executor_get_bitmap_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchJsonResult",
                                "(Ljava/lang/Object;ZIJJJ)V", _executor_get_json_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchCharResult",
                                "(Ljava/lang/Object;ZIJJJZ)V", _executor_get_char_result));

    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchDateResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_date_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchDateV2Result",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_datev2_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchDateTimeResult",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE, _executor_get_datetime_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchDateTimeV2Result",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE,
                                _executor_get_datetimev2_result));

    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchDecimalV2Result",
                                JDBC_EXECUTOR_COPY_BATCH_SIGNATURE,
                                _executor_get_decimalv2_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchDecimal32Result",
                                "(Ljava/lang/Object;ZIJJI)V", _executor_get_decimal32_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchDecimal64Result",
                                "(Ljava/lang/Object;ZIJJI)V", _executor_get_decimal64_result));
    RETURN_IF_ERROR(register_id(_executor_clazz, "copyBatchDecimal128Result",
                                "(Ljava/lang/Object;ZIJJI)V", _executor_get_decimal128_result));

    RETURN_IF_ERROR(register_id(_executor_clazz, "getBlock", JDBC_EXECUTOR_GET_BLOCK_SIGNATURE,
                                _executor_get_blocks_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "getBlock",
                                JDBC_EXECUTOR_GET_BLOCK_WITH_TYPES_SIGNATURE,
                                _executor_get_blocks_new_id));
    RETURN_IF_ERROR(register_id(_executor_list_clazz, "get", "(I)Ljava/lang/Object;",
                                _executor_get_list_id));
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
    return Status::OK();
}

Status JdbcConnector::_cast_string_to_hll(const SlotDescriptor* slot_desc, Block* block,
                                          int column_index, int rows) {
    DataTypePtr _target_data_type = slot_desc->get_data_type_ptr();
    std::string _target_data_type_name = _target_data_type->get_name();
    DataTypePtr _cast_param_data_type = _target_data_type;
    ColumnPtr _cast_param = _cast_param_data_type->create_column_const_with_default_value(1);

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(
            std::move(str_hll_cols[_map_column_idx_to_cast_idx_hll[column_index]]),
            _input_hll_string_types[_map_column_idx_to_cast_idx_hll[column_index]],
            "java.sql.String");
    argument_template.emplace_back(_cast_param, _cast_param_data_type, _target_data_type_name);
    FunctionBasePtr func_cast = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, make_nullable(_target_data_type));

    Block cast_block(argument_template);
    int result_idx = cast_block.columns();
    cast_block.insert({nullptr, make_nullable(_target_data_type), "cast_result"});
    static_cast<void>(func_cast->execute(nullptr, cast_block, {0, 1}, result_idx, rows));

    auto res_col = cast_block.get_by_position(result_idx).column;
    if (_target_data_type->is_nullable()) {
        block->replace_by_position(column_index, res_col);
    } else {
        auto nested_ptr = reinterpret_cast<const vectorized::ColumnNullable*>(res_col.get())
                                  ->get_nested_column_ptr();
        block->replace_by_position(column_index, nested_ptr);
    }
    str_hll_cols[_map_column_idx_to_cast_idx_hll[column_index]] =
            _input_hll_string_types[_map_column_idx_to_cast_idx_hll[column_index]]->create_column();
    return Status::OK();
}

Status JdbcConnector::_cast_string_to_bitmap(const SlotDescriptor* slot_desc, Block* block,
                                             int column_index, int rows) {
    DataTypePtr _target_data_type = slot_desc->get_data_type_ptr();
    std::string _target_data_type_name = _target_data_type->get_name();
    DataTypePtr _cast_param_data_type = _target_data_type;
    ColumnPtr _cast_param = _cast_param_data_type->create_column_const_with_default_value(1);

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(
            std::move(str_bitmap_cols[_map_column_idx_to_cast_idx_bitmap[column_index]]),
            _input_bitmap_string_types[_map_column_idx_to_cast_idx_bitmap[column_index]],
            "java.sql.String");
    argument_template.emplace_back(_cast_param, _cast_param_data_type, _target_data_type_name);
    FunctionBasePtr func_cast = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, make_nullable(_target_data_type));

    Block cast_block(argument_template);
    int result_idx = cast_block.columns();
    cast_block.insert({nullptr, make_nullable(_target_data_type), "cast_result"});
    static_cast<void>(func_cast->execute(nullptr, cast_block, {0, 1}, result_idx, rows));

    auto res_col = cast_block.get_by_position(result_idx).column;
    if (_target_data_type->is_nullable()) {
        block->replace_by_position(column_index, res_col);
    } else {
        auto nested_ptr = reinterpret_cast<const vectorized::ColumnNullable*>(res_col.get())
                                  ->get_nested_column_ptr();
        block->replace_by_position(column_index, nested_ptr);
    }
    str_bitmap_cols[_map_column_idx_to_cast_idx_bitmap[column_index]] =
            _input_bitmap_string_types[_map_column_idx_to_cast_idx_bitmap[column_index]]
                    ->create_column();
    return Status::OK();
}

Status JdbcConnector::_cast_string_to_array(const SlotDescriptor* slot_desc, Block* block,
                                            int column_index, int rows) {
    DataTypePtr _target_data_type = slot_desc->get_data_type_ptr();
    std::string _target_data_type_name = _target_data_type->get_name();
    DataTypePtr _cast_param_data_type = _target_data_type;
    ColumnPtr _cast_param = _cast_param_data_type->create_column_const_with_default_value(1);

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
    static_cast<void>(func_cast->execute(nullptr, cast_block, {0, 1}, result_idx, rows));

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

Status JdbcConnector::_cast_string_to_json(const SlotDescriptor* slot_desc, Block* block,
                                           int column_index, int rows) {
    DataTypePtr _target_data_type = slot_desc->get_data_type_ptr();
    std::string _target_data_type_name = _target_data_type->get_name();
    DataTypePtr _cast_param_data_type = _target_data_type;
    ColumnPtr _cast_param = _cast_param_data_type->create_column_const(1, "{}");

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(
            std::move(str_json_cols[_map_column_idx_to_cast_idx_json[column_index]]),
            _input_json_string_types[_map_column_idx_to_cast_idx_json[column_index]],
            "java.sql.String");
    argument_template.emplace_back(_cast_param, _cast_param_data_type, _target_data_type_name);
    FunctionBasePtr func_cast = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, make_nullable(_target_data_type));

    Block cast_block(argument_template);
    int result_idx = cast_block.columns();
    cast_block.insert({nullptr, make_nullable(_target_data_type), "cast_result"});
    static_cast<void>(func_cast->execute(nullptr, cast_block, {0, 1}, result_idx, rows));

    auto res_col = cast_block.get_by_position(result_idx).column;
    if (_target_data_type->is_nullable()) {
        block->replace_by_position(column_index, res_col);
    } else {
        auto nested_ptr = reinterpret_cast<const vectorized::ColumnNullable*>(res_col.get())
                                  ->get_nested_column_ptr();
        block->replace_by_position(column_index, nested_ptr);
    }
    str_json_cols[_map_column_idx_to_cast_idx_json[column_index]] =
            _input_json_string_types[_map_column_idx_to_cast_idx_json[column_index]]
                    ->create_column();
    return Status::OK();
}

Status JdbcConnector::exec_stmt_write(Block* block, const VExprContextSPtrs& output_vexpr_ctxs,
                                      uint32_t* num_rows_sent) {
    SCOPED_TIMER(_result_send_timer);
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));

    // prepare table meta information
    std::unique_ptr<long[]> meta_data;
    RETURN_IF_ERROR(JniConnector::to_java_table(block, meta_data));
    long meta_address = (long)meta_data.get();
    auto table_schema = JniConnector::parse_table_schema(block);

    // prepare constructor parameters
    std::map<String, String> write_params = {{"meta_address", std::to_string(meta_address)},
                                             {"required_fields", table_schema.first},
                                             {"columns_types", table_schema.second}};
    jobject hashmap_object = JniUtil::convert_to_java_map(env, write_params);
    env->CallNonvirtualIntMethod(_executor_obj, _executor_clazz, _executor_stmt_write_id,
                                 hashmap_object);
    env->DeleteLocalRef(hashmap_object);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    *num_rows_sent = block->rows();
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

Status JdbcConnector::begin_trans() {
    if (_use_tranaction) {
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_begin_trans_id);
        RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
        _is_in_transaction = true;
    }
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
    if (_use_tranaction && _is_in_transaction) {
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
        env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_finish_trans_id);
        RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
        _is_in_transaction = false;
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
