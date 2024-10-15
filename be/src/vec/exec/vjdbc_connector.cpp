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
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath> // IWYU pragma: keep
#include <memory>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "exec/table_connector.h"
#include "gutil/strings/substitute.h"
#include "jni.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "runtime/user_function_cache.h"
#include "util/jni-util.h"
#include "util/runtime_profile.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exec/jni_connector.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
const char* JDBC_EXECUTOR_FACTORY_CLASS = "org/apache/doris/jdbc/JdbcExecutorFactory";
const char* JDBC_EXECUTOR_CTOR_SIGNATURE = "([B)V";
const char* JDBC_EXECUTOR_STMT_WRITE_SIGNATURE = "(Ljava/util/Map;)I";
const char* JDBC_EXECUTOR_HAS_NEXT_SIGNATURE = "()Z";
const char* JDBC_EXECUTOR_CLOSE_SIGNATURE = "()V";
const char* JDBC_EXECUTOR_TRANSACTION_SIGNATURE = "()V";

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

Status JdbcConnector::close(Status /*unused*/) {
    SCOPED_RAW_TIMER(&_jdbc_statistic._connector_close_timer);
    _closed = true;
    if (!_is_open) {
        return Status::OK();
    }
    if (_is_in_transaction) {
        RETURN_IF_ERROR(abort_trans());
    }
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_close_id);
    env->DeleteGlobalRef(_executor_factory_clazz);
    env->DeleteGlobalRef(_executor_clazz);
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
    RETURN_IF_ERROR(JniUtil::get_jni_scanner_class(env, JDBC_EXECUTOR_FACTORY_CLASS,
                                                   &_executor_factory_clazz));

    _executor_factory_ctor_id =
            env->GetStaticMethodID(_executor_factory_clazz, "getExecutorClass",
                                   "(Lorg/apache/doris/thrift/TOdbcTableType;)Ljava/lang/String;");
    if (_executor_factory_ctor_id == nullptr) {
        return Status::InternalError("Failed to find method ID for getExecutorClass");
    }

    jobject jtable_type = _get_java_table_type(env, _conn_param.table_type);

    jstring executor_name = (jstring)env->CallStaticObjectMethod(
            _executor_factory_clazz, _executor_factory_ctor_id, jtable_type);
    if (executor_name == nullptr) {
        return Status::InternalError("getExecutorClass returned null");
    }
    const char* executor_name_str = env->GetStringUTFChars(executor_name, nullptr);

    RETURN_IF_ERROR(JniUtil::get_jni_scanner_class(env, executor_name_str, &_executor_clazz));
    env->DeleteLocalRef(jtable_type);
    env->ReleaseStringUTFChars(executor_name, executor_name_str);
    env->DeleteLocalRef(executor_name);

#undef GET_BASIC_JAVA_CLAZZ
    RETURN_IF_ERROR(_register_func_id(env));

    // Add a scoped cleanup jni reference object. This cleans up local refs made below.
    JniLocalFrame jni_frame;
    {
        std::string local_location;
        std::hash<std::string> hash_str;
        auto* function_cache = UserFunctionCache::instance();
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
        ctor_params.__set_catalog_id(_conn_param.catalog_id);
        ctor_params.__set_jdbc_url(_conn_param.jdbc_url);
        ctor_params.__set_jdbc_user(_conn_param.user);
        ctor_params.__set_jdbc_password(_conn_param.passwd);
        ctor_params.__set_jdbc_driver_class(_conn_param.driver_class);
        ctor_params.__set_driver_path(local_location);
        if (state == nullptr) {
            ctor_params.__set_batch_size(read ? 1 : 0);
        } else {
            ctor_params.__set_batch_size(read ? state->batch_size() : 0);
        }
        ctor_params.__set_op(read ? TJdbcOperation::READ : TJdbcOperation::WRITE);
        ctor_params.__set_table_type(_conn_param.table_type);
        ctor_params.__set_connection_pool_min_size(_conn_param.connection_pool_min_size);
        ctor_params.__set_connection_pool_max_size(_conn_param.connection_pool_max_size);
        ctor_params.__set_connection_pool_max_wait_time(_conn_param.connection_pool_max_wait_time);
        ctor_params.__set_connection_pool_max_life_time(_conn_param.connection_pool_max_life_time);
        ctor_params.__set_connection_pool_cache_clear_time(
                config::jdbc_connection_pool_cache_clear_time_sec);
        ctor_params.__set_connection_pool_keep_alive(_conn_param.connection_pool_keep_alive);

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
    RETURN_IF_ERROR(begin_trans());

    return Status::OK();
}

Status JdbcConnector::test_connection() {
    RETURN_IF_ERROR(open(nullptr, true));

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));

    env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_test_connection_id);
    return JniUtil::GetJniExceptionMsg(env);
}

Status JdbcConnector::clean_datasource() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    env->CallNonvirtualVoidMethod(_executor_obj, _executor_clazz, _executor_clean_datasource_id);
    return JniUtil::GetJniExceptionMsg(env);
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
    return Status::OK();
}

Status JdbcConnector::get_next(bool* eos, Block* block, int batch_size) {
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

    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));

    auto column_size = _tuple_desc->slots().size();
    auto slots = _tuple_desc->slots();

    jobject map = _get_reader_params(block, env, column_size);
    SCOPED_RAW_TIMER(&_jdbc_statistic._get_block_address_timer);
    long address =
            env->CallLongMethod(_executor_obj, _executor_get_block_address_id, batch_size, map);
    RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));
    env->DeleteLocalRef(map);

    std::vector<size_t> all_columns;
    for (size_t i = 0; i < column_size; ++i) {
        all_columns.push_back(i);
    }
    SCOPED_RAW_TIMER(&_jdbc_statistic._fill_block_timer);
    Status fill_block_status = JniConnector::fill_block(block, all_columns, address);
    if (!fill_block_status) {
        return fill_block_status;
    }

    Status cast_status = _cast_string_to_special(block, env, column_size);

    if (!cast_status) {
        return cast_status;
    }

    return JniUtil::GetJniExceptionMsg(env);
}

Status JdbcConnector::append(vectorized::Block* block,
                             const vectorized::VExprContextSPtrs& output_vexpr_ctxs,
                             uint32_t start_send_row, uint32_t* num_rows_sent,
                             TOdbcTableType::type table_type) {
    RETURN_IF_ERROR(exec_stmt_write(block, output_vexpr_ctxs, num_rows_sent));
    COUNTER_UPDATE(_sent_rows_counter, *num_rows_sent);
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
    RETURN_IF_ERROR(register_id(_executor_clazz, "write", JDBC_EXECUTOR_STMT_WRITE_SIGNATURE,
                                _executor_stmt_write_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "read", "()I", _executor_read_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "close", JDBC_EXECUTOR_CLOSE_SIGNATURE,
                                _executor_close_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "hasNext", JDBC_EXECUTOR_HAS_NEXT_SIGNATURE,
                                _executor_has_next_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "getBlockAddress", "(ILjava/util/Map;)J",
                                _executor_get_block_address_id));
    RETURN_IF_ERROR(
            register_id(_executor_clazz, "getCurBlockRows", "()I", _executor_block_rows_id));

    RETURN_IF_ERROR(register_id(_executor_clazz, "openTrans", JDBC_EXECUTOR_TRANSACTION_SIGNATURE,
                                _executor_begin_trans_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "commitTrans", JDBC_EXECUTOR_TRANSACTION_SIGNATURE,
                                _executor_finish_trans_id));
    RETURN_IF_ERROR(register_id(_executor_clazz, "rollbackTrans",
                                JDBC_EXECUTOR_TRANSACTION_SIGNATURE, _executor_abort_trans_id));
    RETURN_IF_ERROR(
            register_id(_executor_clazz, "testConnection", "()V", _executor_test_connection_id));
    RETURN_IF_ERROR(
            register_id(_executor_clazz, "cleanDataSource", "()V", _executor_clean_datasource_id));
    return Status::OK();
}

jobject JdbcConnector::_get_reader_params(Block* block, JNIEnv* env, size_t column_size) {
    std::ostringstream columns_nullable;
    std::ostringstream columns_replace_string;
    std::ostringstream required_fields;
    std::ostringstream columns_types;

    for (int i = 0; i < column_size; ++i) {
        auto* slot = _tuple_desc->slots()[i];
        if (slot->is_materialized()) {
            auto type = slot->type();
            // Record if column is nullable
            columns_nullable << (slot->is_nullable() ? "true" : "false") << ",";
            // Check column type and replace accordingly
            std::string replace_type = "not_replace";
            if (type.is_bitmap_type()) {
                replace_type = "bitmap";
            } else if (type.is_hll_type()) {
                replace_type = "hll";
            } else if (type.is_json_type()) {
                replace_type = "jsonb";
            }
            columns_replace_string << replace_type << ",";
            if (replace_type != "not_replace") {
                block->get_by_position(i).column = std::make_shared<DataTypeString>()
                                                           ->create_column()
                                                           ->convert_to_full_column_if_const();
                block->get_by_position(i).type = std::make_shared<DataTypeString>();
                if (slot->is_nullable()) {
                    block->get_by_position(i).column =
                            make_nullable(block->get_by_position(i).column);
                    block->get_by_position(i).type = make_nullable(block->get_by_position(i).type);
                }
            }
        }
        // Record required fields and column types
        std::string field = slot->col_name();
        std::string jni_type;
        if (slot->type().is_bitmap_type() || slot->type().is_hll_type() ||
            slot->type().is_json_type()) {
            jni_type = "string";
        } else {
            jni_type = JniConnector::get_jni_type(slot->type());
        }
        required_fields << (i != 0 ? "," : "") << field;
        columns_types << (i != 0 ? "#" : "") << jni_type;
    }

    std::map<String, String> reader_params = {{"is_nullable", columns_nullable.str()},
                                              {"replace_string", columns_replace_string.str()},
                                              {"required_fields", required_fields.str()},
                                              {"columns_types", columns_types.str()}};
    return JniUtil::convert_to_java_map(env, reader_params);
}

Status JdbcConnector::_cast_string_to_special(Block* block, JNIEnv* env, size_t column_size) {
    for (size_t column_index = 0; column_index < column_size; ++column_index) {
        auto* slot_desc = _tuple_desc->slots()[column_index];
        // because the fe planner filter the non_materialize column
        if (!slot_desc->is_materialized()) {
            continue;
        }
        jint num_rows = env->CallNonvirtualIntMethod(_executor_obj, _executor_clazz,
                                                     _executor_block_rows_id);

        RETURN_IF_ERROR(JniUtil::GetJniExceptionMsg(env));

        if (slot_desc->type().is_hll_type()) {
            RETURN_IF_ERROR(_cast_string_to_hll(slot_desc, block, column_index, num_rows));
        } else if (slot_desc->type().is_json_type()) {
            RETURN_IF_ERROR(_cast_string_to_json(slot_desc, block, column_index, num_rows));
        } else if (slot_desc->type().is_bitmap_type()) {
            RETURN_IF_ERROR(_cast_string_to_bitmap(slot_desc, block, column_index, num_rows));
        }
    }
    return Status::OK();
}

Status JdbcConnector::_cast_string_to_hll(const SlotDescriptor* slot_desc, Block* block,
                                          int column_index, int rows) {
    _map_column_idx_to_cast_idx_hll[column_index] = _input_hll_string_types.size();
    if (slot_desc->is_nullable()) {
        _input_hll_string_types.push_back(make_nullable(std::make_shared<DataTypeString>()));
    } else {
        _input_hll_string_types.push_back(std::make_shared<DataTypeString>());
    }

    DataTypePtr _target_data_type = slot_desc->get_data_type_ptr();
    std::string _target_data_type_name = _target_data_type->get_name();
    DataTypePtr _cast_param_data_type = _target_data_type;
    ColumnPtr _cast_param = _cast_param_data_type->create_column_const_with_default_value(1);

    auto& input_col = block->get_by_position(column_index).column;

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(
            std::move(input_col),
            _input_hll_string_types[_map_column_idx_to_cast_idx_hll[column_index]],
            "java.sql.String");
    argument_template.emplace_back(_cast_param, _cast_param_data_type, _target_data_type_name);
    FunctionBasePtr func_cast = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, make_nullable(_target_data_type));

    Block cast_block(argument_template);
    int result_idx = cast_block.columns();
    cast_block.insert({nullptr, make_nullable(_target_data_type), "cast_result"});
    RETURN_IF_ERROR(func_cast->execute(nullptr, cast_block, {0}, result_idx, rows));

    auto res_col = cast_block.get_by_position(result_idx).column;
    block->get_by_position(column_index).type = _target_data_type;
    if (_target_data_type->is_nullable()) {
        block->replace_by_position(column_index, res_col);
    } else {
        auto nested_ptr = reinterpret_cast<const vectorized::ColumnNullable*>(res_col.get())
                                  ->get_nested_column_ptr();
        block->replace_by_position(column_index, nested_ptr);
    }

    return Status::OK();
}

Status JdbcConnector::_cast_string_to_bitmap(const SlotDescriptor* slot_desc, Block* block,
                                             int column_index, int rows) {
    _map_column_idx_to_cast_idx_bitmap[column_index] = _input_bitmap_string_types.size();
    if (slot_desc->is_nullable()) {
        _input_bitmap_string_types.push_back(make_nullable(std::make_shared<DataTypeString>()));
    } else {
        _input_bitmap_string_types.push_back(std::make_shared<DataTypeString>());
    }

    DataTypePtr _target_data_type = slot_desc->get_data_type_ptr();
    std::string _target_data_type_name = _target_data_type->get_name();
    DataTypePtr _cast_param_data_type = _target_data_type;
    ColumnPtr _cast_param = _cast_param_data_type->create_column_const_with_default_value(1);

    auto& input_col = block->get_by_position(column_index).column;

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(
            std::move(input_col),
            _input_bitmap_string_types[_map_column_idx_to_cast_idx_bitmap[column_index]],
            "java.sql.String");
    argument_template.emplace_back(_cast_param, _cast_param_data_type, _target_data_type_name);
    FunctionBasePtr func_cast = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, make_nullable(_target_data_type));

    Block cast_block(argument_template);
    int result_idx = cast_block.columns();
    cast_block.insert({nullptr, make_nullable(_target_data_type), "cast_result"});
    RETURN_IF_ERROR(func_cast->execute(nullptr, cast_block, {0}, result_idx, rows));

    auto res_col = cast_block.get_by_position(result_idx).column;
    block->get_by_position(column_index).type = _target_data_type;
    if (_target_data_type->is_nullable()) {
        block->replace_by_position(column_index, res_col);
    } else {
        auto nested_ptr = reinterpret_cast<const vectorized::ColumnNullable*>(res_col.get())
                                  ->get_nested_column_ptr();
        block->replace_by_position(column_index, nested_ptr);
    }

    return Status::OK();
}

// Deprecated, this code is retained only for compatibility with query problems that may be encountered when upgrading the version that maps JSON to JSONB to this version, and will be deleted in subsequent versions.
Status JdbcConnector::_cast_string_to_json(const SlotDescriptor* slot_desc, Block* block,
                                           int column_index, int rows) {
    _map_column_idx_to_cast_idx_json[column_index] = _input_json_string_types.size();
    if (slot_desc->is_nullable()) {
        _input_json_string_types.push_back(make_nullable(std::make_shared<DataTypeString>()));
    } else {
        _input_json_string_types.push_back(std::make_shared<DataTypeString>());
    }
    DataTypePtr _target_data_type = slot_desc->get_data_type_ptr();
    std::string _target_data_type_name = _target_data_type->get_name();
    DataTypePtr _cast_param_data_type = _target_data_type;
    ColumnPtr _cast_param = _cast_param_data_type->create_column_const(1, "{}");

    auto& input_col = block->get_by_position(column_index).column;

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(
            std::move(input_col),
            _input_json_string_types[_map_column_idx_to_cast_idx_json[column_index]],
            "java.sql.String");
    argument_template.emplace_back(_cast_param, _cast_param_data_type, _target_data_type_name);
    FunctionBasePtr func_cast = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, make_nullable(_target_data_type));

    Block cast_block(argument_template);
    int result_idx = cast_block.columns();
    cast_block.insert({nullptr, make_nullable(_target_data_type), "cast_result"});
    RETURN_IF_ERROR(func_cast->execute(nullptr, cast_block, {0}, result_idx, rows));

    auto res_col = cast_block.get_by_position(result_idx).column;
    block->get_by_position(column_index).type = _target_data_type;
    if (_target_data_type->is_nullable()) {
        block->replace_by_position(column_index, res_col);
    } else {
        auto nested_ptr = reinterpret_cast<const vectorized::ColumnNullable*>(res_col.get())
                                  ->get_nested_column_ptr();
        block->replace_by_position(column_index, nested_ptr);
    }

    return Status::OK();
}

jobject JdbcConnector::_get_java_table_type(JNIEnv* env, TOdbcTableType::type tableType) {
    jclass enumClass = env->FindClass("org/apache/doris/thrift/TOdbcTableType");
    jmethodID findByValueMethod = env->GetStaticMethodID(
            enumClass, "findByValue", "(I)Lorg/apache/doris/thrift/TOdbcTableType;");
    jobject javaEnumObj =
            env->CallStaticObjectMethod(enumClass, findByValueMethod, static_cast<jint>(tableType));
    return javaEnumObj;
}
} // namespace doris::vectorized
