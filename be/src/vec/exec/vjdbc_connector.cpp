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

#include <filesystem>
#include <memory>
#include <ostream>

#include "cloud/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "jni.h"
#include "runtime/plugin/cloud_plugin_downloader.h"
#include "util/jni-util.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

const char* JDBC_EXECUTOR_FACTORY_CLASS = "org/apache/doris/jdbc/JdbcExecutorFactory";
const char* JDBC_EXECUTOR_CTOR_SIGNATURE = "([B)V";
const char* JDBC_EXECUTOR_CLOSE_SIGNATURE = "()V";

JdbcConnector::JdbcConnector(const JdbcConnectorParam& param)
        : _conn_param(param), _closed(false), _is_open(false) {}

JdbcConnector::~JdbcConnector() {
    if (!_closed) {
        static_cast<void>(close());
    }
}

Status JdbcConnector::close() {
    if (_closed) {
        return Status::OK();
    }
    if (!_is_open) {
        _closed = true;
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    RETURN_IF_ERROR(
            _executor_obj.call_nonvirtual_void_method(env, _executor_clazz, _executor_close_id)
                    .call());
    _closed = true;
    return Status::OK();
}

Status JdbcConnector::open() {
    if (_is_open) {
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    RETURN_IF_ERROR(Jni::Util::get_jni_scanner_class(env, JDBC_EXECUTOR_FACTORY_CLASS,
                                                     &_executor_factory_clazz));

    RETURN_IF_ERROR(_executor_factory_clazz.get_static_method(
            env, "getExecutorClass", "(Lorg/apache/doris/thrift/TOdbcTableType;)Ljava/lang/String;",
            &_executor_factory_ctor_id));

    Jni::LocalObject jtable_type;
    RETURN_IF_ERROR(_get_java_table_type(env, _conn_param.table_type, &jtable_type));

    Jni::LocalString executor_name;
    RETURN_IF_ERROR(
            _executor_factory_clazz.call_static_object_method(env, _executor_factory_ctor_id)
                    .with_arg(jtable_type)
                    .call(&executor_name));

    Jni::LocalStringBufferGuard executor_name_str;
    RETURN_IF_ERROR(executor_name.get_string_chars(env, &executor_name_str));

    RETURN_IF_ERROR(
            Jni::Util::get_jni_scanner_class(env, executor_name_str.get(), &_executor_clazz));

    RETURN_IF_ERROR(_register_func_id(env));

    std::string driver_path;
    RETURN_IF_ERROR(_get_real_url(_conn_param.driver_path, &driver_path));

    TJdbcExecutorCtorParams ctor_params;
    ctor_params.__set_statement(_conn_param.query_string);
    ctor_params.__set_catalog_id(_conn_param.catalog_id);
    ctor_params.__set_jdbc_url(_conn_param.jdbc_url);
    ctor_params.__set_jdbc_user(_conn_param.user);
    ctor_params.__set_jdbc_password(_conn_param.passwd);
    ctor_params.__set_jdbc_driver_class(_conn_param.driver_class);
    ctor_params.__set_driver_path(driver_path);
    ctor_params.__set_jdbc_driver_checksum(_conn_param.driver_checksum);
    ctor_params.__set_batch_size(1);
    ctor_params.__set_op(TJdbcOperation::READ);
    ctor_params.__set_table_type(_conn_param.table_type);
    ctor_params.__set_connection_pool_min_size(_conn_param.connection_pool_min_size);
    ctor_params.__set_connection_pool_max_size(_conn_param.connection_pool_max_size);
    ctor_params.__set_connection_pool_max_wait_time(_conn_param.connection_pool_max_wait_time);
    ctor_params.__set_connection_pool_max_life_time(_conn_param.connection_pool_max_life_time);
    ctor_params.__set_connection_pool_cache_clear_time(
            config::jdbc_connection_pool_cache_clear_time_sec);
    ctor_params.__set_connection_pool_keep_alive(_conn_param.connection_pool_keep_alive);

    Jni::LocalArray ctor_params_bytes;
    RETURN_IF_ERROR(Jni::Util::SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));

    RETURN_IF_ERROR(_executor_clazz.new_object(env, _executor_ctor_id)
                            .with_arg(ctor_params_bytes)
                            .call(&_executor_obj));

    _is_open = true;
    return Status::OK();
}

Status JdbcConnector::test_connection() {
    RETURN_IF_ERROR(open());

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    return _executor_obj
            .call_nonvirtual_void_method(env, _executor_clazz, _executor_test_connection_id)
            .call();
}

Status JdbcConnector::clean_datasource() {
    if (!_is_open) {
        return Status::OK();
    }
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    return _executor_obj
            .call_nonvirtual_void_method(env, _executor_clazz, _executor_clean_datasource_id)
            .call();
}

Status JdbcConnector::_register_func_id(JNIEnv* env) {
    RETURN_IF_ERROR(_executor_clazz.get_method(env, "<init>", JDBC_EXECUTOR_CTOR_SIGNATURE,
                                               &_executor_ctor_id));
    RETURN_IF_ERROR(_executor_clazz.get_method(env, "close", JDBC_EXECUTOR_CLOSE_SIGNATURE,
                                               &_executor_close_id));
    RETURN_IF_ERROR(_executor_clazz.get_method(env, "testConnection", "()V",
                                               &_executor_test_connection_id));
    RETURN_IF_ERROR(_executor_clazz.get_method(env, "cleanDataSource", "()V",
                                               &_executor_clean_datasource_id));
    return Status::OK();
}

Status JdbcConnector::_get_java_table_type(JNIEnv* env, TOdbcTableType::type table_type,
                                           Jni::LocalObject* java_enum_obj) {
    Jni::LocalClass enum_class;
    RETURN_IF_ERROR(
            Jni::Util::find_class(env, "org/apache/doris/thrift/TOdbcTableType", &enum_class));

    Jni::MethodId find_by_value_method;
    RETURN_IF_ERROR(enum_class.get_static_method(env, "findByValue",
                                                 "(I)Lorg/apache/doris/thrift/TOdbcTableType;",
                                                 &find_by_value_method));

    return enum_class.call_static_object_method(env, find_by_value_method)
            .with_arg(static_cast<jint>(table_type))
            .call(java_enum_obj);
}

Status JdbcConnector::_get_real_url(const std::string& url, std::string* result_url) {
    if (url.find(":/") == std::string::npos) {
        return _check_and_return_default_driver_url(url, result_url);
    }
    *result_url = url;
    return Status::OK();
}

Status JdbcConnector::_check_and_return_default_driver_url(const std::string& url,
                                                           std::string* result_url) {
    const char* doris_home = std::getenv("DORIS_HOME");
    std::string default_url = std::string(doris_home) + "/plugins/jdbc_drivers";
    std::string default_old_url = std::string(doris_home) + "/jdbc_drivers";

    if (config::jdbc_drivers_dir == default_url) {
        std::string target_path = default_url + "/" + url;
        std::string old_target_path = default_old_url + "/" + url;
        if (std::filesystem::exists(target_path)) {
            *result_url = "file://" + target_path;
            return Status::OK();
        } else if (std::filesystem::exists(old_target_path)) {
            *result_url = "file://" + old_target_path;
            return Status::OK();
        } else if (config::is_cloud_mode()) {
            std::string downloaded_path;
            Status status = CloudPluginDownloader::download_from_cloud(
                    CloudPluginDownloader::PluginType::JDBC_DRIVERS, url, target_path,
                    &downloaded_path);
            if (status.ok() && !downloaded_path.empty()) {
                *result_url = "file://" + downloaded_path;
                return Status::OK();
            }
            LOG(WARNING) << "Failed to download JDBC driver from cloud: " << status.to_string()
                         << ", fallback to old directory";
        } else {
            return Status::InternalError("JDBC driver file does not exist: " + url);
        }
    } else {
        *result_url = "file://" + config::jdbc_drivers_dir + "/" + url;
    }
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
