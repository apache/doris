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

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <jni.h>
#include <stdint.h>
#include <util/jni-util.h>

#include <string>

#include "common/status.h"

namespace doris {
namespace vectorized {

/**
 * JdbcConnectorParam holds JDBC connection parameters.
 *
 * Note: After Phase 3 refactoring, JdbcConnectorParam is only used by
 * internal_service.cpp for test_jdbc_connection(). JDBC read/write
 * now goes through JdbcJniReader/JdbcJniWriter respectively.
 */
struct JdbcConnectorParam {
    // use -1 as default value to find error earlier.
    int64_t catalog_id = -1;
    std::string driver_path;
    std::string driver_class;
    std::string resource_name;
    std::string driver_checksum;
    std::string jdbc_url;
    std::string user;
    std::string passwd;
    std::string query_string;
    std::string table_name;
    bool use_transaction = false;
    TOdbcTableType::type table_type;
    int32_t connection_pool_min_size = -1;
    int32_t connection_pool_max_size = -1;
    int32_t connection_pool_max_wait_time = -1;
    int32_t connection_pool_max_life_time = -1;
    bool connection_pool_keep_alive = false;
};

/**
 * JdbcConnector is now a reduced utility class used ONLY for:
 * - test_connection() — called by PInternalService::test_jdbc_connection
 * - clean_datasource() — called after test_connection
 *
 * All JDBC read operations have been moved to JdbcJniReader + JdbcJniScanner.
 * All JDBC write operations have been moved to VJdbcTableWriter + JdbcJniWriter.
 *
 * @deprecated This class should be further simplified or removed when
 * test_jdbc_connection is refactored to use the new JNI framework.
 */
class JdbcConnector {
public:
    JdbcConnector(const JdbcConnectorParam& param);
    ~JdbcConnector();

    Status open();
    Status close();

    // Test JDBC connection by opening a connection and calling testConnection()
    Status test_connection();
    // Clean connection pool data source
    Status clean_datasource();

private:
    Status _register_func_id(JNIEnv* env);
    Status _get_java_table_type(JNIEnv* env, TOdbcTableType::type table_type,
                                Jni::LocalObject* java_enum_obj);
    Status _get_real_url(const std::string& url, std::string* result_url);
    Status _check_and_return_default_driver_url(const std::string& url, std::string* result_url);

    bool _closed = false;
    bool _is_open = false;
    JdbcConnectorParam _conn_param;

    Jni::GlobalClass _executor_factory_clazz;
    Jni::GlobalClass _executor_clazz;
    Jni::GlobalObject _executor_obj;
    Jni::MethodId _executor_factory_ctor_id;
    Jni::MethodId _executor_ctor_id;
    Jni::MethodId _executor_close_id;
    Jni::MethodId _executor_test_connection_id;
    Jni::MethodId _executor_clean_datasource_id;
};

} // namespace vectorized
} // namespace doris
