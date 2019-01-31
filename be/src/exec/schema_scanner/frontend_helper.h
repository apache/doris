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

#ifndef  DORIS_BE_SRC_QUERY_EXEC_SCHEMA_SCANNER_FRONTEND_HELPER_H
#define  DORIS_BE_SRC_QUERY_EXEC_SCHEMA_SCANNER_FRONTEND_HELPER_H

#include "common/status.h"
#include "gen_cpp/FrontendService_types.h"

namespace doris {

class ExecEnv;
class FrontendServiceClient;
template <class T> class ClientConnection;

// this class is a helper for jni call. easy for unit test
class FrontendHelper {
public:
    static void setup(ExecEnv* exec_env);
    static Status get_db_names(
            const std::string& ip,
            const int32_t port,
            const TGetDbsParams &db_params,
            TGetDbsResult *db_result);
    static Status get_table_names(
            const std::string& ip,
            const int32_t port,
            const TGetTablesParams &table_params,
            TGetTablesResult *table_result);
    static Status list_table_status(
            const std::string& ip,
            const int32_t port,
            const TGetTablesParams &table_params,
            TListTableStatusResult *table_result);
    static Status describe_table(
            const std::string& ip,
            const int32_t port,
            const TDescribeTableParams &desc_params,
            TDescribeTableResult *desc_result);
    static Status show_varialbes(
            const std::string& ip,
            const int32_t port,
            const TShowVariableRequest &var_params,
            TShowVariableResult *var_result);

    static std::string extract_db_name(const std::string& full_name);

    // for default timeout
    static Status rpc(
        const std::string& ip,
        const int32_t port,
        std::function<void (ClientConnection<FrontendServiceClient>&)> callback) {

        return rpc(ip, port, callback, config::thrift_rpc_timeout_ms);
    }

    static Status rpc(
        const std::string& ip,
        const int32_t port,
        std::function<void (ClientConnection<FrontendServiceClient>&)> callback,
        int timeout_ms);

private:
    static ExecEnv* _s_exec_env;
};

}

#endif

