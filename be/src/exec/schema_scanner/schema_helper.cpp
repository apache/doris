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

#include "exec/schema_scanner/schema_helper.h"

#include <gen_cpp/FrontendService.h>

#include "runtime/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace doris {
class TDescribeTableParams;
class TDescribeTableResult;
class TDescribeTablesParams;
class TDescribeTablesResult;
class TGetDbsParams;
class TGetDbsResult;
class TGetTablesParams;
class TGetTablesResult;
class TListPrivilegesResult;
class TListTableStatusResult;
class TShowVariableRequest;
class TShowVariableResult;
class TShowProcessListRequest;
class TShowProcessListResult;
class TShowUserRequest;
class TShowUserResult;

Status SchemaHelper::get_db_names(const std::string& ip, const int32_t port,
                                  const TGetDbsParams& request, TGetDbsResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->getDbNames(*result, request);
            });
}

Status SchemaHelper::get_table_names(const std::string& ip, const int32_t port,
                                     const TGetTablesParams& request, TGetTablesResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->getTableNames(*result, request);
            });
}

Status SchemaHelper::list_table_status(const std::string& ip, const int32_t port,
                                       const TGetTablesParams& request,
                                       TListTableStatusResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->listTableStatus(*result, request);
            });
}
Status SchemaHelper::list_table_metadata_name_ids(const std::string& ip, const int32_t port,
                                                  const doris::TGetTablesParams& request,
                                                  TListTableMetadataNameIdsResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->listTableMetadataNameIds(*result, request);
            });
}

Status SchemaHelper::describe_tables(const std::string& ip, const int32_t port,
                                     const TDescribeTablesParams& request,
                                     TDescribeTablesResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->describeTables(*result, request);
            });
}

Status SchemaHelper::show_variables(const std::string& ip, const int32_t port,
                                    const TShowVariableRequest& request,
                                    TShowVariableResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->showVariables(*result, request);
            });
}

Status SchemaHelper::list_table_privilege_status(const std::string& ip, const int32_t port,
                                                 const TGetTablesParams& request,
                                                 TListPrivilegesResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->listTablePrivilegeStatus(*result, request);
            });
}

Status SchemaHelper::list_schema_privilege_status(const std::string& ip, const int32_t port,
                                                  const TGetTablesParams& request,
                                                  TListPrivilegesResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->listSchemaPrivilegeStatus(*result, request);
            });
}

Status SchemaHelper::list_user_privilege_status(const std::string& ip, const int32_t port,
                                                const TGetTablesParams& request,
                                                TListPrivilegesResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->listUserPrivilegeStatus(*result, request);
            });
}

std::string SchemaHelper::extract_db_name(const std::string& full_name) {
    auto found = full_name.find(':');
    if (found == std::string::npos) {
        return full_name;
    }
    found++;
    return std::string(full_name.c_str() + found, full_name.size() - found);
}

Status SchemaHelper::show_process_list(const std::string& ip, const int32_t port,
                                       const TShowProcessListRequest& request,
                                       TShowProcessListResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->showProcessList(*result, request);
            });
}

Status SchemaHelper::show_user(const std::string& ip, const int32_t port,
                               const TShowUserRequest& request, TShowUserResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->showUser(*result, request);
            });
}

} // namespace doris
