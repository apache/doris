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

#include <sstream>
#include <thread>

#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

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

Status SchemaHelper::describe_table(const std::string& ip, const int32_t port,
                                    const TDescribeTableParams& request,
                                    TDescribeTableResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->describeTable(*result, request);
            });
}

Status SchemaHelper::describe_tables(const std::string& ip, const int32_t port,
                                     const TDescribeTablesParams& request,
                                     TDescribeTablesResult* result) {
    Status rpcStatus = ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) {
                client->describeTables(*result, request);
            });
    // FE have no describeTables rpc service
    if (!rpcStatus.ok()) {
        TDescribeTableParams single_table_request;
        single_table_request.__set_db(request.db);
        if (request.__isset.catalog) {
            single_table_request.__set_catalog(request.catalog);
        }

        if (request.__isset.current_user_ident) {
            single_table_request.__set_current_user_ident(request.current_user_ident);
        } else {
            if (request.__isset.user) {
                single_table_request.__set_user(request.user);
            }
            if (request.__isset.user_ip) {
                single_table_request.__set_user_ip(request.user_ip);
            }
        }
        const auto& tables = request.tables_name;
        result->columns.clear();
        result->tables_offset.clear();
        for (int i = 0; i < tables.size(); ++i) {
            single_table_request.__set_table_name(tables[i]);
            TDescribeTableResult single_table_result;
            RETURN_IF_ERROR(describe_table(ip, port, single_table_request, &single_table_result));
            const auto& columns = single_table_result.columns;
            for (int j = 0; j < columns.size(); ++j) {
                result->columns.emplace_back(columns[j]);
            }
            result->tables_offset.emplace_back(result->columns.size());
        }
    }

    return rpcStatus;
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

} // namespace doris
