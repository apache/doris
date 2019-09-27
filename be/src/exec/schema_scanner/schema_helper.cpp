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

#include <boost/foreach.hpp>
#include <boost/functional/hash.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

#include "exec/text_converter.hpp"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/runtime_state.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "runtime/client_cache.h"
#include "util/debug_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/network_util.h"
#include "util/thrift_util.h"
#include "util/runtime_profile.h"
#include "runtime/client_cache.h"

namespace doris {

Status SchemaHelper::get_db_names(
         const std::string& ip,
         const int32_t port,
         const TGetDbsParams &request,
         TGetDbsResult *result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->getDbNames(*result, request);
               });
}

Status SchemaHelper::get_table_names(
         const std::string& ip,
         const int32_t port,
         const TGetTablesParams &request,
         TGetTablesResult *result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->getTableNames(*result, request);
               });
}

Status SchemaHelper::list_table_status(
         const std::string& ip,
         const int32_t port,
         const TGetTablesParams &request,
         TListTableStatusResult *result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->listTableStatus(*result, request);
               });
}

Status SchemaHelper::describe_table(
         const std::string& ip,
         const int32_t port,
         const TDescribeTableParams &request,
         TDescribeTableResult *result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->describeTable(*result, request);
               });
}

Status SchemaHelper::show_varialbes(
         const std::string& ip,
         const int32_t port,
         const TShowVariableRequest &request,
         TShowVariableResult *result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->showVariables(*result, request);
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

}

