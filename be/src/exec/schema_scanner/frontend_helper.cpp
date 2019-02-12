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

#include "exec/schema_scanner/frontend_helper.h"

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
#include "util/network_util.h"
#include "util/thrift_util.h"
#include "util/runtime_profile.h"
#include "runtime/client_cache.h"

namespace doris {

ExecEnv* FrontendHelper::_s_exec_env;

using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TBufferedTransport;

void FrontendHelper::setup(ExecEnv* exec_env) {
    _s_exec_env = exec_env;
}

Status FrontendHelper::get_db_names(
         const std::string& ip,
         const int32_t port,
         const TGetDbsParams &request,
         TGetDbsResult *result) {
    return rpc(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->getDbNames(*result, request);
               });
}

Status FrontendHelper::get_table_names(
         const std::string& ip,
         const int32_t port,
         const TGetTablesParams &request,
         TGetTablesResult *result) {
    return rpc(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->getTableNames(*result, request);
               });
}

Status FrontendHelper::list_table_status(
         const std::string& ip,
         const int32_t port,
         const TGetTablesParams &request,
         TListTableStatusResult *result) {
    return rpc(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->listTableStatus(*result, request);
               });
}

Status FrontendHelper::describe_table(
         const std::string& ip,
         const int32_t port,
         const TDescribeTableParams &request,
         TDescribeTableResult *result) {
    return rpc(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->describeTable(*result, request);
               });
}

Status FrontendHelper::show_varialbes(
         const std::string& ip,
         const int32_t port,
         const TShowVariableRequest &request,
         TShowVariableResult *result) {
    return rpc(ip, port,
               [&request, &result] (FrontendServiceConnection& client) {
                client->showVariables(*result, request);
               });
}

Status FrontendHelper::rpc(
        const std::string& ip,
        const int32_t port,
        std::function<void (FrontendServiceConnection&)> callback,
        int timeout_ms) {
    TNetworkAddress address = make_network_address(ip, port);
    Status status;
    FrontendServiceConnection client(
            _s_exec_env->frontend_client_cache(), address, timeout_ms, &status);
    if (!status.ok()) {
        LOG(WARNING) << "Connect frontent failed, address=" << address
            << ", status=" << status.get_error_msg();
        return status;
    }
    try {
        try {
            callback(client);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "retrying call frontend service, address="
                    << address << ", reason=" << e.what();
            status = client.reopen(timeout_ms);
            if (!status.ok()) {
                LOG(WARNING) << "client repoen failed. address=" << address
                    << ", status=" << status.get_error_msg();
                return status;
            }
            callback(client);
        }
    } catch (apache::thrift::TException& e) {
        // just reopen to disable this connection
        client.reopen(timeout_ms);
        LOG(WARNING) << "call frontend service failed, address=" << address
            << ", reason=" << e.what();
        return Status(TStatusCode::THRIFT_RPC_ERROR,
                      "failed to call frontend service", false);
    }
    return Status::OK;
}

std::string FrontendHelper::extract_db_name(const std::string& full_name) {
    auto found = full_name.find(':');
    if (found == std::string::npos) {
        return full_name;
    }
    found++;
    return std::string(full_name.c_str() + found, full_name.size() - found);
}

}

