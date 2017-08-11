// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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
#include "runtime/row_batch.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "runtime/client_cache.h"
#include "util/debug_util.h"
#include "util/network_util.h"
#include "util/thrift_util.h"
#include "util/runtime_profile.h"

namespace palo {

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
         const TGetDbsParams &db_params,
         TGetDbsResult *db_result) {
    Status status;
    TNetworkAddress address = make_network_address(ip, port);
    try {
        // 500ms is enough
        FrontendServiceConnection client(
            _s_exec_env->frontend_client_cache(), 
            address, 
            500, 
            &status);
        if (!status.ok()) {
            return status;
        }

        try {
            client->getDbNames(*db_result, db_params);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->getDbNames(*db_result, db_params);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "getDbNames from " << address << " failed:" << e.what();
        return Status(TStatusCode::THRIFT_RPC_ERROR, ss.str(), false);
    }
    return Status::OK;
}

Status FrontendHelper::get_table_names(
         const std::string& ip,
         const int32_t port,
         const TGetTablesParams &table_params,
         TGetTablesResult *table_result) {
    Status status;
    TNetworkAddress address = make_network_address(ip, port);
    try {
        // 500ms is enough
        FrontendServiceConnection client(
            _s_exec_env->frontend_client_cache(), 
            address, 
            500, 
            &status);
        if (!status.ok()) {
            return status;
        }

        try {
            client->getTableNames(*table_result, table_params);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->getTableNames(*table_result, table_params);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "getTableNames from " << address << " failed:" << e.what();
        return Status(TStatusCode::THRIFT_RPC_ERROR, ss.str(), false);
    }
    return Status::OK;
}

Status FrontendHelper::list_table_status(
         const std::string& ip,
         const int32_t port,
         const TGetTablesParams &table_params,
         TListTableStatusResult *table_result) {
    Status status;
    TNetworkAddress address = make_network_address(ip, port);
    try {
        // 500ms is enough
        FrontendServiceConnection client(
            _s_exec_env->frontend_client_cache(), 
            address, 
            500, 
            &status);
        if (!status.ok()) {
            return status;
        }

        try {
            client->listTableStatus(*table_result, table_params);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->listTableStatus(*table_result, table_params);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "getTableNames from " << address << " failed:" << e.what();
        return Status(TStatusCode::THRIFT_RPC_ERROR, ss.str(), false);
    }
    return Status::OK;
}

Status FrontendHelper::describe_table(
         const std::string& ip,
         const int32_t port,
         const TDescribeTableParams &desc_params,
         TDescribeTableResult *desc_result) {
    Status status;
    TNetworkAddress address = make_network_address(ip, port);
    try {
        // 500ms is enough
        FrontendServiceConnection client(
            _s_exec_env->frontend_client_cache(), 
            address, 
            500, 
            &status);
        if (!status.ok()) {
            return status;
        }

        try {
            client->describeTable(*desc_result, desc_params);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->describeTable(*desc_result, desc_params);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "describeTable from " << address << " failed:" << e.what();
        return Status(TStatusCode::THRIFT_RPC_ERROR, ss.str(), false);
    }
    return Status::OK;
}

Status FrontendHelper::show_varialbes(
         const std::string& ip,
         const int32_t port,
         const TShowVariableRequest &var_params,
         TShowVariableResult *var_result) {
    Status status;
    TNetworkAddress address = make_network_address(ip, port);
    try {
        // 500ms is enough
        FrontendServiceConnection client(
            _s_exec_env->frontend_client_cache(), 
            address, 
            500, 
            &status);
        if (!status.ok()) {
            return status;
        }

        try {
            client->showVariables(*var_result, var_params);
        } catch (apache::thrift::transport::TTransportException& e) {
            RETURN_IF_ERROR(client.reopen());
            client->showVariables(*var_result, var_params);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "showVariables from " << address << " failed:" << e.what();
        return Status(TStatusCode::THRIFT_RPC_ERROR, ss.str(), false);
    }
    return Status::OK;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
