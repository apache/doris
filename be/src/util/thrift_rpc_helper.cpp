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

#include "util/thrift_rpc_helper.h"

#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportException.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <sstream>
#include <thread>

#include "common/status.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h" // IWYU pragma: keep
#include "util/bvar_helper.h"
#include "util/network_util.h"

namespace apache {
namespace thrift {
namespace protocol {
class TProtocol;
} // namespace protocol
namespace transport {
class TBufferedTransport;
class TSocket;
class TTransport;
} // namespace transport
} // namespace thrift
} // namespace apache

namespace doris {

using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TBufferedTransport;

bvar::LatencyRecorder g_bvar_frontend_service_report_commit_txn_result_latency(
        "frontend_service", "report_commit_txn_result");
bvar::LatencyRecorder g_bvar_frontend_service_stream_load_put_latency("frontend_service",
                                                                      "stream_load_put");
bvar::LatencyRecorder g_bvar_frontend_service_stream_load_multi_table_put_latency(
        "frontend_service", "stream_load_multi_table_put");
bvar::LatencyRecorder g_bvar_frontend_service_get_tablet_rplica_infos_latency(
        "frontend_service", "get_tablet_rplica_infos");
bvar::LatencyRecorder g_bvar_frontend_service_fetch_schema_table_data_latency(
        "frontend_service", "fetch_schema_table_data");
bvar::LatencyRecorder g_bvar_frontend_service_get_db_names_latency("frontend_service",
                                                                   "get_db_names");
bvar::LatencyRecorder g_bvar_frontend_service_get_table_names_latency("frontend_service",
                                                                      "get_table_names");
bvar::LatencyRecorder g_bvar_frontend_service_list_table_status_latency("frontend_service",
                                                                        "list_table_status");
bvar::LatencyRecorder g_bvar_frontend_service_list_table_metadata_name_ids_latency(
        "frontend_service", "list_table_metadata_name_ids");
bvar::LatencyRecorder g_bvar_frontend_service_describe_tables_latency("frontend_service",
                                                                      "describe_tables");
bvar::LatencyRecorder g_bvar_frontend_service_show_variables_latency("frontend_service",
                                                                     "show_variables");
bvar::LatencyRecorder g_bvar_frontend_service_list_table_privilege_status_latency(
        "frontend_service", "list_table_privilege_status");
bvar::LatencyRecorder g_bvar_frontend_service_list_schema_privilege_status_latency(
        "frontend_service", "list_schema_privilege_status");
bvar::LatencyRecorder g_bvar_frontend_service_list_user_privilege_status_latency(
        "frontend_service", "list_user_privilege_status");
bvar::LatencyRecorder g_bvar_frontend_service_show_process_list_latency("frontend_service",
                                                                        "show_process_list");
bvar::LatencyRecorder g_bvar_frontend_service_show_user_latency("frontend_service", "show_user");
bvar::LatencyRecorder g_bvar_frontend_service_get_column_info_latency("frontend_service",
                                                                      "get_column_info");
bvar::LatencyRecorder g_bvar_frontend_service_load_txn_begin_latency("frontend_service",
                                                                     "load_txn_begin");
bvar::LatencyRecorder g_bvar_frontend_service_load_txn_pre_commit_latency("frontend_service",
                                                                          "load_txn_pre_commit");
bvar::LatencyRecorder g_bvar_frontend_service_load_txn_2pc_latency("frontend_service",
                                                                   "load_txn_2pc");
bvar::LatencyRecorder g_bvar_frontend_service_load_txn_commit_latency("frontend_service",
                                                                      "load_txn_commit");
bvar::LatencyRecorder g_bvar_frontend_service_load_txn_rollback_latency("frontend_service",
                                                                        "load_txn_rollback");
bvar::LatencyRecorder g_bvar_frontend_service_get_auto_increment_range_latency(
        "frontend_service", "get_auto_increment_range");
bvar::LatencyRecorder g_bvar_frontend_service_create_partition_latency("frontend_service",
                                                                       "create_partition");
bvar::LatencyRecorder g_bvar_frontend_service_replace_partition_latency("frontend_service",
                                                                        "replace_partition");
bvar::LatencyRecorder g_bvar_frontend_service_check_auth_latency("frontend_service", "check_auth");
bvar::LatencyRecorder g_bvar_frontend_service_snapshot_loader_report_latency(
        "frontend_service", "snapshot_loader_report");

bvar::LatencyRecorder g_bvar_backend_service_make_snapshot_latency("backend_service",
                                                                   "make_snapshot");
bvar::LatencyRecorder g_bvar_backend_service_release_snapshot_latency("backend_service",
                                                                      "release_snapshot");

ExecEnv* ThriftRpcHelper::_s_exec_env;

void ThriftRpcHelper::setup(ExecEnv* exec_env) {
    _s_exec_env = exec_env;
}

template <typename T>
Status ThriftRpcHelper::rpc(const std::string& ip, const int32_t port,
                            std::function<void(ClientConnection<T>&)> callback, int timeout_ms,
                            bvar::LatencyRecorder& metric) {
    SCOPED_BVAR_LATENCY(metric);
    TNetworkAddress address = make_network_address(ip, port);
    Status status;
    ClientConnection<T> client(_s_exec_env->get_client_cache<T>(), address, timeout_ms, &status);
    if (!status.ok()) {
        LOG(WARNING) << "Connect frontend failed, address=" << address << ", status=" << status;
        return status;
    }
    try {
        try {
            callback(client);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::cerr << "thrift error, reason=" << e.what();
#ifdef ADDRESS_SANITIZER
            return Status::RpcError<false>(
                    "failed to call frontend service, FE address={}:{}, reason: {}", ip, port,
                    e.what());
#else
            LOG(WARNING) << "retrying call frontend service after "
                         << config::thrift_client_retry_interval_ms << " ms, address=" << address
                         << ", reason=" << e.what();
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(config::thrift_client_retry_interval_ms));
            status = client.reopen(timeout_ms);
            if (!status.ok()) {
                LOG(WARNING) << "client reopen failed. address=" << address
                             << ", status=" << status;
                return status;
            }
            callback(client);
#endif
        }
    } catch (apache::thrift::TException& e) {
        LOG(WARNING) << "call frontend service failed, address=" << address
                     << ", reason=" << e.what();
        std::this_thread::sleep_for(
                std::chrono::milliseconds(config::thrift_client_retry_interval_ms * 2));
        // just reopen to disable this connection
        static_cast<void>(client.reopen(timeout_ms));
        return Status::RpcError("failed to call frontend service, FE address={}:{}, reason: {}", ip,
                                port, e.what());
    }
    return Status::OK();
}

template Status ThriftRpcHelper::rpc<FrontendServiceClient>(
        const std::string& ip, const int32_t port,
        std::function<void(ClientConnection<FrontendServiceClient>&)> callback, int timeout_ms,
        bvar::LatencyRecorder& metric);

template Status ThriftRpcHelper::rpc<BackendServiceClient>(
        const std::string& ip, const int32_t port,
        std::function<void(ClientConnection<BackendServiceClient>&)> callback, int timeout_ms,
        bvar::LatencyRecorder& metric);

template Status ThriftRpcHelper::rpc<TPaloBrokerServiceClient>(
        const std::string& ip, const int32_t port,
        std::function<void(ClientConnection<TPaloBrokerServiceClient>&)> callback, int timeout_ms,
        bvar::LatencyRecorder& metric);

} // namespace doris
