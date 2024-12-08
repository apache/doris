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

#include <bvar/reducer.h>
#include <stdint.h>

#include <functional>
#include <string>

#include "common/config.h"
#include "common/status.h"

namespace doris {

extern bvar::LatencyRecorder g_bvar_frontend_service_report_commit_txn_result_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_stream_load_put_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_stream_load_multi_table_put_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_get_tablet_rplica_infos_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_fetch_schema_table_data_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_get_db_names_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_get_table_names_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_list_table_status_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_list_table_metadata_name_ids_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_describe_tables_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_show_variables_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_list_table_privilege_status_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_list_schema_privilege_status_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_list_user_privilege_status_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_show_process_list_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_show_user_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_get_column_info_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_load_txn_begin_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_load_txn_pre_commit_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_load_txn_2pc_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_load_txn_commit_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_load_txn_rollback_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_get_auto_increment_range_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_create_partition_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_replace_partition_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_check_auth_latency;
extern bvar::LatencyRecorder g_bvar_frontend_service_snapshot_loader_report_latency;

extern bvar::LatencyRecorder g_bvar_backend_service_make_snapshot_latency;
extern bvar::LatencyRecorder g_bvar_backend_service_release_snapshot_latency;

class ExecEnv;
template <class T>
class ClientConnection;

// this class is a helper for jni call. easy for unit test
class ThriftRpcHelper {
public:
    static void setup(ExecEnv* exec_env);

    // for default timeout
    template <typename T>
    static Status rpc(const std::string& ip, const int32_t port,
                      std::function<void(ClientConnection<T>&)> callback,
                      bvar::LatencyRecorder& metric) {
        return rpc(ip, port, callback, config::thrift_rpc_timeout_ms, metric);
    }

    template <typename T>
    static Status rpc(const std::string& ip, const int32_t port,
                      std::function<void(ClientConnection<T>&)> callback, int timeout_ms,
                      bvar::LatencyRecorder& metric);

private:
    static ExecEnv* _s_exec_env;
};

} // namespace doris
