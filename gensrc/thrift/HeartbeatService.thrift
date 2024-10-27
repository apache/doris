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

namespace cpp doris
namespace java org.apache.doris.thrift

include "AgentService.thrift"
include "Status.thrift"
include "Types.thrift"

const i64 IS_SET_DEFAULT_ROWSET_TO_BETA_BIT = 0x01;

struct TFrontendInfo {
    1: optional Types.TNetworkAddress coordinator_address
    2: optional i64 process_uuid
}

struct TMasterInfo {
    1: required Types.TNetworkAddress network_address
    2: required Types.TClusterId cluster_id
    3: required Types.TEpoch epoch
    4: optional string token 
    5: optional string backend_ip //This may be an IP or domain name, and it should be renamed 'backend_host', as it requires compatibility with historical versions, the name is still 'backend_ ip'
    6: optional Types.TPort http_port
    7: optional i64 heartbeat_flags
    8: optional i64 backend_id
    9: optional list<TFrontendInfo> frontend_infos
    10: optional string meta_service_endpoint;
    11: optional string cloud_unique_id;
    // See configuration item Config.java rehash_tablet_after_be_dead_seconds for meaning
    12: optional i64 tablet_report_inactive_duration_ms;
}

struct TBackendInfo {
    1: required Types.TPort be_port
    2: required Types.TPort http_port
    3: optional Types.TPort be_rpc_port
    4: optional Types.TPort brpc_port
    5: optional string version
    6: optional i64 be_start_time // This field will also be uesd to identify a be process
    7: optional string be_node_role
    8: optional bool is_shutdown
    9: optional Types.TPort arrow_flight_sql_port
    10: optional i64 be_mem // The physical memory available for use by BE.
    // For cloud
    1000: optional i64 fragment_executing_count
    1001: optional i64 fragment_last_active_time
}

struct THeartbeatResult {
    1: required Status.TStatus status 
    2: required TBackendInfo backend_info
}

service HeartbeatService {
    THeartbeatResult heartbeat(1:TMasterInfo master_info);
}
