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

#include "agent/heartbeat_server.h"

#include <gen_cpp/HeartbeatService.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <string>

#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "olap/storage_engine.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/heartbeat_flags.h"
#include "service/backend_options.h"
#include "util/debug_util.h"
#include "util/mem_info.h"
#include "util/network_util.h"
#include "util/thrift_server.h"
#include "util/time.h"

namespace apache {
namespace thrift {
class TProcessor;
} // namespace thrift
} // namespace apache

namespace doris {

HeartbeatServer::HeartbeatServer(ClusterInfo* cluster_info)
        : _engine(ExecEnv::GetInstance()->storage_engine()),
          _cluster_info(cluster_info),
          _fe_epoch(0) {
    _be_epoch = GetCurrentTimeMicros() / 1000;
}

void HeartbeatServer::init_cluster_id() {
    _cluster_info->cluster_id = _engine.effective_cluster_id();
}

void HeartbeatServer::heartbeat(THeartbeatResult& heartbeat_result,
                                const TMasterInfo& master_info) {
    //print heartbeat in every minute
    LOG_EVERY_N(INFO, 12) << "get heartbeat from FE."
                          << "host:" << master_info.network_address.hostname
                          << ", rpc port:" << master_info.network_address.port
                          << ", cluster id:" << master_info.cluster_id
                          << ", frontend_info:" << PrintFrontendInfos(master_info.frontend_infos)
                          << ", counter:" << google::COUNTER << ", BE start time: " << _be_epoch;

    MonotonicStopWatch watch;
    watch.start();
    // do heartbeat
    Status st = _heartbeat(master_info);
    st.to_thrift(&heartbeat_result.status);

    if (st.ok()) {
        heartbeat_result.backend_info.__set_be_port(config::be_port);
        heartbeat_result.backend_info.__set_http_port(config::webserver_port);
        heartbeat_result.backend_info.__set_be_rpc_port(-1);
        heartbeat_result.backend_info.__set_brpc_port(config::brpc_port);
        heartbeat_result.backend_info.__set_arrow_flight_sql_port(config::arrow_flight_sql_port);
        heartbeat_result.backend_info.__set_version(get_short_version());
        heartbeat_result.backend_info.__set_be_start_time(_be_epoch);
        heartbeat_result.backend_info.__set_be_node_role(config::be_node_role);
        // If be is gracefully stop, then k_doris_exist is set to true
        heartbeat_result.backend_info.__set_is_shutdown(doris::k_doris_exit);
        heartbeat_result.backend_info.__set_fragment_executing_count(
                get_fragment_executing_count());
        heartbeat_result.backend_info.__set_fragment_last_active_time(
                get_fragment_last_active_time());
        heartbeat_result.backend_info.__set_be_mem(MemInfo::physical_mem());
    }
    watch.stop();
    if (watch.elapsed_time() > 1000L * 1000L * 1000L) {
        LOG(WARNING) << "heartbeat consume too much time. time=" << watch.elapsed_time()
                     << ", host:" << master_info.network_address.hostname
                     << ", port:" << master_info.network_address.port
                     << ", cluster id:" << master_info.cluster_id
                     << ", frontend_info:" << PrintFrontendInfos(master_info.frontend_infos)
                     << ", counter:" << google::COUNTER << ", BE start time: " << _be_epoch;
    }
}

Status HeartbeatServer::_heartbeat(const TMasterInfo& master_info) {
    std::lock_guard<std::mutex> lk(_hb_mtx);

    // Check cluster id
    if (_cluster_info->cluster_id == -1) {
        LOG(INFO) << "get first heartbeat. update cluster id";
        // write and update cluster id
        RETURN_IF_ERROR(_engine.set_cluster_id(master_info.cluster_id));

        _cluster_info->cluster_id = master_info.cluster_id;
        LOG(INFO) << "record cluster id. host: " << master_info.network_address.hostname
                  << ". port: " << master_info.network_address.port
                  << ". cluster id: " << master_info.cluster_id
                  << ". frontend_infos: " << PrintFrontendInfos(master_info.frontend_infos);
    } else {
        if (_cluster_info->cluster_id != master_info.cluster_id) {
            return Status::InternalError(
                    "invalid cluster id. ignore. Record cluster id ={}, record frontend info {}. "
                    "Invalid cluster_id={}, invalid frontend info {}",
                    _cluster_info->cluster_id,
                    PrintFrontendInfos(ExecEnv::GetInstance()->get_frontends()),
                    master_info.cluster_id, PrintFrontendInfos(master_info.frontend_infos));
        }
    }

    if (master_info.__isset.backend_ip) {
        // master_info.backend_ip may be an IP or domain name, and it should be renamed 'backend_host', as it requires compatibility with historical versions, the name is still 'backend_ ip'
        if (master_info.backend_ip != BackendOptions::get_localhost()) {
            LOG(INFO) << master_info.backend_ip << " not equal to to backend localhost "
                      << BackendOptions::get_localhost();
            // step1: check master_info.backend_ip is IP or FQDN
            if (!is_valid_ip(master_info.backend_ip)) {
                //step2: resolve FQDN to IP
                std::string ip;
                Status status =
                        hostname_to_ip(master_info.backend_ip, ip, BackendOptions::is_bind_ipv6());
                if (!status.ok()) {
                    std::stringstream ss;
                    ss << "can not get ip from fqdn: " << status.to_string();
                    LOG(WARNING) << ss.str();
                    return status;
                }
                LOG(INFO) << "master_info.backend_ip: " << master_info.backend_ip
                          << ", hostname_to_ip: " << ip;
                //step3: get all ips of the interfaces on this machine
                std::vector<InetAddress> hosts;
                status = get_hosts(&hosts);
                if (!status.ok() || hosts.empty()) {
                    return Status::InternalError(
                            "the status was not ok when get_hosts, error is {}",
                            status.to_string());
                }

                //step4: check if the IP of FQDN belongs to the current machine and update BackendOptions._s_localhost
                bool set_new_localhost = false;
                for (auto& addr : hosts) {
                    if (addr.get_host_address() == ip) {
                        BackendOptions::set_localhost(master_info.backend_ip);
                        set_new_localhost = true;
                        break;
                    }
                }

                if (!set_new_localhost) {
                    return Status::InternalError(
                            "the host recorded in master is {}, but we cannot found the local ip "
                            "that mapped to that host. backend={}",
                            master_info.backend_ip, BackendOptions::get_localhost());
                }
            } else {
                // if is ip,not check anything,use it
                BackendOptions::set_localhost(master_info.backend_ip);
            }

            LOG(WARNING) << "update localhost done, the new localhost is "
                         << BackendOptions::get_localhost();
        }
    }

    bool need_report = false;
    if (_cluster_info->master_fe_addr.hostname != master_info.network_address.hostname ||
        _cluster_info->master_fe_addr.port != master_info.network_address.port) {
        if (master_info.epoch > _fe_epoch) {
            _cluster_info->master_fe_addr.hostname = master_info.network_address.hostname;
            _cluster_info->master_fe_addr.port = master_info.network_address.port;
            _fe_epoch = master_info.epoch;
            need_report = true;
            LOG(INFO) << "master change. new master host: "
                      << _cluster_info->master_fe_addr.hostname
                      << ". port: " << _cluster_info->master_fe_addr.port
                      << ". epoch: " << _fe_epoch;
        } else {
            return Status::InternalError(
                    "epoch is not greater than local. ignore heartbeat. host: {}, port: {}, local "
                    "epoch: {}, received epoch: {}",
                    _cluster_info->master_fe_addr.hostname, _cluster_info->master_fe_addr.port,
                    _fe_epoch, master_info.epoch);
        }
    } else {
        // when Master FE restarted, host and port remains the same, but epoch will be increased.
        if (master_info.epoch > _fe_epoch) {
            _fe_epoch = master_info.epoch;
            need_report = true;
            LOG(INFO) << "master restarted. epoch: " << _fe_epoch;
        }
    }

    if (master_info.__isset.token) {
        if (_cluster_info->token == "") {
            _cluster_info->token = master_info.token;
            LOG(INFO) << "get token. token: " << _cluster_info->token;
        } else if (_cluster_info->token != master_info.token) {
            return Status::InternalError("invalid token. local: {}, master: {}",
                                         _cluster_info->token, master_info.token);
        }
    }

    if (master_info.__isset.http_port) {
        _cluster_info->master_fe_http_port = master_info.http_port;
    }

    if (master_info.__isset.heartbeat_flags) {
        HeartbeatFlags* heartbeat_flags = ExecEnv::GetInstance()->heartbeat_flags();
        heartbeat_flags->update(master_info.heartbeat_flags);
    }

    if (master_info.__isset.backend_id) {
        _cluster_info->backend_id = master_info.backend_id;
        BackendOptions::set_backend_id(master_info.backend_id);
    }
    if (master_info.__isset.frontend_infos) {
        ExecEnv::GetInstance()->update_frontends(master_info.frontend_infos);
    } else {
        LOG_EVERY_N(WARNING, 2) << fmt::format(
                "Heartbeat from {}:{} does not have frontend_infos, this may because we are "
                "upgrading cluster",
                master_info.network_address.hostname, master_info.network_address.port);
    }

    if (master_info.__isset.meta_service_endpoint != config::is_cloud_mode()) {
        LOG(WARNING) << "Detected mismatch in cloud mode configuration between FE and BE. "
                     << "FE cloud mode: "
                     << (master_info.__isset.meta_service_endpoint ? "true" : "false")
                     << ", BE cloud mode: " << (config::is_cloud_mode() ? "true" : "false")
                     << ". If fe is earlier than version 3.0.2, the message can be ignored.";
    }

    if (master_info.__isset.meta_service_endpoint) {
        if (config::meta_service_endpoint.empty() && !master_info.meta_service_endpoint.empty()) {
            auto st = config::set_config("meta_service_endpoint", master_info.meta_service_endpoint,
                                         true);
            LOG(INFO) << "set config meta_service_endpoing " << master_info.meta_service_endpoint
                      << " " << st;
        }

        if (master_info.meta_service_endpoint != config::meta_service_endpoint) {
            LOG(WARNING) << "Detected mismatch in meta_service_endpoint configuration between FE "
                            "and BE. "
                         << "FE meta_service_endpoint: " << master_info.meta_service_endpoint
                         << ", BE meta_service_endpoint: " << config::meta_service_endpoint;
            return Status::InvalidArgument<false>(
                    "fe and be do not work in same mode, fe meta_service_endpoint: {},"
                    " be meta_service_endpoint: {}",
                    master_info.meta_service_endpoint, config::meta_service_endpoint);
        }
    }

    if (master_info.__isset.cloud_unique_id &&
        config::cloud_unique_id != master_info.cloud_unique_id &&
        config::enable_use_cloud_unique_id_from_fe) {
        auto st = config::set_config("cloud_unique_id", master_info.cloud_unique_id, true);
        LOG(INFO) << "set config cloud_unique_id " << master_info.cloud_unique_id << " " << st;
    }

    if (master_info.__isset.tablet_report_inactive_duration_ms) {
        doris::g_tablet_report_inactive_duration_ms =
                master_info.tablet_report_inactive_duration_ms;
    }

    if (master_info.__isset.auth_token) {
        if (_cluster_info->curr_auth_token == "") {
            _cluster_info->curr_auth_token = master_info.auth_token;
            LOG(INFO) << "set new auth token: " << master_info.auth_token;
        } else if (_cluster_info->curr_auth_token != master_info.auth_token) {
            LOG(INFO) << "last auth token: " << _cluster_info->last_auth_token
                      << "set new auth token: " << master_info.auth_token;
            _cluster_info->last_auth_token = _cluster_info->curr_auth_token;
            _cluster_info->curr_auth_token = master_info.auth_token;
        }
    }

    if (need_report) {
        LOG(INFO) << "Master FE is changed or restarted. report tablet and disk info immediately";
        _engine.notify_listeners();
    }

    return Status::OK();
}

Status create_heartbeat_server(ExecEnv* exec_env, uint32_t server_port,
                               std::unique_ptr<ThriftServer>* thrift_server,
                               uint32_t worker_thread_num, ClusterInfo* cluster_info) {
    HeartbeatServer* heartbeat_server = new HeartbeatServer(cluster_info);
    if (heartbeat_server == nullptr) {
        return Status::InternalError("Get heartbeat server failed");
    }

    heartbeat_server->init_cluster_id();

    std::shared_ptr<HeartbeatServer> handler(heartbeat_server);
    std::shared_ptr<HeartbeatServiceProcessor::TProcessor> server_processor(
            new HeartbeatServiceProcessor(handler));
    *thrift_server = std::make_unique<ThriftServer>("heartbeat", server_processor, server_port,
                                                    worker_thread_num);
    return Status::OK();
}
} // namespace doris
