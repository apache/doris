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

#include "common/config.h"
#include "common/status.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/heartbeat_flags.h"
#include "service/backend_options.h"
#include "util/debug_util.h"
#include "util/network_util.h"
#include "util/thrift_server.h"
#include "util/time.h"

namespace apache {
namespace thrift {
class TProcessor;
} // namespace thrift
} // namespace apache

namespace doris {

HeartbeatServer::HeartbeatServer(TMasterInfo* master_info)
        : _master_info(master_info), _fe_epoch(0) {
    _olap_engine = StorageEngine::instance();
    _be_epoch = GetCurrentTimeMicros() / 1000;
}

void HeartbeatServer::init_cluster_id() {
    _master_info->cluster_id = _olap_engine->effective_cluster_id();
}

void HeartbeatServer::heartbeat(THeartbeatResult& heartbeat_result,
                                const TMasterInfo& master_info) {
    //print heartbeat in every minute
    LOG_EVERY_N(INFO, 12) << "get heartbeat from FE."
                          << "host:" << master_info.network_address.hostname
                          << ", port:" << master_info.network_address.port
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
    }
    watch.stop();
    if (watch.elapsed_time() > 1000L * 1000L * 1000L) {
        LOG(WARNING) << "heartbeat consume too much time. time=" << watch.elapsed_time();
    }
}

Status HeartbeatServer::_heartbeat(const TMasterInfo& master_info) {
    std::lock_guard<std::mutex> lk(_hb_mtx);

    // Check cluster id
    if (_master_info->cluster_id == -1) {
        LOG(INFO) << "get first heartbeat. update cluster id";
        // write and update cluster id
        RETURN_IF_ERROR(_olap_engine->set_cluster_id(master_info.cluster_id));

        _master_info->cluster_id = master_info.cluster_id;
        LOG(INFO) << "record cluster id. host: " << master_info.network_address.hostname
                  << ". port: " << master_info.network_address.port
                  << ". cluster id: " << master_info.cluster_id
                  << ". frontend_infos: " << PrintFrontendInfos(master_info.frontend_infos);
    } else {
        if (_master_info->cluster_id != master_info.cluster_id) {
            return Status::InternalError(
                    "invalid cluster id. ignore. Record cluster id ={}, record frontend info {}. "
                    "Invalid cluster_id={}, invalid frontend info {}",
                    _master_info->cluster_id, PrintFrontendInfos(_master_info->frontend_infos),
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
    if (_master_info->network_address.hostname != master_info.network_address.hostname ||
        _master_info->network_address.port != master_info.network_address.port) {
        if (master_info.epoch > _fe_epoch) {
            _master_info->network_address.hostname = master_info.network_address.hostname;
            _master_info->network_address.port = master_info.network_address.port;
            _fe_epoch = master_info.epoch;
            need_report = true;
            LOG(INFO) << "master change. new master host: "
                      << _master_info->network_address.hostname
                      << ". port: " << _master_info->network_address.port
                      << ". epoch: " << _fe_epoch;
        } else {
            return Status::InternalError(
                    "epoch is not greater than local. ignore heartbeat. host: {}, port: {}, local "
                    "epoch: {}, received epoch: {}",
                    _master_info->network_address.hostname, _master_info->network_address.port,
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
        if (!_master_info->__isset.token) {
            _master_info->__set_token(master_info.token);
            LOG(INFO) << "get token. token: " << _master_info->token;
        } else if (_master_info->token != master_info.token) {
            return Status::InternalError("invalid token. local_token: {}, token: {}",
                                         _master_info->token, master_info.token);
        }
    }

    if (master_info.__isset.http_port) {
        _master_info->__set_http_port(master_info.http_port);
    }

    if (master_info.__isset.heartbeat_flags) {
        HeartbeatFlags* heartbeat_flags = ExecEnv::GetInstance()->heartbeat_flags();
        heartbeat_flags->update(master_info.heartbeat_flags);
    }

    if (master_info.__isset.backend_id) {
        _master_info->__set_backend_id(master_info.backend_id);
    }

    if (master_info.__isset.frontend_infos) {
        ExecEnv::GetInstance()->update_frontends(master_info.frontend_infos);
    } else {
        LOG_EVERY_N(WARNING, 2) << fmt::format(
                "Heartbeat from {}:{} does not have frontend_infos, this may because we are "
                "upgrading cluster",
                master_info.network_address.hostname, master_info.network_address.port);
    }

    if (need_report) {
        LOG(INFO) << "Master FE is changed or restarted. report tablet and disk info immediately";
        _olap_engine->notify_listeners();
    }

    return Status::OK();
}

Status create_heartbeat_server(ExecEnv* exec_env, uint32_t server_port,
                               std::unique_ptr<ThriftServer>* thrift_server,
                               uint32_t worker_thread_num, TMasterInfo* local_master_info) {
    HeartbeatServer* heartbeat_server = new HeartbeatServer(local_master_info);
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
