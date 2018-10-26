// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
#include <ctime>
#include <fstream>
#include "boost/filesystem.hpp"
#include "thrift/TProcessor.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/Status_types.h"

#include "common/status.h"
#include "olap/olap_rootpath.h"
#include "olap/olap_engine.h"
#include "olap/utils.h"
#include "service/backend_options.h"

using std::fstream;
using std::nothrow;
using std::string;
using std::vector;
using apache::thrift::transport::TProcessor;

namespace palo {

HeartbeatServer::HeartbeatServer(TMasterInfo* master_info) :
        _master_info(master_info),
        _epoch(0) {
    _olap_rootpath_instance = OLAPRootPath::get_instance();
}

void HeartbeatServer::init_cluster_id() {
    _master_info->cluster_id = _olap_rootpath_instance->effective_cluster_id();
}

void HeartbeatServer::heartbeat(
        THeartbeatResult& heartbeat_result,
        const TMasterInfo& master_info) {

    //print heartbeat in every minute
    LOG_EVERY_N(INFO, 12) << "get heartbeat from FE."
        << "host:" << master_info.network_address.hostname << ", "
        << "port:" << master_info.network_address.port << ", "
        << "cluster id:" << master_info.cluster_id << ", "
        << "counter:" << google::COUNTER;

    // do heartbeat
    Status st = _heartbeat(master_info); 
    st.to_thrift(&heartbeat_result.status);

    if (st.ok()) {
        heartbeat_result.backend_info.__set_be_port(config::be_port);
        heartbeat_result.backend_info.__set_http_port(config::webserver_port);
        heartbeat_result.backend_info.__set_be_rpc_port(-1);
        heartbeat_result.backend_info.__set_brpc_port(config::brpc_port);
    }
}

Status HeartbeatServer::_heartbeat(
        const TMasterInfo& master_info) {

    if (master_info.__isset.backend_ip) {
        if (master_info.backend_ip != BackendOptions::get_localhost()) {
            LOG(WARNING) << "backend ip saved in master does not equal to backend local ip"
                    << master_info.backend_ip << " vs. " << BackendOptions::get_localhost();
            std::stringstream ss;
            ss << "actual backend local ip: " << BackendOptions::get_localhost();
            return Status(ss.str());
        }
    }

    // Check cluster id
    if (_master_info->cluster_id == -1) {
        OLAP_LOG_INFO("get first heartbeat. update cluster id");
        // write and update cluster id
        OLAPStatus res = _olap_rootpath_instance->set_cluster_id(master_info.cluster_id);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to set cluster id. [res=%d]", res);
            return Status("fail to set cluster id.");
        } else {
            _master_info->cluster_id = master_info.cluster_id;
            OLAP_LOG_INFO("record cluster id."
                          "host: %s, port: %d, cluster id: %d",
                          master_info.network_address.hostname.c_str(),
                          master_info.network_address.port,
                          master_info.cluster_id);
        }
    } else {
        if (_master_info->cluster_id != master_info.cluster_id) {
            OLAP_LOG_WARNING("invalid cluster id: %d. ignore.", master_info.cluster_id);
            return Status("invalid cluster id. ignore.");
        }
    }

    if (_master_info->network_address.hostname != master_info.network_address.hostname
            || _master_info->network_address.port != master_info.network_address.port) {
        if (master_info.epoch > _epoch) {
            _master_info->network_address.hostname = master_info.network_address.hostname;
            _master_info->network_address.port = master_info.network_address.port;
            _epoch = master_info.epoch;
            OLAP_LOG_INFO("master change, new master host: %s, port: %d, epoch: %ld",
                           _master_info->network_address.hostname.c_str(),
                           _master_info->network_address.port,
                           _epoch);
        } else {
            OLAP_LOG_WARNING("epoch is not greater than local. ignore heartbeat."
                    "host: %s, port: %d, local epoch: %ld, received epoch: %ld",
                    _master_info->network_address.hostname.c_str(),
                    _master_info->network_address.port,
                    _epoch, master_info.epoch);
            return Status("epoch is not greater than local. ignore heartbeat.");
        }
    }

    if (master_info.__isset.token) {
        if (!_master_info->__isset.token) {
            _master_info->__set_token(master_info.token);
            OLAP_LOG_INFO("get token.  token: %s", _master_info->token.c_str());
        } else if (_master_info->token != master_info.token) {
            LOG(WARNING) << "invalid token. local_token:" << _master_info->token
                         << ". token:" << master_info.token;
            return Status("invalid token.");
        }
    }

    return Status::OK;
}

AgentStatus create_heartbeat_server(
        ExecEnv* exec_env,
        uint32_t server_port,
        ThriftServer** thrift_server,
        uint32_t worker_thread_num,
        TMasterInfo* local_master_info) {
    HeartbeatServer* heartbeat_server = new (nothrow) HeartbeatServer(local_master_info);
    if (heartbeat_server == NULL) {
        return PALO_ERROR;
    }

    heartbeat_server->init_cluster_id();

    boost::shared_ptr<HeartbeatServer> handler(heartbeat_server);
    boost::shared_ptr<TProcessor> server_processor(new HeartbeatServiceProcessor(handler));
    string server_name("heartbeat");
    *thrift_server = new ThriftServer(
            server_name,
            server_processor,
            server_port,
            exec_env->metrics(),
            worker_thread_num);
    return PALO_SUCCESS;
}
}  // namesapce palo
