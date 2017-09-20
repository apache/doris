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
#include "olap/olap_rootpath.h"
#include "olap/utils.h"

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
    AgentStatus status = PALO_SUCCESS;
    TStatusCode::type status_code = TStatusCode::OK;
    vector<string> error_msgs;
    TStatus heartbeat_status;
    OLAP_LOG_INFO("get heartbeat, host: %s, port: %d, cluster id: %d",
                  master_info.network_address.hostname.c_str(),
                  master_info.network_address.port,
                  master_info.cluster_id);

    // Check cluster id
    if (_master_info->cluster_id == -1) {
        OLAP_LOG_INFO("get first heartbeat. update cluster id");
        // write and update cluster id
        OLAPStatus res = _olap_rootpath_instance->set_cluster_id(master_info.cluster_id);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to set cluster id. [res=%d]", res);
            error_msgs.push_back("fail to set cluster id.");
            status = PALO_ERROR;
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
            error_msgs.push_back("invalid cluster id. ignore.");
            status = PALO_ERROR;
        }
    }

    if (status == PALO_SUCCESS) {
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
                error_msgs.push_back("epoch is not greater than local. ignore heartbeat.");
                status = PALO_ERROR;
            }
        }
    }

    if (status == PALO_SUCCESS && master_info.__isset.token) {
        if (!_master_info->__isset.token) {
            _master_info->__set_token(master_info.token);
            OLAP_LOG_INFO("get token.  token: %s", _master_info->token.c_str());
        } else if (_master_info->token != master_info.token) {
            OLAP_LOG_WARNING("invalid token. local_token:%s, token:%s",
                    _master_info->token.c_str(),
                    master_info.token.c_str());
            error_msgs.push_back("invalid token.");
            status = PALO_ERROR;
        }
    }

    TBackendInfo backend_info;
    if (status == PALO_SUCCESS) {
        backend_info.__set_be_port(config::be_port);
        backend_info.__set_http_port(config::webserver_port);
        backend_info.__set_be_rpc_port(config::be_rpc_port);
    } else {
        status_code = TStatusCode::RUNTIME_ERROR;
    }
    heartbeat_status.__set_status_code(status_code);
    heartbeat_status.__set_error_msgs(error_msgs);
    heartbeat_result.__set_status(heartbeat_status);
    heartbeat_result.__set_backend_info(backend_info);
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
