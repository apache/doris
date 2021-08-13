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

#include "agent/user_resource_listener.h"

#include <thrift/TApplicationException.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include <future>
#include <map>

#include "common/logging.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"

namespace doris {

using std::string;
using apache::thrift::TException;
using apache::thrift::transport::TTransportException;

// Initialize the resource to cgroups file mapping
// TRESOURCE_IOPS  not mapped

UserResourceListener::UserResourceListener(ExecEnv* exec_env, const TMasterInfo& master_info)
        : _master_info(master_info),
          _exec_env(exec_env),
          _cgroups_mgr(*(exec_env->cgroups_mgr())) {}

UserResourceListener::~UserResourceListener() {}

void UserResourceListener::handle_update(const TAgentServiceVersion::type& protocol_version,
                                         const TTopicUpdate& topic_update) {
    std::vector<TTopicItem> updates = topic_update.updates;
    if (updates.size() > 0) {
        int64_t new_version = updates[0].int_value;
        // Async call to update users resource method
        auto res = std::async(std::launch::async, &UserResourceListener::update_users_resource, this,
                   new_version);
        res.get();
    }
}

void UserResourceListener::update_users_resource(int64_t new_version) {
    if (new_version <= _cgroups_mgr.get_cgroups_version()) {
        return;
    }
    // Call fe to get latest user resource
    Status master_status;
    // using 500ms as default timeout value
    FrontendServiceConnection client(_exec_env->frontend_client_cache(),
                                     _master_info.network_address, config::thrift_rpc_timeout_ms,
                                     &master_status);
    TFetchResourceResult new_fetched_resource;
    if (!master_status.ok()) {
        LOG(ERROR) << "Get frontend client failed, with address:"
                   << _master_info.network_address.hostname << ":"
                   << _master_info.network_address.port;
        return;
    }
    try {
        try {
            client->fetchResource(new_fetched_resource);
        } catch (TTransportException& e) {
            // reopen the client and set timeout to 500ms
            master_status = client.reopen(config::thrift_rpc_timeout_ms);

            if (!master_status.ok()) {
                LOG(WARNING) << "Reopen to get frontend client failed, with address:"
                             << _master_info.network_address.hostname << ":"
                             << _master_info.network_address.port;
                return;
            }
            LOG(WARNING) << "fetchResource from frontend failed, retry!";
            client->fetchResource(new_fetched_resource);
        }
    } catch (TException& e) {
        // Already try twice, log here
        client.reopen(config::thrift_rpc_timeout_ms);
        LOG(WARNING) << "retry to fetchResource from  " << _master_info.network_address.hostname
                     << ":" << _master_info.network_address.port << " failed:\n"
                     << e.what();
        return;
    }
    _cgroups_mgr.update_local_cgroups(new_fetched_resource);
}
} // namespace doris
