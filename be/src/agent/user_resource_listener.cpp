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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <thrift/Thrift.h>
#include <thrift/transport/TTransportException.h>

#include <future>
#include <ostream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"

namespace doris {

using std::string;
using apache::thrift::TException;
using apache::thrift::transport::TTransportException;

// Initialize the resource to cgroups file mapping
// TRESOURCE_IOPS  not mapped

UserResourceListener::UserResourceListener(ExecEnv* exec_env, const TMasterInfo& master_info)
        : _master_info(master_info), _exec_env(exec_env) {}

UserResourceListener::~UserResourceListener() {}

void UserResourceListener::handle_update(const TAgentServiceVersion::type& protocol_version,
                                         const TTopicUpdate& topic_update) {
    std::vector<TTopicItem> updates = topic_update.updates;
    if (updates.size() > 0) {
        int64_t new_version = updates[0].int_value;
        // Async call to update users resource method
        auto res = std::async(std::launch::async, &UserResourceListener::update_users_resource,
                              this, new_version);
        res.get();
    }
}

void UserResourceListener::update_users_resource(int64_t new_version) {
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
                             << _master_info.network_address.port << ", reason=" << e.what();
                return;
            }
            LOG(WARNING) << "fetchResource from frontend failed"
                         << ", reason=" << e.what();
            client->fetchResource(new_fetched_resource);
        }
    } catch (TException& e) {
        // Already try twice, log here
        static_cast<void>(client.reopen(config::thrift_rpc_timeout_ms));
        LOG(WARNING) << "retry to fetchResource from  " << _master_info.network_address.hostname
                     << ":" << _master_info.network_address.port << " failed:\n"
                     << e.what();
        return;
    }
}
} // namespace doris
