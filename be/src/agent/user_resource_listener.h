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

#ifndef DORIS_BE_SRC_AGENT_USER_RESOURCE_LISTENER_H
#define DORIS_BE_SRC_AGENT_USER_RESOURCE_LISTENER_H

#include <string>

#include "agent/cgroups_mgr.h"
#include "agent/topic_listener.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "runtime/exec_env.h"

namespace doris {

class ExecEnv;

class UserResourceListener : public TopicListener {
public:
    ~UserResourceListener();
    // Input parameters:
    //   root_cgroups_path: root cgroups allocated by admin to doris
    UserResourceListener(ExecEnv* exec_env, const TMasterInfo& master_info);
    // This method should be async
    virtual void handle_update(const TAgentServiceVersion::type& protocol_version,
                               const TTopicUpdate& topic_update);

private:
    const TMasterInfo& _master_info;
    ExecEnv* _exec_env;
    CgroupsMgr& _cgroups_mgr;
    // Call cgroups mgr to update user's cgroups resource share
    // Also refresh local user resource's cache
    void update_users_resource(int64_t new_version);
};
} // namespace doris
#endif
