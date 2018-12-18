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

#ifndef DORIS_BE_SRC_AGENT_PUSHER_H
#define DORIS_BE_SRC_AGENT_PUSHER_H

#include <utility>
#include <vector>
#include "agent/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"

namespace doris {

const uint32_t MAX_RETRY = 3;
const uint32_t DEFAULT_DOWNLOAD_TIMEOUT = 3600;
class OLAPEngine;

class Pusher {
public:
    explicit Pusher(OLAPEngine* engine, const TPushReq& push_req);
    virtual ~Pusher();
    
    // The initial function of pusher
    virtual AgentStatus init();

    // The process of push data to olap engine
    //
    // Output parameters:
    // * tablet_infos: The info of pushed tablet after push data
    virtual AgentStatus process(std::vector<TTabletInfo>* tablet_infos);

private:
    AgentStatus _get_tmp_file_dir(const std::string& root_path, std::string* local_path);
    void _get_file_name_from_path(const std::string& file_path, std::string* file_name);
    
    TPushReq _push_req;
    OLAPEngine* _engine;
    std::string _remote_file_path;
    std::string _local_file_path;

    DISALLOW_COPY_AND_ASSIGN(Pusher);
};  // class Pusher
}  // namespace doris
#endif  // DORIS_BE_SRC_AGENT_SERVICE_PUSHER_H
