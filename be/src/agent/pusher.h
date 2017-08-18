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

#ifndef BDG_PALO_BE_SRC_AGENT_PUSHER_H
#define BDG_PALO_BE_SRC_AGENT_PUSHER_H

#include <utility>
#include <vector>
#include "agent/file_downloader.h"
#include "agent/status.h"
#include "gen_cpp/AgentService_types.h"
#include "olap/command_executor.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"

namespace palo {

const uint32_t MAX_RETRY = 3;
const uint32_t DEFAULT_DOWNLOAD_TIMEOUT = 3600;

class Pusher {
public:
    explicit Pusher(const TPushReq& push_req);
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
    AgentStatus _download_file();
    void _get_file_name_from_path(const std::string& file_path, std::string* file_name);
    
    bool _is_init = false;
    TPushReq _push_req;
    FileDownloader::FileDownloaderParam _downloader_param;
    CommandExecutor* _command_executor;
    FileDownloader* _file_downloader;
    AgentStatus _download_status;

    DISALLOW_COPY_AND_ASSIGN(Pusher);
};  // class Pusher
}  // namespace palo
#endif  // BDG_PALO_BE_SRC_AGENT_SERVICE_PUSHER_H
