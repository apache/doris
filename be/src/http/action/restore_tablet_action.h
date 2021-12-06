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

#ifndef DORIS_BE_SRC_HTTP_RESTORE_TABLET_ACTION_H
#define DORIS_BE_SRC_HTTP_RESTORE_TABLET_ACTION_H

#include <map>
#include <mutex>

#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "http/http_handler.h"

namespace doris {

class ExecEnv;

class RestoreTabletAction : public HttpHandler {
public:
    RestoreTabletAction(ExecEnv* exec_env);

    virtual ~RestoreTabletAction() {}

    void handle(HttpRequest* req) override;

private:
    Status _handle(HttpRequest* req);

    Status _restore(const std::string& key, int64_t tablet_id, int32_t schema_hash);

    Status _reload_tablet(const std::string& key, const std::string& shard_path, int64_t tablet_id,
                          int32_t schema_hash);

    bool _get_latest_tablet_path_from_trash(int64_t tablet_id, int32_t schema_hash,
                                            std::string* path);

    bool _get_timestamp_and_count_from_schema_hash_path(const std::string& time_label,
                                                        uint64_t* timestamp, uint64_t* counter);

    void _clear_key(const std::string& key);

    Status _create_hard_link_recursive(const std::string& src, const std::string& dst);

private:
    ExecEnv* _exec_env;
    std::mutex _tablet_restore_lock;
    // store all current restoring tablet_id + schema_hash
    // key: tablet_id + schema_hash
    // value: "" or tablet path in trash
    std::map<std::string, std::string> _tablet_path_map;
}; // end class RestoreTabletAction

} // end namespace doris
#endif // DORIS_BE_SRC_HTTP_RESTORE_TABLET_ACTION_H
