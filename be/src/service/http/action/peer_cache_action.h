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

#pragma once

#include <string>

#include "common/status.h"
#include "service/http/http_handler_with_auth.h"

namespace doris {

class HttpRequest;
class ExecEnv;
class CloudStorageEngine;

// HTTP action for viewing and force-setting peer cache candidates.
//
// GET  /api/peer_cache?op=show&tablet_id=12345
//   Show TabletPeerCandidates for a specific tablet.
//
// GET  /api/peer_cache?op=show_all[&limit=100]
//   Show all tablets with peer candidates (default limit 1000).
//
// POST /api/peer_cache?op=set&tablet_id=12345
//   Body (JSON):
//   {
//     "candidates": [
//       {"host":"10.0.0.1","brpc_port":8060,"compute_group_id":"cg1"}
//     ],
//     "last_successful_compute_group_id": "cg1",
//     "consecutive_all_miss": 0,
//     "cooldown_until_ms": 0
//   }
//
// POST /api/peer_cache?op=remove&tablet_id=12345
//   Remove all peer candidates for a tablet.
//
// POST /api/peer_cache?op=reset_cooldown&tablet_id=12345
//   Reset cooldown state for a tablet.
class PeerCacheAction : public HttpHandlerWithAuth {
public:
    PeerCacheAction(ExecEnv* exec_env, CloudStorageEngine& engine)
            : HttpHandlerWithAuth(exec_env), _engine(engine) {}

    ~PeerCacheAction() override = default;

    void handle(HttpRequest* req) override;

private:
    Status _handle_show(HttpRequest* req, std::string* result);
    Status _handle_show_all(HttpRequest* req, std::string* result);
    Status _handle_set(HttpRequest* req, std::string* result);
    Status _handle_remove(HttpRequest* req, std::string* result);
    Status _handle_reset_cooldown(HttpRequest* req, std::string* result);

    CloudStorageEngine& _engine;
};

} // namespace doris
