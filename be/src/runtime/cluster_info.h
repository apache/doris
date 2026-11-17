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

#include <gen_cpp/Types_types.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <string>
#include <vector>

namespace doris {

// This class is used to save the cluster info
// like cluster id, epoch, cloud_unique_id, etc.
// These info are usually in heartbeat from Master FE.
class ClusterInfo {
public:
    // Heartbeat validation precedes publication. Replace the complete queue rather
    // than accumulating tokens, so FE eviction also takes effect on this BE.
    void update_auth_tokens(const std::vector<std::string>& tokens, int32_t id) {
        std::lock_guard lock(_auth_token_mutex);
        _auth_tokens = tokens;
        _auth_token_cluster_id = id;
    }

    // Compatibility with FEs that only send one token: retain the last two observed
    // generations. Such heartbeats cannot supply missing older keys to a new BE.
    void update_auth_token(const std::string& token, int32_t id) {
        std::lock_guard lock(_auth_token_mutex);
        if (_auth_tokens.empty()) {
            _auth_tokens.push_back(token);
        } else if (token != _auth_tokens.back()) {
            _auth_tokens = {_auth_tokens.back(), token};
        }
        _auth_token_cluster_id = id;
    }

    // Copy the retained queue and its cluster identity under one lock.
    void get_auth_tokens(std::vector<std::string>* tokens, int32_t* id) const {
        std::lock_guard lock(_auth_token_mutex);
        *tokens = _auth_tokens;
        *id = _auth_token_cluster_id;
    }

    bool check_auth_token(const std::string& auth_token) const {
        std::lock_guard lock(_auth_token_mutex);
        return !auth_token.empty() && std::find(_auth_tokens.begin(), _auth_tokens.end(),
                                                auth_token) != _auth_tokens.end();
    }

    std::string current_auth_token() const {
        std::lock_guard lock(_auth_token_mutex);
        return _auth_tokens.empty() ? std::string() : _auth_tokens.back();
    }

    std::string get_sign_token() const {
        std::lock_guard lock(_auth_token_mutex);
        // A peer with [A, B, C, D] still accepts D after this BE receives [B, C, D, E].
        // Before the first heartbeat there is no token; an old FE may supply only one.
        if (_auth_tokens.size() < 2) {
            return _auth_tokens.empty() ? std::string() : _auth_tokens.back();
        }
        return _auth_tokens[_auth_tokens.size() - 2];
    }

    int64_t changelog_ttl_reference_tso() const {
        return _changelog_ttl_reference_tso.load(std::memory_order_acquire);
    }

    // Heartbeat identity validation must precede this call. Never persist this value:
    // after a restart cleanup waits for a fresh, authenticated master heartbeat.
    void advance_changelog_ttl_reference_tso(int64_t tso) {
        auto previous = _changelog_ttl_reference_tso.load(std::memory_order_acquire);
        while (tso > previous && !_changelog_ttl_reference_tso.compare_exchange_weak(
                                         previous, tso, std::memory_order_acq_rel)) {
        }
    }

    // Unique cluster id
    int32_t cluster_id = 0;
    // Master FE addr: ip:rpc_port
    TNetworkAddress master_fe_addr;
    // Master FE http_port
    int32_t master_fe_http_port = 0;
    // Unique cluster token
    std::string token = "";
    // Backend ID
    int64_t backend_id = 0;

private:
    mutable std::mutex _auth_token_mutex;
    std::vector<std::string> _auth_tokens; // Oldest first, newest last.
    int32_t _auth_token_cluster_id = -1;
    std::atomic<int64_t> _changelog_ttl_reference_tso {0};
};

} // namespace doris
