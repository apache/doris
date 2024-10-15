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

namespace doris {

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))
#endif

struct AuthInfo {
    std::string user;
    std::string passwd;
    std::string cluster;
    std::string user_ip;
    // -1 as unset
    int64_t auth_code = -1;
    std::string token;
};

template <class T>
void set_request_auth(T* req, const AuthInfo& auth) {
    req->user = auth.user; // always set user, because it may be used by FE
    if (auth.auth_code != -1) {
        // if auth_code is set, no need to set other info
        req->__set_auth_code(auth.auth_code);
        // user name and passwd is unused, but they are required field.
        // so they have to be set.
        req->passwd = "";
    } else if (auth.token != "") {
        req->__isset.token = true;
        req->token = auth.token;
    } else {
        req->passwd = auth.passwd;
        if (!auth.cluster.empty()) {
            req->__set_cluster(auth.cluster);
        }
        req->__set_user_ip(auth.user_ip);
    }
}

// This is the threshold used to periodically release the memory occupied by the expression.
// RELEASE_CONTEXT_COUNTER should be power of 2
// GCC will optimize the modulo operation to &(release_context_counter - 1)
// _conjunct_ctxs will free local alloc after this probe calculations
static constexpr int RELEASE_CONTEXT_COUNTER = 1 << 7;
static_assert((RELEASE_CONTEXT_COUNTER & (RELEASE_CONTEXT_COUNTER - 1)) == 0,
              "should be power of 2");

template <typename To, typename From>
To convert_to(From from) {
    union {
        From _from;
        To _to;
    };
    _from = from;
    return _to;
}

} // namespace doris
