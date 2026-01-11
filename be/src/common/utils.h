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

#include "common/client_cert_utils.h"
#include "gen_cpp/FrontendService_types.h"

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
    int64_t auth_code = -1; // deprecated
    std::string token;

    // Client TLS certificate info for certificate-based authentication
    ClientCertInfo cert_info;
};

template <class T>
void set_request_auth(T* req, const AuthInfo& auth) {
    req->user = auth.user; // always set user, because it may be used by FE
    // auth code is deprecated and should be removed in 3.1
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

    // Set certificate-based authentication info if present
    if (!auth.cert_info.is_empty()) {
        TCertBasedAuth cert_auth;
        cert_auth.__set_cert_pem(auth.cert_info.cert_pem);
        cert_auth.__set_subject(auth.cert_info.subject);
        cert_auth.__set_san(auth.cert_info.san);
        cert_auth.__set_issuer(auth.cert_info.issuer);
        cert_auth.__set_cipher(auth.cert_info.cipher);
        cert_auth.__set_validity_not_before(auth.cert_info.validity_not_before);
        cert_auth.__set_validity_not_after(auth.cert_info.validity_not_after);
        req->__set_cert_based_auth(cert_auth);
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
