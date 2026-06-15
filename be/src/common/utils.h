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

#include <random>
#include <string>

#include "gen_cpp/FrontendService_types.h"

namespace doris {

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))
#endif

inline constexpr const char* HTTP_HEADER_CLIENT_CERT_PEM = "x-doris-client-cert-pem";
inline constexpr const char* HTTP_HEADER_CLIENT_CERT_SUBJECT = "x-doris-client-cert-subject";
inline constexpr const char* HTTP_HEADER_CLIENT_CERT_SAN = "x-doris-client-cert-san";
inline constexpr const char* HTTP_HEADER_CLIENT_CERT_ISSUER = "x-doris-client-cert-issuer";
inline constexpr const char* HTTP_HEADER_CLIENT_CERT_CIPHER = "x-doris-client-cert-cipher";
inline constexpr const char* HTTP_HEADER_CLIENT_CERT_NOT_BEFORE = "x-doris-client-cert-not-before";
inline constexpr const char* HTTP_HEADER_CLIENT_CERT_NOT_AFTER = "x-doris-client-cert-not-after";

struct AuthInfo {
    std::string user;
    std::string passwd;
    std::string cluster;
    std::string user_ip;
    // -1 as unset
    int64_t auth_code = -1; // deprecated
    std::string token;

    std::string cert_pem;
    std::string cert_subject;
    std::string cert_san;
    std::string cert_issuer;
    std::string cert_cipher;
    std::string cert_validity_not_before;
    std::string cert_validity_not_after;

    bool has_cert_info() const { return !cert_san.empty() || !cert_subject.empty(); }
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

    if constexpr (requires(T req_obj, TCertBasedAuth cert_auth_obj) {
                      req_obj.__set_cert_based_auth(cert_auth_obj);
                  }) {
        if (auth.has_cert_info()) {
            TCertBasedAuth cert_auth;
            cert_auth.__set_cert_pem(auth.cert_pem);
            cert_auth.__set_subject(auth.cert_subject);
            cert_auth.__set_san(auth.cert_san);
            cert_auth.__set_issuer(auth.cert_issuer);
            cert_auth.__set_cipher(auth.cert_cipher);
            cert_auth.__set_validity_not_before(auth.cert_validity_not_before);
            cert_auth.__set_validity_not_after(auth.cert_validity_not_after);
            req->__set_cert_based_auth(cert_auth);
        }
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

inline bool random_bool_slow(double probability_of_true = 0.5) {
    // Due to an unknown JNI bug, we cannot use thread_local variables here.
    static std::random_device seed;
    static std::mt19937 gen(seed());
    std::bernoulli_distribution d(probability_of_true);
    return d(gen);
}
} // namespace doris
