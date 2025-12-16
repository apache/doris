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

#include "util/ssl_key_logger.h"

#include <glog/logging.h>
#include <openssl/ssl.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>

namespace doris {

#if OPENSSL_VERSION_NUMBER >= 0x10101000L || defined(OPENSSL_IS_BORINGSSL)
static std::once_flag g_keylog_init_once;
static FILE* g_keylog_file = nullptr;
static bool g_keylog_init_failed = false;

static void ssl_keylog_callback(const SSL*, const char* line) {
    FILE* file = g_keylog_file;
    if (file == nullptr || line == nullptr) {
        return;
    }
    std::fprintf(file, "%s\n", line);
    std::fflush(file);
}

static void init_keylog_file() {
    const char* path = std::getenv("SSLKEYLOGFILE");
    if (path == nullptr || path[0] == '\0') {
        return;
    }

    FILE* file = std::fopen(path, "a");
    if (file == nullptr) {
        g_keylog_init_failed = true;
        LOG(WARNING) << "SSLKEYLOGFILE is set but open failed for '" << path
                     << "': " << std::strerror(errno);
        return;
    }

    g_keylog_file = file;
}
#endif

void maybe_set_ssl_keylog_callback(SSL_CTX* ctx) {
    if (ctx == nullptr) {
        return;
    }

#if OPENSSL_VERSION_NUMBER >= 0x10101000L || defined(OPENSSL_IS_BORINGSSL)
    std::call_once(g_keylog_init_once, init_keylog_file);
    if (g_keylog_file != nullptr) {
        SSL_CTX_set_keylog_callback(ctx, ssl_keylog_callback);
    } else if (g_keylog_init_failed) {
        LOG_EVERY_N(WARNING, 1)
                << "SSLKEYLOGFILE is set but key log file cannot be opened; key logging disabled";
    }
#else
    const char* path = std::getenv("SSLKEYLOGFILE");
    if (path != nullptr && path[0] != '\0') {
        static std::once_flag warn_once;
        std::call_once(warn_once, [] {
            LOG(WARNING) << "SSLKEYLOGFILE is set but OpenSSL key log callback requires "
                            "OpenSSL 1.1.1 or later";
        });
    }
    (void)ctx;
#endif
}

} // namespace doris
