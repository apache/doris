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

#include "util/native_file_encryption.h"

#include <arrow/util/config.h>

#include "common/config.h"
#include "util/private_rpc_tls.h"

namespace doris {

int native_file_encryption_version() {
#if defined(DORIS_OSS_PRIVATE_RPC_PROVIDERS) && defined(PARQUET_REQUIRE_ENCRYPTION) && \
        __has_include(<parquet/encryption/encryption_internal.h>)
    // The concrete OSS listeners validate these immutable settings and fail startup on invalid PEM.
    // Configuration alone cannot prove what an independently supplied transport module implements.
    return rpc_tls::enabled("brpc") && rpc_tls::enabled("thrift") &&
                           config::tls_verify_mode == "verify_fail_if_no_peer_cert" &&
                           !config::tls_certificate_path.empty() &&
                           !config::tls_private_key_path.empty() &&
                           !config::tls_ca_certificate_path.empty() &&
                           config::tls_cert_refresh_interval_seconds == 0 &&
                           config::tls_peer_cert_required_san_dns.empty() &&
                           config::tls_private_key_password.empty()
                   ? 1
                   : 0;
#else
    return 0;
#endif
}

} // namespace doris
