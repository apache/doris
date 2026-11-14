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

#include "io/object_storage/aws_client_settings.h"

#include <aws/core/client/ClientConfiguration.h>

#include "common/config.h"
#include "cpp/aws_common.h"
#include "cpp/obj-storage/ca_bundle_cache.h"

namespace doris {

std::string be_ca_bundle_path() {
    static CaBundleCache cache;
    return cache.path(config::ca_cert_file_paths);
}

Aws::Client::ClientConfiguration be_default_aws_client_configuration() {
    ensure_aws_sdk_initialized(config::aws_log_level);
    auto defaults = default_aws_client_configuration();
    defaults.requestTimeoutMs = config::aws_client_request_timeout_ms;
    return defaults;
}

} // namespace doris
