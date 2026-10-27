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

#include "client/client_connection_provider.h"

#include "common/config.h"
#include "common/logging.h"

namespace doris::cloud::client {
namespace {

bool validate_tls_support() {
    if (!config::enable_tls) {
        return true;
    }
    LOG(ERROR) << "Cloud TLS requires TLS module";
    return false;
}

} // namespace

bool configure_fdb_network_options(const FdbNetworkOptionSetter& set_option) {
    (void)set_option;
    return validate_tls_support();
}

bool configure_meta_service_channel_options(brpc::ChannelOptions* options) {
    (void)options;
    return validate_tls_support();
}

} // namespace doris::cloud::client
