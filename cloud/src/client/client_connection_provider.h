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

#include <foundationdb/fdb_c.h>

#include <functional>
#include <string_view>

namespace brpc {
struct ChannelOptions;
}

namespace doris::cloud::client {

using FdbNetworkOptionSetter = std::function<bool(FDBNetworkOption option, std::string_view value)>;

// Applies build-specific FoundationDB options before fdb_setup_network().
// The caller must abort network setup when this function returns false.
bool configure_fdb_network_options(const FdbNetworkOptionSetter& set_option);

// Applies build-specific options before initializing an internal peer meta-service channel.
// The caller must not initialize the channel when this function returns false.
bool configure_meta_service_channel_options(brpc::ChannelOptions* options);

} // namespace doris::cloud::client
