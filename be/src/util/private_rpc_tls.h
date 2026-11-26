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

#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"

namespace apache::thrift::transport {
class TSocket;
}
namespace brpc {
struct ChannelOptions;
struct ServerOptions;
} // namespace brpc
namespace doris {
class ThriftServer;
}

namespace doris::rpc_tls {

// Native encrypted reads require both the PME dependency and authenticated key transports.
bool supports_native_file_encryption();
bool enabled(std::string_view protocol);
bool enabled_for_java_thrift();
Status configure_channel(brpc::ChannelOptions* options);
Status configure_server(brpc::ServerOptions* options);
Status configure_thrift_server(ThriftServer* server, const std::string& address, int port);
void set_peer_hostname(apache::thrift::transport::TSocket* socket, const std::string& hostname);
std::shared_ptr<apache::thrift::transport::TSocket> create_client_socket(const std::string& host,
                                                                         int port);

} // namespace doris::rpc_tls
