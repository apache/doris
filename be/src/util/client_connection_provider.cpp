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

#include "util/client_connection_provider.h"

namespace doris::client {

Status configure_brpc_channel_options(brpc::ChannelOptions* options,
                                      [[maybe_unused]] RpcPurpose purpose) {
#ifdef DORIS_OSS_PRIVATE_RPC_PROVIDERS
    // FE validates external RPC functions over plaintext. Cluster TLS must not change their
    // execution transport, and enabling TLS for external services is a separate contract.
    if (purpose == RpcPurpose::EXTERNAL_SERVICE) {
        return Status::OK();
    }
#endif
    return configure_brpc_channel_options(options);
}

std::shared_ptr<apache::thrift::transport::TSocket> create_thrift_client_socket(
        const std::string& host, int port, [[maybe_unused]] RpcPurpose purpose) {
#ifdef DORIS_OSS_PRIVATE_RPC_PROVIDERS
    // Broker filesystem calls share the external endpoint policy used by FE, including on reopen.
    if (purpose == RpcPurpose::EXTERNAL_SERVICE) {
        return std::make_shared<apache::thrift::transport::TSocket>(host, port);
    }
#endif
    return create_thrift_client_socket(host, port);
}

} // namespace doris::client
