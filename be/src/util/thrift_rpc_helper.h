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

#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <functional>
#include <string>

#include "common/config.h"
#include "common/status.h"

namespace doris {

class ExecEnv;
class FrontendServiceClient;
class TStatus;
template <class T>
class ClientConnection;

// this class is a helper for jni call. easy for unit test
class ThriftRpcHelper {
public:
    static void setup(ExecEnv* exec_env);

    // for default timeout
    template <typename T>
    static Status rpc(const std::string& ip, const int32_t port,
                      std::function<void(ClientConnection<T>&)> callback) {
        return rpc(ip, port, callback, config::thrift_rpc_timeout_ms);
    }

    template <typename T>
    static Status rpc(std::function<TNetworkAddress()> address_provider,
                      std::function<void(ClientConnection<T>&)> callback) {
        return rpc(address_provider, callback, config::thrift_rpc_timeout_ms);
    }

    template <typename T>
    static Status rpc(const std::string& ip, const int32_t port,
                      std::function<void(ClientConnection<T>&)> callback, int timeout_ms);

    template <typename T>
    static Status rpc(std::function<TNetworkAddress()> address_provider,
                      std::function<void(ClientConnection<T>&)> callback, int timeout_ms);

    static ExecEnv* get_exec_env() { return _s_exec_env; }

    // Only for stream-load operations with a stable request_id / transaction id.
    // Application errors other than NOT_MASTER are returned without replaying the RPC.
    static Status rpc_fe_with_master_refresh(
            std::function<TNetworkAddress()> address_provider,
            std::function<void(ClientConnection<FrontendServiceClient>&)> callback,
            std::function<TStatus()> response_status,
            std::function<const TNetworkAddress*()> master_hint,
            int timeout_ms = config::thrift_rpc_timeout_ms);

private:
    static Status _retry_fe_rpc(const std::function<TNetworkAddress()>& address_provider,
                                const std::function<Status(const TNetworkAddress&)>& attempt,
                                const std::function<TStatus()>& response_status,
                                const std::function<const TNetworkAddress*()>& master_hint,
                                bool enabled, int retry_interval_ms);

    static ExecEnv* _s_exec_env;
};

} // namespace doris
