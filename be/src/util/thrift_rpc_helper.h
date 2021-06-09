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

#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/FrontendService_types.h"

namespace doris {

class ExecEnv;
class FrontendServiceClient;
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
    static Status rpc(const std::string& ip, const int32_t port,
                      std::function<void(ClientConnection<T>&)> callback, int timeout_ms);

private:
    static ExecEnv* _s_exec_env;
};

} // namespace doris
