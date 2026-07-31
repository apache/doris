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

    // Wrapper around `rpc<FrontendServiceClient>` for stream-load FE RPCs
    // (loadTxnBegin / loadTxnPreCommit / loadTxnCommit / loadTxn2PC /
    // loadTxnRollback / streamLoadPut). It transparently recovers in two
    // narrow master-transition situations:
    //   1) Transport-layer failure (master FE not reachable, e.g. restarting):
    //      the first RPC never reached FE, so retrying with the same txn_id is
    //      side-effect-free. If the cached master is still stale after the
    //      built-in rpc retry, this helper probes a bounded number of running
    //      non-master FEs to discover or hit the new master.
    //   2) The FE returned TStatusCode::NOT_MASTER (BE was still pointing at
    //      the previous master after a leadership transfer). FE early-returns
    //      before touching the txn, and now also sets `result.master_address`
    //      so this helper can refresh `cluster_info()->master_fe_addr` without
    //      any extra round trip, then retry the original request once against
    //      the refreshed master.
    // Other failures (ANALYSIS_ERROR, LABEL_ALREADY_EXISTS, ...) are returned
    // verbatim — they are not master-related and retrying is unsafe.
    //
    // `status_extractor` returns the response's TStatus.
    // `master_addr_extractor` returns a pointer to the response's
    // master_address (or nullptr if not set). Wiring these as callbacks keeps
    // this helper free of any compile-time dependency on the specific Resp
    // struct.
    static Status rpc_fe_with_master_refresh(
            std::function<TNetworkAddress()> address_provider,
            std::function<void(ClientConnection<FrontendServiceClient>&)> callback,
            std::function<TStatus()> status_extractor,
            std::function<const TNetworkAddress*()> master_addr_extractor,
            int timeout_ms = config::thrift_rpc_timeout_ms);

    static ExecEnv* get_exec_env() { return _s_exec_env; }

private:
    static ExecEnv* _s_exec_env;
};

} // namespace doris
