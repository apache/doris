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

namespace doris {
namespace cloud {
class HostLevelMSRpcRateLimiters;
class TableRpcQpsRegistry;
class TableRpcThrottler;
class MSBackpressureHandler;
} // namespace cloud

class MSRpcRateLimitServices {
public:
    MSRpcRateLimitServices();
    ~MSRpcRateLimitServices();

    cloud::HostLevelMSRpcRateLimiters* host_level_ms_rpc_rate_limiters() {
        return _host_level_ms_rpc_rate_limiters.get();
    }

    cloud::TableRpcQpsRegistry* table_rpc_qps_registry() { return _table_rpc_qps_registry.get(); }

    cloud::TableRpcThrottler* table_rpc_throttler() { return _table_rpc_throttler.get(); }

    cloud::MSBackpressureHandler* ms_backpressure_handler() {
        return _ms_backpressure_handler.get();
    }

    void reset_host_level_rate_limiters();
    void update_backpressure_throttle_params(int top_k, double ratio, double floor_qps);
    void update_backpressure_coordinator_params(int upgrade_cooldown_ticks,
                                                int downgrade_after_ticks);

private:
    std::unique_ptr<cloud::HostLevelMSRpcRateLimiters> _host_level_ms_rpc_rate_limiters;
    std::unique_ptr<cloud::TableRpcQpsRegistry> _table_rpc_qps_registry;
    std::unique_ptr<cloud::TableRpcThrottler> _table_rpc_throttler;
    std::unique_ptr<cloud::MSBackpressureHandler> _ms_backpressure_handler;
};

} // namespace doris
