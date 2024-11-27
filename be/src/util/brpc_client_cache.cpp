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

#include "util/brpc_client_cache.h"

#include <gen_cpp/function_service.pb.h> // IWYU pragma: keep
#include <gen_cpp/internal_service.pb.h> // IWYU pragma: keep

#include "util/doris_metrics.h"
#include "util/metrics.h"

namespace doris {
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(brpc_endpoint_stub_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(brpc_stream_endpoint_stub_count, MetricUnit::NOUNIT);

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(brpc_function_endpoint_stub_count, MetricUnit::NOUNIT);

template <>
BrpcClientCache<PBackendService_Stub>::BrpcClientCache(std::string protocol,
                                                       std::string connection_type,
                                                       std::string connection_group)
        : _protocol(protocol),
          _connection_type(connection_type),
          _connection_group(connection_group) {
    if (connection_group == "streaming") {
        REGISTER_HOOK_METRIC(brpc_stream_endpoint_stub_count,
                             [this]() { return _stub_map.size(); });
    } else {
        REGISTER_HOOK_METRIC(brpc_endpoint_stub_count, [this]() { return _stub_map.size(); });
    }
}

template <>
BrpcClientCache<PBackendService_Stub>::~BrpcClientCache() {
    DEREGISTER_HOOK_METRIC(brpc_endpoint_stub_count);
}

template <>
BrpcClientCache<PFunctionService_Stub>::BrpcClientCache(std::string protocol,
                                                        std::string connection_type,
                                                        std::string connection_group)
        : _protocol(protocol),
          _connection_type(connection_type),
          _connection_group(connection_group) {
    REGISTER_HOOK_METRIC(brpc_function_endpoint_stub_count, [this]() { return _stub_map.size(); });
}

template <>
BrpcClientCache<PFunctionService_Stub>::~BrpcClientCache() {
    DEREGISTER_HOOK_METRIC(brpc_function_endpoint_stub_count);
}
} // namespace doris
