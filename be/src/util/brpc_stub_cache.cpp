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

#include "util/brpc_stub_cache.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(brpc_endpoint_stub_count, MetricUnit::NOUNIT);

BrpcStubCache::BrpcStubCache() {
    _stub_map.init(239);
    REGISTER_HOOK_METRIC(brpc_endpoint_stub_count, [this]() {
        std::lock_guard<SpinLock> l(_lock);
        return _stub_map.size();
    });
}

BrpcStubCache::~BrpcStubCache() {
    DEREGISTER_HOOK_METRIC(brpc_endpoint_stub_count);
    for (auto& stub : _stub_map) {
        delete stub.second;
    }
}
} // namespace doris
