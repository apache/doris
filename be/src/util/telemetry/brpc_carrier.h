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

#include <brpc/controller.h>

#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/context/runtime_context.h"
#include "opentelemetry/trace/context.h"

namespace doris::telemetry {

class RpcServerCarrier : public opentelemetry::context::propagation::TextMapCarrier {
public:
    explicit RpcServerCarrier(const brpc::Controller* cntl) : cntl_(cntl) {}

    RpcServerCarrier() = default;

    opentelemetry::nostd::string_view Get(
            opentelemetry::nostd::string_view key) const noexcept override;

    void Set(opentelemetry::nostd::string_view key,
             opentelemetry::nostd::string_view value) noexcept override;


private:
    const brpc::Controller* cntl_ {};
};

inline decltype(auto) get_span_from_rpc(brpc::Controller* controller) {
    RpcServerCarrier carrier(controller);
    auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();

    auto prop = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    auto new_context = prop->Extract(carrier, current_ctx);
    return opentelemetry::trace::GetSpan(new_context);
}

} // namespace doris::telemetry
