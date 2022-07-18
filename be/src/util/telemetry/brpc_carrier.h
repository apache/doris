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
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/span_startoptions.h"
#include "util/telemetry/telemetry.h"

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

inline OpentelemetrySpan start_rpc_server_span(std::string span_name,
                                               google::protobuf::RpcController* cntl_base) {
    RpcServerCarrier carrier(static_cast<brpc::Controller*>(cntl_base));
    auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
    auto prop = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    auto parent_context = prop->Extract(carrier, current_ctx);

    if (opentelemetry::trace::GetSpan(parent_context)->GetContext().IsValid()) {
        opentelemetry::trace::StartSpanOptions options;
        options.kind = opentelemetry::trace::SpanKind::kServer;
        options.parent = parent_context;
        return telemetry::get_tracer("tracer")->StartSpan(std::move(span_name), options);
    } else {
        return telemetry::get_noop_tracer()->StartSpan("");
    }
}
} // namespace doris::telemetry
