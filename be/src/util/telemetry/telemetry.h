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

#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/context/context.h"

namespace doris::telemetry {

using OpentelemetryTracer = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>;

void initTracerForZipkin();

inline OpentelemetryTracer& get_noop_tracer() {
    static OpentelemetryTracer noop_tracer =
            opentelemetry::nostd::shared_ptr<opentelemetry::trace::NoopTracer>(
                    new opentelemetry::trace::NoopTracer);
    return noop_tracer;
}

inline OpentelemetryTracer get_tracer(const std::string& tracer_name) {
    return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(tracer_name);
}

} // namespace doris::telemetry
