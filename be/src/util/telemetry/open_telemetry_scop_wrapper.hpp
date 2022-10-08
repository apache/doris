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

#include "common/compiler_util.h"
#include "opentelemetry/trace/provider.h"

namespace doris {

using OpentelemetryTracer = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>;
using OpentelemetryScope = opentelemetry::trace::Scope;
using OpentelemetrySpan = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>;

class OpenTelemetryScopeWrapper {
public:
    OpenTelemetryScopeWrapper(bool enable, OpentelemetryTracer tracer, const std::string& name) {
        if (enable) {
            auto span = tracer->StartSpan(name);
            _scope.reset(new OpentelemetryScope(span));
        }
    }

    OpenTelemetryScopeWrapper(bool enable, OpentelemetryTracer tracer, OpentelemetrySpan span,
                              const std::string& name) {
        if (enable) {
            if (UNLIKELY(!span)) {
                span = tracer->StartSpan(name);
            }
            _scope.reset(new OpentelemetryScope(span));
        }
    }

private:
    std::unique_ptr<OpentelemetryScope> _scope;
};

} // namespace doris
