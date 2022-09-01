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

#include "opentelemetry/trace/provider.h"
#include "runtime/runtime_state.h"

namespace doris {

using OpentelemetryScope = opentelemetry::trace::Scope;
using OpentelemetrySpan = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>;

class OpenTelemetryScopeWrapper {
public:
    OpenTelemetryScopeWrapper(RuntimeState* state, const std::string& name) {
        if (state->enable_profile()) {
            auto span = state->get_tracer()->StartSpan(name);
            _scope.reset(new OpentelemetryScope(span));
        }
    }

    OpenTelemetryScopeWrapper(RuntimeState* state, OpentelemetrySpan& span,
                              const std::string& name) {
        if (state->enable_profile()) {
            if (UNLIKELY(!span)) {
                span = state->get_tracer()->StartSpan(name);
            }
            _scope.reset(new OpentelemetryScope(span));
        }
    }

private:
    std::unique_ptr<OpentelemetryScope> _scope;
};

} // namespace doris