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

#include "telemetry.h"

#include "common/config.h"
#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/context/propagation/noop_propagator.h"
#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/exporters/zipkin/zipkin_exporter.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/trace/noop.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"
#include "opentelemetry/trace/provider.h"

namespace trace = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace zipkin = opentelemetry::exporter::zipkin;
namespace resource = opentelemetry::sdk::resource;
namespace propagation = opentelemetry::context::propagation;

void doris::telemetry::initTracerForZipkin() {
    if (!doris::config::enable_tracing) {
        return;
    }

    zipkin::ZipkinExporterOptions opts;
    opts.endpoint = doris::config::trace_export_url;
    resource::ResourceAttributes attributes = {{"service.name", "Backend"}};
    auto resource = resource::Resource::Create(attributes);
    auto exporter = std::unique_ptr<trace_sdk::SpanExporter>(new zipkin::ZipkinExporter(opts));

    trace_sdk::BatchSpanProcessorOptions batchOptions;
    auto processor = std::unique_ptr<trace_sdk::SpanProcessor>(
            new trace_sdk::BatchSpanProcessor(std::move(exporter), batchOptions));
    auto provider = nostd::shared_ptr<trace::TracerProvider>(
            new trace_sdk::TracerProvider(std::move(processor), resource));
    // Set the global trace provider
    trace::Provider::SetTracerProvider(std::move(provider));

    propagation::GlobalTextMapPropagator::SetGlobalPropagator(
            nostd::shared_ptr<propagation::TextMapPropagator>(
                    new opentelemetry::trace::propagation::HttpTraceContext()));
}
