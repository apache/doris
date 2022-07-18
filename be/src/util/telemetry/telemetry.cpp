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
#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/exporters/zipkin/zipkin_exporter.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/trace/noop.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"
#include "opentelemetry/trace/provider.h"
#include "service/backend_options.h"

namespace trace = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace zipkin = opentelemetry::exporter::zipkin;
namespace resource = opentelemetry::sdk::resource;
namespace propagation = opentelemetry::context::propagation;

void doris::telemetry::initTracer() {
    if (!doris::config::enable_tracing) {
        return;
    }

    // ZipkinExporter converts span to zipkin's format and exports span to zipkin.
    zipkin::ZipkinExporterOptions opts;
    opts.endpoint = doris::config::trace_export_url;
    auto exporter = std::unique_ptr<trace_sdk::SpanExporter>(new zipkin::ZipkinExporter(opts));

    // BatchSpanProcessor exports span by batch.
    trace_sdk::BatchSpanProcessorOptions batchOptions;
    batchOptions.schedule_delay_millis =
            std::chrono::milliseconds(doris::config::export_span_schedule_delay_millis);
    batchOptions.max_queue_size = doris::config::max_span_queue_size;
    batchOptions.max_export_batch_size = doris::config::max_span_export_batch_size;
    auto processor = std::unique_ptr<trace_sdk::SpanProcessor>(
            new trace_sdk::BatchSpanProcessor(std::move(exporter), batchOptions));

    std::string service_name = "BACKEND:" + BackendOptions::get_localhost();
    resource::ResourceAttributes attributes = {{"service.name", service_name}};
    auto resource = resource::Resource::Create(attributes);

    auto provider = nostd::shared_ptr<trace::TracerProvider>(
            new trace_sdk::TracerProvider(std::move(processor), resource));
    // Set the global trace provider
    trace::Provider::SetTracerProvider(std::move(provider));

    // Specifies the format for parsing trace and span messages propagated via http or gpc.
    propagation::GlobalTextMapPropagator::SetGlobalPropagator(
            nostd::shared_ptr<propagation::TextMapPropagator>(
                    new opentelemetry::trace::propagation::HttpTraceContext()));
}
