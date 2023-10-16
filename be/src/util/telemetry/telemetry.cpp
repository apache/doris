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

#include <fmt/format.h>
#include <opentelemetry/exporters/otlp/otlp_environment.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/resource/resource_detector.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/recordable.h>
#include <opentelemetry/trace/canonical_code.h>
#include <opentelemetry/trace/provider.h>

#include <boost/algorithm/string/case_conv.hpp>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <memory>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter.h"
#include "opentelemetry/exporters/zipkin/zipkin_exporter.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/sdk/common/attribute_utils.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/batch_span_processor_options.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"
#include "service/backend_options.h"

namespace trace = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace zipkin = opentelemetry::exporter::zipkin;
namespace resource = opentelemetry::sdk::resource;
namespace propagation = opentelemetry::context::propagation;
namespace otlp = opentelemetry::exporter::otlp;
namespace internal_log = opentelemetry::sdk::common::internal_log;

class OpenTelemetryLogHandler : public internal_log::LogHandler {
public:
    void Handle(internal_log::LogLevel level, const char* file, int line, const char* msg,
                const opentelemetry::sdk::common::AttributeMap& attributes) noexcept override {
        if ((level == internal_log::LogLevel::Error || level == internal_log::LogLevel::Warning) &&
            file != nullptr && msg != nullptr) {
            LOG(WARNING) << fmt::format("OpenTelemetry File: {}:{} {}", file, line, msg);
        }
    }
};

void doris::telemetry::init_tracer() {
    if (!doris::config::enable_tracing) {
        return;
    }

    std::unique_ptr<trace_sdk::SpanExporter> exporter = nullptr;
    std::string trace_exporter = config::trace_exporter;
    boost::to_lower(trace_exporter);
    if (trace_exporter.compare("collector") == 0) {
        otlp::OtlpHttpExporterOptions opts {};
        opts.url = doris::config::trace_export_url;
        exporter = std::make_unique<otlp::OtlpHttpExporter>(opts);
    } else if (trace_exporter.compare("zipkin") == 0) {
        // ZipkinExporter converts span to zipkin's format and exports span to zipkin.
        zipkin::ZipkinExporterOptions opts {};
        opts.endpoint = doris::config::trace_export_url;
        exporter = std::make_unique<zipkin::ZipkinExporter>(opts);
    } else {
        LOG(FATAL) << "unknown value " << trace_exporter << " of trace_exporter in be.conf";
    }

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

    // Output OpenTelemetry logs by glog
    internal_log::GlobalLogHandler::SetLogHandler(
            nostd::shared_ptr<internal_log::LogHandler>(new OpenTelemetryLogHandler()));
}
