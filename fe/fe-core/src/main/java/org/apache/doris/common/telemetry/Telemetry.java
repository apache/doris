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

package org.apache.doris.common.telemetry;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Managing OpenTelemetry sdk.
 */
public class Telemetry {
    private static final Logger LOG = LogManager.getLogger(Telemetry.class);

    private static OpenTelemetry openTelemetry = OpenTelemetry.noop();

    /**
     * Initialize {@link OpenTelemetry} with {@link SdkTracerProvider}, {@link BatchSpanProcessor},
     * {@link ZipkinSpanExporter} and {@link W3CTraceContextPropagator}.
     */
    public static void initOpenTelemetry() {
        if (!Config.enable_tracing) {
            return;
        }

        // todo: It may be possible to use oltp exporter to export telemetry data to otel collector,
        //  which in turn processes and sends telemetry data to multiple back-ends (e.g. zipkin, Prometheus,
        //  Fluent Bit, etc.) to improve scalability.
        String httpUrl = Config.trace_export_url;
        SpanExporter spanExporter = zipkinExporter(httpUrl);

        String serviceName = "FRONTEND:" + Catalog.getCurrentCatalog().getSelfNode().first;
        Resource serviceNameResource =
                Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), serviceName));
        // Send a batch of spans if ScheduleDelay time or MaxExportBatchSize is reached
        BatchSpanProcessor spanProcessor =
                BatchSpanProcessor.builder(spanExporter).setScheduleDelay(100, TimeUnit.MILLISECONDS)
                        .setMaxExportBatchSize(1000).build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder().addSpanProcessor(spanProcessor)
                .setResource(Resource.getDefault().merge(serviceNameResource)).build();
        openTelemetry = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance())).build();

        // add a shutdown hook to shut down the SDK
        Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::shutdown));
    }

    private static SpanExporter zipkinExporter(String httpUrl) {
        return ZipkinSpanExporter.builder().setEndpoint(httpUrl).build();
    }

    private static SpanExporter oltpExporter(String httpUrl) {
        return OtlpGrpcSpanExporter.builder().setEndpoint(httpUrl).build();
    }

    public static OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }

    public static Tracer getNoopTracer() {
        return OpenTelemetry.noop().getTracer("noop");
    }

    public static Span getNoopSpan() {
        return getNoopTracer().spanBuilder("noopSpan").startSpan();
    }
}
