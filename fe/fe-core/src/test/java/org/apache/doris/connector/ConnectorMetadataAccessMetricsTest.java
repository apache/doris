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

package org.apache.doris.connector;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadataAccessEvent;
import org.apache.doris.connector.spi.ConnectorMetadataAccessObserver;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricRepo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConnectorMetadataAccessMetricsTest {

    private final List<DefaultConnectorContext> contexts = new ArrayList<>();
    private boolean originalMetricInit;

    @BeforeEach
    public void enableMetrics() {
        originalMetricInit = MetricRepo.isInit;
        MetricRepo.isInit = true;
    }

    @AfterEach
    public void restoreMetrics() throws Exception {
        for (DefaultConnectorContext context : contexts) {
            context.close();
        }
        MetricRepo.isInit = originalMetricInit;
    }

    @Test
    public void catalogValidationContextDoesNotAcquireRuntimeMetrics() {
        String catalog = "hms_validation_context_metrics_test";
        DefaultConnectorContext.forCatalogCreationValidation(catalog, 9875L, Collections.emptyMap())
                .getMetadataAccessObserver().record(event("hms.get_partitions_by_names"));
        Assertions.assertEquals(0, catalogMetricCount("connector_metadata_access_requests_total", catalog));
    }

    @Test
    public void contextCloseUnregistersMetricsAndDisablesCapturedObserver() throws Exception {
        String catalog = "hms_metrics_lifecycle_test";
        DefaultConnectorContext context = context(catalog, 9876L);
        ConnectorMetadataAccessObserver observer = context.getMetadataAccessObserver();
        observer.record(event("hms.get_partitions_by_names"));
        Assertions.assertEquals(1, catalogMetricCount("connector_metadata_access_requests_total", catalog));
        context.close();
        Assertions.assertEquals(0, catalogMetricCount("connector_metadata_access_requests_total", catalog));
        observer.record(event("hms.get_partitions_by_names"));
        Assertions.assertEquals(0, catalogMetricCount("connector_metadata_access_requests_total", catalog));
    }

    @Test
    public void closingOldContextDoesNotUnregisterNewContextMetricsWithTheSameLabels() throws Exception {
        String catalog = "hms_metrics_overlapping_context_test";
        DefaultConnectorContext oldContext = context(catalog, 9877L);
        DefaultConnectorContext newContext = context(catalog, 9877L);
        ConnectorMetadataAccessObserver oldObserver = oldContext.getMetadataAccessObserver();
        oldObserver.record(event("hms.get_partitions_by_names"));
        newContext.getMetadataAccessObserver().record(event("hms.get_partitions_by_names"));
        oldObserver.record(event("hms.get_partitions_by_names"));
        Assertions.assertEquals(3L, catalogMetricValue("connector_metadata_access_requests_total", catalog));
        oldContext.close();
        oldObserver.record(event("hms.get_partitions_by_names"));
        Assertions.assertEquals(3L, catalogMetricValue("connector_metadata_access_requests_total", catalog));
        newContext.getMetadataAccessObserver().record(event("hms.get_partitions_by_names"));
        Assertions.assertEquals(4L, catalogMetricValue("connector_metadata_access_requests_total", catalog));
        newContext.close();
        Assertions.assertEquals(0, catalogMetricCount("connector_metadata_access_requests_total", catalog));
    }

    @Test
    public void sameNameDifferentCatalogIdsKeepIndependentMetrics() throws Exception {
        String catalog = "hms_metrics_recreated_catalog_test";
        DefaultConnectorContext oldContext = context(catalog, 9880L);
        DefaultConnectorContext newContext = context(catalog, 9881L);
        oldContext.getMetadataAccessObserver().record(event("hms.get_partitions_by_names"));
        newContext.getMetadataAccessObserver().record(event("hms.get_partitions_by_names"));
        Assertions.assertEquals(2, catalogMetricCount("connector_metadata_access_requests_total", catalog));
        oldContext.close();
        Assertions.assertEquals(1, catalogMetricCount("connector_metadata_access_requests_total", catalog));
    }

    @Test
    public void replacementRuntimeFailureStillReleasesOldContextMetrics() throws Exception {
        String catalog = "hms_metrics_throwing_replacement_test";
        DefaultConnectorContext oldContext = context(catalog, 9878L);
        DefaultConnectorContext newContext = context(catalog, 9878L);
        ConnectorMetadataAccessObserver oldObserver = oldContext.getMetadataAccessObserver();
        Connector oldConnector = Mockito.mock(Connector.class);
        Connector newConnector = Mockito.mock(Connector.class);
        Mockito.doThrow(new RuntimeException("close failed")).when(oldConnector).close();
        ReplacingCatalog replacingCatalog = new ReplacingCatalog(
                catalog, oldConnector, oldContext, newConnector, newContext);
        oldObserver.record(event("hms.get_partitions_by_names"));
        Assertions.assertDoesNotThrow(replacingCatalog::replaceConnector);
        oldObserver.record(event("hms.get_partitions_by_names"));
        Assertions.assertEquals(1L, catalogMetricValue("connector_metadata_access_requests_total", catalog));
        newContext.getMetadataAccessObserver().record(event("hms.get_partitions_by_names"));
        newContext.close();
        Assertions.assertEquals(0, catalogMetricCount("connector_metadata_access_requests_total", catalog));
    }

    @Test
    public void replacementErrorCleansOldContextBeforePropagation() throws Exception {
        DefaultConnectorContext oldContext = Mockito.mock(DefaultConnectorContext.class);
        DefaultConnectorContext newContext = Mockito.mock(DefaultConnectorContext.class);
        Connector oldConnector = Mockito.mock(Connector.class);
        Connector newConnector = Mockito.mock(Connector.class);
        AssertionError failure = new AssertionError("fatal close failure");
        Mockito.doThrow(failure).when(oldConnector).close();
        ReplacingCatalog replacingCatalog = new ReplacingCatalog(
                "fatal-replacement", oldConnector, oldContext, newConnector, newContext);

        Assertions.assertSame(failure,
                Assertions.assertThrows(AssertionError.class, replacingCatalog::replaceConnector));
        Mockito.verify(oldContext).close();
    }

    @Test
    public void distinctOperationMetricsAreBoundedAndOverflowIsAggregated() throws Exception {
        String catalog = "hms_metrics_operation_limit_test";
        DefaultConnectorContext context = context(catalog, 9882L);
        int requests = ConnectorMetadataAccessMetrics.MAX_DISTINCT_OPERATIONS_PER_CATALOG + 5;
        for (int i = 0; i < requests; i++) {
            context.getMetadataAccessObserver().record(event("operation." + i));
        }
        Assertions.assertEquals(ConnectorMetadataAccessMetrics.MAX_DISTINCT_OPERATIONS_PER_CATALOG + 1,
                catalogMetricCount("connector_metadata_access_requests_total", catalog));
        Assertions.assertEquals(requests, catalogMetricTotal("connector_metadata_access_requests_total", catalog));
    }

    private static ConnectorMetadataAccessEvent event(String operation) {
        return ConnectorMetadataAccessEvent.builder()
                .operation(operation)
                .source("QUERY")
                .success(true)
                .build();
    }

    private static long catalogMetricValue(String name, String catalog) {
        return catalogMetricTotal(name, catalog);
    }

    private static long catalogMetricCount(String name, String catalog) {
        return MetricRepo.DORIS_METRIC_REGISTER.getMetricsByName(name).stream()
                .filter(metric -> ((Metric<?>) metric).getLabels().stream().anyMatch(
                        label -> "catalog".equals(label.getKey()) && catalog.equals(label.getValue())))
                .count();
    }

    private static long catalogMetricTotal(String name, String catalog) {
        return MetricRepo.DORIS_METRIC_REGISTER.getMetricsByName(name).stream()
                .filter(metric -> ((Metric<?>) metric).getLabels().stream().anyMatch(
                        label -> "catalog".equals(label.getKey()) && catalog.equals(label.getValue())))
                .mapToLong(metric -> ((Number) metric.getValue()).longValue()).sum();
    }

    private DefaultConnectorContext context(String catalog, long catalogId) {
        DefaultConnectorContext context = new DefaultConnectorContext(catalog, catalogId);
        contexts.add(context);
        return context;
    }

    private static class ReplacingCatalog extends PluginDrivenExternalCatalog {
        private final Connector newConnector;
        private final DefaultConnectorContext newContext;

        ReplacingCatalog(String name, Connector oldConnector, DefaultConnectorContext oldContext,
                Connector newConnector, DefaultConnectorContext newContext) {
            super(1L, name, null, Collections.emptyMap(), "", oldConnector);
            this.newConnector = newConnector;
            this.newContext = newContext;
            Deencapsulation.setField(this, "connectorContext", oldContext);
        }

        @Override
        protected Connector createConnectorFromProperties() {
            Deencapsulation.setField(this, "connectorContext", newContext);
            return newConnector;
        }

        void replaceConnector() {
            super.initLocalObjectsImpl();
        }
    }
}
