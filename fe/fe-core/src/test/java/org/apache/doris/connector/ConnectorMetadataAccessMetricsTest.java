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
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class ConnectorMetadataAccessMetricsTest {

    @Test
    public void catalogValidationContextDoesNotAcquireRuntimeMetrics() {
        String catalog = "hms_validation_context_metrics_test";
        boolean originalMetricInit = MetricRepo.isInit;
        try {
            MetricRepo.isInit = true;
            DefaultConnectorContext.forCatalogCreationValidation(catalog, 9875L, Collections.emptyMap())
                    .getMetadataAccessObserver().record(event());
            Assertions.assertFalse(hasCatalogMetric("connector_metadata_access_requests_total", catalog));
        } finally {
            MetricRepo.isInit = originalMetricInit;
        }
    }

    @Test
    public void contextCloseUnregistersMetricsAndDisablesCapturedObserver() throws Exception {
        String catalog = "hms_metrics_lifecycle_test";
        boolean originalMetricInit = MetricRepo.isInit;
        DefaultConnectorContext context = new DefaultConnectorContext(catalog, 9876L);
        ConnectorMetadataAccessObserver observer = context.getMetadataAccessObserver();
        try {
            MetricRepo.isInit = true;
            observer.record(event());
            Assertions.assertTrue(hasCatalogMetric(
                    "connector_metadata_access_requests_total", catalog));
            Assertions.assertTrue(hasCatalogMetric(
                    "connector_metadata_access_rpc_elapsed_ms_total", catalog));

            context.close();
            Assertions.assertFalse(hasCatalogMetric(
                    "connector_metadata_access_requests_total", catalog));
            Assertions.assertFalse(hasCatalogMetric(
                    "connector_metadata_access_rpc_elapsed_ms_total", catalog));

            observer.record(event());
            Assertions.assertFalse(hasCatalogMetric(
                    "connector_metadata_access_requests_total", catalog),
                    "an observer captured by a closed context must not recreate metric series");
        } finally {
            context.close();
            MetricRepo.isInit = originalMetricInit;
        }
    }

    @Test
    public void closingOldContextDoesNotUnregisterNewContextMetricsWithTheSameLabels() throws Exception {
        String catalog = "hms_metrics_overlapping_context_test";
        boolean originalMetricInit = MetricRepo.isInit;
        DefaultConnectorContext oldContext = new DefaultConnectorContext(catalog, 9877L);
        DefaultConnectorContext newContext = new DefaultConnectorContext(catalog, 9877L);
        ConnectorMetadataAccessObserver oldObserver = oldContext.getMetadataAccessObserver();
        try {
            MetricRepo.isInit = true;
            oldObserver.record(event());
            Assertions.assertEquals(1L, catalogMetricValue(
                    "connector_metadata_access_requests_total", catalog));
            newContext.getMetadataAccessObserver().record(event());
            Assertions.assertEquals(2L, catalogMetricValue(
                    "connector_metadata_access_requests_total", catalog),
                    "a replacement context must reuse the cumulative counter instead of resetting it");
            oldObserver.record(event());
            Assertions.assertEquals(3L, catalogMetricValue(
                    "connector_metadata_access_requests_total", catalog),
                    "an old context's completed in-flight event must remain visible during overlap");

            oldContext.close();

            Assertions.assertTrue(hasCatalogMetric("connector_metadata_access_requests_total", catalog),
                    "closing the replaced context must not delete the new context's metric instance");
            oldObserver.record(event());
            Assertions.assertEquals(3L, catalogMetricValue(
                    "connector_metadata_access_requests_total", catalog),
                    "a closed context's captured observer must be disabled");
            newContext.getMetadataAccessObserver().record(event());
            Assertions.assertEquals(4L, catalogMetricValue(
                    "connector_metadata_access_requests_total", catalog));
            Assertions.assertTrue(hasCatalogMetric("connector_metadata_access_rpc_elapsed_ms_total", catalog));

            newContext.close();
            Assertions.assertFalse(hasCatalogMetric("connector_metadata_access_requests_total", catalog));
        } finally {
            oldContext.close();
            newContext.close();
            MetricRepo.isInit = originalMetricInit;
        }
    }

    @Test
    public void sameNameDifferentCatalogIdsKeepIndependentMetrics() throws Exception {
        String catalog = "hms_metrics_recreated_catalog_test";
        boolean originalMetricInit = MetricRepo.isInit;
        DefaultConnectorContext oldContext = new DefaultConnectorContext(catalog, 9880L);
        DefaultConnectorContext newContext = new DefaultConnectorContext(catalog, 9881L);
        try {
            MetricRepo.isInit = true;
            oldContext.getMetadataAccessObserver().record(event());
            newContext.getMetadataAccessObserver().record(event());
            Assertions.assertEquals(2, catalogMetricCount(
                    "connector_metadata_access_requests_total", catalog));

            oldContext.close();
            Assertions.assertEquals(1, catalogMetricCount(
                    "connector_metadata_access_requests_total", catalog));
        } finally {
            oldContext.close();
            newContext.close();
            MetricRepo.isInit = originalMetricInit;
        }
    }

    @Test
    public void replacementRuntimeFailureStillReleasesOldContextMetrics() throws Exception {
        String catalog = "hms_metrics_throwing_replacement_test";
        boolean originalMetricInit = MetricRepo.isInit;
        DefaultConnectorContext oldContext = new DefaultConnectorContext(catalog, 9878L);
        DefaultConnectorContext newContext = new DefaultConnectorContext(catalog, 9878L);
        ConnectorMetadataAccessObserver oldObserver = oldContext.getMetadataAccessObserver();
        Connector oldConnector = Mockito.mock(Connector.class);
        Connector newConnector = Mockito.mock(Connector.class);
        Mockito.doThrow(new RuntimeException("close failed")).when(oldConnector).close();
        ReplacingCatalog replacingCatalog = new ReplacingCatalog(
                catalog, oldConnector, oldContext, newConnector, newContext);
        try {
            MetricRepo.isInit = true;
            oldObserver.record(event());
            Assertions.assertEquals(1L, catalogMetricValue(
                    "connector_metadata_access_requests_total", catalog));

            Assertions.assertDoesNotThrow(replacingCatalog::replaceConnector);
            oldObserver.record(event());
            Assertions.assertEquals(1L, catalogMetricValue(
                    "connector_metadata_access_requests_total", catalog),
                    "replacement must close and disable the predecessor context");
            newContext.getMetadataAccessObserver().record(event());

            newContext.close();
            Assertions.assertFalse(hasCatalogMetric("connector_metadata_access_requests_total", catalog),
                    "closing the replacement must unregister metrics after the old reference was released");
        } finally {
            oldContext.close();
            newContext.close();
            MetricRepo.isInit = originalMetricInit;
        }
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

    private static ConnectorMetadataAccessEvent event() {
        return ConnectorMetadataAccessEvent.builder()
                .operation("hms.get_partitions_by_names")
                .source("QUERY")
                .requestedItems(2)
                .rpcCount(1)
                .rpcItems(2)
                .largestBatchSize(2)
                .smallestBatchSize(2)
                .logicalElapsedMillis(5)
                .rpcElapsedMillis(4)
                .maxRpcElapsedMillis(4)
                .success(true)
                .build();
    }

    private static boolean hasCatalogMetric(String name, String catalog) {
        return findCatalogMetric(name, catalog) != null;
    }

    private static long catalogMetricValue(String name, String catalog) {
        Metric<?> metric = findCatalogMetric(name, catalog);
        Assertions.assertNotNull(metric, "missing metric " + name + " for catalog " + catalog);
        return ((Number) metric.getValue()).longValue();
    }

    private static Metric<?> findCatalogMetric(String name, String catalog) {
        for (Metric<?> metric : MetricRepo.DORIS_METRIC_REGISTER.getMetricsByName(name)) {
            for (MetricLabel label : metric.getLabels()) {
                if ("catalog".equals(label.getKey()) && catalog.equals(label.getValue())) {
                    return metric;
                }
            }
        }
        return null;
    }

    private static long catalogMetricCount(String name, String catalog) {
        return MetricRepo.DORIS_METRIC_REGISTER.getMetricsByName(name).stream()
                .filter(metric -> ((Metric<?>) metric).getLabels().stream().anyMatch(
                        label -> "catalog".equals(label.getKey()) && catalog.equals(label.getValue())))
                .count();
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
