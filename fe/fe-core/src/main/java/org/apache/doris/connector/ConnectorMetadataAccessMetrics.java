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

import org.apache.doris.common.Pair;
import org.apache.doris.connector.spi.ConnectorMetadataAccessEvent;
import org.apache.doris.metric.GaugeMetricImpl;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class ConnectorMetadataAccessMetrics implements AutoCloseable {

    static final int MAX_DISTINCT_OPERATIONS_PER_CATALOG = 64;
    static final String OVERFLOW_OPERATION = "other";
    private static final Map<Pair<Long, String>, SharedCatalogMetrics> SHARED_METRICS = new HashMap<>();

    private final Pair<Long, String> catalogIdentity;
    private final SharedCatalogMetrics sharedMetrics;
    private boolean closed;

    ConnectorMetadataAccessMetrics(String catalogName, long catalogId) {
        this.catalogIdentity = Pair.of(catalogId, catalogName);
        this.sharedMetrics = acquire(catalogIdentity);
    }

    synchronized void record(ConnectorMetadataAccessEvent event) {
        if (closed) {
            return;
        }
        sharedMetrics.record(event);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        release(catalogIdentity, sharedMetrics);
    }

    private static synchronized SharedCatalogMetrics acquire(Pair<Long, String> catalogIdentity) {
        SharedCatalogMetrics shared = SHARED_METRICS.computeIfAbsent(
                catalogIdentity, identity -> new SharedCatalogMetrics(identity.second, identity.first));
        shared.references++;
        return shared;
    }

    private static synchronized void release(Pair<Long, String> catalogIdentity, SharedCatalogMetrics shared) {
        shared.references--;
        if (shared.references == 0) {
            SHARED_METRICS.remove(catalogIdentity, shared);
            shared.unregister();
        }
    }

    private static final class SharedCatalogMetrics {
        private final String catalogName;
        private final long catalogId;
        private final Map<String, MetricSet> metrics = new HashMap<>();
        private final Set<String> operations = new HashSet<>();
        private int references;

        private SharedCatalogMetrics(String catalogName, long catalogId) {
            this.catalogName = catalogName;
            this.catalogId = catalogId;
        }

        private synchronized void record(ConnectorMetadataAccessEvent event) {
            if (!MetricRepo.isInit) {
                return;
            }
            String operation = normalizeOperation(event.getOperation());
            String key = operation + '\0' + event.getSource() + '\0' + event.isSuccess();
            MetricSet metricSet = metrics.computeIfAbsent(
                    key, ignored -> new MetricSet(catalogName, catalogId, operation, event));
            metricSet.requests.increase(1L);
            metricSet.requestedItems.increase((long) event.getRequestedItems());
            metricSet.rpcs.increase((long) event.getRpcCount());
            metricSet.rpcItems.increase(event.getRpcItems());
            metricSet.fallbacks.increase((long) event.getFallbackCount());
            metricSet.logicalElapsedMillis.increase(event.getLogicalElapsedMillis());
            metricSet.rpcElapsedMillis.increase(event.getRpcElapsedMillis());
            metricSet.latestMaxRpcElapsedMillis.setValue(event.getMaxRpcElapsedMillis());
            metricSet.latestLargestBatch.setValue(event.getLargestBatchSize());
            metricSet.latestSmallestBatch.setValue(event.getSmallestBatchSize());
        }

        private String normalizeOperation(String operation) {
            if (operations.contains(operation) || operations.size() < MAX_DISTINCT_OPERATIONS_PER_CATALOG) {
                operations.add(operation);
                return operation;
            }
            return OVERFLOW_OPERATION;
        }

        private synchronized void unregister() {
            metrics.values().forEach(MetricSet::unregister);
            metrics.clear();
        }
    }

    private static final class MetricSet {
        private final LongCounterMetric requests;
        private final LongCounterMetric requestedItems;
        private final LongCounterMetric rpcs;
        private final LongCounterMetric rpcItems;
        private final LongCounterMetric fallbacks;
        private final LongCounterMetric logicalElapsedMillis;
        private final LongCounterMetric rpcElapsedMillis;
        private final GaugeMetricImpl<Long> latestMaxRpcElapsedMillis;
        private final GaugeMetricImpl<Integer> latestLargestBatch;
        private final GaugeMetricImpl<Integer> latestSmallestBatch;

        private MetricSet(String catalog, long catalogId, String operation, ConnectorMetadataAccessEvent event) {
            requests = counter("connector_metadata_access_requests_total", MetricUnit.REQUESTS,
                    "Logical connector metadata requests", catalog, catalogId, operation, event);
            requestedItems = counter("connector_metadata_access_requested_items_total", MetricUnit.NOUNIT,
                    "Items requested by logical connector metadata requests", catalog, catalogId, operation, event);
            rpcs = counter("connector_metadata_access_rpc_total", MetricUnit.REQUESTS,
                    "Physical connector metadata RPCs", catalog, catalogId, operation, event);
            rpcItems = counter("connector_metadata_access_rpc_items_total", MetricUnit.NOUNIT,
                    "Items sent by connector metadata RPCs", catalog, catalogId, operation, event);
            fallbacks = counter("connector_metadata_access_fallback_total", MetricUnit.OPERATIONS,
                    "Connector metadata adaptive fallbacks", catalog, catalogId, operation, event);
            logicalElapsedMillis = counter("connector_metadata_access_elapsed_ms_total", MetricUnit.MILLISECONDS,
                    "Cumulative logical connector metadata request time", catalog, catalogId, operation, event);
            rpcElapsedMillis = counter("connector_metadata_access_rpc_elapsed_ms_total", MetricUnit.MILLISECONDS,
                    "Cumulative physical connector metadata RPC attempt time", catalog, catalogId, operation, event);
            latestMaxRpcElapsedMillis = new GaugeMetricImpl<>("connector_metadata_access_max_rpc_elapsed_ms",
                    MetricUnit.MILLISECONDS, "Slowest physical RPC attempt in the latest request", 0L);
            addLabels(latestMaxRpcElapsedMillis, catalog, catalogId, operation, event);
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(latestMaxRpcElapsedMillis);
            latestLargestBatch = new GaugeMetricImpl<>("connector_metadata_access_largest_batch_size",
                    MetricUnit.NOUNIT, "Largest physical batch in the latest request", 0);
            addLabels(latestLargestBatch, catalog, catalogId, operation, event);
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(latestLargestBatch);
            latestSmallestBatch = new GaugeMetricImpl<>("connector_metadata_access_smallest_batch_size",
                    MetricUnit.NOUNIT, "Smallest physical batch in the latest request", 0);
            addLabels(latestSmallestBatch, catalog, catalogId, operation, event);
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(latestSmallestBatch);
        }

        private static LongCounterMetric counter(String name, MetricUnit unit, String description,
                String catalog, long catalogId, String operation, ConnectorMetadataAccessEvent event) {
            LongCounterMetric metric = new LongCounterMetric(name, unit, description);
            addLabels(metric, catalog, catalogId, operation, event);
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
            return metric;
        }

        private static void addLabels(org.apache.doris.metric.Metric<?> metric, String catalog,
                long catalogId, String operation, ConnectorMetadataAccessEvent event) {
            metric.setLabels(Arrays.asList(
                    new MetricLabel("catalog", catalog),
                    new MetricLabel("catalog_id", String.valueOf(catalogId)),
                    new MetricLabel("operation", operation),
                    new MetricLabel("source", event.getSource()),
                    new MetricLabel("status", event.isSuccess() ? "success" : "failure")));
        }

        private void unregister() {
            unregister(requests);
            unregister(requestedItems);
            unregister(rpcs);
            unregister(rpcItems);
            unregister(fallbacks);
            unregister(logicalElapsedMillis);
            unregister(rpcElapsedMillis);
            unregister(latestMaxRpcElapsedMillis);
            unregister(latestLargestBatch);
            unregister(latestSmallestBatch);
        }

        private static void unregister(Metric<?> metric) {
            MetricRepo.DORIS_METRIC_REGISTER.removeMetricsByNameAndLabels(
                    metric.getName(), metric.getLabels());
        }
    }
}
