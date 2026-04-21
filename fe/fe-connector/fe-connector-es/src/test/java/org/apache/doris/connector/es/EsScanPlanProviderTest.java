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

package org.apache.doris.connector.es;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class EsScanPlanProviderTest {

    /**
     * Minimal REST client stub that counts how many times getMapping/searchShards
     * are called. No real HTTP is needed.
     */
    static class CountingRestClient extends EsConnectorRestClient {

        final AtomicInteger getMappingCount = new AtomicInteger();
        final AtomicInteger searchShardsCount = new AtomicInteger();

        CountingRestClient() {
            super(new String[]{"localhost:9200"}, null, null, false, null);
        }

        @Override
        public String getMapping(String indexName) {
            getMappingCount.incrementAndGet();
            // Minimal valid mapping JSON for EsMappingUtils.resolveFieldContext
            return "{\"" + indexName + "\":{\"mappings\":{\"properties\":{}}}}";
        }

        @Override
        public EsShardPartitions searchShards(String indexName) {
            searchShardsCount.incrementAndGet();
            return new EsShardPartitions(indexName);
        }

        @Override
        public Map<String, EsNodeInfo> getHttpNodes() {
            Map<String, EsNodeInfo> nodes = new HashMap<>();
            nodes.put("node1", new EsNodeInfo("node1", "localhost:9200"));
            return nodes;
        }
    }

    private static final org.apache.doris.connector.api.ConnectorSession EMPTY_SESSION =
            new org.apache.doris.connector.api.ConnectorSession() {
                @Override
                public String getQueryId() {
                    return "test-query";
                }

                @Override
                public String getUser() {
                    return "test";
                }

                @Override
                public String getTimeZone() {
                    return "UTC";
                }

                @Override
                public String getLocale() {
                    return "en_US";
                }

                @Override
                public long getCatalogId() {
                    return 0;
                }

                @Override
                public String getCatalogName() {
                    return "test";
                }

                @Override
                public <T> T getProperty(String name, Class<T> type) {
                    return null;
                }

                @Override
                public Map<String, String> getCatalogProperties() {
                    return Collections.emptyMap();
                }

                @Override
                public Map<String, String> getSessionProperties() {
                    return Collections.emptyMap();
                }
            };

    private static Map<String, String> minimalProps() {
        Map<String, String> props = new HashMap<>();
        props.put(EsConnectorProperties.HOSTS, "localhost:9200");
        return props;
    }

    @Test
    void testMetadataFetchedOnceWithinCacheTtl() {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        EsTableHandle handle = new EsTableHandle("test_index");

        // First call fetches from REST
        provider.planScan(EMPTY_SESSION, handle, Collections.emptyList(), java.util.Optional.empty());
        Assertions.assertEquals(1, client.getMappingCount.get(),
                "First planScan should fetch mapping once");

        // Second call should use cache — no additional REST call
        provider.getScanNodeProperties(EMPTY_SESSION, handle, Collections.emptyList(),
                java.util.Optional.empty());
        Assertions.assertEquals(1, client.getMappingCount.get(),
                "getScanNodeProperties should use cached metadata");
        Assertions.assertEquals(1, client.searchShardsCount.get(),
                "searchShards should only be called once total");
    }

    @Test
    void testCacheExpiredRefetches() throws Exception {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        EsTableHandle handle = new EsTableHandle("test_index");

        // First fetch
        provider.planScan(EMPTY_SESSION, handle, Collections.emptyList(), java.util.Optional.empty());
        Assertions.assertEquals(1, client.getMappingCount.get());

        // Clear cache to simulate expiry
        provider.clearMetadataCache();

        // After expiry, should refetch
        provider.planScan(EMPTY_SESSION, handle, Collections.emptyList(), java.util.Optional.empty());
        Assertions.assertEquals(2, client.getMappingCount.get(),
                "After cache clear, should refetch metadata");
    }

    @Test
    void testDifferentIndexesCachedSeparately() {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());

        EsTableHandle handle1 = new EsTableHandle("index_a");
        EsTableHandle handle2 = new EsTableHandle("index_b");

        provider.planScan(EMPTY_SESSION, handle1, Collections.emptyList(), java.util.Optional.empty());
        provider.planScan(EMPTY_SESSION, handle2, Collections.emptyList(), java.util.Optional.empty());

        // Each index should trigger one fetch
        Assertions.assertEquals(2, client.getMappingCount.get(),
                "Different indexes should each trigger a fetch");
        Assertions.assertEquals(2, provider.metadataCacheSize(),
                "Cache should have entries for both indexes");

        // Fetching same indexes again should be cached
        provider.planScan(EMPTY_SESSION, handle1, Collections.emptyList(), java.util.Optional.empty());
        provider.planScan(EMPTY_SESSION, handle2, Collections.emptyList(), java.util.Optional.empty());
        Assertions.assertEquals(2, client.getMappingCount.get(),
                "Cached indexes should not trigger refetch");
    }

    @Test
    void testEsMetadataDoesNotSupportWrite() {
        EsConnectorMetadata metadata = new EsConnectorMetadata(
                new CountingRestClient(), minimalProps());
        Assertions.assertFalse(metadata.supportsInsert(),
                "ES connector metadata should not support INSERT");
        Assertions.assertFalse(metadata.supportsDelete(),
                "ES connector metadata should not support DELETE");
        Assertions.assertFalse(metadata.supportsMerge(),
                "ES connector metadata should not support MERGE");
    }
}
