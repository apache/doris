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
    void testPlanScanAndScanNodePropertiesFetchIndependently() {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        EsTableHandle handle = new EsTableHandle("test_index");

        provider.planScan(EMPTY_SESSION, handle, Collections.emptyList(), java.util.Optional.empty());
        provider.getScanNodeProperties(EMPTY_SESSION, handle, Collections.emptyList(),
                java.util.Optional.empty());
        Assertions.assertEquals(2, client.getMappingCount.get(),
                "Each provider call should fetch mapping for the current request");
        Assertions.assertEquals(2, client.searchShardsCount.get(),
                "Each provider call should fetch shard routing for the current request");
    }

    @Test
    void testDifferentIndexesFetchSeparately() {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        provider.planScan(EMPTY_SESSION, new EsTableHandle("index_a"),
                Collections.emptyList(), java.util.Optional.empty());
        provider.planScan(EMPTY_SESSION, new EsTableHandle("index_b"),
                Collections.emptyList(), java.util.Optional.empty());
        Assertions.assertEquals(2, client.getMappingCount.get(),
                "Different indexes should each fetch their own metadata");
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

    @Test
    void testAppendExplainInfoShowsEsIndex() {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        EsTableHandle handle = new EsTableHandle("my_test_index");

        Map<String, String> props = provider.getScanNodeProperties(
                EMPTY_SESSION, handle, Collections.emptyList(), java.util.Optional.empty());
        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", props);

        Assertions.assertTrue(output.toString().contains("ES index: my_test_index"));
    }

    @Test
    void testAppendExplainInfoMissingIndex() {
        EsScanPlanProvider provider = new EsScanPlanProvider(new CountingRestClient(), minimalProps());
        StringBuilder output = new StringBuilder();

        provider.appendExplainInfo(output, "", Collections.emptyMap());

        Assertions.assertFalse(output.toString().contains("ES index:"));
    }
}
