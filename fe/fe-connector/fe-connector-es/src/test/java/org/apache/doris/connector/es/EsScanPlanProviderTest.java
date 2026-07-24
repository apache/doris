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

import org.apache.doris.connector.api.ConnectorContractValidator;
import org.apache.doris.connector.spi.ConnectorContext;

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
        final AtomicInteger getHttpNodesCount = new AtomicInteger();

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
            getHttpNodesCount.incrementAndGet();
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
    void testPlanScanAndScanNodePropertiesShareOneFetch() {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        EsTableHandle handle = new EsTableHandle("test_index");

        provider.planScan(EMPTY_SESSION, handle, Collections.emptyList(), java.util.Optional.empty());
        provider.getScanNodeProperties(EMPTY_SESSION, handle, Collections.emptyList(),
                java.util.Optional.empty());
        // ES-F1: planScan and getScanNodeProperties of one scan node run on the SAME per-scan-node
        // provider instance, so the metadata state (mapping + shard routing + node topology) is
        // fetched once and shared -- not twice. MUTATION: removing the memoizedState guard makes
        // each call refetch -> these go back to 2 -> red.
        Assertions.assertEquals(1, client.getMappingCount.get(),
                "the two provider calls of one scan node must share a single mapping fetch");
        Assertions.assertEquals(1, client.searchShardsCount.get(),
                "the two provider calls of one scan node must share a single shard-routing fetch");
        Assertions.assertEquals(1, client.getHttpNodesCount.get(),
                "the two provider calls of one scan node must share a single node-topology fetch");
    }

    @Test
    void testDifferentIndexesFetchSeparately() {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        provider.planScan(EMPTY_SESSION, new EsTableHandle("index_a"),
                Collections.emptyList(), java.util.Optional.empty());
        provider.planScan(EMPTY_SESSION, new EsTableHandle("index_b"),
                Collections.emptyList(), java.util.Optional.empty());
        // The per-scan memo is guarded on the index, so a provider reused for a different index
        // still refetches -- distinct indexes never share a memo entry.
        Assertions.assertEquals(2, client.getMappingCount.get(),
                "Different indexes should each fetch their own metadata");
    }

    @Test
    void testSeparateProviderInstancesEachFetchForFreshness() {
        // Each scan node gets its OWN provider instance. Shard routing must stay fresh per scan
        // (ES rebalances), so the memo must live on the provider and never be reused across scans.
        // Two providers for the same index each fetch once -> 2 total, proving the memo is per-scan,
        // never cross-query.
        CountingRestClient client = new CountingRestClient();
        EsTableHandle handle = new EsTableHandle("test_index");

        new EsScanPlanProvider(client, minimalProps())
                .planScan(EMPTY_SESSION, handle, Collections.emptyList(), java.util.Optional.empty());
        new EsScanPlanProvider(client, minimalProps())
                .planScan(EMPTY_SESSION, handle, Collections.emptyList(), java.util.Optional.empty());

        Assertions.assertEquals(2, client.searchShardsCount.get(),
                "a separate scan node (provider) must refetch shard routing -- memo is per-scan, not cross-query");
    }

    @Test
    void testMetadataSchemaMemoizedPerStatement() {
        // ES-F3: EsConnectorMetadata is per-statement; an index's mapping is resolved into columns
        // once and reused, so getTableSchema + the getColumnHandles that re-invokes it share one
        // remote mapping fetch. MUTATION: dropping the schemaMemo makes each call refetch -> 2 -> red.
        CountingRestClient client = new CountingRestClient();
        EsConnectorMetadata metadata = new EsConnectorMetadata(client, minimalProps());
        EsTableHandle handle = new EsTableHandle("test_index");

        metadata.getTableSchema(EMPTY_SESSION, handle);
        metadata.getColumnHandles(EMPTY_SESSION, handle);
        Assertions.assertEquals(1, client.getMappingCount.get(),
                "an index's schema must be resolved once per statement and reused");
    }

    @Test
    void testMetadataSchemaFreshPerStatement() {
        // A fresh EsConnectorMetadata (a new statement) must re-resolve -- the schema memo is
        // per-statement, never cross-query (mappings can change between statements).
        CountingRestClient client = new CountingRestClient();
        EsTableHandle handle = new EsTableHandle("test_index");
        new EsConnectorMetadata(client, minimalProps()).getTableSchema(EMPTY_SESSION, handle);
        new EsConnectorMetadata(client, minimalProps()).getTableSchema(EMPTY_SESSION, handle);
        Assertions.assertEquals(2, client.getMappingCount.get(),
                "a new statement's metadata must re-resolve the schema -- memo is per-statement");
    }

    @Test
    void testEsConnectorDoesNotSupportWrite() {
        EsConnector connector = new EsConnector(minimalProps(), new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }
        });
        Assertions.assertTrue(connector.supportedWriteOperations().isEmpty(),
                "ES connector should declare no supported write operations");
        // Task 6 P2: the structural contract validator must pass for a real connector (positive control).
        ConnectorContractValidator.validate(connector, "es");
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
