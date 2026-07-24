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
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;
import org.apache.doris.connector.api.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

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
            // Minimal valid mapping JSON for EsMappingUtils.resolveFieldContext; two keyword fields
            // so tests that project columns (a/b) resolve a field context without erroring.
            return "{\"" + indexName + "\":{\"mappings\":{\"properties\":"
                    + "{\"a\":{\"type\":\"keyword\"},\"b\":{\"type\":\"keyword\"}}}}}";
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

    /**
     * Test session with a settable statement scope. Defaults to {@link ConnectorStatementScope#NONE}
     * (the offline default); pass a real scope to exercise the per-statement cross-path mapping memo.
     */
    private static final class TestSession implements ConnectorSession {
        private final ConnectorStatementScope scope;

        TestSession(ConnectorStatementScope scope) {
            this.scope = scope;
        }

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

        @Override
        public ConnectorStatementScope getStatementScope() {
            return scope;
        }
    }

    /** A live per-statement scope: a plain CHM-backed arena, like the engine's real scope. */
    private static ConnectorStatementScope liveScope() {
        return new ConnectorStatementScope() {
            private final Map<String, Object> arena = new ConcurrentHashMap<>();

            @Override
            @SuppressWarnings("unchecked")
            public <T> T computeIfAbsent(String key, Supplier<T> loader) {
                return (T) arena.computeIfAbsent(key, k -> loader.get());
            }
        };
    }

    private static final ConnectorSession EMPTY_SESSION = new TestSession(ConnectorStatementScope.NONE);

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
    void testMappingSharedAcrossSchemaAndScanPathsWithinStatement() {
        // ES-F2: the schema path (EsConnectorMetadata) and the scan path (EsScanPlanProvider) each
        // fetched the same index mapping remotely. Routed through the shared per-statement scope, one
        // index's getMapping fires ONCE across both paths within a statement. MUTATION: dropping the
        // EsStatementScope wrapping (or a NONE scope) makes the paths fetch independently -> 2 -> red.
        CountingRestClient client = new CountingRestClient();
        ConnectorSession session = new TestSession(liveScope());
        EsTableHandle handle = new EsTableHandle("test_index");

        new EsConnectorMetadata(client, minimalProps()).getTableSchema(session, handle);
        new EsScanPlanProvider(client, minimalProps())
                .planScan(session, handle, Collections.emptyList(), java.util.Optional.empty());

        Assertions.assertEquals(1, client.getMappingCount.get(),
                "one index's mapping must be fetched once per statement across the schema and scan paths");
    }

    @Test
    void testScopedMappingNotSharedAcrossStatements() {
        // Two statements (distinct live scopes) must NOT share the mapping -- the scope memo is
        // per-statement, never cross-query. Shard routing is likewise never placed in the scope.
        CountingRestClient client = new CountingRestClient();
        EsTableHandle handle = new EsTableHandle("test_index");

        new EsConnectorMetadata(client, minimalProps())
                .getTableSchema(new TestSession(liveScope()), handle);
        new EsConnectorMetadata(client, minimalProps())
                .getTableSchema(new TestSession(liveScope()), handle);

        Assertions.assertEquals(2, client.getMappingCount.get(),
                "a separate statement (scope) must re-fetch the mapping -- the scope memo is per-statement");
    }

    @Test
    void testShardRoutingNeverSharedViaScope() {
        // Hard freshness constraint: shard routing + node topology must NEVER be shared via the
        // per-statement scope (ES rebalances) -- only the raw mapping is. Two scan providers sharing
        // the SAME live statement scope and index each refetch shards/nodes, while the mapping is
        // fetched once. MUTATION: routing searchShards or getHttpNodes through EsStatementScope would
        // make the second scan reuse the first -> those counts drop to 1 -> red.
        CountingRestClient client = new CountingRestClient();
        ConnectorSession session = new TestSession(liveScope());
        EsTableHandle handle = new EsTableHandle("test_index");

        new EsScanPlanProvider(client, minimalProps())
                .planScan(session, handle, Collections.emptyList(), java.util.Optional.empty());
        new EsScanPlanProvider(client, minimalProps())
                .planScan(session, handle, Collections.emptyList(), java.util.Optional.empty());

        Assertions.assertEquals(2, client.searchShardsCount.get(),
                "shard routing must be fetched per scan, never shared via the statement scope");
        Assertions.assertEquals(2, client.getHttpNodesCount.get(),
                "node topology must be fetched per scan, never shared via the statement scope");
        Assertions.assertEquals(1, client.getMappingCount.get(),
                "the raw mapping is the only thing shared across the two scans of one statement");
    }

    @Test
    void testDifferentColumnsRefetch() {
        // The per-scan memo is guarded on the projected columns, not just the index, because the
        // field-context depends on them; a different projection must refetch rather than return a
        // stale field-context. MUTATION: dropping the columns comparison lets the second projection
        // reuse the first index's state -> searchShards stays 1 -> red.
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        EsTableHandle handle = new EsTableHandle("test_index");

        provider.planScan(EMPTY_SESSION, handle,
                Collections.singletonList(new NamedColumnHandle("a")), java.util.Optional.empty());
        provider.planScan(EMPTY_SESSION, handle,
                Collections.singletonList(new NamedColumnHandle("b")), java.util.Optional.empty());

        Assertions.assertEquals(2, client.searchShardsCount.get(),
                "a different projection must refetch -- the memo is guarded on columns, not just index");
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
