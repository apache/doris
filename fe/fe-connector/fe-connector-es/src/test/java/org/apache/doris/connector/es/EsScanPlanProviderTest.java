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

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorContractValidator;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertiesResult;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileScanRangeParams;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        private final Map<String, String> sessionProperties;

        TestSession(ConnectorStatementScope scope) {
            this(scope, Collections.emptyMap());
        }

        TestSession(ConnectorStatementScope scope, Map<String, String> sessionProperties) {
            this.scope = scope;
            this.sessionProperties = sessionProperties;
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
            return sessionProperties;
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

    private static Map<String, String> minimalPropertyMap() {
        Map<String, String> props = new HashMap<>();
        props.put(EsCatalogProperties.HOSTS, "localhost:9200");
        return props;
    }

    private static EsCatalogProperties minimalProps() {
        return EsCatalogProperties.of(minimalPropertyMap());
    }

    @Test
    void testPlanScanAndScanNodePropertiesShareOneFetch() {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        EsTableHandle handle = new EsTableHandle("test_index");

        provider.planScan(EMPTY_SESSION, ConnectorScanRequest.builder(handle, Collections.emptyList()).build());
        provider.getScanNodePropertiesResult(EMPTY_SESSION, handle, Collections.emptyList(),
                java.util.Optional.empty());
        // ES-F1: planScan and getScanNodePropertiesResult of one scan node run on the SAME per-scan-node
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
        provider.planScan(EMPTY_SESSION,
                ConnectorScanRequest.builder(new EsTableHandle("index_a"), Collections.emptyList())
                .build());
        provider.planScan(EMPTY_SESSION,
                ConnectorScanRequest.builder(new EsTableHandle("index_b"), Collections.emptyList())
                .build());
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
                .planScan(EMPTY_SESSION, ConnectorScanRequest.builder(handle, Collections.emptyList()).build());
        new EsScanPlanProvider(client, minimalProps())
                .planScan(EMPTY_SESSION, ConnectorScanRequest.builder(handle, Collections.emptyList()).build());

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
                .planScan(session, ConnectorScanRequest.builder(handle, Collections.emptyList()).build());

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
                .planScan(session, ConnectorScanRequest.builder(handle, Collections.emptyList()).build());
        new EsScanPlanProvider(client, minimalProps())
                .planScan(session, ConnectorScanRequest.builder(handle, Collections.emptyList()).build());

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

        provider.planScan(EMPTY_SESSION,
                ConnectorScanRequest.builder(handle, Collections.singletonList(new NamedColumnHandle("a")))
                .build());
        provider.planScan(EMPTY_SESSION,
                ConnectorScanRequest.builder(handle, Collections.singletonList(new NamedColumnHandle("b")))
                .build());

        Assertions.assertEquals(2, client.searchShardsCount.get(),
                "a different projection must refetch -- the memo is guarded on columns, not just index");
    }

    @Test
    void testEsConnectorDoesNotSupportWrite() {
        EsConnector connector = new EsConnector(minimalPropertyMap(), new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }
        });
        Assertions.assertNull(connector.getWritePlanProvider(),
                "ES connector should expose no write plan provider, so every write is rejected");
        // The ES-compatible FE HTTP endpoints probe for this capability and answer badRequest when it is
        // absent, so ES must expose it (every other connector inherits the null default).
        Assertions.assertNotNull(connector.getRestPassthrough(),
                "ES fronts an HTTP source, so it must expose the passthrough capability the ES endpoints need");
        // Task 6 P2: the structural contract validator must pass for a real connector (positive control).
        ConnectorContractValidator.validate(connector, "es");
    }

    @Test
    void testAppendExplainInfoShowsEsIndex() {
        CountingRestClient client = new CountingRestClient();
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        EsTableHandle handle = new EsTableHandle("my_test_index");

        Map<String, String> props = provider.getScanNodePropertiesResult(
                EMPTY_SESSION, handle, Collections.emptyList(), java.util.Optional.empty()).getProperties();
        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", props);

        Assertions.assertTrue(output.toString().contains("ES index: my_test_index"));
    }

    // ─────────── early-stop (terminate_after), relocated out of the generic scan node ───────────
    //
    // WHY these matter: asking ES to stop after N hits is only correct when the engine has NO filtering left
    // to do after the scan — otherwise ES stops early on rows a leftover engine-side filter would have thrown
    // away, and the query silently returns too few rows. Both facts come from the engine through synthetic
    // property keys. Before the relocation, this decision lived in the generic scan node and was reached by
    // matching this connector's format string; it had NO unit coverage at all, only an ES-cluster suite.

    private static Map<String, String> pushdownProps(String limit, String batchSize, String allPushed) {
        Map<String, String> props = new HashMap<>();
        if (limit != null) {
            props.put(ScanNodePropertyKeys.SYNTHETIC_PUSHDOWN_LIMIT, limit);
        }
        if (batchSize != null) {
            props.put("es.batch_size", batchSize);
        }
        if (allPushed != null) {
            props.put(ScanNodePropertyKeys.SYNTHETIC_ALL_CONJUNCTS_PUSHED, allPushed);
        }
        return props;
    }

    private static TFileScanRangeParams populateWith(Map<String, String> props) {
        TFileScanRangeParams params = new TFileScanRangeParams();
        new EsScanPlanProvider(new CountingRestClient(), minimalProps()).populateScanLevelParams(params, props);
        return params;
    }

    @Test
    void terminateAfterIsRequestedWhenTheLimitFitsOneBatchAndNothingIsLeftToFilter() {
        // "limit" is the BE-side key ES turns into an early stop; the literal is a wire contract.
        Assertions.assertEquals("5",
                populateWith(pushdownProps("5", "1024", "true")).getEsProperties().get("limit"));
    }

    @Test
    void terminateAfterIsNotRequestedWhenFilteringRemains() {
        // The correctness case: the engine still has conjuncts to apply, so stopping ES early would lose rows.
        Assertions.assertFalse(
                populateWith(pushdownProps("5", "1024", "false")).getEsProperties().containsKey("limit"),
                "with filtering left to do, an early stop can drop rows that survive the remaining filter");
    }

    @Test
    void terminateAfterIsNotRequestedWhenTheLimitExceedsOneBatch() {
        Assertions.assertFalse(
                populateWith(pushdownProps("5000", "1024", "true")).getEsProperties().containsKey("limit"));
    }

    @Test
    void terminateAfterIsNotRequestedWhenTheEngineSuppliedNothing() {
        // A property map with none of the synthetic keys (an older engine, or a direct unit-test call) must be
        // treated as "no limit", not as an unbounded one.
        Assertions.assertFalse(
                populateWith(pushdownProps(null, null, null)).getEsProperties().containsKey("limit"));
    }

    @Test
    void explainReportsTheEarlyStopVerbatim() {
        StringBuilder output = new StringBuilder();
        new EsScanPlanProvider(new CountingRestClient(), minimalProps())
                .appendExplainInfo(output, "", pushdownProps("5", "1024", "true"));
        // The exact text is the acceptance baseline: an ES-cluster suite asserts this string, and this
        // relocation must not change it by a character.
        Assertions.assertTrue(output.toString().contains("ES terminate_after: 5\n"), output.toString());
    }

    @Test
    void explainReportsNoEarlyStopWhenFilteringRemainsOrNothingWasPushed() {
        StringBuilder withFilter = new StringBuilder();
        new EsScanPlanProvider(new CountingRestClient(), minimalProps())
                .appendExplainInfo(withFilter, "", pushdownProps("5", "1024", "false"));
        Assertions.assertFalse(withFilter.toString().contains("ES terminate_after"), withFilter.toString());

        StringBuilder noKeys = new StringBuilder();
        new EsScanPlanProvider(new CountingRestClient(), minimalProps())
                .appendExplainInfo(noKeys, "", Collections.emptyMap());
        Assertions.assertFalse(noKeys.toString().contains("ES terminate_after"), noKeys.toString());
    }

    @Test
    void testAppendExplainInfoMissingIndex() {
        EsScanPlanProvider provider = new EsScanPlanProvider(new CountingRestClient(), minimalProps());
        StringBuilder output = new StringBuilder();

        provider.appendExplainInfo(output, "", Collections.emptyMap());

        Assertions.assertFalse(output.toString().contains("ES index:"));
    }

    @Test
    public void beFileCacheAdmissionDoesNotApplyToEs() {
        // ES is read over HTTP, never through BE's file readers, so it must stay out of file-cache admission
        // governance exactly as it was while a catalog-type allow-list decided this.
        Assertions.assertFalse(new EsScanPlanProvider(new CountingRestClient(), minimalProps())
                .supportsFileCache());
    }

    @Test
    void docvalueEligibilityHonorsCatalogFieldLimitAndSourceOnlyColumns() {
        List<ConnectorColumnHandle> one = Collections.singletonList(new NamedColumnHandle("a"));
        List<ConnectorColumnHandle> two = Arrays.asList(new NamedColumnHandle("a"), new NamedColumnHandle("b"));
        List<ConnectorColumnHandle> id = Arrays.asList(new NamedColumnHandle("a"), new NamedColumnHandle("_id"));
        Map<String, String> properties = minimalPropertyMap();
        properties.put(EsCatalogProperties.MAX_DOCVALUE_FIELDS, "1");
        EsScanPlanProvider provider = new EsScanPlanProvider(
                new CountingRestClient(), EsCatalogProperties.of(properties));
        EsTableHandle handle = new EsTableHandle("test_index");
        Assertions.assertEquals("1", provider.getScanNodePropertiesResult(
                EMPTY_SESSION, handle, one, Optional.empty()).getProperties().get("doc_values_mode"));
        Assertions.assertEquals("0", provider.getScanNodePropertiesResult(
                EMPTY_SESSION, handle, two, Optional.empty()).getProperties().get("doc_values_mode"));

        properties.put(EsCatalogProperties.MAX_DOCVALUE_FIELDS, "2");
        provider = new EsScanPlanProvider(new CountingRestClient(), EsCatalogProperties.of(properties));
        Assertions.assertEquals("1", provider.getScanNodePropertiesResult(
                EMPTY_SESSION, handle, two, Optional.empty()).getProperties().get("doc_values_mode"));
        Assertions.assertEquals("0", provider.getScanNodePropertiesResult(
                EMPTY_SESSION, handle, id, Optional.empty()).getProperties().get("doc_values_mode"));

        properties.put(EsCatalogProperties.DOC_VALUE_SCAN, "false");
        provider = new EsScanPlanProvider(new CountingRestClient(), EsCatalogProperties.of(properties));
        Assertions.assertEquals("0", provider.getScanNodePropertiesResult(
                EMPTY_SESSION, handle, one, Optional.empty()).getProperties().get("doc_values_mode"));
    }

    @Test
    void keywordAliasesAndSessionBatchSizeSurviveThriftPopulation() {
        CountingRestClient client = new CountingRestClient() {
            @Override
            public String getMapping(String indexName) {
                return "{\"" + indexName + "\":{\"mappings\":{\"properties\":{"
                        + "\"a\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}},"
                        + "\"b\":{\"type\":\"keyword\",\"doc_values\":false}}}}}";
            }
        };
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        ConnectorSession session = new TestSession(ConnectorStatementScope.NONE,
                Collections.singletonMap("batch_size", "64"));
        EsTableHandle handle = new EsTableHandle("test_index");
        Map<String, String> properties = provider.getScanNodePropertiesResult(session, handle,
                Collections.singletonList(new NamedColumnHandle("a")), Optional.empty()).getProperties();
        Assertions.assertEquals("1", properties.get("doc_values_mode"));
        Assertions.assertEquals("64", properties.get("es.batch_size"));
        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, properties);
        Assertions.assertEquals(Collections.singletonMap("a", "a.keyword"), params.getEsDocvalueContext());
        Assertions.assertEquals(Collections.singletonMap("a", "a.keyword"), params.getEsFieldsContext());
        Assertions.assertEquals("{\"match_all\":{}}", params.getEsProperties().get("query_dsl"));
        Assertions.assertFalse(params.getEsProperties().containsKey("es.batch_size"));

        Map<String, String> sourceOnly = provider.getScanNodePropertiesResult(session, handle,
                Arrays.asList(new NamedColumnHandle("a"), new NamedColumnHandle("b")),
                Optional.empty()).getProperties();
        Assertions.assertEquals("0", sourceOnly.get("doc_values_mode"));
    }

    @Test
    void parallelAndWholeIndexPlansPreserveShardRouting() {
        CountingRestClient client = new CountingRestClient() {
            @Override
            public EsShardPartitions searchShards(String indexName) {
                EsShardPartitions partitions = new EsShardPartitions(indexName);
                for (int shard : new int[]{0, 1}) {
                    partitions.addShardRouting(shard, Collections.singletonList(
                            new EsShardRouting(indexName, shard, true, "node1")));
                }
                return partitions;
            }
        };
        EsScanPlanProvider provider = new EsScanPlanProvider(client, minimalProps());
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                new EsTableHandle("test_index"), Collections.emptyList()).build();
        List<ConnectorScanRange> parallel = provider.planScan(new TestSession(ConnectorStatementScope.NONE,
                Collections.singletonMap("enable_es_parallel_scroll", "true")), request);
        Assertions.assertEquals(2, parallel.size());
        Assertions.assertEquals(java.util.Set.of("0", "1"), parallel.stream()
                .map(range -> range.getProperties().get("shard_id")).collect(java.util.stream.Collectors.toSet()));
        List<ConnectorScanRange> serial = provider.planScan(new TestSession(ConnectorStatementScope.NONE,
                Collections.singletonMap("enable_es_parallel_scroll", "false")), request);
        Assertions.assertEquals(1, serial.size());
        Assertions.assertEquals("-1", serial.get(0).getProperties().get("shard_id"));
        Assertions.assertEquals("localhost:9200", serial.get(0).getProperties().get("es_hosts"));
        Assertions.assertEquals("test_index", serial.get(0).getProperties().get("index"));
    }

    @Test
    void earlyStopRequiresValidPositiveNumbersAndExplicitPushdownTracking() {
        Assertions.assertEquals("64", populateWith(pushdownProps("64", "64", "true"))
                .getEsProperties().get("limit"));
        for (String[] values : new String[][]{
                {"0", "64", "true"}, {"-1", "64", "true"}, {"65", "64", "true"},
                {"1", "0", "true"}, {"1", "-1", "true"}, {"invalid", "64", "true"},
                {"1", "invalid", "true"}, {"1", "64", null}, {"1", "64", "false"}}) {
            Assertions.assertFalse(populateWith(pushdownProps(values[0], values[1], values[2]))
                    .getEsProperties().containsKey("limit"), Arrays.toString(values));
        }
    }

    private static ConnectorColumnRef sourceColumn(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("VARCHAR"));
    }

    private static ConnectorExpression sourceEquals(String name, String value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ,
                sourceColumn(name), ConnectorLiteral.ofString(value));
    }

    private static ScanNodePropertiesResult propertiesForMapping(String mapping, ConnectorExpression filter) {
        CountingRestClient client = new CountingRestClient() {
            @Override
            public String getMapping(String indexName) {
                return mapping;
            }
        };
        return new EsScanPlanProvider(client, minimalProps()).getScanNodePropertiesResult(
                EMPTY_SESSION, new EsTableHandle("test"),
                Arrays.asList(new NamedColumnHandle("a"), new NamedColumnHandle("b")), Optional.of(filter));
    }

    private static String mappingWithField(String field) {
        return "{\"test\":{\"mappings\":{\"properties\":{\"a\":" + field
                + ",\"b\":{\"type\":\"keyword\"}}}}}";
    }

    @Test
    void sourceChangingMappingsRetainEverySqlPredicateAndDisableDocvalues() {
        List<String> unsafeFields = Arrays.asList(
                "{\"type\":\"keyword\",\"ignore_above\":4,\"doc_values\":true}",
                "{\"type\":\"keyword\",\"normalizer\":\"lowercase\"}",
                "{\"type\":\"keyword\",\"null_value\":\"missing\"}",
                "{\"type\":\"keyword\",\"index\":false}",
                "{\"type\":\"long\",\"ignore_malformed\":true}",
                "{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\",\"ignore_above\":4}}}",
                "{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\",\"normalizer\":\"lowercase\"}}}",
                "{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\",\"null_value\":\"missing\"}}}");
        List<ConnectorExpression> predicates = new java.util.ArrayList<>();
        for (ConnectorComparison.Operator op : ConnectorComparison.Operator.values()) {
            predicates.add(new ConnectorComparison(op, sourceColumn("a"), ConnectorLiteral.ofString("elephant")));
        }
        predicates.add(new ConnectorIn(sourceColumn("a"),
                Collections.singletonList(ConnectorLiteral.ofString("elephant")), false));
        predicates.add(new ConnectorIn(sourceColumn("a"),
                Collections.singletonList(ConnectorLiteral.ofString("elephant")), true));
        predicates.add(new ConnectorIsNull(sourceColumn("a"), false));
        predicates.add(new ConnectorIsNull(sourceColumn("a"), true));
        predicates.add(new ConnectorLike(ConnectorLike.Operator.LIKE, sourceColumn("a"),
                ConnectorLiteral.ofString("ele%")));
        predicates.add(new ConnectorLike(ConnectorLike.Operator.REGEXP, sourceColumn("a"),
                ConnectorLiteral.ofString("ele.*")));
        for (String field : unsafeFields) {
            for (ConnectorExpression predicate : predicates) {
                ScanNodePropertiesResult result = propertiesForMapping(mappingWithField(field), predicate);
                Assertions.assertEquals(Collections.singleton(0), result.getNotPushedConjunctIndices(), field);
                Assertions.assertEquals(EsQueryDslBuilder.matchAllQuery(),
                        result.getProperties().get("query_dsl"), field);
                Assertions.assertEquals("0", result.getProperties().get("doc_values_mode"), field);
            }
        }
    }

    @Test
    void unsafeSourcePredicatesPreserveBooleanResidualsAndNativeEsquery() {
        String mapping = mappingWithField("{\"type\":\"keyword\",\"ignore_above\":4}");
        ConnectorExpression unsafe = sourceEquals("a", "elephant");
        ConnectorExpression safe = sourceEquals("b", "ok");
        ScanNodePropertiesResult and = propertiesForMapping(mapping,
                new ConnectorAnd(Arrays.asList(unsafe, safe)));
        Assertions.assertEquals(Collections.singleton(0), and.getNotPushedConjunctIndices());
        Assertions.assertEquals("{\"term\":{\"b\":\"ok\"}}", and.getProperties().get("query_dsl"));
        for (ConnectorExpression expression : Arrays.asList(
                new ConnectorOr(Arrays.asList(unsafe, safe)), new ConnectorNot(unsafe),
                new ConnectorNot(new ConnectorAnd(Arrays.asList(unsafe, safe))),
                new ConnectorOr(Arrays.asList(new ConnectorAnd(Arrays.asList(unsafe, safe)), safe)))) {
            ScanNodePropertiesResult result = propertiesForMapping(mapping, expression);
            Assertions.assertEquals(Collections.singleton(0), result.getNotPushedConjunctIndices());
            Assertions.assertEquals(EsQueryDslBuilder.matchAllQuery(), result.getProperties().get("query_dsl"));
        }
        ConnectorExpression nativeQuery = new ConnectorFunctionCall("esquery", ConnectorType.of("BOOLEAN"),
                Arrays.asList(sourceColumn("a"), ConnectorLiteral.ofString("{\"term\":{\"a\":\"elephant\"}}")));
        Assertions.assertTrue(propertiesForMapping(mapping, nativeQuery).getNotPushedConjunctIndices().isEmpty());
        Assertions.assertTrue(propertiesForMapping(mappingWithField("{\"type\":\"keyword\"}"),
                unsafe).getNotPushedConjunctIndices().isEmpty());
    }

    @Test
    void sourceFilteringCopyTargetsRuntimeAndEveryBackingMappingAreChecked() {
        String safe = mappingWithField("{\"type\":\"keyword\"}");
        List<String> mappings = Arrays.asList(
                safe.replace("\"properties\":", "\"_source\":{\"excludes\":[\"a\"]},\"properties\":"),
                safe.replace("\"properties\":", "\"_source\":{\"includes\":[\"b\"]},\"properties\":"),
                safe.replace("\"properties\":", "\"runtime\":{\"a\":{\"type\":\"keyword\"}},\"properties\":"),
                safe.replace("\"b\":{\"type\":\"keyword\"}",
                        "\"b\":{\"type\":\"keyword\",\"copy_to\":\"a\"}"),
                safe.substring(0, safe.length() - 1) + ",\"second\":{\"mappings\":{\"properties\":"
                        + "{\"a\":{\"type\":\"keyword\",\"ignore_above\":4},\"b\":{\"type\":\"keyword\"}}}}}");
        for (String mapping : mappings) {
            ScanNodePropertiesResult result = propertiesForMapping(mapping, sourceEquals("a", "elephant"));
            Assertions.assertEquals(Collections.singleton(0), result.getNotPushedConjunctIndices(), mapping);
            Assertions.assertEquals(EsQueryDslBuilder.matchAllQuery(), result.getProperties().get("query_dsl"));
            Assertions.assertEquals("0", result.getProperties().get("doc_values_mode"));
        }
    }

    @Test
    void aliasesUseOneOriginalTargetAcrossBackingIndicesAndSharedShardIds() {
        for (int backingCount : new int[]{1, 2}) {
            CountingRestClient client = new CountingRestClient() {
                @Override
                public EsShardPartitions searchShards(String indexName) {
                    EsShardPartitions partitions = new EsShardPartitions(indexName);
                    for (int backing = 0; backing < backingCount; backing++) {
                        for (int shard = 0; shard < 2; shard++) {
                            partitions.addShardRouting(backing * 2 + shard, Collections.singletonList(
                                    new EsShardRouting("backing_" + backing, shard, true, "node1")));
                        }
                    }
                    return partitions;
                }
            };
            ConnectorScanRequest request = ConnectorScanRequest.builder(
                    new EsTableHandle("filtered_alias"), Collections.emptyList()).build();
            for (boolean parallel : new boolean[]{false, true}) {
                List<ConnectorScanRange> ranges = new EsScanPlanProvider(client, minimalProps()).planScan(
                        new TestSession(ConnectorStatementScope.NONE,
                                Collections.singletonMap("enable_es_parallel_scroll", String.valueOf(parallel))),
                        request);
                Assertions.assertEquals(1, ranges.size());
                Assertions.assertEquals("filtered_alias", ranges.get(0).getProperties().get("index"));
                Assertions.assertEquals("-1", ranges.get(0).getProperties().get("shard_id"));
            }
        }
    }

    @Test
    void unprojectedUnsafeFilterColumnIsStillRetained() {
        CountingRestClient client = new CountingRestClient() {
            @Override
            public String getMapping(String indexName) {
                return mappingWithField("{\"type\":\"text\",\"fields\":{\"raw\":"
                        + "{\"type\":\"keyword\",\"ignore_above\":4}}}");
            }
        };
        ScanNodePropertiesResult result = new EsScanPlanProvider(client, minimalProps())
                .getScanNodePropertiesResult(EMPTY_SESSION, new EsTableHandle("test"),
                        Collections.singletonList(new NamedColumnHandle("b")),
                        Optional.of(sourceEquals("a", "elephant")));
        Assertions.assertEquals(Collections.singleton(0), result.getNotPushedConjunctIndices());
        Assertions.assertEquals(EsQueryDslBuilder.matchAllQuery(), result.getProperties().get("query_dsl"));
    }

}
