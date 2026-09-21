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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/** Statement-scoped scan reuse key construction for Hudi (offline; no table environment needed). */
class HudiScanReuseKeyTest {

    @Test
    void scanReuseNamespaceUsesConnectorType() {
        String prefix = new HudiConnectorProvider().getType() + ".";
        Assertions.assertTrue(HudiScanPlanProvider.SCAN_REUSE_NAMESPACE.startsWith(prefix));
    }

    private static HudiTableHandle handle() {
        return new HudiTableHandle.Builder("db", "t", "/warehouse/t", "COPY_ON_WRITE")
                .inputFormat("org.apache.hudi.hadoop.HoodieParquetInputFormat")
                .partitionKeyNames(Arrays.asList("year", "month"))
                .prunedPartitionPaths(Arrays.asList("year=2025/month=01", "year=2025/month=02"))
                .queryInstant("20250429000000000")
                .build();
    }

    private static HudiScanPlanProvider.HudiScanReuseKey key(HudiTableHandle handle) {
        return HudiScanPlanProvider.hudiScanReuseKey(handle, "test-generation");
    }

    @Test
    void sameScanYieldsSameKey() {
        Assertions.assertEquals(key(handle()), key(handle()),
                "two identical handles must produce the same reuse key");
    }

    @Test
    void differentQueryInstantYieldsDifferentKey() {
        HudiTableHandle other = handle().toBuilder()
                .queryInstant("20250430000000000")
                .build();
        Assertions.assertNotEquals(key(handle()), key(other),
                "a different snapshot instant must not reuse the cached ranges");
    }

    @Test
    void incrementalWindowYieldsDifferentKey() {
        HudiTableHandle incremental = handle().toBuilder()
                .beginInstant("20250420000000000")
                .endInstant("20250430000000000")
                .build();
        Assertions.assertNotEquals(key(handle()), key(incremental),
                "an incremental window must not reuse a snapshot scan's ranges");

        HudiTableHandle otherWindow = incremental.toBuilder()
                .endInstant("20250425000000000")
                .build();
        Assertions.assertNotEquals(key(incremental), key(otherWindow),
                "a different incremental window must not reuse the cached ranges");
    }

    @Test
    void incrementalParamsYieldsDifferentKey() {
        HudiTableHandle incrementalBase = handle().toBuilder()
                .beginInstant("20250420000000000")
                .endInstant("20250430000000000")
                .build();
        Map<String, String> params = new HashMap<>();
        params.put("hoodie.datasource.read.incr.path.glob", "/warehouse/t/*/*");
        HudiTableHandle withParams = incrementalBase.toBuilder()
                .incrementalParams(params)
                .build();
        Assertions.assertNotEquals(key(incrementalBase), key(withParams),
                "incremental options must participate in the reuse key");
    }

    @Test
    void differentPrunedPartitionsYieldDifferentKey() {
        HudiTableHandle other = handle().toBuilder()
                .prunedPartitionPaths(Collections.singletonList("year=2025/month=01"))
                .build();
        Assertions.assertNotEquals(key(handle()), key(other),
                "a different pruned partition set must not reuse the cached ranges");
    }

    @Test
    void unprunedAndZeroPrunedStatesYieldDifferentKeys() {
        HudiTableHandle unpruned = handle().toBuilder()
                .prunedPartitionPaths(null)
                .build();
        HudiTableHandle zeroPruned = handle().toBuilder()
                .prunedPartitionPaths(Collections.emptyList())
                .build();

        Assertions.assertNotEquals(key(unpruned), key(zeroPruned),
                "null means enumerate all snapshot partitions while empty means the filter matched none");
    }

    @Test
    void statementReuseKeepsSameInstantZeroPrunedAndUnprunedScansDistinct() {
        RecordingScanProvider provider = new RecordingScanProvider();
        HudiTableHandle unpruned = handle().toBuilder()
                .prunedPartitionPaths(null)
                .build();
        HudiTableHandle zeroPruned = handle().toBuilder()
                .prunedPartitionPaths(Collections.emptyList())
                .build();
        ConnectorSession session = new MemoSession(new MemoScope());

        List<ConnectorScanRange> zeroRanges = provider.planScan(session,
                ConnectorScanRequest.builder(
                        zeroPruned, Collections.<ConnectorColumnHandle>emptyList()).build());
        List<ConnectorScanRange> allRanges = provider.planScan(session,
                ConnectorScanRequest.builder(
                        unpruned, Collections.<ConnectorColumnHandle>emptyList()).build());

        Assertions.assertTrue(zeroRanges.isEmpty());
        Assertions.assertEquals(1, allRanges.size(),
                "an unpruned time-travel alias must not reuse a zero-pruned alias's empty ranges");
        Assertions.assertEquals(2, provider.planCalls,
                "the two semantic partition states must occupy separate statement-cache entries");
    }

    @Test
    void statementReusePlansAnIdenticalScanOnce() {
        RecordingScanProvider provider = new RecordingScanProvider();
        ConnectorSession session = new MemoSession(new MemoScope());
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle().toBuilder().prunedPartitionPaths(null).build(),
                Collections.<ConnectorColumnHandle>emptyList()).build();

        List<ConnectorScanRange> first = provider.planScan(session, request);
        List<ConnectorScanRange> second = provider.planScan(session, request);

        Assertions.assertSame(first, second, "an identical scan must reuse the statement's planned range list");
        Assertions.assertEquals(1, provider.planCalls, "the underlying Hudi planner must run once");
    }

    @Test
    void statementReuseRetriesASchemaResolutionThatFailedOnce() {
        RecordingScanProvider provider = new RecordingScanProvider();
        provider.failNextSchemaIdResolution = true;
        ConnectorSession session = new MemoSession(new MemoScope());
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle().toBuilder().prunedPartitionPaths(null).build(),
                Collections.<ConnectorColumnHandle>emptyList()).build();

        List<ConnectorScanRange> degraded = provider.planScan(session, request);
        List<ConnectorScanRange> recovered = provider.planScan(session, request);
        List<ConnectorScanRange> reused = provider.planScan(session, request);

        Assertions.assertNotSame(degraded, recovered,
                "a BY_NAME fallback caused by transient schema resolution must not enter statement reuse");
        Assertions.assertSame(recovered, reused,
                "the successful schema-resolution retry should enter statement reuse");
        Assertions.assertEquals(2, provider.planCalls,
                "the second alias must retry once, while the third alias reuses the recovered plan");
    }

    @Test
    void statementReuseCanBeDisabled() {
        RecordingScanProvider provider = new RecordingScanProvider();
        ConnectorSession session = new MemoSession(new MemoScope(),
                Collections.singletonMap("enable_external_scan_task_reuse", "false"));
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle().toBuilder().prunedPartitionPaths(null).build(),
                Collections.<ConnectorColumnHandle>emptyList()).build();

        provider.planScan(session, request);
        provider.planScan(session, request);

        Assertions.assertEquals(2, provider.planCalls, "disabled split reuse must bypass the statement memo");
    }

    @Test
    void missingReusePropertyIsDisabledForMixedVersionSessions() {
        RecordingScanProvider provider = new RecordingScanProvider();
        ConnectorSession session = new MemoSession(new MemoScope(), Collections.emptyMap());
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle().toBuilder().prunedPartitionPaths(null).build(),
                Collections.<ConnectorColumnHandle>emptyList()).build();

        provider.planScan(session, request);
        provider.planScan(session, request);

        Assertions.assertEquals(2, provider.planCalls,
                "a session from an older planning FE must not implicitly enable split reuse");
    }

    @Test
    void planningAndPropertiesUseTheSamePhysicalGeneration() {
        RecordingScanProvider provider = new RecordingScanProvider();
        ConnectorSession session = new MemoSession(new MemoScope());
        HudiTableHandle handle = handle().toBuilder().prunedPartitionPaths(null).build();

        provider.planScan(session, ConnectorScanRequest.builder(
                handle, Collections.<ConnectorColumnHandle>emptyList()).build());
        provider.getScanNodeProperties(
                session, handle, Collections.<ConnectorColumnHandle>emptyList(), Optional.empty());

        Assertions.assertSame(provider.statementTable, provider.plannedTable,
                "split planning must use the meta client whose generation fenced the reuse key");
        Assertions.assertSame(provider.statementTable, provider.propertiesTable,
                "scan properties must use the same meta client as split planning");
    }

    @Test
    void tableResolutionKeyIncludesThePhysicalBasePath() {
        HudiTableHandle recreated = new HudiTableHandle.Builder(
                "db", "t", "/warehouse/recreated-t", "COPY_ON_WRITE")
                .inputFormat("org.apache.hudi.hadoop.HoodieParquetInputFormat")
                .build();

        Assertions.assertNotEquals(
                new HudiScanPlanProvider.HudiTableResolutionKey(handle()),
                new HudiScanPlanProvider.HudiTableResolutionKey(recreated),
                "same logical name at a different path must resolve a different meta client");
    }

    private static final class RecordingScanProvider extends HudiScanPlanProvider {
        private static final ConnectorScanRange RANGE = Collections::emptyMap;
        private final HudiStatementTable statementTable =
                new HudiStatementTable(null, "generation-a", new Configuration(false));
        private int planCalls;
        private boolean failNextSchemaIdResolution;
        private HudiStatementTable plannedTable;
        private HudiStatementTable propertiesTable;

        private RecordingScanProvider() {
            super(Collections.emptyMap(), null);
        }

        @Override
        HudiStatementTable resolveHudiTable(ConnectorSession session, HudiTableHandle handle) {
            return statementTable;
        }

        @Override
        List<ConnectorScanRange> doPlanScan(ConnectorSession session, ConnectorScanRequest request) {
            planCalls++;
            HudiTableHandle scanHandle = (HudiTableHandle) request.getTableHandle();
            return scanHandle.getPrunedPartitionPaths() == null
                    ? Collections.singletonList(RANGE)
                    : Collections.emptyList();
        }

        @Override
        List<ConnectorScanRange> doPlanScan(ConnectorSession session, ConnectorScanRequest request,
                HudiStatementTable statementTable) {
            plannedTable = statementTable;
            return doPlanScan(session, request);
        }

        @Override
        PlannedScan doPlanScanForReuse(ConnectorSession session, ConnectorScanRequest request,
                HudiStatementTable statementTable) {
            PlanCompleteness completeness = new PlanCompleteness();
            HudiSchemaUtils.ResolvedInternalSchema baseSchema = new HudiSchemaUtils.ResolvedInternalSchema(
                    new InternalSchema(1L, Types.RecordType.get(Collections.emptyList())), true);
            Function<String, Long> resolver = buildSchemaIdResolver(baseSchema, null, completeness);
            resolver.apply("/warehouse/t/file.parquet");
            List<ConnectorScanRange> ranges = doPlanScan(session, request, statementTable);
            return new PlannedScan(ranges, completeness.isComplete());
        }

        @Override
        long resolveFileSchemaId(String filePath, HudiSchemaUtils.ResolvedInternalSchema baseSchema,
                HoodieTableMetaClient metaClient) {
            if (failNextSchemaIdResolution) {
                failNextSchemaIdResolution = false;
                throw new IllegalStateException("transient schema lookup failure");
            }
            return 1L;
        }

        @Override
        Optional<String> schemaEvolutionDict(HudiStatementTable statementTable, HudiTableHandle handle,
                List<ConnectorColumnHandle> columns) {
            propertiesTable = statementTable;
            return Optional.empty();
        }
    }

    private static final class MemoScope implements ConnectorStatementScope {
        private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();

        @Override
        @SuppressWarnings("unchecked")
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            return (T) cache.computeIfAbsent(key, ignored -> loader.get());
        }
    }

    private static final class MemoSession implements ConnectorSession {
        private final ConnectorStatementScope scope;
        private final Map<String, String> sessionProperties;

        private MemoSession(ConnectorStatementScope scope) {
            this(scope, Collections.singletonMap("enable_external_scan_task_reuse", "true"));
        }

        private MemoSession(ConnectorStatementScope scope, Map<String, String> sessionProperties) {
            this.scope = scope;
            this.sessionProperties = sessionProperties;
        }

        @Override
        public long getCatalogId() {
            return 7L;
        }

        @Override
        public String getQueryId() {
            return "same-statement";
        }

        @Override
        public ConnectorStatementScope getStatementScope() {
            return scope;
        }

        @Override
        public String getUser() {
            return "u";
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
        public String getCatalogName() {
            return "c";
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
    }
}
