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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Statement-scoped scan reuse key construction for Hudi (offline; no table environment needed). */
class HudiScanReuseKeyTest {

    private static HudiTableHandle handle() {
        return new HudiTableHandle.Builder("db", "t", "/warehouse/t", "COPY_ON_WRITE")
                .inputFormat("org.apache.hudi.hadoop.HoodieParquetInputFormat")
                .partitionKeyNames(Arrays.asList("year", "month"))
                .prunedPartitionPaths(Arrays.asList("year=2025/month=01", "year=2025/month=02"))
                .queryInstant("20250429000000000")
                .build();
    }

    private static HudiScanPlanProvider.HudiScanReuseKey key(HudiTableHandle handle) {
        return HudiScanPlanProvider.hudiScanReuseKey(handle);
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
    void statementReuseCanBeDisabled() {
        RecordingScanProvider provider = new RecordingScanProvider();
        ConnectorSession session = new MemoSession(new MemoScope(),
                Collections.singletonMap(ConnectorSession.ENABLE_EXTERNAL_SCAN_TASK_REUSE, "false"));
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle().toBuilder().prunedPartitionPaths(null).build(),
                Collections.<ConnectorColumnHandle>emptyList()).build();

        provider.planScan(session, request);
        provider.planScan(session, request);

        Assertions.assertEquals(2, provider.planCalls, "disabled split reuse must bypass the statement memo");
    }

    private static final class RecordingScanProvider extends HudiScanPlanProvider {
        private static final ConnectorScanRange RANGE = Collections::emptyMap;
        private int planCalls;

        private RecordingScanProvider() {
            super(Collections.emptyMap(), null);
        }

        @Override
        List<ConnectorScanRange> doPlanScan(ConnectorSession session, ConnectorScanRequest request) {
            planCalls++;
            HudiTableHandle scanHandle = (HudiTableHandle) request.getTableHandle();
            return scanHandle.getPrunedPartitionPaths() == null
                    ? Collections.singletonList(RANGE)
                    : Collections.emptyList();
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
            this(scope, Collections.emptyMap());
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
