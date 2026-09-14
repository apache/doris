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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests {@link HiveConnectorMetadata#listPartitions} / {@link HiveConnectorMetadata#listPartitionNames}
 * (HMS cutover §4.2).
 *
 * <p>WHY these assertions matter:</p>
 * <ul>
 *   <li><b>Names-only, no per-partition round-trip.</b> The single most important invariant: listing
 *       partitions must call {@code get_partition_names} ONLY and never {@code get_partitions_by_names}.
 *       Legacy's hot partition-pruning path ({@code HiveExternalMetaCache.loadPartitionValues}) listed names
 *       only; paying the heavier per-partition fetch here would regress every partitioned-hive query. The
 *       fake fails loud if {@code getPartitions} is touched.</li>
 *   <li><b>{@code lastModifiedMillis} and the stat fields are UNKNOWN(-1).</b> This is the deliberate
 *       §4.2 decision (freshness deferred to the MVCC/MTMV step); a regression that silently started filling
 *       them would re-introduce the round-trip this method exists to avoid.</li>
 *   <li><b>Value maps are keyed by remote partition-column name and unescaped.</b>
 *       {@code PluginDrivenExternalTable.getNameToPartitionItems} reads values back by remote name, and
 *       legacy decoded Hive path-escaping ({@code %2F} -&gt; {@code /}); getting either wrong corrupts the
 *       partition-value view.</li>
 *   <li><b>Unpartitioned tables list nothing without any metastore call</b> (parity guard).</li>
 *   <li><b>A partition predicate is resolved before listing.</b> A filter carrying partition equality / IN
 *       predicates is pushed to the metastore; when the client cannot serve that dialect the connector falls
 *       back to listing names locally and fetching ONLY the surviving partitions by name. A filter with no
 *       usable partition predicate still returns the full set.</li>
 * </ul>
 */
public class HiveConnectorMetadataPartitionListTest {

    private static final List<String> PARTITIONS = Arrays.asList(
            "year=2023/month=12",
            "year=2024/month=01",
            "year=2024/month=02");

    private static final List<String> PART_KEYS = Arrays.asList("year", "month");

    private HiveConnectorMetadata metadata(FakeHmsClient client) {
        return new HiveConnectorMetadata(client, HiveTestProperties.minimal(), new FakeConnectorContext());
    }

    private HiveTableHandle partitionedHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .build();
    }

    @Test
    public void testListPartitionNames() {
        FakeHmsClient client = new FakeHmsClient(PARTITIONS);
        List<String> names = metadata(client).listPartitionNames(null, partitionedHandle());
        Assertions.assertEquals(PARTITIONS, names);
        // The connector must request ALL partitions via the -1 "unbounded" sentinel; requesting a finite
        // cap would silently truncate a >32767-partition table. That -1 maps to an unbounded HMS listing
        // (not Short.MAX_VALUE) is pinned separately by ThriftHmsClientMaxPartsTest.
        Assertions.assertEquals(-1, client.lastMaxParts);
        Assertions.assertFalse(client.getPartitionsCalled,
                "listPartitionNames must not touch get_partitions_by_names");
    }

    @Test
    public void testListPartitionsNamesAndValues() {
        FakeHmsClient client = new FakeHmsClient(PARTITIONS);
        List<ConnectorPartitionInfo> parts =
                metadata(client).listPartitions(null, partitionedHandle(), Optional.empty());

        Assertions.assertEquals(3, parts.size());
        ConnectorPartitionInfo first = parts.get(0);
        Assertions.assertEquals("year=2023/month=12", first.getPartitionName());
        Map<String, String> values = first.getPartitionValues();
        Assertions.assertEquals(2, values.size());
        Assertions.assertEquals("2023", values.get("year"));
        Assertions.assertEquals("12", values.get("month"));
    }

    @Test
    public void testListPartitionsLeavesStatsUnknown() {
        FakeHmsClient client = new FakeHmsClient(PARTITIONS);
        List<ConnectorPartitionInfo> parts =
                metadata(client).listPartitions(null, partitionedHandle(), Optional.empty());
        for (ConnectorPartitionInfo part : parts) {
            Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, part.getLastModifiedMillis(),
                    "lastModifiedMillis must stay UNKNOWN (freshness deferred to the MVCC/MTMV step)");
            Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, part.getRowCount());
            Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, part.getSizeBytes());
            Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, part.getFileCount());
        }
    }

    @Test
    public void testListPartitionsNeverFetchesPerPartitionMetadata() {
        FakeHmsClient client = new FakeHmsClient(PARTITIONS);
        metadata(client).listPartitions(null, partitionedHandle(), Optional.empty());
        Assertions.assertFalse(client.getPartitionsCalled,
                "listPartitions must list names only, not get_partitions_by_names (hot-path parity)");
    }

    @Test
    public void testEscapedPartitionValueIsUnescaped() {
        // A Hive partition value containing '/' is path-escaped as %2F in the partition name; the value
        // map must decode it (byte-parity with legacy HiveUtil.toPartitionValues).
        FakeHmsClient client = new FakeHmsClient(Collections.singletonList("year=2024/month=a%2Fb"));
        List<ConnectorPartitionInfo> parts =
                metadata(client).listPartitions(null, partitionedHandle(), Optional.empty());
        Assertions.assertEquals("a/b", parts.get(0).getPartitionValues().get("month"));
    }

    @Test
    public void testPartitionPredicateResolvesThroughLocalFallback() {
        FakeHmsClient client = new FakeHmsClient(PARTITIONS);
        ConnectorExpression filter = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("year", org.apache.doris.connector.spi.ConnectorType.of("STRING")),
                new ConnectorLiteral(org.apache.doris.connector.spi.ConnectorType.of("STRING"), "2024"));
        List<ConnectorPartitionInfo> parts =
                metadata(client).listPartitions(null, partitionedHandle(), Optional.of(filter));
        // The predicate is now resolved by the connector: this client serves no HMS filter dialect, so the
        // connector falls back to listing names locally and fetching only the surviving partitions by name.
        Assertions.assertEquals(2, parts.size());
        Assertions.assertEquals("year=2024/month=01", parts.get(0).getPartitionName());
        Assertions.assertTrue(client.getPartitionsCalled,
                "the local fallback fetches only the surviving partitions by name");
    }

    @Test
    public void testUnpartitionedTableListsNothingWithoutMetastoreCall() {
        FakeHmsClient client = new FakeHmsClient(PARTITIONS);
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(Collections.emptyList())
                .build();
        HiveConnectorMetadata metadata = metadata(client);
        Assertions.assertTrue(metadata.listPartitionNames(null, handle).isEmpty());
        Assertions.assertTrue(metadata.listPartitions(null, handle, Optional.empty()).isEmpty());
        Assertions.assertFalse(client.listPartitionNamesCalled,
                "an unpartitioned table must not call the metastore");
    }

    @Test
    public void listPartitionsMarksHiveDefaultSentinelNull() {
        // A genuine-NULL partition on the `year` column: HMS renders it as year=__HIVE_DEFAULT_PARTITION__.
        // The connector must supply isNull=true for that value (byte-parity with legacy
        // HiveExternalMetaCache:309) and false for the ordinary `month` value, positionally aligned to the
        // name parse fe-core re-runs -> fe-core builds a typed NullLiteral for `year` (INT/DATE-safe).
        FakeHmsClient client = new FakeHmsClient(Collections.singletonList(
                "year=__HIVE_DEFAULT_PARTITION__/month=01"));
        List<ConnectorPartitionInfo> parts =
                metadata(client).listPartitions(null, partitionedHandle(), Optional.empty());
        // MUTATION: dropping the flag (empty list) or marking the wrong position -> red.
        Assertions.assertEquals(Arrays.asList(true, false),
                parts.get(0).getPartitionValueNullFlags(),
                "year (sentinel) -> null flag true; month -> false");
        // The raw value string is still carried (the flag, not the string, drives nullness downstream).
        Assertions.assertEquals("__HIVE_DEFAULT_PARTITION__", parts.get(0).getPartitionValues().get("year"));
        // Stats stay UNKNOWN — the opt-in flag must not perturb the names-only contract.
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, parts.get(0).getLastModifiedMillis());
    }

    @Test
    public void listPartitionsMarksNoNullForOrdinaryValues() {
        // Regression floor: ordinary partitions get all-false flags (no value is the sentinel), so fe-core
        // builds plain typed literals exactly as before this fix.
        FakeHmsClient client = new FakeHmsClient(PARTITIONS);
        List<ConnectorPartitionInfo> parts =
                metadata(client).listPartitions(null, partitionedHandle(), Optional.empty());
        for (ConnectorPartitionInfo part : parts) {
            Assertions.assertEquals(Arrays.asList(false, false), part.getPartitionValueNullFlags());
        }
    }

    /**
     * Minimal {@link HmsClient} double: {@code listPartitionNames} returns a fixed list and records the
     * requested {@code maxParts}; {@code getPartitions} echoes the requested names back and records the call,
     * so an unfiltered listing can assert it never paid that per-partition round-trip while a predicate
     * fallback can assert it did. The rest are unsupported.
     */
    private static final class FakeHmsClient implements HmsClient {
        private final List<String> partitionNames;
        private boolean listPartitionNamesCalled;
        private boolean getPartitionsCalled;
        private int lastMaxParts;

        FakeHmsClient(List<String> partitionNames) {
            this.partitionNames = partitionNames;
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            listPartitionNamesCalled = true;
            lastMaxParts = maxParts;
            return partitionNames;
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            getPartitionsCalled = true;
            List<HmsPartitionInfo> result = new ArrayList<>();
            for (String name : partNames) {
                result.add(new HmsPartitionInfo(HiveWriteUtils.toPartitionValues(name), name,
                        null, null, null, Collections.emptyMap()));
            }
            return result;
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
