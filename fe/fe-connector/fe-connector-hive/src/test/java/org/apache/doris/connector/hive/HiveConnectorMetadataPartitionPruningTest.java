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

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests {@link HiveConnectorMetadata#applyFilter} partition pruning (P3-T07 batch C).
 *
 * <p>WHY: this is the direct analog of fe-connector-hudi's HudiPartitionPruningTest —
 * both exercise the same EQ/IN partition-pruning helpers (the Hudi T05 fix was mirrored
 * from this Hive code). The tests are intentionally near-identical; they differ only in
 * the handle type and that Hive resolves matched partition NAMES to
 * {@link HmsPartitionInfo} via {@code getPartitions} (capped at 100000), whereas Hudi
 * keeps the matched relative paths. Consolidating the two is deferred to the P7 Hive
 * migration. These assertions pin: EQ / IN on partition columns prune; predicates on
 * non-partition columns never prune; a no-effect predicate leaves the handle untouched
 * ({@code Optional.empty()}); a zero-match predicate yields an empty pruned set.</p>
 */
public class HiveConnectorMetadataPartitionPruningTest {

    private static final List<String> PARTITIONS = Arrays.asList(
            "year=2023/month=12",
            "year=2024/month=01",
            "year=2024/month=02");

    private static final List<String> PART_KEYS = Arrays.asList("year", "month");

    @Test
    public void testEqOnPartitionColumnPrunes() {
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), eq("year", "2024"));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2024/month=01", "year=2024/month=02"),
                prunedLocations(result));
    }

    @Test
    public void testInOnPartitionColumnPrunes() {
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), in("month", "01", "12"));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2023/month=12", "year=2024/month=01"),
                prunedLocations(result));
    }

    @Test
    public void testAndOfTwoPartitionColumnsPrunes() {
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), and(eq("year", "2024"), eq("month", "01")));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Collections.singletonList("year=2024/month=01"),
                prunedLocations(result));
    }

    @Test
    public void testNonPartitionColumnInAndIsIgnored() {
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), and(eq("year", "2024"), eq("price", "100")));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2024/month=01", "year=2024/month=02"),
                prunedLocations(result));
    }

    @Test
    public void testNonPartitionPredicateOnlyLeavesHandleUntouched() {
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), eq("price", "100"));
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testPredicateMatchingAllPartitionsHasNoEffect() {
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), in("year", "2023", "2024"));
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testPredicateMatchingNoPartitionYieldsEmptyPrunedList() {
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), eq("year", "1999"));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(prunedLocations(result).isEmpty());
    }

    @Test
    public void testUnpartitionedTableIsNotTouched() {
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(Collections.emptyList())
                .build();
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(handle, eq("year", "2024"));
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void parsePartitionNameUnescapesValues() {
        // H1 (unit, durable): the pruning-decision parse must unescape values the same way the sibling
        // HiveWriteUtils.toPartitionValues does, or an escaped partition value never string-equals the
        // unescaped predicate literal. RED before the fix: "US%3ACA".
        Map<String, String> values = HiveConnectorMetadata.parsePartitionName(
                "code=US%3ACA", Collections.singletonList("code"));
        Assertions.assertEquals("US:CA", values.get("code"), "colon-escaped value must be decoded");
    }

    @Test
    public void testEscapedPartitionValuePrunesInsteadOfDropping() {
        // H1 (end-to-end via applyFilter): a partition value with a Hive-escaped char (":" stored as "%3A")
        // must still match its unescaped predicate literal. RED before the fix: both escaped names fail the
        // string compare -> the pruned set is EMPTY, dropping the real partition (silent row loss).
        List<String> escaped = Arrays.asList("code=US%3ACA", "code=EU%3ADE");
        HiveConnectorMetadata metadata = new HiveConnectorMetadata(
                new FakeHmsClient(escaped), Collections.emptyMap(), new FakeConnectorContext());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(Collections.singletonList("code"))
                .build();
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, handle, new ConnectorFilterConstraint(eq("code", "US:CA")));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(Collections.singletonList("code=US%3ACA"), prunedLocations(result));
    }

    @Test
    public void hiveDateTimeStringRendersHiveCanonicalText() {
        // H2 (unit): a DATETIME/TIMESTAMP predicate literal (LocalDateTime) must render Hive-canonical text.
        // String.valueOf would yield ISO "2024-01-01T10:00", never matching "2024-01-01 10:00:00". RED before.
        Assertions.assertEquals("2024-01-01 10:00:00",
                HiveConnectorMetadata.hiveDateTimeString(LocalDateTime.of(2024, 1, 1, 10, 0, 0)));
        Assertions.assertEquals("2024-01-01 00:00:00",
                HiveConnectorMetadata.hiveDateTimeString(LocalDateTime.of(2024, 1, 1, 0, 0, 0)));
        Assertions.assertEquals("2024-01-01 10:00:30",
                HiveConnectorMetadata.hiveDateTimeString(LocalDateTime.of(2024, 1, 1, 10, 0, 30)));
        Assertions.assertEquals("2024-01-01 10:00:00.123456",
                HiveConnectorMetadata.hiveDateTimeString(LocalDateTime.of(2024, 1, 1, 10, 0, 0, 123456 * 1000)));
    }

    @Test
    public void testDatetimePartitionPredicatePrunesWithHiveCanonicalText() {
        // H1+H2 composed end-to-end: a DATETIME partition value stored escaped in HMS ("dt=2024-01-01 10%3A00%3A00")
        // must prune-in against a DATETIME predicate literal. RED before H2: the literal renders ISO
        // "2024-01-01T10:00" and matches nothing; RED before H1: the stored ":" stays "%3A". Both are required for
        // the real partition to survive pruning (else silent row loss).
        List<String> parts = Arrays.asList("dt=2024-01-01 10%3A00%3A00", "dt=2024-01-02 00%3A00%3A00");
        HiveConnectorMetadata metadata = new HiveConnectorMetadata(
                new FakeHmsClient(parts), Collections.emptyMap(), new FakeConnectorContext());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(Collections.singletonList("dt"))
                .build();
        ConnectorComparison dtEq = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("dt", ConnectorType.of("DATETIMEV2", 6, 0)),
                new ConnectorLiteral(ConnectorType.of("DATETIMEV2", 6, 0), LocalDateTime.of(2024, 1, 1, 10, 0, 0)));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, handle, new ConnectorFilterConstraint(dtEq));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Collections.singletonList("dt=2024-01-01 10%3A00%3A00"), prunedLocations(result));
    }

    // ===== helpers =====

    private Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            HiveTableHandle handle, ConnectorExpression expr) {
        HiveConnectorMetadata metadata = new HiveConnectorMetadata(
                new FakeHmsClient(PARTITIONS), Collections.emptyMap(), new FakeConnectorContext());
        return metadata.applyFilter(null, handle, new ConnectorFilterConstraint(expr));
    }

    private HiveTableHandle partitionedHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .build();
    }

    private List<String> prunedLocations(Optional<FilterApplicationResult<ConnectorTableHandle>> result) {
        List<HmsPartitionInfo> pruned =
                ((HiveTableHandle) result.get().getHandle()).getPrunedPartitions();
        List<String> locations = new ArrayList<>();
        for (HmsPartitionInfo p : pruned) {
            locations.add(p.getLocation());
        }
        return locations;
    }

    private static ConnectorColumnRef colRef(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("STRING"));
    }

    private static ConnectorLiteral lit(String value) {
        return new ConnectorLiteral(ConnectorType.of("STRING"), value);
    }

    private static ConnectorComparison eq(String col, String value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ, colRef(col), lit(value));
    }

    private static ConnectorIn in(String col, String... values) {
        List<ConnectorExpression> inList = new ArrayList<>();
        for (String v : values) {
            inList.add(lit(v));
        }
        return new ConnectorIn(colRef(col), inList, false);
    }

    private static ConnectorAnd and(ConnectorExpression... children) {
        return new ConnectorAnd(Arrays.asList(children));
    }

    /**
     * Minimal {@link HmsClient} double. {@code listPartitionNames} returns a fixed list;
     * {@code getPartitions} echoes each requested name back as an {@link HmsPartitionInfo}
     * whose location IS the partition name (so the pruning selection can be asserted).
     * The rest fail loud.
     */
    private static final class FakeHmsClient implements HmsClient {
        private final List<String> partitionNames;

        FakeHmsClient(List<String> partitionNames) {
            this.partitionNames = partitionNames;
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            return partitionNames;
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName,
                List<String> partNames) {
            List<HmsPartitionInfo> result = new ArrayList<>();
            for (String name : partNames) {
                result.add(new HmsPartitionInfo(Collections.emptyList(), name,
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
