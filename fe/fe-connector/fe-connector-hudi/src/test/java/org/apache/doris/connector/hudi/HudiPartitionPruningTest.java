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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Tests {@link HudiConnectorMetadata#applyFilter} partition pruning (P3-T05).
 *
 * <p>WHY: the SPI Hudi path previously listed ALL partitions unconditionally and
 * stored them as {@code prunedPartitionPaths}, doing no EQ/IN pruning at all and
 * silently forcing the partition source to HMS for any filtered query. These tests
 * pin the corrected behavior, mirroring {@code HiveConnectorMetadata}:
 * <ul>
 *   <li>EQ / IN predicates on partition columns reduce the scanned partition set;</li>
 *   <li>predicates on non-partition columns (or range predicates) never prune;</li>
 *   <li>when no partition predicate applies, the handle is left untouched
 *       ({@code Optional.empty()}) so scan planning falls back to Hudi's own listing;</li>
 *   <li>a predicate that matches every / no partition is handled correctly.</li>
 * </ul>
 * A test that passed against the old stub (which always returned all partitions)
 * would be wrong — each assertion checks the precise pruned set.</p>
 */
public class HudiPartitionPruningTest {

    private static final List<String> PARTITIONS = Arrays.asList(
            "year=2023/month=12",
            "year=2024/month=01",
            "year=2024/month=02");

    private static final List<String> PART_KEYS = Arrays.asList("year", "month");

    @Test
    public void testEqOnPartitionColumnPrunes() {
        // year = '2024' -> only the two 2024 partitions
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), eq("year", "2024"));

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2024/month=01", "year=2024/month=02"),
                prunedPaths(result));
    }

    @Test
    public void testInOnPartitionColumnPrunes() {
        // month IN ('01', '12') -> spans years, keeps original order
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), in("month", "01", "12"));

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2023/month=12", "year=2024/month=01"),
                prunedPaths(result));
    }

    @Test
    public void testAndOfTwoPartitionColumnsPrunes() {
        // year = '2024' AND month = '01' -> a single partition
        ConnectorExpression expr = and(eq("year", "2024"), eq("month", "01"));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), expr);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Collections.singletonList("year=2024/month=01"),
                prunedPaths(result));
    }

    @Test
    public void testNonPartitionColumnInAndIsIgnored() {
        // year = '2024' AND price = '100' -> prune on year only; non-partition pred ignored
        ConnectorExpression expr = and(eq("year", "2024"), eq("price", "100"));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), expr);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2024/month=01", "year=2024/month=02"),
                prunedPaths(result));
    }

    @Test
    public void testNonPartitionPredicateOnlyLeavesHandleUntouched() {
        // price = '100' -> no partition predicate -> Optional.empty() (no source switch)
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), eq("price", "100"));

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testPredicateMatchingAllPartitionsHasNoEffect() {
        // year IN ('2023', '2024') -> matches every partition -> Optional.empty()
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), in("year", "2023", "2024"));

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testPredicateMatchingNoPartitionYieldsEmptyPrunedList() {
        // year = '1999' -> matches nothing -> present handle with empty pruned set (scan 0)
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(partitionedHandle(), eq("year", "1999"));

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(prunedPaths(result).isEmpty());
    }

    @Test
    public void testUnpartitionedTableIsNotTouched() {
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.emptyList())
                .build();
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                applyFilter(handle, eq("year", "2024"));

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testNonHiveStylePositionalPathsPruneToRelativePaths() {
        // H3 core: a non-hive-style Hudi table (hive_style_partitioning=false, the DEFAULT) has a POSITIONAL
        // physical layout ("2024/01"), NOT the HMS hive-style name ("year=2024/month=01"). applyFilter must prune
        // the RELATIVE storage paths (the Hudi metadata listing that the scan also feeds fsView), so the pruned
        // set is the shape fsView is keyed by. RED before the fix: applyFilter fed HMS hive-style names to fsView,
        // which finds nothing on a non-hive-style table -> 0 splits for any filtered query.
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS),                 // HMS hive-style names -- must NOT be used here
                Collections.emptyMap(),                        // use_hive_sync_partition=false -> non-hive-sync
                new StubMetaClientExecutor(Arrays.asList("2024/01", "2024/02", "2023/12")));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, partitionedHandle(), new ConnectorFilterConstraint(eq("year", "2024")));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(Arrays.asList("2024/01", "2024/02"), prunedPaths(result));
    }

    @Test
    public void testHiveSyncBranchPrunesHmsNames() {
        // use_hive_sync_partition=true: partitions are registered in HMS and the hive-style name IS the relative
        // storage layout, so applyFilter prunes the HMS names directly (no Hudi metadata listing / stub unused).
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS),
                Collections.singletonMap("use_hive_sync_partition", "true"),
                new StubMetaClientExecutor(Collections.emptyList()));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, partitionedHandle(), new ConnectorFilterConstraint(eq("year", "2024")));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(
                Arrays.asList("year=2024/month=01", "year=2024/month=02"), prunedPaths(result));
    }

    @Test
    public void prunePartitionPathsMatchesPositionalLayout() {
        // Direct offline unit for the non-hive-sync prune helper: positional relative paths matched by the values
        // parsePartitionValues extracts positionally.
        Assertions.assertEquals(
                Arrays.asList("2024/01", "2024/02"),
                HudiConnectorMetadata.prunePartitionPaths(
                        Arrays.asList("2024/01", "2024/02", "2023/12"),
                        PART_KEYS,
                        Collections.singletonMap("year", Collections.singletonList("2024"))));
    }

    @Test
    public void testDatePartitionPredicatePrunesUnchanged() {
        // H2 non-regression: a DATE predicate literal is a LocalDate (not LocalDateTime), so it is NOT diverted to
        // hiveDateTimeString -- String.valueOf(LocalDate) = "2024-01-01" already matches the stored DATE value.
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS), Collections.emptyMap(),
                new StubMetaClientExecutor(Arrays.asList("2024-01-01", "2024-01-02")));
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.singletonList("dt"))
                .build();
        ConnectorComparison dateEq = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("dt", ConnectorType.of("DATEV2")),
                new ConnectorLiteral(ConnectorType.of("DATEV2"), LocalDate.of(2024, 1, 1)));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, handle, new ConnectorFilterConstraint(dateEq));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(Collections.singletonList("2024-01-01"), prunedPaths(result));
    }

    @Test
    public void testDecimalPartitionPredicatePrunesTrailingZeros() {
        // WHY: a DECIMAL predicate literal arrives as a BigDecimal carrying the column's declared scale
        // (decimal(8,4) -> "1.0000"), while the stored Hudi partition value is Hive-canonical trailing-zero
        // trimmed ("1"). String.valueOf(BigDecimal) keeps the scale, so the prune string-compare misses and
        // every row under d=1 is silently dropped. extractLiteralValue must render "1" via
        // stripTrailingZeros().toPlainString() to string-equal the stored value. Mirrors the sibling
        // HiveConnectorMetadata fix (#65473).
        // MUTATION: dropping the `instanceof BigDecimal` branch (falling through to String.valueOf) ->
        // literal renders "1.0000", ["1.0000"].contains("1") == false -> partition dropped -> prunedPaths
        // empty -> red.
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS), Collections.emptyMap(),
                new StubMetaClientExecutor(Arrays.asList("1", "2")));
        HudiTableHandle handle = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(Collections.singletonList("d"))
                .build();
        ConnectorComparison decimalEq = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("d", ConnectorType.of("DECIMALV3")),
                new ConnectorLiteral(ConnectorType.of("DECIMALV3"), new BigDecimal("1.0000")));
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, handle, new ConnectorFilterConstraint(decimalEq));
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(Collections.singletonList("1"), prunedPaths(result));
    }

    // ========== helpers ==========

    private Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            HudiTableHandle handle, ConnectorExpression expr) {
        // Default (use_hive_sync_partition=false) -> the non-hive-sync branch, whose candidate source is the Hudi
        // metadata listing. Feed the canned partition list via the stub executor (no live metaClient). The
        // hive-style names here parse the same via parsePartitionValues, so the pruning assertions are unchanged.
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS), Collections.emptyMap(),
                new StubMetaClientExecutor(PARTITIONS));
        return metadata.applyFilter(null, handle, new ConnectorFilterConstraint(expr));
    }

    private HudiTableHandle partitionedHandle() {
        return new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(PART_KEYS)
                .build();
    }

    @SuppressWarnings("unchecked")
    private List<String> prunedPaths(Optional<FilterApplicationResult<ConnectorTableHandle>> result) {
        return ((HudiTableHandle) result.get().getHandle()).getPrunedPartitionPaths();
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
     * Minimal {@link HmsClient} double returning a fixed partition-name list.
     * Only {@code listPartitionNames} is exercised by partition pruning; the rest fail loud.
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
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName,
                List<String> partNames) {
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

    /**
     * {@link HudiMetaClientExecutor} test double that returns a canned value WITHOUT running the action, so a test
     * can supply the non-hive-sync {@code applyFilter} branch's {@code listAllPartitionPaths} result offline (no
     * live metaClient / filesystem). Mirrors the stub pattern in HudiConnectorPartitionListingTest.
     */
    private static final class StubMetaClientExecutor implements HudiMetaClientExecutor {
        private final Object canned;

        StubMetaClientExecutor(Object canned) {
            this.canned = canned;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T execute(Callable<T> action) {
            return (T) canned;
        }
    }
}
