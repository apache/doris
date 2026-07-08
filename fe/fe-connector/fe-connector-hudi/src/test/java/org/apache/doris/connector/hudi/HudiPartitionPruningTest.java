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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    // ========== helpers ==========

    private Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            HudiTableHandle handle, ConnectorExpression expr) {
        HudiConnectorMetadata metadata = new HudiConnectorMetadata(
                new FakeHmsClient(PARTITIONS), Collections.emptyMap(),
                new DirectHudiMetaClientExecutor());
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
}
