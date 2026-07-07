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

import org.apache.doris.connector.api.ConnectorColumnStatistics;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsColumnStatistics;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests {@link HiveConnectorMetadata#getColumnStatistics}, the query-planner column-stat fast path ported from
 * legacy {@code HMSExternalTable.getHiveColumnStats} (HMS cutover §4.2a, dormant).
 *
 * <p>WHY: the connector must serve the no-scan HMS column stats as RAW facts (rowCount / ndv / numNulls /
 * avgColLen) and gate exactly as legacy did — a positive {@code numRows} is required as the data-size basis
 * (and, unlike the table-size branch, there is NO spark-count fallback here), only a plain-hive table is
 * served (iceberg-on-HMS goes to the sibling; hudi had no fast path), and a missing basis/stat degrades to
 * empty so fe-core falls back to a full ANALYZE — WITHOUT paying the HMS column-stat round-trip.</p>
 */
public class HiveConnectorMetadataColumnStatsTest {

    private HiveConnectorMetadata metadata(FakeHmsClient client) {
        return new HiveConnectorMetadata(client, Collections.emptyMap(), new FakeConnectorContext());
    }

    private HiveTableHandle hiveHandle(Map<String, String> params) {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .tableParameters(params)
                .build();
    }

    private static Map<String, String> numRows(String value) {
        Map<String, String> m = new HashMap<>();
        m.put("numRows", value);
        return m;
    }

    @Test
    public void serviceHiveColumnStats() {
        FakeHmsClient client = new FakeHmsClient(
                Collections.singletonList(new HmsColumnStatistics("c", 10, 2, 5.0)));
        Optional<ConnectorColumnStatistics> stats =
                metadata(client).getColumnStatistics(null, hiveHandle(numRows("1000")), "c");
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(1000, stats.get().getRowCount());
        Assertions.assertEquals(10, stats.get().getNdv());
        Assertions.assertEquals(2, stats.get().getNumNulls());
        Assertions.assertEquals(5.0, stats.get().getAvgSizeBytes(), 0.0);
    }

    @Test
    public void missingNumRowsReturnsEmptyWithoutHmsCall() {
        FakeHmsClient client = new FakeHmsClient(
                Collections.singletonList(new HmsColumnStatistics("c", 10, 2, 5.0)));
        Assertions.assertFalse(
                metadata(client).getColumnStatistics(null, hiveHandle(Collections.emptyMap()), "c").isPresent());
        Assertions.assertFalse(client.columnStatsCalled,
                "no numRows basis => must not pay the HMS column-stat round-trip");
    }

    @Test
    public void zeroOrMalformedNumRowsReturnsEmpty() {
        FakeHmsClient client = new FakeHmsClient(
                Collections.singletonList(new HmsColumnStatistics("c", 10, 2, 5.0)));
        Assertions.assertFalse(
                metadata(client).getColumnStatistics(null, hiveHandle(numRows("0")), "c").isPresent());
        Assertions.assertFalse(
                metadata(client).getColumnStatistics(null, hiveHandle(numRows("abc")), "c").isPresent());
    }

    @Test
    public void noHmsStatsReturnsEmpty() {
        FakeHmsClient client = new FakeHmsClient(Collections.emptyList());
        Assertions.assertFalse(
                metadata(client).getColumnStatistics(null, hiveHandle(numRows("1000")), "c").isPresent());
    }

    @Test
    public void nonHiveTableReturnsEmptyWithoutHmsCall() {
        FakeHmsClient client = new FakeHmsClient(
                Collections.singletonList(new HmsColumnStatistics("c", 10, 2, 5.0)));
        HiveTableHandle hudiHandle = new HiveTableHandle.Builder("db", "t", HiveTableType.HUDI)
                .tableParameters(numRows("1000"))
                .build();
        Assertions.assertFalse(
                metadata(client).getColumnStatistics(null, hudiHandle, "c").isPresent());
        Assertions.assertFalse(client.columnStatsCalled,
                "iceberg/hudi-on-HMS are served by their own connector; the hive fast path must not run");
    }

    /** Minimal {@link HmsClient} double serving preset column stats and recording the fetch. */
    private static final class FakeHmsClient implements HmsClient {
        private final List<HmsColumnStatistics> columnStats;
        private boolean columnStatsCalled;

        FakeHmsClient(List<HmsColumnStatistics> columnStats) {
            this.columnStats = columnStats;
        }

        @Override
        public List<HmsColumnStatistics> getTableColumnStatistics(String dbName, String tableName,
                List<String> columns) {
            columnStatsCalled = true;
            return columnStats;
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
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
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
