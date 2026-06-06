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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * COW vs MOR table-type classification on the SPI Hudi metadata path (P3-T07, batch C).
 *
 * <p>WHY: schema derivation is table-type-agnostic, so the ONLY place the metadata SPI
 * distinguishes Copy-On-Write from Merge-On-Read is {@code detectHudiTableType}, surfaced
 * through {@code getTableHandle}. Misclassifying the type routes scan planning to the wrong
 * split/reader strategy. These tests pin the detection from the HMS input format and the
 * Spark provider table parameter — the "COW & MOR each one" parity requirement — plus the
 * UNKNOWN fallback when no Hudi signal is present.</p>
 */
public class HudiTableTypeTest {

    private String detect(String inputFormat, Map<String, String> parameters) {
        HmsTableInfo info = HmsTableInfo.builder()
                .dbName("db").tableName("t")
                .location("s3://b/t")
                .inputFormat(inputFormat)
                .parameters(parameters)
                .build();
        HudiConnectorMetadata metadata =
                new HudiConnectorMetadata(new FakeHmsClient(info), Collections.emptyMap());
        Optional<ConnectorTableHandle> handle = metadata.getTableHandle(null, "db", "t");
        Assertions.assertTrue(handle.isPresent());
        return ((HudiTableHandle) handle.get()).getHudiTableType();
    }

    @Test
    public void testCowDetectedFromInputFormat() {
        Assertions.assertEquals("COPY_ON_WRITE",
                detect("org.apache.hudi.hadoop.HoodieParquetInputFormat", Collections.emptyMap()));
    }

    @Test
    public void testCowDetectedFromSparkProviderParam() {
        // A Spark-registered Hudi table may carry no Hudi input format; the provider
        // parameter still identifies it as COW.
        Assertions.assertEquals("COPY_ON_WRITE",
                detect(null, Collections.singletonMap("spark.sql.sources.provider", "hudi")));
    }

    @Test
    public void testMorDetectedFromRealtimeInputFormat() {
        Assertions.assertEquals("MERGE_ON_READ",
                detect("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat",
                        Collections.emptyMap()));
    }

    @Test
    public void testUnknownWhenNoHudiSignal() {
        Assertions.assertEquals("UNKNOWN",
                detect("org.apache.hadoop.mapred.TextInputFormat", Collections.emptyMap()));
    }

    /**
     * Minimal {@link HmsClient} double returning a fixed table. Only {@code tableExists}
     * and {@code getTable} are exercised by {@code getTableHandle}; the rest fail loud.
     */
    private static final class FakeHmsClient implements HmsClient {
        private final HmsTableInfo tableInfo;

        FakeHmsClient(HmsTableInfo tableInfo) {
            this.tableInfo = tableInfo;
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return true;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            return tableInfo;
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
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
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
