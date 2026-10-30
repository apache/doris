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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class DeltaLakeHmsTableResolverTest {
    @Test
    void onlyDeltaTablesAreVisibleAndSchemaIsNotReadFromHms() {
        RecordingClient client = new RecordingClient();
        client.tables.put("delta", table("delta", "DeLtA", "s3://bucket/table"));
        client.tables.put("hive", table("hive", "parquet", "s3://bucket/hive"));
        DeltaLakeHmsTableResolver resolver = new DeltaLakeHmsTableResolver(client);
        Assertions.assertEquals(Collections.singletonList("db"), resolver.listDatabases());
        Assertions.assertEquals(Collections.singletonList("delta"), resolver.listTables("db"));
        Assertions.assertEquals("s3://bucket/table", resolver.tableLocation("db", "delta").orElseThrow());
        Assertions.assertTrue(resolver.tableLocation("db", "hive").isEmpty());
        Assertions.assertTrue(resolver.tableLocation("db", "missing").isEmpty());
    }

    @Test
    void missingLocationAndMetastoreFailuresRemainErrors() {
        RecordingClient client = new RecordingClient();
        client.tables.put("broken", table("broken", "delta", null));
        DeltaLakeHmsTableResolver resolver = new DeltaLakeHmsTableResolver(client);
        Assertions.assertThrows(DorisConnectorException.class, () -> resolver.tableLocation("db", "broken"));
        client.failure = new HmsClientException("metastore denied the request");
        Assertions.assertThrows(HmsClientException.class, () -> resolver.listTables("db"));
        Assertions.assertThrows(HmsClientException.class, () -> resolver.tableLocation("db", "broken"));
    }

    private static HmsTableInfo table(String name, String provider, String location) {
        return HmsTableInfo.builder().dbName("db").tableName(name).location(location)
                .parameters(Collections.singletonMap("spark.sql.sources.provider", provider)).build();
    }

    private static final class RecordingClient implements HmsClient {
        private final Map<String, HmsTableInfo> tables = new LinkedHashMap<>();
        private HmsClientException failure;

        @Override
        public List<String> listDatabases() {
            return Collections.singletonList("db");
        }

        @Override
        public List<String> listTables(String database) {
            checkFailure();
            return new ArrayList<>(tables.keySet());
        }

        @Override
        public boolean tableExists(String database, String table) {
            checkFailure();
            return tables.containsKey(table);
        }

        @Override
        public HmsTableInfo getTable(String database, String table) {
            checkFailure();
            return tables.get(table);
        }

        private void checkFailure() {
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public HmsDatabaseInfo getDatabase(String database) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String database, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listPartitionNames(String database, String table, int maxParts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String database, String table, List<String> partitions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String database, String table, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
