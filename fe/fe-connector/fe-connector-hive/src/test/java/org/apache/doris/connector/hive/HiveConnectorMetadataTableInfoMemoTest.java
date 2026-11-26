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
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class HiveConnectorMetadataTableInfoMemoTest {
    private final RecordingHmsClient client = new RecordingHmsClient();
    private final HiveConnectorMetadata metadata = new HiveConnectorMetadata(
            client, HiveTestProperties.minimal(), new FakeConnectorContext());

    @Test
    void reusesPositiveMetadataWithinOneStatement() {
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = new ScopeSession(1, "q1", scope);
        HiveTableHandle first = handle(session, "db", "t");
        client.generation = 2;
        HiveTableHandle second = handle(session, "db", "t");
        Assertions.assertEquals(first.getLocation(), second.getLocation());
        Assertions.assertEquals(1, client.existsCalls);
        Assertions.assertEquals(1, client.tableCalls);
        Assertions.assertTrue(HiveConnectorMetadata.TABLE_INFO_NAMESPACE.startsWith(
                new HiveConnectorProvider().getType() + "."));
    }

    @Test
    void isolatesQueryCatalogAndTableIdentities() {
        TestStatementScope scope = new TestStatementScope();
        handle(new ScopeSession(1, "q1", scope), "db", "t");
        client.generation = 2;
        Assertions.assertTrue(handle(new ScopeSession(1, "q2", scope), "db", "t").getLocation().endsWith("/v2"));
        handle(new ScopeSession(2, "q2", scope), "db", "t");
        handle(new ScopeSession(1, "q2", scope), "other_db", "t");
        handle(new ScopeSession(1, "q2", scope), "db", "other_t");
        Assertions.assertEquals(5, client.existsCalls);
        Assertions.assertEquals(5, client.tableCalls);
    }

    @Test
    void doesNotRememberAbsenceBeforeCreateOrConcurrentCreation() {
        ConnectorSession session = new ScopeSession(1, "q1", new TestStatementScope());
        client.exists = false;
        Assertions.assertFalse(metadata.getTableHandle(session, "db", "t").isPresent());
        client.exists = true;
        handle(session, "db", "t");
        handle(session, "db", "t");
        Assertions.assertEquals(2, client.existsCalls);
        Assertions.assertEquals(1, client.tableCalls);
    }

    @Test
    void propagatesLoadFailureWithoutRememberingIt() {
        ConnectorSession session = new ScopeSession(1, "q1", new TestStatementScope());
        client.failure = new IllegalStateException("HMS unavailable");
        Assertions.assertSame(client.failure, Assertions.assertThrows(IllegalStateException.class,
                () -> metadata.getTableHandle(session, "db", "t")));
        client.failure = null;
        handle(session, "db", "t");
        Assertions.assertEquals(2, client.existsCalls);
        Assertions.assertEquals(1, client.tableCalls);
    }

    @Test
    void noStatementScopeKeepsRevalidating() {
        ConnectorSession session = new ScopeSession(1, "q1", ConnectorStatementScope.NONE);
        handle(session, "db", "t");
        handle(session, "db", "t");
        handle(null, "db", "t");
        handle(null, "db", "t");
        Assertions.assertEquals(4, client.existsCalls);
        Assertions.assertEquals(4, client.tableCalls);
    }

    @Test
    void explicitFreshReadBypassesTheMemo() {
        ConnectorSession session = new ScopeSession(1, "q1", new TestStatementScope());
        HiveTableHandle first = handle(session, "db", "t");
        client.generation = 2;
        String ddl = metadata.renderShowCreateTableDdl(session, first).get();
        Assertions.assertTrue(ddl.contains("/v2"), ddl);
        Assertions.assertTrue(handle(session, "db", "t").getLocation().endsWith("/v1"));
        Assertions.assertEquals(1, client.freshCalls);
        Assertions.assertEquals(1, client.existsCalls);
        Assertions.assertEquals(1, client.tableCalls);
    }

    private HiveTableHandle handle(ConnectorSession session, String database, String table) {
        ConnectorTableHandle handle = metadata.getTableHandle(session, database, table).get();
        return (HiveTableHandle) handle;
    }

    private static final class RecordingHmsClient implements HmsClient {
        private int existsCalls;
        private int tableCalls;
        private int freshCalls;
        private int generation = 1;
        private boolean exists = true;
        private RuntimeException failure;

        @Override
        public boolean tableExists(String database, String table) {
            existsCalls++;
            if (failure != null) {
                throw failure;
            }
            return exists;
        }

        @Override
        public HmsTableInfo getTable(String database, String table) {
            tableCalls++;
            return table(database, table);
        }

        @Override
        public HmsTableInfo getTableFresh(String database, String table) {
            freshCalls++;
            return table(database, table);
        }

        private HmsTableInfo table(String database, String table) {
            return HmsTableInfo.builder().dbName(database).tableName(table)
                    .tableType("MANAGED_TABLE")
                    .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                    .outputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
                    .serializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                    .location("s3://bucket/" + database + "/" + table + "/v" + generation)
                    .columns(Collections.singletonList(new ConnectorColumn(
                            "id", ConnectorType.of("BIGINT"), null, true, null)))
                    .partitionKeys(Collections.emptyList()).parameters(Collections.emptyMap()).build();
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String database) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String database) {
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
        public List<HmsPartitionInfo> getPartitions(String database, String table, List<String> names) {
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
