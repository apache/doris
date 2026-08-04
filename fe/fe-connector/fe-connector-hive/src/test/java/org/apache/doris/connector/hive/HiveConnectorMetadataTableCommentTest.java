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
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pins {@link HiveConnectorMetadata#getTableComment} for an iceberg-on-HMS table read through the gateway.
 *
 * <p><b>WHY this matters:</b> the comment accessor takes a database and table NAME, never a
 * {@code ConnectorTableHandle}, so the gateway cannot route it to the iceberg sibling the way it routes every
 * other foreign-table operation — there is no handle to discriminate on. Before this was handled explicitly,
 * an iceberg table showed its comment when read through a dedicated iceberg catalog and showed nothing when
 * read through an HMS gateway catalog: a blank {@code COMMENT} clause in SHOW CREATE TABLE, a blank
 * {@code information_schema.tables.TABLE_COMMENT} and a blank SHOW TABLE STATUS Comment column, for the same
 * table. Iceberg's HMS catalog mirrors the comment into the HMS table parameters on every commit, so the
 * gateway answers from the metastore table it already reads.</p>
 *
 * <p>The plain-hive assertions are the other half of the contract: a plain hive table must keep the empty
 * comment legacy returned, so this change cannot alter what any existing hive deployment displays.</p>
 */
public class HiveConnectorMetadataTableCommentTest {

    private final ConnectorSession session = new ScopeSession(1L, "q1", ConnectorStatementScope.NONE);

    @Test
    public void icebergOnHmsTableReportsTheCommentStoredInTheMetastore() {
        Map<String, String> params = new HashMap<>();
        params.put("table_type", "ICEBERG");
        params.put("comment", "a comment written through the iceberg catalog");
        HiveConnectorMetadata md = metadataFor(icebergTable(params));

        Assertions.assertEquals("a comment written through the iceberg catalog",
                md.getTableComment(session, "db", "t"),
                "an iceberg-on-HMS table must report the same comment the dedicated iceberg catalog reports");
    }

    @Test
    public void icebergOnHmsTableWithoutACommentReportsEmpty() {
        HiveConnectorMetadata md = metadataFor(icebergTable(Collections.singletonMap("table_type", "ICEBERG")));

        Assertions.assertEquals("", md.getTableComment(session, "db", "t"),
                "an iceberg table with no comment must report the empty comment, never null");
    }

    @Test
    public void plainHiveTableKeepsItsLegacyEmptyComment() {
        Map<String, String> params = new HashMap<>();
        params.put("comment", "a hive table comment that legacy never surfaced");
        HmsTableInfo hive = HmsTableInfo.builder()
                .dbName("db").tableName("t").tableType("MANAGED_TABLE")
                .inputFormat("org.apache.hadoop.mapred.TextInputFormat")
                .parameters(params)
                .build();

        Assertions.assertEquals("", metadataFor(hive).getTableComment(session, "db", "t"),
                "a plain hive table must keep the empty comment legacy HMSExternalTable returned; surfacing it "
                        + "here would change what every existing hive deployment displays");
    }

    @Test
    public void anUnreadableTableDegradesToTheEmptyCommentInsteadOfFailing() {
        HiveConnectorMetadata md = new HiveConnectorMetadata(new ThrowingHmsClient(), Collections.emptyMap(),
                new FakeConnectorContext());

        Assertions.assertEquals("", md.getTableComment(session, "db", "gone"),
                "the comment is a display value fetched opportunistically; a metastore miss must not fail the "
                        + "statement that is merely rendering a table list");
    }

    private static HmsTableInfo icebergTable(Map<String, String> params) {
        return HmsTableInfo.builder()
                .dbName("db").tableName("t").tableType("EXTERNAL_TABLE")
                .parameters(params)
                .build();
    }

    private HiveConnectorMetadata metadataFor(HmsTableInfo tableInfo) {
        // The 3-arg constructor installs fail-loud sibling suppliers: reaching for the iceberg sibling here would
        // blow up, which is exactly the point — the comment must be answered from the metastore table alone,
        // without building a sibling connector or loading iceberg metadata.
        return new HiveConnectorMetadata(new FakeHmsClient(tableInfo), Collections.emptyMap(),
                new FakeConnectorContext());
    }

    /** Serves one prebuilt table; every other operation fails loud. */
    private static class FakeHmsClient implements HmsClient {
        private final HmsTableInfo table;

        FakeHmsClient(HmsTableInfo table) {
            this.table = table;
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return true;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            return table;
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

    /** A client whose table lookup fails the way a dropped or unauthorized table does. */
    private static final class ThrowingHmsClient extends FakeHmsClient {

        ThrowingHmsClient() {
            super(null);
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            throw new HmsClientException("table not found: " + dbName + "." + tableName);
        }
    }
}
