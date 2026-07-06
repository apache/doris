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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorBucketSpec;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsCreateDatabaseRequest;
import org.apache.doris.connector.hms.HmsCreateTableRequest;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DDL tests for {@link HiveConnectorMetadata}'s create/drop database, create/drop/truncate table overrides
 * (P7.1). They run offline against a recording {@link org.apache.doris.connector.hms.HmsClient} fake (no
 * live metastore, no Mockito), asserting the neutral request is turned into the same metastore write spec the
 * legacy {@code HiveMetadataOps.createTableImpl}/{@code createDbImpl}/{@code dropTableImpl}/
 * {@code truncateTableImpl} produced: file-format / owner defaults, the {@code doris.}-prefixed round-trip
 * parameters, the bucket gate, the transactional-table rejections, and the partition rules.
 */
public class HiveConnectorMetadataDdlTest {

    private static final String CATALOG_USER = "hive_user";

    // ==================== createTable: file format + owner + doris.version ====================

    @Test
    public void createTableUsesEnvDefaultFileFormatAndStampsOwnerAndVersion() {
        RecordingHmsClient client = new RecordingHmsClient();
        // WHY: with no file_format property the connector must fall back to the FE-global
        // hive_default_file_format threaded through the environment (legacy Config.hive_default_file_format),
        // stamp the connecting user as the owner (legacy set owner from ConnectContext), and stamp the build
        // version threaded via the environment. MUTATION: dropping any of the three assertions' sources
        // (env fallback / owner default / doris_version) flips it red.
        Map<String, String> env = new LinkedHashMap<>();
        env.put(HiveConnectorProperties.ENV_HIVE_DEFAULT_FILE_FORMAT, "parquet");
        env.put(HiveConnectorProperties.ENV_DORIS_VERSION, "9.9-deadbeef");
        metadata(client, Collections.emptyMap(), env)
                .createTable(session(), request().build());

        HmsCreateTableRequest req = client.lastCreateTable;
        Assertions.assertNotNull(req);
        Assertions.assertEquals("parquet", req.getFileFormat());
        Assertions.assertEquals(CATALOG_USER, req.getProperties().get("owner"));
        Assertions.assertEquals("9.9-deadbeef", req.getDorisVersion());
    }

    @Test
    public void createTableUserFileFormatOverridesEnvAndRoundTripsUnderDorisPrefix() {
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> props = new LinkedHashMap<>();
        props.put("file_format", "orc");
        props.put("location", "s3://bucket/t");
        props.put("some_key", "v");
        Map<String, String> env = Collections.singletonMap(
                HiveConnectorProperties.ENV_HIVE_DEFAULT_FILE_FORMAT, "parquet");

        // WHY: a user-set file_format wins over the env default; and file_format/location must round-trip as
        // metastore parameters under a doris. prefix while an ordinary property keeps its plain key (legacy
        // ddlProps loop). location is ALSO surfaced as the storage-descriptor location. MUTATION: dropping
        // the doris. prefix, or not honoring the user file_format, flips these.
        metadata(client, Collections.emptyMap(), env)
                .createTable(session(), request().properties(props).build());

        HmsCreateTableRequest req = client.lastCreateTable;
        Assertions.assertEquals("orc", req.getFileFormat());
        Assertions.assertEquals("s3://bucket/t", req.getLocation());
        Assertions.assertEquals("orc", req.getProperties().get("doris.file_format"));
        Assertions.assertEquals("s3://bucket/t", req.getProperties().get("doris.location"));
        Assertions.assertEquals("v", req.getProperties().get("some_key"));
    }

    @Test
    public void createTableFallsBackToOrcWhenEnvMissing() {
        RecordingHmsClient client = new RecordingHmsClient();
        // WHY: a direct-construction context with no environment (getEnvironment() empty) must still create a
        // table, degrading to the hard-coded orc default (matches Config.hive_default_file_format's default).
        metadata(client, Collections.emptyMap(), Collections.emptyMap())
                .createTable(session(), request().build());
        Assertions.assertEquals("orc", client.lastCreateTable.getFileFormat());
    }

    // ==================== createTable: transactional rejection ====================

    @Test
    public void createTableRejectsTransactional() {
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> props = Collections.singletonMap("transactional", "TRUE");
        // WHY: legacy rejects creating a hive transactional table (it only appears to accept inserts). The
        // value check is case-insensitive. MUTATION: dropping the reject lets the create through -> the seam
        // records a createTable and the assertThrows fails.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(client, Collections.emptyMap(), Collections.emptyMap())
                        .createTable(session(), request().properties(props).build()));
        Assertions.assertTrue(ex.getMessage().contains("transactional"));
        Assertions.assertNull(client.lastCreateTable, "reject must happen before the metastore create");
    }

    // ==================== createTable: bucketing gate ====================

    @Test
    public void createTableBucketRejectedWhenGloballyDisabled() {
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> env = Collections.singletonMap(
                HiveConnectorProperties.ENV_ENABLE_CREATE_HIVE_BUCKET_TABLE, "false");
        ConnectorBucketSpec bucket = new ConnectorBucketSpec(
                Collections.singletonList("id"), 8, "doris_default");
        // WHY: bucketed hive tables require the FE-global enable_create_hive_bucket_table toggle (default
        // off). The gate is checked BEFORE the hash requirement (legacy order). MUTATION: skipping the gate
        // lets a bucket table through.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(client, Collections.emptyMap(), env)
                        .createTable(session(), request().bucketSpec(bucket).build()));
        Assertions.assertTrue(ex.getMessage().contains("enable_create_hive_bucket_table"));
    }

    @Test
    public void createTableBucketRejectsNonHashDistribution() {
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> env = Collections.singletonMap(
                HiveConnectorProperties.ENV_ENABLE_CREATE_HIVE_BUCKET_TABLE, "true");
        ConnectorBucketSpec random = new ConnectorBucketSpec(
                Collections.singletonList("id"), 8, HiveConnectorProperties.BUCKET_ALGO_RANDOM);
        // WHY: hive external tables only support hash bucketing; a random distribution is rejected AFTER the
        // enable gate passed. MUTATION: accepting random creates an unsupported table.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(client, Collections.emptyMap(), env)
                        .createTable(session(), request().bucketSpec(random).build()));
        Assertions.assertTrue(ex.getMessage().contains("hash bucketing"));
    }

    @Test
    public void createTableHashBucketThreadsColsAndCount() {
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> env = Collections.singletonMap(
                HiveConnectorProperties.ENV_ENABLE_CREATE_HIVE_BUCKET_TABLE, "true");
        ConnectorBucketSpec hash = new ConnectorBucketSpec(
                Collections.singletonList("id"), 16, "doris_default");
        // WHY: an enabled hash bucket spec must reach the write spec with its columns + count intact.
        metadata(client, Collections.emptyMap(), env)
                .createTable(session(), request().bucketSpec(hash).build());
        Assertions.assertEquals(Collections.singletonList("id"), client.lastCreateTable.getBucketCols());
        Assertions.assertEquals(16, client.lastCreateTable.getNumBuckets());
    }

    // ==================== createTable: partitioning ====================

    @Test
    public void createTableRejectsRangePartition() {
        RecordingHmsClient client = new RecordingHmsClient();
        ConnectorPartitionSpec range = new ConnectorPartitionSpec(
                ConnectorPartitionSpec.Style.RANGE,
                Collections.singletonList(new ConnectorPartitionField("dt", "identity",
                        Collections.emptyList())),
                Collections.emptyList());
        // WHY: hive supports only LIST-style partitioning (legacy rejected RANGE). MUTATION: accepting RANGE
        // would build an invalid hive partition spec.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(client, Collections.emptyMap(), Collections.emptyMap())
                        .createTable(session(), request().partitionSpec(range).build()));
        Assertions.assertTrue(ex.getMessage().contains("'LIST' partition type"));
    }

    @Test
    public void createTableRejectsExplicitPartitionValues() {
        RecordingHmsClient client = new RecordingHmsClient();
        ConnectorPartitionSpec listWithValues = new ConnectorPartitionSpec(
                ConnectorPartitionSpec.Style.LIST,
                Collections.singletonList(new ConnectorPartitionField("dt", "identity",
                        Collections.emptyList())),
                Collections.emptyList(),
                true /* hasExplicitPartitionValues */);
        // WHY: a hive external table discovers partitions from the data layout, so explicit partition value
        // definitions are rejected (legacy parity). The neutral converter drops the value expressions but
        // threads this presence flag so the rejection survives. MUTATION: ignoring the flag turns a hard
        // error into a silent success.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(client, Collections.emptyMap(), Collections.emptyMap())
                        .createTable(session(), request().partitionSpec(listWithValues).build()));
        Assertions.assertTrue(ex.getMessage().contains("Partition values expressions"));
    }

    @Test
    public void createTableListPartitionThreadsPartitionKeys() {
        RecordingHmsClient client = new RecordingHmsClient();
        ConnectorPartitionSpec list = new ConnectorPartitionSpec(
                ConnectorPartitionSpec.Style.LIST,
                Arrays.asList(
                        new ConnectorPartitionField("dt", "identity", Collections.emptyList()),
                        new ConnectorPartitionField("region", "identity", Collections.emptyList())),
                Collections.emptyList());
        // WHY: the LIST partition columns become the metastore partition keys (order preserved). MUTATION:
        // dropping the field-name threading yields a non-partitioned table.
        metadata(client, Collections.emptyMap(), Collections.emptyMap())
                .createTable(session(), request().partitionSpec(list).build());
        Assertions.assertEquals(Arrays.asList("dt", "region"),
                client.lastCreateTable.getPartitionKeys());
    }

    // ==================== createTable: DLF default-value guard ====================

    @Test
    public void createTableRejectsColumnDefaultsOnDlfCatalog() {
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> catalogProps = Collections.singletonMap("hive.metastore.type", "dlf");
        List<ConnectorColumn> cols = Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), null, false, null),
                new ConnectorColumn("v", ConnectorType.of("INT"), null, true, "5"));
        // WHY: DLF catalogs cannot honor per-column default values, so a create carrying one is rejected
        // (legacy parity). This depends on the shared converter now threading the default value onto the
        // column. MUTATION: dropping the DLF guard, or the column default reaching the connector, flips it.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(client, catalogProps, Collections.emptyMap())
                        .createTable(session(), request().columns(cols).build()));
        Assertions.assertTrue(ex.getMessage().contains("DLF"));
    }

    @Test
    public void createTableAllowsColumnDefaultsOnNonDlfCatalog() {
        RecordingHmsClient client = new RecordingHmsClient();
        List<ConnectorColumn> cols = Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), null, false, null),
                new ConnectorColumn("v", ConnectorType.of("INT"), null, true, "5"));
        // WHY: a plain HMS catalog keeps column defaults; the guard must only fire for DLF. MUTATION: an
        // unconditional guard would wrongly reject this.
        metadata(client, Collections.emptyMap(), Collections.emptyMap())
                .createTable(session(), request().columns(cols).build());
        Assertions.assertNotNull(client.lastCreateTable);
    }

    // ==================== createTable: text compression default ====================

    @Test
    public void createTableThreadsTextCompressionSessionDefaultMappingUncompressedToPlain() {
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> sessionProps = Collections.singletonMap(
                HiveConnectorProperties.SESSION_HIVE_TEXT_COMPRESSION, "uncompressed");
        // WHY: a text table's fallback compression comes from the hive_text_compression session variable, and
        // legacy maps the "uncompressed" alias to "plain". MUTATION: skipping the alias mapping would thread
        // "uncompressed" (an unsupported metastore value).
        metadata(client, Collections.emptyMap(), Collections.emptyMap())
                .createTable(sessionWith(sessionProps), request().build());
        Assertions.assertEquals("plain", client.lastCreateTable.getDefaultTextCompression());
    }

    // ==================== createDatabase ====================

    @Test
    public void createDatabaseSplitsLocationCommentAndParams() {
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> props = new LinkedHashMap<>();
        props.put("location", "s3://bucket/db");
        props.put("comment", "my db");
        props.put("k", "v");
        // WHY (legacy createDbImpl): the location property becomes the db location URI and is REMOVED from the
        // parameters; the comment becomes the description; the rest stay as db parameters. MUTATION: leaving
        // location in the parameters, or dropping the description, flips these.
        metadata(client, Collections.emptyMap(), Collections.emptyMap())
                .createDatabase(session(), "db1", props);
        HmsCreateDatabaseRequest req = client.lastCreateDatabase;
        Assertions.assertEquals("db1", req.getDbName());
        Assertions.assertEquals("s3://bucket/db", req.getLocationUri());
        Assertions.assertEquals("my db", req.getComment());
        Assertions.assertFalse(req.getProperties().containsKey("location"),
                "location must be removed from db parameters");
        Assertions.assertEquals("v", req.getProperties().get("k"));
    }

    // ==================== dropTable / truncateTable ====================

    @Test
    public void dropTableRejectsTransactionalTable() {
        RecordingHmsClient client = new RecordingHmsClient();
        HiveTableHandle handle = new HiveTableHandle.Builder("db1", "t1", HiveTableType.HIVE)
                .tableParameters(Collections.singletonMap("transactional", "true"))
                .build();
        // WHY: legacy dropTableImpl rejects dropping a hive transactional table (via
        // AcidUtils.isTransactionalTable). MUTATION: dropping the check lets the drop through -> the seam
        // records a dropTable and assertThrows fails.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(client, Collections.emptyMap(), Collections.emptyMap())
                        .dropTable(session(), handle));
        Assertions.assertTrue(ex.getMessage().contains("transactional"));
        Assertions.assertTrue(client.log.isEmpty(), "reject must happen before the metastore drop");
    }

    @Test
    public void dropTableDelegatesForNonTransactionalTable() {
        RecordingHmsClient client = new RecordingHmsClient();
        HiveTableHandle handle = new HiveTableHandle.Builder("db1", "t1", HiveTableType.HIVE)
                .tableParameters(Collections.emptyMap())
                .build();
        metadata(client, Collections.emptyMap(), Collections.emptyMap()).dropTable(session(), handle);
        Assertions.assertEquals(Collections.singletonList("dropTable:db1.t1"), client.log);
    }

    @Test
    public void truncateTableDelegatesWithPartitions() {
        RecordingHmsClient client = new RecordingHmsClient();
        HiveTableHandle handle = new HiveTableHandle.Builder("db1", "t1", HiveTableType.HIVE).build();
        List<String> partitions = Collections.singletonList("dt=2024-01-01");
        metadata(client, Collections.emptyMap(), Collections.emptyMap())
                .truncateTable(session(), handle, partitions);
        Assertions.assertEquals(
                Collections.singletonList("truncateTable:db1.t1:[dt=2024-01-01]"), client.log);
    }

    // ==================== dropDatabase (force cascade) ====================

    @Test
    public void dropDatabaseForceCascadesTableDropsThenDb() {
        RecordingHmsClient client = new RecordingHmsClient();
        client.tables.put("t1", tableInfo("db1", "t1", Collections.emptyMap()));
        client.tables.put("t2", tableInfo("db1", "t2", Collections.emptyMap()));
        // WHY (legacy dropDbImpl with force): every table is dropped first, then the database. MUTATION:
        // dropping the cascade would call dropDatabase on a non-empty database.
        metadata(client, Collections.emptyMap(), Collections.emptyMap())
                .dropDatabase(session(), "db1", false, true);
        Assertions.assertEquals(
                Arrays.asList("dropTable:db1.t1", "dropTable:db1.t2", "dropDatabase:db1"), client.log);
    }

    @Test
    public void dropDatabaseForceRejectsTransactionalTableInCascade() {
        RecordingHmsClient client = new RecordingHmsClient();
        client.tables.put("t1", tableInfo("db1", "t1",
                Collections.singletonMap("transactional", "true")));
        // WHY: the force cascade drops each table through the SAME transactional check as a direct DROP TABLE,
        // so a transactional table aborts the whole force drop (legacy dropTableImpl propagated the error).
        // MUTATION: cascading without the check would silently drop a transactional table.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(client, Collections.emptyMap(), Collections.emptyMap())
                        .dropDatabase(session(), "db1", false, true));
        Assertions.assertTrue(client.log.isEmpty(), "transactional table must abort before any drop");
    }

    @Test
    public void dropDatabaseNonForceJustDropsDb() {
        RecordingHmsClient client = new RecordingHmsClient();
        client.tables.put("t1", tableInfo("db1", "t1", Collections.emptyMap()));
        // WHY: without force the tables are NOT cascaded (legacy left the metastore to reject a non-empty db).
        metadata(client, Collections.emptyMap(), Collections.emptyMap())
                .dropDatabase(session(), "db1", false, false);
        Assertions.assertEquals(Collections.singletonList("dropDatabase:db1"), client.log);
    }

    // ==================== helpers ====================

    private static HiveConnectorMetadata metadata(RecordingHmsClient client,
            Map<String, String> catalogProps, Map<String, String> env) {
        return new HiveConnectorMetadata(client, catalogProps, new FakeConnectorContext(env));
    }

    private static ConnectorCreateTableRequest.Builder request() {
        List<ConnectorColumn> cols = Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "id", false, null),
                new ConnectorColumn("dt", ConnectorType.of("STRING"), null, true, null));
        return ConnectorCreateTableRequest.builder()
                .dbName("db1")
                .tableName("t1")
                .columns(cols)
                .comment("t comment")
                .properties(new LinkedHashMap<>());
    }

    private static ConnectorSession session() {
        return sessionWith(Collections.emptyMap());
    }

    private static ConnectorSession sessionWith(Map<String, String> sessionProps) {
        return new FakeSession(CATALOG_USER, sessionProps);
    }

    private static HmsTableInfo tableInfo(String db, String table, Map<String, String> params) {
        return HmsTableInfo.builder().dbName(db).tableName(table).parameters(params).build();
    }

    /** Minimal recording {@link org.apache.doris.connector.hms.HmsClient}: records writes, serves canned reads. */
    private static final class RecordingHmsClient implements org.apache.doris.connector.hms.HmsClient {
        private final List<String> log = new ArrayList<>();
        private final Map<String, HmsTableInfo> tables = new LinkedHashMap<>();
        private HmsCreateTableRequest lastCreateTable;
        private HmsCreateDatabaseRequest lastCreateDatabase;

        @Override
        public List<String> listDatabases() {
            return Collections.emptyList();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new HmsClientException("no database: " + dbName);
        }

        @Override
        public List<String> listTables(String dbName) {
            return new ArrayList<>(tables.keySet());
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return tables.containsKey(tableName);
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            HmsTableInfo info = tables.get(tableName);
            if (info == null) {
                throw new HmsClientException("no table: " + tableName);
            }
            return info;
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            return Collections.emptyList();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            return Collections.emptyList();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new HmsClientException("no partition");
        }

        @Override
        public void createDatabase(HmsCreateDatabaseRequest request) {
            lastCreateDatabase = request;
            log.add("createDatabase:" + request.getDbName());
        }

        @Override
        public void dropDatabase(String dbName) {
            log.add("dropDatabase:" + dbName);
        }

        @Override
        public void createTable(HmsCreateTableRequest request) {
            lastCreateTable = request;
            log.add("createTable:" + request.getDbName() + "." + request.getTableName());
        }

        @Override
        public void dropTable(String dbName, String tableName) {
            log.add("dropTable:" + dbName + "." + tableName);
        }

        @Override
        public void truncateTable(String dbName, String tableName, List<String> partitions) {
            log.add("truncateTable:" + dbName + "." + tableName + ":" + partitions);
        }

        @Override
        public void close() {
        }
    }

    /** Minimal {@link ConnectorSession}: fixed user + a configurable session-property map. */
    private static final class FakeSession implements ConnectorSession {
        private final String user;
        private final Map<String, String> sessionProperties;

        private FakeSession(String user, Map<String, String> sessionProperties) {
            this.user = user;
            this.sessionProperties = sessionProperties;
        }

        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return user;
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
        public long getCatalogId() {
            return 0L;
        }

        @Override
        public String getCatalogName() {
            return "hive_catalog";
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
