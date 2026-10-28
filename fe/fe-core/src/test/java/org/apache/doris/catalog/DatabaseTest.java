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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DatabaseTest {

    private Database db;
    private long dbId = 10000;

    private Env env = Mockito.mock(Env.class);
    private EditLog editLog = Mockito.mock(EditLog.class);

    private MockedStatic<Env> mockedEnvStatic;

    @Test
    public void enablingChangelogValidatesTheMergedRetention() throws Exception {
        for (String ttl : new String[] {"-1", "0"}) {
            db.updateDbProperties(Map.of("changelog.enable", "false", "changelog.ttl_seconds", ttl));
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> db.updateDbProperties(Map.of("changelog.enable", "true")));
            Assertions.assertTrue(exception.getMessage().contains("changelog.ttl_seconds must be greater than 0"),
                    exception.getMessage());
            Assertions.assertFalse(db.getChangelogConfig().isEnable());
        }
        db.updateDbProperties(Map.of("changelog.enable", "true", "changelog.ttl_seconds", "60"));
        Assertions.assertTrue(db.getChangelogConfig().isEnable());
        Assertions.assertFalse(db.getBinlogConfig().getEnable());
        Assertions.assertEquals(60, db.getChangelogConfigForCreateTable(Map.of()).getTtlSeconds());
    }

    @Test
    public void changelogTtlDefaultsAndInheritance() throws Exception {
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("changelog.enable", "true");
        ChangelogConfig withoutTtl = db.getChangelogConfigForCreateTable(tableProperties);
        Assertions.assertEquals(86400L, withoutTtl.getTtlSeconds());
        Assertions.assertTrue(withoutTtl.isTtlEnabled());

        tableProperties.put("changelog.ttl_seconds", "0");
        Assertions.assertThrows(AnalysisException.class, () -> db.getChangelogConfigForCreateTable(tableProperties));
        tableProperties.put("changelog.ttl_seconds", "11");
        ChangelogConfig explicitTtl = db.getChangelogConfigForCreateTable(tableProperties);
        Assertions.assertEquals(11L, explicitTtl.getTtlSeconds());
        Assertions.assertTrue(explicitTtl.isTtlEnabled());

        db.replayUpdateDbProperties(Map.of(
                "changelog.enable", "true",
                "changelog.ttl_seconds", "7"));
        tableProperties.remove("changelog.ttl_seconds");
        ChangelogConfig inheritedTtl = db.getChangelogConfigForCreateTable(tableProperties);
        Assertions.assertEquals(7L, inheritedTtl.getTtlSeconds());
        Assertions.assertTrue(inheritedTtl.isTtlEnabled());

        db.replayUpdateDbProperties(Map.of("changelog.ttl_seconds", "-1"));
        Assertions.assertThrows(AnalysisException.class, () -> db.getChangelogConfigForCreateTable(tableProperties));
        tableProperties.put("changelog.ttl_seconds", "9");
        Assertions.assertEquals(9L, db.getChangelogConfigForCreateTable(tableProperties).getTtlSeconds());

        // The CCR retention is not the default of a changelog.
        db.replayUpdateDbProperties(Map.of("changelog.ttl_seconds", "7", "binlog.ttl_seconds", "5"));
        tableProperties.remove("changelog.ttl_seconds");
        Assertions.assertEquals(7L, db.getChangelogConfigForCreateTable(tableProperties).getTtlSeconds());
    }

    @Test
    public void tableWithoutChangelogSetsNoChangelogProperty() throws Exception {
        Assertions.assertFalse(db.getChangelogConfigForCreateTable(Map.of("changelog.enable", "false")).isEnable());
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> db.getChangelogConfigForCreateTable(Map.of("changelog.ttl_seconds", "10")));
        Assertions.assertTrue(exception.getMessage().contains(
                "Property changelog.ttl_seconds requires \"changelog.enable\" = \"true\""), exception.getMessage());
        // A database default without changelog stays a default.
        db.replayUpdateDbProperties(Map.of("changelog.ttl_seconds", "10", "changelog.need_historical_value", "true"));
        Assertions.assertFalse(db.getChangelogConfigForCreateTable(Map.of()).isEnable());
        ChangelogConfig inherited = db.getChangelogConfigForCreateTable(Map.of("changelog.enable", "true"));
        Assertions.assertEquals(new ChangelogConfig(true, 10, true), inherited);
    }

    @Test
    public void databaseCanNotEnableBothBinlogAndChangelog() throws Exception {
        db.updateDbProperties(Map.of("changelog.enable", "true"));
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> db.updateDbProperties(Map.of("binlog.enable", "true")));
        Assertions.assertTrue(exception.getMessage().contains("Database dbTest can not enable both binlog and "
                + "changelog"), exception.getMessage());
        Assertions.assertFalse(db.getBinlogConfig().isEnableForCCR());
        db.updateDbProperties(Map.of("changelog.enable", "false", "binlog.enable", "true"));
        Assertions.assertTrue(db.getBinlogConfig().isEnableForCCR());
        Assertions.assertThrows(DdlException.class, () -> db.updateDbProperties(Map.of("changelog.enable", "true")));
        Assertions.assertFalse(db.getChangelogConfig().isEnable());
    }

    @Test
    public void imageLoadAndReplayDropTheReplacedBinlogKeys() throws Exception {
        // A CCR database written before changelog.* replaced the format keys.
        db.getDbProperties().getProperties().putAll(legacyDbProperties("true", "STATEMENT_AND_SNAPSHOT"));
        db.gsonPostProcess();
        assertLegacyKeysDropped(db.getDbProperties().getProperties());
        Assertions.assertEquals("true", db.getDbProperties().get("binlog.enable"));
        Assertions.assertTrue(db.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(db.getChangelogConfig().isEnable());

        Database replayed = new Database(dbId + 1, "replayed");
        replayed.replayUpdateDbProperties(legacyDbProperties("false", "STATEMENT_AND_SNAPSHOT"));
        assertLegacyKeysDropped(replayed.getDbProperties().getProperties());
        Assertions.assertFalse(replayed.getBinlogConfig().isEnableForCCR());
    }

    @Test
    public void changelogDefaultDatabaseDoesNotTurnIntoCcrDatabase() throws Exception {
        // "binlog.enable" = "true" with "binlog.format" = "ROW" made changelog the default of new tables.
        db.getDbProperties().getProperties().putAll(legacyDbProperties("true", "row"));
        db.gsonPostProcess();
        assertLegacyKeysDropped(db.getDbProperties().getProperties());
        Assertions.assertEquals("false", db.getDbProperties().get("binlog.enable"));
        Assertions.assertFalse(db.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(db.getChangelogConfig().isEnable());
        Assertions.assertEquals("3600", db.getDbProperties().get("binlog.ttl_seconds"));

        Database replayed = new Database(dbId + 1, "replayed");
        replayed.replayUpdateDbProperties(Map.of("binlog.enable", "true", "binlog.format", "ROW"));
        assertLegacyKeysDropped(replayed.getDbProperties().getProperties());
        Assertions.assertEquals("false", replayed.getDbProperties().get("binlog.enable"));
        Assertions.assertFalse(replayed.getBinlogConfig().isEnableForCCR());
        // The database can then turn on CCR binlog or changelog like any other.
        replayed.updateDbProperties(Map.of("changelog.enable", "true"));
        Assertions.assertTrue(replayed.getChangelogConfig().isEnable());
    }

    private static Map<String, String> legacyDbProperties(String enable, String format) {
        Map<String, String> properties = new HashMap<>();
        properties.put("binlog.enable", enable);
        properties.put("binlog.ttl_seconds", "3600");
        properties.put("binlog.format", format);
        properties.put("binlog.need_historical_value", "false");
        properties.put("__binlog_config_version", "0");
        return properties;
    }

    private static void assertLegacyKeysDropped(Map<String, String> properties) {
        Assertions.assertFalse(properties.containsKey("binlog.format"), properties.toString());
        Assertions.assertFalse(properties.containsKey("binlog.need_historical_value"), properties.toString());
        Assertions.assertFalse(properties.containsKey("__binlog_config_version"), properties.toString());
    }

    @BeforeEach
    public void setup() {
        FeConstants.runningUnitTest = true;
        db = new Database(dbId, "dbTest");

        Mockito.when(env.getEditLog()).thenReturn(editLog);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
    }

    @AfterEach
    public void tearDown() {
        mockedEnvStatic.close();
    }

    @Test
    public void lockTest() {
        db.readLock();
        try {
            Assertions.assertFalse(db.tryWriteLock(0, TimeUnit.SECONDS));
        } finally {
            db.readUnlock();
        }

        db.writeLock();
        try {
            Assertions.assertTrue(db.tryWriteLock(1000, TimeUnit.SECONDS));
            db.writeUnlock();
        } finally {
            db.writeUnlock();
        }

        db.markDropped();
        Assertions.assertFalse(db.writeLockIfExist());
        Assertions.assertFalse(db.isWriteLockHeldByCurrentThread());
        db.unmarkDropped();
        Assertions.assertTrue(db.writeLockIfExist());
        Assertions.assertTrue(db.isWriteLockHeldByCurrentThread());
        db.writeUnlock();
    }

    @Test
    public void lockTestWithException() {
        db.markDropped();
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = unknown db, dbName=dbTest", () -> db.writeLockOrDdlException());
        db.unmarkDropped();
    }

    @Test
    public void getTablesOnIdOrderOrThrowExceptionTest() throws MetaNotFoundException {
        List<Column> baseSchema1 = new LinkedList<>();
        OlapTable table1 = new OlapTable(2000L, "baseTable1", baseSchema1, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        List<Column> baseSchema2 = new LinkedList<>();
        OlapTable table2 = new OlapTable(2001L, "baseTable2", baseSchema2, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        db.registerTable(table1);
        db.registerTable(table2);
        List<Long> tableIdList = Lists.newArrayList(2001L, 2000L);
        List<Table> tableList = db.getTablesOnIdOrderOrThrowException(tableIdList);
        Assertions.assertEquals(2, tableList.size());
        Assertions.assertEquals(2000L, tableList.get(0).getId());
        Assertions.assertEquals(2001L, tableList.get(1).getId());
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class, "table not found, tableId=3000",
                () -> db.getTablesOnIdOrderOrThrowException(Lists.newArrayList(3000L)));
    }

    @Test
    public void getTableOrThrowExceptionTest() throws MetaNotFoundException {
        List<Column> baseSchema = new LinkedList<>();
        OlapTable table = new OlapTable(2000L, "baseTable", baseSchema, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        db.registerTable(table);
        Table resultTable1 = db.getTableOrMetaException(2000L, Table.TableType.OLAP);
        Table resultTable2 = db.getTableOrMetaException("baseTable", Table.TableType.OLAP);
        Assertions.assertEquals(table, resultTable1);
        Assertions.assertEquals(table, resultTable2);
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class, "table not found, tableId=3000",
                () -> db.getTableOrMetaException(3000L, Table.TableType.OLAP));
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class, "table not found, tableName=baseTable1",
                () -> db.getTableOrMetaException("baseTable1", Table.TableType.OLAP));
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "table type is not BROKER, tableId=2000, type=OLAP",
                () -> db.getTableOrMetaException(2000L, Table.TableType.BROKER));
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "table type is not BROKER, tableName=baseTable, type=OLAP",
                () -> db.getTableOrMetaException("baseTable", Table.TableType.BROKER));
    }

    @Test
    public void createAndDropPartitionTest() {
        Assertions.assertEquals("dbTest", db.getFullName());
        Assertions.assertEquals(dbId, db.getId());

        MaterializedIndex baseIndex = new MaterializedIndex(10001, IndexState.NORMAL);
        Partition partition = new Partition(20000L, "baseTable", baseIndex, new RandomDistributionInfo(10));
        List<Column> baseSchema = new LinkedList<Column>();
        OlapTable table = new OlapTable(2000, "baseTable", baseSchema, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        table.addPartition(partition);

        // create
        Assertions.assertTrue(db.registerTable(table));
        // duplicate
        Assertions.assertFalse(db.registerTable(table));

        Assertions.assertEquals(table, db.getTableNullable(table.getId()));
        Assertions.assertEquals(table, db.getTableNullable(table.getName()));

        Assertions.assertEquals(1, db.getTables().size());
        Assertions.assertEquals(table, db.getTables().get(0));

        Assertions.assertEquals(1, db.getTableNamesWithLock().size());
        for (String tableFamilyGroupName : db.getTableNamesWithLock()) {
            Assertions.assertEquals(table.getName(), tableFamilyGroupName);
        }

        // drop
        // drop not exist tableFamily
        db.unregisterTable("invalid");
        Assertions.assertEquals(1, db.getTables().size());

        db.registerTable(table);
        db.unregisterTable(table.getName());
        Assertions.assertEquals(0, db.getTables().size());
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        final Path path = Files.createTempFile("database", "tmp");
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        // db1
        Database db1 = new Database();
        db1.write(dos);

        // db2
        Database db2 = new Database(2, "db2");
        Column column2 = new Column("column2",
                ScalarType.createType(PrimitiveType.TINYINT), false, AggregateType.MIN, "", "");

        ImmutableList<Column> columns = ImmutableList.<Column>builder()
                .add(column2)
                .add(new Column("column3", ScalarType.createType(PrimitiveType.SMALLINT), false, AggregateType.SUM, "", ""))
                .add(new Column("column4", ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""))
                .add(new Column("column5", ScalarType.createType(PrimitiveType.BIGINT), false, AggregateType.REPLACE, "", ""))
                .add(new Column("column6", ScalarType.createType(PrimitiveType.FLOAT), false, AggregateType.REPLACE, "", ""))
                .add(new Column("column7", ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.REPLACE, "", ""))
                .add(new Column("column8", ScalarType.createChar(10), true, null, "", ""))
                .add(new Column("column9", ScalarType.createVarchar(10), true, null, "", ""))
                .add(new Column("column10", ScalarType.createType(PrimitiveType.DATE), true, null, "", ""))
                .add(new Column("column11", ScalarType.createType(PrimitiveType.DATETIME), true, null, "", ""))
                .build();

        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);
        Partition partition = new Partition(20000L, "table", index, new RandomDistributionInfo(10));
        OlapTable table = new OlapTable(1000, "table", columns, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        short shortKeyColumnCount = 1;
        table.setIndexMeta(1000, "group1", columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);

        List<Column> column = Lists.newArrayList(column2);
        table.setIndexMeta(1L, "test", column, 1, 1, shortKeyColumnCount,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.setIndexMeta(1L, "test", column, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);
        Deencapsulation.setField(table, "baseIndexId", 1);
        table.addPartition(partition);
        db2.registerTable(table);
        db2.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));

        Database rDb1 = Database.read(dis);
        Assertions.assertEquals(rDb1, db1);

        Database rDb2 = Database.read(dis);
        Assertions.assertEquals(rDb2, db2);

        // 3. delete files
        dis.close();
        Files.delete(path);
    }
}
