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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DatabaseTest {

    private Database db;
    private long dbId = 10000;

    @Mocked
    private Env env;
    @Mocked
    private EditLog editLog;

    @Before
    public void setup() {
        db = new Database(dbId, "dbTest");
        new Expectations() {
            {
                editLog.logCreateTable((CreateTableInfo) any);
                minTimes = 0;

                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new Expectations(env) {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;
            }
        };
    }

    @Test
    public void lockTest() {
        db.readLock();
        try {
            Assert.assertFalse(db.tryWriteLock(0, TimeUnit.SECONDS));
        } finally {
            db.readUnlock();
        }

        db.writeLock();
        try {
            Assert.assertTrue(db.tryWriteLock(1000, TimeUnit.SECONDS));
            db.writeUnlock();
        } finally {
            db.writeUnlock();
        }

        db.markDropped();
        Assert.assertFalse(db.writeLockIfExist());
        Assert.assertFalse(db.isWriteLockHeldByCurrentThread());
        db.unmarkDropped();
        Assert.assertTrue(db.writeLockIfExist());
        Assert.assertTrue(db.isWriteLockHeldByCurrentThread());
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
        Assert.assertEquals(2, tableList.size());
        Assert.assertEquals(2000L, tableList.get(0).getId());
        Assert.assertEquals(2001L, tableList.get(1).getId());
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
        Assert.assertEquals(table, resultTable1);
        Assert.assertEquals(table, resultTable2);
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
        Assert.assertEquals("dbTest", db.getFullName());
        Assert.assertEquals(dbId, db.getId());

        MaterializedIndex baseIndex = new MaterializedIndex(10001, IndexState.NORMAL);
        Partition partition = new Partition(20000L, "baseTable", baseIndex, new RandomDistributionInfo(10));
        List<Column> baseSchema = new LinkedList<Column>();
        OlapTable table = new OlapTable(2000, "baseTable", baseSchema, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        table.addPartition(partition);

        // create
        Assert.assertTrue(db.registerTable(table));
        // duplicate
        Assert.assertFalse(db.registerTable(table));

        Assert.assertEquals(table, db.getTableNullable(table.getId()));
        Assert.assertEquals(table, db.getTableNullable(table.getName()));

        Assert.assertEquals(1, db.getTables().size());
        Assert.assertEquals(table, db.getTables().get(0));

        Assert.assertEquals(1, db.getTableNamesWithLock().size());
        for (String tableFamilyGroupName : db.getTableNamesWithLock()) {
            Assert.assertEquals(table.getName(), tableFamilyGroupName);
        }

        // drop
        // drop not exist tableFamily
        db.unregisterTable("invalid");
        Assert.assertEquals(1, db.getTables().size());

        db.registerTable(table);
        db.unregisterTable(table.getName());
        Assert.assertEquals(0, db.getTables().size());
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
        Assert.assertEquals(rDb1, db1);

        Database rDb2 = Database.read(dis);
        Assert.assertEquals(rDb2, db2);

        // 3. delete files
        dis.close();
        Files.delete(path);
    }
}
