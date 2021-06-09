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
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import mockit.Expectations;
import mockit.Mocked;

public class DatabaseTest {

    private Database db;
    private long dbId = 10000;

    @Mocked
    private Catalog catalog;
    @Mocked
    private EditLog editLog;

    @Before
    public void Setup() {
        db = new Database(dbId, "dbTest");
        new Expectations() {
            {
                editLog.logCreateTable((CreateTableInfo) any);
                minTimes = 0;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                Catalog.getCurrentCatalogJournalVersion();
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
            Assert.assertTrue(db.tryWriteLock(0, TimeUnit.SECONDS));
        } finally {
            db.writeUnlock();
        }
    }

    @Test
    public void getTablesOnIdOrderOrThrowExceptionTest() throws MetaNotFoundException {
        List<Column> baseSchema1 = new LinkedList<>();
        OlapTable table1 = new OlapTable(2000L, "baseTable1", baseSchema1, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        List<Column> baseSchema2 = new LinkedList<>();
        OlapTable table2 = new OlapTable(2001L, "baseTable2", baseSchema2, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        db.createTable(table1);
        db.createTable(table2);
        List<Long> tableIdList = Lists.newArrayList(2001L, 2000L);
        List<Table> tableList = db.getTablesOnIdOrderOrThrowException(tableIdList);
        Assert.assertEquals(2, tableList.size());
        Assert.assertEquals(2000L, tableList.get(0).getId());
        Assert.assertEquals(2001L, tableList.get(1).getId());
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class, "unknown table, tableId=3000",
                () -> db.getTablesOnIdOrderOrThrowException(Lists.newArrayList(3000L)));
    }

    @Test
    public void getTableOrThrowExceptionTest() throws MetaNotFoundException {
        List<Column> baseSchema = new LinkedList<>();
        OlapTable table = new OlapTable(2000L, "baseTable", baseSchema, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        db.createTable(table);
        Table resultTable1 = db.getTableOrThrowException(2000L, Table.TableType.OLAP);
        Table resultTable2 = db.getTableOrThrowException("baseTable", Table.TableType.OLAP);
        Assert.assertEquals(table, resultTable1);
        Assert.assertEquals(table, resultTable2);
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class, "unknown table, tableId=3000",
                () -> db.getTableOrThrowException(3000L, Table.TableType.OLAP));
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class, "unknown table, table=baseTable1",
                () -> db.getTableOrThrowException("baseTable1", Table.TableType.OLAP));
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "table type is not BROKER, tableId=2000, type=class org.apache.doris.catalog.OlapTable",
                () -> db.getTableOrThrowException(2000L, Table.TableType.BROKER));
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "table type is not BROKER, table=baseTable, type=class org.apache.doris.catalog.OlapTable",
                () -> db.getTableOrThrowException("baseTable", Table.TableType.BROKER));
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
        Assert.assertTrue(db.createTable(table));
        // duplicate
        Assert.assertFalse(db.createTable(table));

        Assert.assertEquals(table, db.getTable(table.getId()));
        Assert.assertEquals(table, db.getTable(table.getName()));

        Assert.assertEquals(1, db.getTables().size());
        Assert.assertEquals(table, db.getTables().get(0));

        Assert.assertEquals(1, db.getTableNamesWithLock().size());
        for (String tableFamilyGroupName : db.getTableNamesWithLock()) {
            Assert.assertEquals(table.getName(), tableFamilyGroupName);
        }

        // drop
        // drop not exist tableFamily
        db.dropTable("invalid");
        Assert.assertEquals(1, db.getTables().size());

        db.createTable(table);
        db.dropTable(table.getName());
        Assert.assertEquals(0, db.getTables().size());
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./database");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        
        // db1
        Database db1 = new Database();
        db1.write(dos);
        
        // db2
        Database db2 = new Database(2, "db2");
        List<Column> columns = new ArrayList<Column>();
        Column column2 = new Column("column2",
                ScalarType.createType(PrimitiveType.TINYINT), false, AggregateType.MIN, "", "");
        columns.add(column2);
        columns.add(new Column("column3",
                        ScalarType.createType(PrimitiveType.SMALLINT), false, AggregateType.SUM, "", ""));
        columns.add(new Column("column4", 
                        ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column5", 
                        ScalarType.createType(PrimitiveType.BIGINT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column6", 
                        ScalarType.createType(PrimitiveType.FLOAT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column7", 
                        ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column8", ScalarType.createChar(10), true, null, "", ""));
        columns.add(new Column("column9", ScalarType.createVarchar(10), true, null, "", ""));
        columns.add(new Column("column10", ScalarType.createType(PrimitiveType.DATE), true, null, "", ""));
        columns.add(new Column("column11", ScalarType.createType(PrimitiveType.DATETIME), true, null, "", ""));

        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);
        Partition partition = new Partition(20000L, "table", index, new RandomDistributionInfo(10));
        OlapTable table = new OlapTable(1000, "table", columns, KeysType.AGG_KEYS,
                                        new SinglePartitionInfo(), new RandomDistributionInfo(10));
        short shortKeyColumnCount = 1;
        table.setIndexMeta(1000, "group1", columns, 1,1,shortKeyColumnCount,TStorageType.COLUMN, KeysType.AGG_KEYS);

        List<Column> column = Lists.newArrayList();
        column.add(column2);
        table.setIndexMeta(new Long(1), "test", column, 1, 1, shortKeyColumnCount,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.setIndexMeta(new Long(1), "test", column, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);
        Deencapsulation.setField(table, "baseIndexId", 1);
        table.addPartition(partition);
        db2.createTable(table);
        db2.write(dos);
        
        dos.flush();
        dos.close();
        
        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        
        Database rDb1 = new Database();
        rDb1.readFields(dis);
        Assert.assertTrue(rDb1.equals(db1));
        
        Database rDb2 = new Database();
        rDb2.readFields(dis);
        Assert.assertTrue(rDb2.equals(db2));
        
        // 3. delete files
        dis.close();
        file.delete();
    }
}
