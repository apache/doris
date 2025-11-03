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

import org.apache.doris.alter.AlterCancelException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TableTest {

    public static OlapTable newOlapTable(long tableId, String tableName, int hashColumn) {
        List<Column> columns = ImmutableList.of(
                new Column("id", Type.INT, true, AggregateType.NONE, "0", ""));

        return newOlapTable(tableId, tableName, hashColumn, columns);
    }

    public static OlapTable newOlapTable(long tableId, String tableName, int hashColumn, List<Column> columns) {
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(3,
                ImmutableList.of(columns.get(hashColumn)));

        OlapTable table = new OlapTable(tableId, tableName, columns,
                KeysType.PRIMARY_KEYS, null, hashDistributionInfo);
        table.setIndexMeta(-1,
                "base",
                table.getFullSchema(),
                0, 0, (short) 0,
                TStorageType.COLUMN,
                KeysType.PRIMARY_KEYS);
        return table;
    }

    private FakeEnv fakeEnv;

    private Table table;

    @Before
    public void setUp() {
        table = newOlapTable(10000, "test", 0);
        fakeEnv = new FakeEnv();
        Env env = Deencapsulation.newInstance(Env.class);
        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void lockTest() {
        table.readLock();
        try {
            Assert.assertFalse(table.tryWriteLock(0, TimeUnit.SECONDS));
        } finally {
            table.readUnlock();
        }

        Assert.assertFalse(table.isWriteLockHeldByCurrentThread());
        table.writeLock();
        try {
            Assert.assertTrue(table.tryWriteLock(1000, TimeUnit.SECONDS));
            Assert.assertTrue(table.isWriteLockHeldByCurrentThread());
            table.writeUnlock();
        } finally {
            table.writeUnlock();
            Assert.assertFalse(table.isWriteLockHeldByCurrentThread());
        }

        Assert.assertFalse(table.isWriteLockHeldByCurrentThread());
        table.markDropped();
        Assert.assertFalse(table.writeLockIfExist());
        Assert.assertFalse(table.isWriteLockHeldByCurrentThread());
        table.unmarkDropped();
        Assert.assertTrue(table.writeLockIfExist());
        Assert.assertTrue(table.writeLockIfExist());
        Assert.assertTrue(table.isWriteLockHeldByCurrentThread());
        table.writeUnlock();
    }

    @Test
    public void lockTestWithException() {
        table.markDropped();
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = unknown table, tableName=test",
                () -> table.writeLockOrDdlException());
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "errCode = 7, detailMessage = unknown table, tableName=test",
                () -> table.writeLockOrMetaException());
        ExceptionChecker.expectThrowsWithMsg(AlterCancelException.class,
                "errCode = 2, detailMessage = unknown table, tableName=test",
                () -> table.writeLockOrAlterCancelException());
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "errCode = 7, detailMessage = unknown table, tableName=test",
                () -> table.tryWriteLockOrMetaException(1000, TimeUnit.MILLISECONDS));
        table.unmarkDropped();
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        final Path path = Files.createTempFile("tableFamilyGroup", "tmp");
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

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

        OlapTable table1 = new OlapTable(1000L, "group1", columns, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        short shortKeyColumnCount = 1;
        table1.setIndexMeta(1000, "group1", columns, 1, 1,
                shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);
        List<Column> column = Lists.newArrayList();
        column.add(column2);

        table1.setIndexMeta(2L, "test", column, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);
        Deencapsulation.setField(table1, "baseIndexId", 1000);
        table1.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));

        Table rFamily1 = Table.read(dis);
        Assert.assertEquals(table1, rFamily1);
        Assert.assertEquals(table1.getCreateTime(), rFamily1.getCreateTime());
        Assert.assertEquals(table1.getIndexMetaByIndexId(2).getKeysType(), KeysType.AGG_KEYS);

        // 3. delete files
        dis.close();
        Files.delete(path);
    }
}
