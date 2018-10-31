// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.baidu.palo.analysis.Analyzer;
import com.baidu.palo.analysis.CreateTableStmt;
import com.baidu.palo.analysis.KeysDesc;
import com.baidu.palo.analysis.RandomDistributionDesc;
import com.baidu.palo.analysis.TableName;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IMockBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
@PrepareForTest({ Catalog.class, SystemInfoService.class })
public class CreateTableTest {

    private Catalog catalog;
    private SystemInfoService systemInfoService;
    IMockBuilder<Catalog> mockBuilder;

    private String dbName = "testDb";
    private String tableName = "testTbl";

    private TableName dbTableName;
    private List<Column> cols;
    private List<String> colsName;

    private Analyzer analyzer;
    private AtomicLong nextId = new AtomicLong(0L);

    private Backend backend;

    private RandomDistributionInfo distributionInfo;

    @Before
    public void setUp() {
        mockBuilder = EasyMock.createMockBuilder(Catalog.class);
        // analyzer
        analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn(dbName);
        EasyMock.replay(analyzer);
        // table name
        dbTableName = new TableName(dbName, tableName);
        // col
        cols = Lists.newArrayList();
        cols.add(new Column("col1", ColumnType.createType(PrimitiveType.INT)));
        cols.add(new Column("col2", ColumnType.createVarchar(10)));
        colsName = Lists.newArrayList();
        colsName.add("col1");
        colsName.add("col2");

        backend = EasyMock.createMock(Backend.class);
        EasyMock.expect(backend.isAlive()).andReturn(true).anyTimes();
        EasyMock.replay(backend);

        distributionInfo = new RandomDistributionInfo(10);

        Catalog.getCurrentInvertedIndex().clear();
    }

    @Test(expected = DdlException.class)
    public void createTableTest_invalidColumn() throws Exception {
        catalog = Catalog.getInstance();

        // 1. error order
        List<Column> invalidCols1 = Lists.newArrayList();
        invalidCols1.add(new Column("v1", ColumnType.createType(PrimitiveType.INT), false, AggregateType.SUM, "1",
                                    ""));
        invalidCols1.add(new Column("k1", ColumnType.createVarchar(10)));
        invalidCols1.get(1).setIsKey(true);
        CreateTableStmt stmt1 = new CreateTableStmt(false, false, dbTableName, invalidCols1, "olap",
                                                    new KeysDesc(), null,
                new RandomDistributionDesc(10), null, null);
        try {
            catalog.createTable(stmt1);
        } catch (DdlException e) {
            System.err.println(e.getMessage());
        }

        // 2. no key column
        List<Column> invalidCols2 = Lists.newArrayList();
        invalidCols2.add(new Column("v1", ColumnType.createType(PrimitiveType.INT), false, AggregateType.SUM, "1",
                                    ""));
        invalidCols2.add(new Column("v2", ColumnType.createVarchar(10), false, AggregateType.REPLACE, "abc", ""));
        CreateTableStmt stmt2 = new CreateTableStmt(false, false, dbTableName, invalidCols2, "olap",
                                                    new KeysDesc(), null,
                new RandomDistributionDesc(10), null, null);

        try {
            catalog.createTable(stmt2);
        } catch (DdlException e) {
            System.err.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = DdlException.class)
    public void olapDbNotFoundTest() throws Exception {
        catalog = mockBuilder.addMockedMethod("getDb", String.class).createMock();
        catalog.getDb(dbName);
        EasyMock.expectLastCall().andReturn(null);
        EasyMock.replay(catalog);

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, cols, "olap",
                                                   new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                new RandomDistributionDesc(10), null, null);
        try {
            catalog.createTable(stmt);
        } catch (DdlException e) {
            System.err.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = DdlException.class)
    public void olapTableExistsTest() throws Exception {
        Database db = new Database();
        List<Column> baseSchema = Lists.newArrayList();
        db.createTable(new OlapTable(1000L, tableName, baseSchema,
                                     KeysType.AGG_KEYS, new SinglePartitionInfo(), distributionInfo));

        catalog = mockBuilder.addMockedMethod("getDb", String.class).createMock();
        catalog.getDb(dbName);
        EasyMock.expectLastCall().andReturn(db);
        EasyMock.replay(catalog);

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, cols, "olap",
                                                   new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                new RandomDistributionDesc(10), null, null);

        try {
            catalog.createTable(stmt);
        } catch (DdlException e) {
            System.err.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = DdlException.class)
    public void olapNotEnoughBackendTest() throws Exception {
        Database db = new Database();

        catalog = mockBuilder.addMockedMethod("getDb", String.class).addMockedMethod("getNextId").createMock();
        catalog.getDb(dbName);
        EasyMock.expectLastCall().andReturn(db);
        EasyMock.expect(catalog.getNextId()).andReturn(nextId.incrementAndGet()).anyTimes();
        EasyMock.replay(catalog);

        // SystemInfoService
        systemInfoService = EasyMock.createMock(SystemInfoService.class);
        systemInfoService.seqChooseBackendIds(EasyMock.anyInt(), EasyMock.anyBoolean(), EasyMock.anyBoolean(),
                EasyMock.anyString());
        EasyMock.expectLastCall().andAnswer(new IAnswer<List<Long>>() {
            @Override
            public List<Long> answer() throws Throwable {
                List<Long> ids = Lists.newArrayList();
                ids.add(1L);
                ids.add(2L);
                ids.add(3L);
                return ids;
            }
        }).anyTimes();
        systemInfoService.getBackend(EasyMock.anyLong());
        EasyMock.expectLastCall().andReturn(backend).anyTimes();
        systemInfoService.checkClusterCapacity(EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(systemInfoService);

        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getCurrentInvertedIndex()).andReturn(invertedIndex).anyTimes();
        EasyMock.expect(Catalog.isCheckpointThread()).andReturn(false).anyTimes();
        EasyMock.expect(Catalog.calcShortKeyColumnCount(EasyMock.anyObject(List.class), EasyMock.anyObject(Map.class)))
                .andReturn((short) 2).anyTimes();
        PowerMock.replay(Catalog.class);

        CreateTableStmt stmt1 = new CreateTableStmt(false, false, dbTableName, cols, "olap",
                                                    new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                new RandomDistributionDesc(1), null, null);
        try {
            catalog.createTable(stmt1);
        } catch (DdlException e) {
            System.err.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = DdlException.class)
    public void olapShortKeyTest() throws Exception {
        Database db = new Database();
        catalog = mockBuilder.addMockedMethod("getDb", String.class).addMockedMethod("getNextId").createMock();
        catalog.getDb(dbName);
        EasyMock.expectLastCall().andReturn(db).anyTimes();
        EasyMock.expect(catalog.getNextId()).andReturn(nextId.incrementAndGet()).anyTimes();
        EasyMock.replay(catalog);

        // SystemInfoService
        systemInfoService = EasyMock.createMock(SystemInfoService.class);
        systemInfoService.seqChooseBackendIds(EasyMock.anyInt(), EasyMock.anyBoolean(), EasyMock.anyBoolean(),
                EasyMock.anyString());
        EasyMock.expectLastCall().andAnswer(new IAnswer<List<Long>>() {
            @Override
            public List<Long> answer() throws Throwable {
                List<Long> ids = Lists.newArrayList();
                ids.add(1L);
                ids.add(2L);
                ids.add(3L);
                return ids;
            }
        }).anyTimes();
        systemInfoService.getBackend(EasyMock.anyLong());
        EasyMock.expectLastCall().andReturn(backend).anyTimes();
        systemInfoService.checkClusterCapacity(EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(systemInfoService);

        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getCurrentInvertedIndex()).andReturn(invertedIndex).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalogJournalVersion()).andReturn(FeMetaVersion.VERSION_45).anyTimes();
        EasyMock.expect(Catalog.isCheckpointThread()).andReturn(false).anyTimes();
        EasyMock.expect(Catalog.calcShortKeyColumnCount(EasyMock.anyObject(List.class), EasyMock.anyObject(Map.class)))
                .andReturn((short) 2).anyTimes();
        PowerMock.replay(Catalog.class);

        List<Column> cols2 = new ArrayList<Column>();
        // invalid property
        cols2.add(new Column("k1_int", ColumnType.createType(PrimitiveType.INT)));
        cols2.add(new Column("k2_varchar", ColumnType.createType(PrimitiveType.VARCHAR)));
        cols2.add(new Column("v1", ColumnType.createType(PrimitiveType.INT),
                             true, AggregateType.MAX, "0", ""));
        cols2.get(0).setIsKey(true);
        cols2.get(1).setIsKey(true);

        List<String> cols2Name = Lists.newArrayList();
        cols2Name.add("k1_int");
        cols2Name.add("k2_varchar");
        cols2Name.add("v1");

        Map<String, String> properties = new HashMap<String, String>();

        // 1. larger then indexColumns size
        properties.put("short_key_num", "3");
        CreateTableStmt stmt1 = new CreateTableStmt(false, false, dbTableName, cols2, "olap",
                                                    new KeysDesc(KeysType.AGG_KEYS, cols2Name), null,
                new RandomDistributionDesc(1), null, null);
        try {
            catalog.createTable(stmt1);
        } catch (DdlException e) {
            System.out.print(e.getMessage());
        }

        // 2. first is varchar
        properties.clear();
        cols2.clear();
        cols2.add(new Column("k1_varchar", ColumnType.createType(PrimitiveType.VARCHAR)));
        cols2.add(new Column("k2_varchar", ColumnType.createType(PrimitiveType.VARCHAR)));
        cols2.add(new Column("v1", ColumnType.createType(PrimitiveType.INT),
                             true, AggregateType.MAX, "0", ""));
        stmt1 = new CreateTableStmt(false, false, dbTableName, cols2, "olap",
                                    new KeysDesc(KeysType.AGG_KEYS, cols2Name), null,
                new RandomDistributionDesc(1), null, null);

        try {
            catalog.createTable(stmt1);
        } catch (DdlException e) {
            System.out.print(e.getMessage());
            throw e;
        }
    }

    @Test(expected = DdlException.class)
    public void olapNormalTest() throws Exception {
        Database db = new Database();
        catalog = mockBuilder.addMockedMethod("getDb", String.class).addMockedMethod("getNextId").createMock();
        catalog.getDb(dbName);
        EasyMock.expectLastCall().andReturn(db).anyTimes();
        EasyMock.expect(catalog.getNextId()).andReturn(nextId.incrementAndGet()).anyTimes();
        EasyMock.replay(catalog);

        // SystemInfoService
        systemInfoService = EasyMock.createMock(SystemInfoService.class);
        systemInfoService.seqChooseBackendIds(EasyMock.anyInt(), EasyMock.anyBoolean(), EasyMock.anyBoolean(),
                EasyMock.anyString());
        EasyMock.expectLastCall().andAnswer(new IAnswer<List<Long>>() {
            @Override
            public List<Long> answer() throws Throwable {
                List<Long> ids = Lists.newArrayList();
                ids.add(1L);
                ids.add(2L);
                ids.add(3L);
                return ids;
            }
        }).anyTimes();
        systemInfoService.getBackend(EasyMock.anyLong());
        EasyMock.expectLastCall().andReturn(backend).anyTimes();
        systemInfoService.checkClusterCapacity(EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(systemInfoService);

        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getCurrentInvertedIndex()).andReturn(invertedIndex).anyTimes();
        EasyMock.expect(Catalog.isCheckpointThread()).andReturn(false).anyTimes();
        EasyMock.expect(Catalog.calcShortKeyColumnCount(EasyMock.anyObject(List.class), EasyMock.anyObject(Map.class)))
                .andReturn((short) 2).anyTimes();
        PowerMock.replay(Catalog.class);

        CreateTableStmt stmt1 = new CreateTableStmt(false, false, dbTableName, cols, "olap",
                                                    new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                new RandomDistributionDesc(1), null, null);

        try {
            catalog.createTable(stmt1);
        } catch (DdlException e) {
            System.out.print(e.getMessage());
            throw e;
        }
    }

}
