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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.RandomDistributionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

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
    private List<ColumnDef> columnDefs;
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
        columnDefs = Lists.newArrayList();
        columnDefs.add(new ColumnDef("col1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        columnDefs.add(new ColumnDef("col2", new TypeDef(ScalarType.createVarchar(10))));
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
        List<ColumnDef> invalidCols1 = Lists.newArrayList();
        invalidCols1.add(new ColumnDef(
                "v1", new TypeDef(ScalarType.createType(PrimitiveType.INT)), false,
                AggregateType.SUM, false, "1",
                ""));
        invalidCols1.add(new ColumnDef("k1", new TypeDef(ScalarType.createVarchar(10))));
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
        List<ColumnDef> invalidCols2 = Lists.newArrayList();
        invalidCols2.add(new ColumnDef(
                "v1", new TypeDef(ScalarType.createType(PrimitiveType.INT)), false, AggregateType.SUM,
                false, "1", ""));
        invalidCols2.add(new ColumnDef("v2", new TypeDef(ScalarType.createVarchar(10)), false,
                AggregateType.REPLACE, false, "abc", ""));
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

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
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

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
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

        CreateTableStmt stmt1 = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
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

        List<ColumnDef> cols2 = Lists.newArrayList();
        // invalid property
        cols2.add(new ColumnDef("k1_int", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        cols2.add(new ColumnDef("k2_varchar", new TypeDef(ScalarType.createType(PrimitiveType.VARCHAR))));
        cols2.add(new ColumnDef("v1", new TypeDef(ScalarType.createType(PrimitiveType.INT)),
                             true, AggregateType.MAX, false, "0", ""));
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
        cols2.add(new ColumnDef("k1_varchar", new TypeDef(ScalarType.createType(PrimitiveType.VARCHAR))));
        cols2.add(new ColumnDef("k2_varchar", new TypeDef(ScalarType.createType(PrimitiveType.VARCHAR))));
        cols2.add(new ColumnDef("v1", new TypeDef(ScalarType.createType(PrimitiveType.INT)),
                             true, AggregateType.MAX, false, "0", ""));
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

        CreateTableStmt stmt1 = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
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
