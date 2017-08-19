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

package com.baidu.palo.analysis;

import java.util.LinkedList;
import java.util.List;

import org.easymock.EasyMock;

import com.baidu.palo.alter.RollupHandler;
import com.baidu.palo.alter.SchemaChangeHandler;
import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.KeysType;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.MaterializedIndex.IndexState;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.PrimitiveType;
import com.baidu.palo.catalog.RandomDistributionInfo;
import com.baidu.palo.catalog.SinglePartitionInfo;
import com.baidu.palo.catalog.UserPropertyMgr;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.load.Load;
import com.baidu.palo.persist.EditLog;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.system.SystemInfoService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AccessTestUtil {
    public static UserPropertyMgr fetchAdminAccess() {
        UserPropertyMgr userPropertyMgr = EasyMock.createMock(UserPropertyMgr.class);
        EasyMock.expect(userPropertyMgr.checkAccess(EasyMock.isA(String.class), EasyMock.isA(String.class),
                EasyMock.isA(AccessPrivilege.class))).andReturn(true).anyTimes();
        EasyMock.expect(userPropertyMgr.isAdmin(EasyMock.isA(String.class))).andReturn(true).anyTimes();
        EasyMock.expect(userPropertyMgr.isSuperuser(EasyMock.isA(String.class))).andReturn(true).anyTimes();
        EasyMock.expect(userPropertyMgr.checkUserAccess(EasyMock.isA(String.class), EasyMock.eq("blockUser")))
                .andReturn(false).anyTimes();
        EasyMock.expect(userPropertyMgr.checkUserAccess(EasyMock.isA(String.class), EasyMock.isA(String.class)))
                .andReturn(true).anyTimes();
        EasyMock.expect(userPropertyMgr.getMaxConn(EasyMock.isA(String.class))).andReturn(1000L).anyTimes();
        try {
            userPropertyMgr.setPasswd(EasyMock.endsWith("testCluster:testUser"), EasyMock.isA(byte[].class));
            EasyMock.expectLastCall().anyTimes();
            userPropertyMgr.setPasswd(EasyMock.endsWith("root"), EasyMock.isA(byte[].class));
            EasyMock.expectLastCall().andThrow(new DdlException("No privilege to change password")).anyTimes();
        } catch (DdlException e) {
            return null;
        }
        EasyMock.replay(userPropertyMgr);
        return userPropertyMgr;
    }

    public static SystemInfoService fetchSystemInfoService() {
        SystemInfoService clusterInfo = EasyMock.createMock(SystemInfoService.class);
        EasyMock.replay(clusterInfo);
        return clusterInfo;
    }

    public static Catalog fetchAdminCatalog() {
        try {
            Catalog catalog = EasyMock.createMock(Catalog.class);
            EasyMock.expect(catalog.getUserMgr()).andReturn(fetchAdminAccess()).anyTimes();
            Database db = new Database(50000L, "testCluster:testDb");
            MaterializedIndex baseIndex = new MaterializedIndex(30000, IndexState.NORMAL);

            RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);

            Partition partition = new Partition(20000L, "testTbl", baseIndex, distributionInfo);
            List<Column> baseSchema = new LinkedList<Column>();
            OlapTable table = new OlapTable(30000, "testTbl", baseSchema,
                    KeysType.AGG_KEYS, new SinglePartitionInfo(), distributionInfo);
            table.setIndexSchemaInfo(baseIndex.getId(), "testTbl", baseSchema, 0, 1, (short) 1);
            table.addPartition(partition);
            db.createTable(table);

            EasyMock.expect(catalog.getDb("testCluster:testDb")).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDb("testCluster:emptyDb")).andReturn(null).anyTimes();
            EasyMock.expect(catalog.getDb(db.getId())).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDb(EasyMock.isA(String.class))).andReturn(new Database()).anyTimes();
            EasyMock.expect(catalog.getDbNames()).andReturn(Lists.newArrayList("testCluster:testDb")).anyTimes();
            EasyMock.expect(catalog.getLoadInstance()).andReturn(new Load()).anyTimes();
            EasyMock.expect(catalog.getSchemaChangeHandler()).andReturn(new SchemaChangeHandler()).anyTimes();
            EasyMock.expect(catalog.getRollupHandler()).andReturn(new RollupHandler()).anyTimes();
            EasyMock.expect(catalog.getEditLog()).andReturn(EasyMock.createMock(EditLog.class)).anyTimes();
            EasyMock.expect(catalog.getClusterDbNames("testCluster"))
                    .andReturn(Lists.newArrayList("testCluster:testDb")).anyTimes();
            catalog.changeDb(EasyMock.isA(ConnectContext.class), EasyMock.eq("blockDb"));
            EasyMock.expectLastCall().andThrow(new DdlException("failed.")).anyTimes();
            catalog.changeDb(EasyMock.isA(ConnectContext.class), EasyMock.isA(String.class));
            EasyMock.expectLastCall().anyTimes();
            EasyMock.replay(catalog);
            return catalog;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    public static UserPropertyMgr fetchBlockAccess() {
        UserPropertyMgr userPropertyMgr = EasyMock.createMock(UserPropertyMgr.class);
        EasyMock.expect(userPropertyMgr.checkAccess(EasyMock.isA(String.class), EasyMock.isA(String.class),
                EasyMock.isA(AccessPrivilege.class))).andReturn(false).anyTimes();
        EasyMock.expect(userPropertyMgr.isAdmin(EasyMock.isA(String.class))).andReturn(false).anyTimes();
        EasyMock.expect(userPropertyMgr.isSuperuser(EasyMock.isA(String.class))).andReturn(false).anyTimes();
        EasyMock.expect(userPropertyMgr.checkUserAccess(EasyMock.isA(String.class), EasyMock.isA(String.class)))
                .andReturn(false).anyTimes();
        EasyMock.replay(userPropertyMgr);
        return userPropertyMgr;
    }

    public static OlapTable mockTableFamilyGroup(String name) {
        OlapTable table = EasyMock.createMock(OlapTable.class);
        Partition partition = EasyMock.createMock(Partition.class);
        MaterializedIndex index = EasyMock.createMock(MaterializedIndex.class);
        Column column1 = new Column("col1", PrimitiveType.BIGINT);
        Column column2 = new Column("col2", PrimitiveType.DOUBLE);
        EasyMock.expect(table.getBaseSchema()).andReturn(Lists.newArrayList(column1, column2)).anyTimes();
        EasyMock.expect(table.getPartition(40000L)).andReturn(partition).anyTimes();
        EasyMock.expect(partition.getBaseIndex()).andReturn(index).anyTimes();
        EasyMock.expect(partition.getIndex(30000L)).andReturn(index).anyTimes();
        EasyMock.expect(index.getId()).andReturn(30000L).anyTimes();
        EasyMock.replay(index);
        EasyMock.replay(partition);
        return table;
    }

    public static Database mockDb(String name) {
        Database db = EasyMock.createMock(Database.class);
        OlapTable olapTable = mockTableFamilyGroup("testTable");
        EasyMock.expect(db.getTable("testTable")).andReturn(olapTable).anyTimes();
        EasyMock.expect(db.getTable("emptyTable")).andReturn(null).anyTimes();
        EasyMock.expect(db.getTableNamesWithLock()).andReturn(Sets.newHashSet("testTable")).anyTimes();
        db.getTables();
        EasyMock.expectLastCall().andReturn(Lists.newArrayList(olapTable)).anyTimes();
        db.readLock();
        EasyMock.expectLastCall().anyTimes();
        db.readUnlock();
        EasyMock.expectLastCall().anyTimes();
        db.getName();
        EasyMock.expectLastCall().andReturn(name).anyTimes();
        EasyMock.replay(db);
        return db;
    }

    public static Catalog fetchBlockCatalog() {
        try {
            Catalog catalog = EasyMock.createMock(Catalog.class);
            EasyMock.expect(catalog.getUserMgr()).andReturn(fetchBlockAccess()).anyTimes();
            catalog.changeDb(EasyMock.isA(ConnectContext.class), EasyMock.isA(String.class));
            EasyMock.expectLastCall().andThrow(new DdlException("failed.")).anyTimes();

            Database db = mockDb("testCluster:testDb");
            EasyMock.expect(catalog.getDb("testCluster:testDb")).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDb("testCluster:emptyDb")).andReturn(null).anyTimes();
            EasyMock.expect(catalog.getDb(EasyMock.isA(String.class))).andReturn(new Database()).anyTimes();
            EasyMock.expect(catalog.getDbNames()).andReturn(Lists.newArrayList("testCluster:testDb")).anyTimes();
            EasyMock.expect(catalog.getClusterDbNames("testCluster"))
                    .andReturn(Lists.newArrayList("testCluster:testDb")).anyTimes();
            EasyMock.expect(catalog.getDb("emptyCluster")).andReturn(null).anyTimes();
            EasyMock.replay(catalog);
            return catalog;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    public static Analyzer fetchAdminAnalyzer(boolean withCluster) {
        String prefix = "";
        if (withCluster) {
            prefix = "testCluster:";
        }
        Analyzer analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn(prefix + "testDb").anyTimes();
        EasyMock.expect(analyzer.getUser()).andReturn(prefix + "testUser").anyTimes();
        EasyMock.expect(analyzer.getCatalog()).andReturn(fetchAdminCatalog()).anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("testCluster").anyTimes();

        try {
            analyzer.checkPrivilege(EasyMock.isA(String.class), EasyMock.isA(AccessPrivilege.class));
        } catch (AnalysisException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(analyzer.getContext()).andReturn(new ConnectContext(null)).anyTimes();
        EasyMock.replay(analyzer);
        return analyzer;
    }

    public static Analyzer fetchBlockAnalyzer() throws AnalysisException {
        Analyzer analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("testCluster:testDb").anyTimes();
        EasyMock.expect(analyzer.getUser()).andReturn("testCluster:testUser").anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.expect(analyzer.getCatalog()).andReturn(AccessTestUtil.fetchBlockCatalog()).anyTimes();
        analyzer.checkPrivilege(EasyMock.isA(String.class), EasyMock.isA(AccessPrivilege.class));
        EasyMock.expectLastCall().andThrow(new AnalysisException(""));
        EasyMock.replay(analyzer);
        return analyzer;
    }

    public static Analyzer fetchEmptyDbAnalyzer() {
        Analyzer analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("").anyTimes();
        EasyMock.expect(analyzer.getUser()).andReturn("testCluster:testUser").anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.expect(analyzer.getCatalog()).andReturn(AccessTestUtil.fetchBlockCatalog()).anyTimes();
        EasyMock.expect(analyzer.getContext()).andReturn(new ConnectContext(null)).anyTimes();
        EasyMock.replay(analyzer);
        return analyzer;
    }
}
