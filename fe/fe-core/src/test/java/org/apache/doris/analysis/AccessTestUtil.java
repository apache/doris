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

package org.apache.doris.analysis;

import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.mockito.Mockito;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class AccessTestUtil {
    // FakeEditLog field removed - using Mockito.mock(EditLog.class) directly

    public static SystemInfoService fetchSystemInfoService() {
        SystemInfoService clusterInfo = new SystemInfoService();
        return clusterInfo;
    }

    public static AccessControllerManager fetchAdminAccess() {
        Auth auth = Mockito.spy(new Auth());
        try {
            Mockito.doNothing().when(auth).setPassword(Mockito.any(UserIdentity.class), Mockito.any(byte[].class));
        } catch (DdlException e) {
            e.printStackTrace();
        }
        AccessControllerManager accessManager = Mockito.spy(new AccessControllerManager(auth));
        Mockito.doReturn(true).when(accessManager).checkGlobalPriv(Mockito.nullable(ConnectContext.class),
                Mockito.any(PrivPredicate.class));
        Mockito.doReturn(true).when(accessManager).checkDbPriv(Mockito.nullable(ConnectContext.class),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class));
        Mockito.doReturn(true).when(accessManager).checkTblPriv(Mockito.nullable(ConnectContext.class),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(PrivPredicate.class));
        Mockito.doReturn(auth).when(accessManager).getAuth();
        return accessManager;
    }

    public static Env fetchAdminCatalog() {
        try {
            Env env = Mockito.spy(Deencapsulation.newInstance(Env.class));

            AccessControllerManager accessManager = fetchAdminAccess();

            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(editLog.getNumEditStreams()).thenReturn(1);
            env.setEditLog(editLog);

            Database db = new Database(50000L, "testDb");
            MaterializedIndex baseIndex = new MaterializedIndex(30001, IndexState.NORMAL);
            RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
            Partition partition = new Partition(20000L, "testTbl", baseIndex, distributionInfo);
            List<Column> baseSchema = new LinkedList<Column>();
            Column column = new Column();
            baseSchema.add(column);
            OlapTable table = new OlapTable(30000, "testTbl", baseSchema, KeysType.AGG_KEYS, new SinglePartitionInfo(),
                    distributionInfo);
            table.setIndexMeta(baseIndex.getId(), "testTbl", baseSchema, 0, 1, (short) 1, TStorageType.COLUMN,
                    KeysType.AGG_KEYS);
            table.addPartition(partition);
            table.setBaseIndexId(baseIndex.getId());
            db.registerTable(table);

            InternalCatalog catalog = Mockito.spy(Deencapsulation.newInstance(InternalCatalog.class));
            Mockito.doReturn(db).when(catalog).getDbNullable(50000L);
            Mockito.doReturn(new Database()).when(catalog).getDbNullable(Mockito.anyString());
            Mockito.doReturn(db).when(catalog).getDbNullable("testDb");
            Mockito.doReturn(null).when(catalog).getDbNullable("emptyDb");
            Mockito.doReturn(Lists.newArrayList("testDb")).when(catalog).getDbNames();

            CatalogMgr dsMgr = Mockito.spy(new CatalogMgr());
            Mockito.doReturn(catalog).when(dsMgr).getCatalog(Mockito.anyString());
            Mockito.doReturn(catalog).when(dsMgr).getCatalogOrException(Mockito.anyString(),
                    Mockito.any(Function.class));
            Mockito.doReturn(catalog).when(dsMgr).getCatalogOrAnalysisException(Mockito.anyString());

            Mockito.doReturn(accessManager).when(env).getAccessManager();
            Mockito.doReturn(catalog).when(env).getCurrentCatalog();
            Mockito.doReturn(catalog).when(env).getInternalCatalog();
            Mockito.doReturn(editLog).when(env).getEditLog();
            Mockito.doNothing().when(env).changeDb(Mockito.nullable(ConnectContext.class), Mockito.anyString());
            Mockito.doThrow(new DdlException("failed")).when(env).changeDb(Mockito.nullable(ConnectContext.class),
                    Mockito.eq("blockDb"));
            Mockito.doReturn(new BrokerMgr()).when(env).getBrokerMgr();
            Mockito.doReturn(dsMgr).when(env).getCatalogMgr();
            return env;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    public static AccessControllerManager fetchBlockAccess() {
        Auth auth = new Auth();
        AccessControllerManager accessManager = Mockito.spy(new AccessControllerManager(auth));
        Mockito.doReturn(false).when(accessManager).checkGlobalPriv(Mockito.nullable(ConnectContext.class),
                Mockito.any(PrivPredicate.class));
        Mockito.doReturn(false).when(accessManager).checkDbPriv(Mockito.nullable(ConnectContext.class),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class));
        Mockito.doReturn(false).when(accessManager).checkTblPriv(Mockito.nullable(ConnectContext.class),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class));
        return accessManager;
    }

    public static OlapTable mockTable(String name) {
        Column column1 = new Column("col1", PrimitiveType.BIGINT);
        Column column2 = new Column("col2", PrimitiveType.DOUBLE);
        Column column3 = new Column("k1", PrimitiveType.VARCHAR);
        Column column4 = new Column("k2", PrimitiveType.VARCHAR);
        Column column5 = new Column("k3", PrimitiveType.VARCHAR);
        Column column6 = new Column("k4", PrimitiveType.BIGINT);

        MaterializedIndex index = Mockito.spy(new MaterializedIndex());
        Mockito.doReturn(30000L).when(index).getId();

        Partition partition = Mockito.spy(Deencapsulation.newInstance(Partition.class));
        Mockito.doReturn(index).when(partition).getBaseIndex();
        Mockito.doReturn(index).when(partition).getIndex(30000L);

        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn(Lists.newArrayList(column1, column2)).when(table).getBaseSchema();
        Mockito.doReturn(partition).when(table).getPartition(40000L);
        Mockito.doReturn(column3).when(table).getColumn("k1");
        Mockito.doReturn(column4).when(table).getColumn("k2");
        Mockito.doReturn(column5).when(table).getColumn("k3");
        Mockito.doReturn(column6).when(table).getColumn("k4");
        return table;
    }

    public static Database mockDb(String name) {
        Database db = Mockito.spy(new Database());
        OlapTable olapTable = mockTable("testTable");

        Mockito.doReturn(olapTable).when(db).getTableNullable("testTable");
        Mockito.doReturn(olapTable).when(db).getTableNullable("t");
        Mockito.doReturn(null).when(db).getTableNullable("emptyTable");
        Mockito.doReturn(Sets.newHashSet("testTable")).when(db).getTableNamesWithLock();
        Mockito.doReturn(Lists.newArrayList(olapTable)).when(db).getTables();
        Mockito.doNothing().when(db).readLock();
        Mockito.doNothing().when(db).readUnlock();
        Mockito.doReturn(name).when(db).getFullName();
        return db;
    }

    public static Env fetchBlockCatalog() {
        try {
            Env env = Mockito.spy(Deencapsulation.newInstance(Env.class));

            AccessControllerManager accessManager = fetchBlockAccess();
            Database db = mockDb("testDb");

            InternalCatalog catalog = Mockito.spy(Deencapsulation.newInstance(InternalCatalog.class));
            Mockito.doReturn(new Database()).when(catalog).getDbNullable(Mockito.anyString());
            Mockito.doReturn(db).when(catalog).getDbNullable("testDb");
            Mockito.doReturn(db).when(catalog).getDbNullable("testdb");
            Mockito.doReturn(null).when(catalog).getDbNullable("emptyDb");
            Mockito.doReturn(null).when(catalog).getDbNullable("emptyCluster");
            Mockito.doReturn(db).when(catalog).getDbOrAnalysisException("testdb");
            Mockito.doReturn(Lists.newArrayList("testDb")).when(catalog).getDbNames();

            CatalogMgr ctlMgr = Mockito.spy(new CatalogMgr());
            Mockito.doReturn(catalog).when(ctlMgr).getCatalog(Mockito.anyString());
            Mockito.doReturn(catalog).when(ctlMgr).getCatalogOrException(Mockito.anyString(),
                    Mockito.any(Function.class));
            Mockito.doReturn(catalog).when(ctlMgr).getCatalogOrAnalysisException(Mockito.anyString());

            Mockito.doReturn(accessManager).when(env).getAccessManager();
            Mockito.doThrow(new DdlException("failed")).when(env).changeDb(Mockito.nullable(ConnectContext.class),
                    Mockito.anyString());
            Mockito.doReturn(catalog).when(env).getInternalCatalog();
            Mockito.doReturn(catalog).when(env).getCurrentCatalog();
            Mockito.doReturn(ctlMgr).when(env).getCatalogMgr();
            return env;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }
}
