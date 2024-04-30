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
import org.apache.doris.catalog.FakeEditLog;
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
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class AccessTestUtil {
    private static FakeEditLog fakeEditLog;

    public static SystemInfoService fetchSystemInfoService() {
        SystemInfoService clusterInfo = new SystemInfoService();
        return clusterInfo;
    }

    public static AccessControllerManager fetchAdminAccess() {
        Auth auth = new Auth();
        AccessControllerManager accessManager = new AccessControllerManager(auth);
        try {
            new Expectations(auth) {
                {
                    auth.setPassword((SetPassVar) any);
                    minTimes = 0;
                }
            };

            new Expectations(accessManager) {
                {
                    accessManager.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                    minTimes = 0;
                    result = true;

                    accessManager.checkDbPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                    minTimes = 0;
                    result = true;

                    accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                            (PrivPredicate) any);
                    minTimes = 0;
                    result = true;

                    accessManager.getAuth();
                    minTimes = 0;
                    result = auth;
                }
            };
        } catch (DdlException e) {
            e.printStackTrace();
        }
        return accessManager;
    }

    public static Env fetchAdminCatalog() {
        try {
            Env env = Deencapsulation.newInstance(Env.class);

            AccessControllerManager accessManager = fetchAdminAccess();

            fakeEditLog = new FakeEditLog();
            EditLog editLog = new EditLog("name");
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

            InternalCatalog catalog = Deencapsulation.newInstance(InternalCatalog.class);
            new Expectations(catalog) {
                {
                    catalog.getDbNullable(50000L);
                    minTimes = 0;
                    result = db;

                    catalog.getDbNullable("testDb");
                    minTimes = 0;
                    result = db;

                    catalog.getDbNullable("emptyDb");
                    minTimes = 0;
                    result = null;

                    catalog.getDbNullable(anyString);
                    minTimes = 0;
                    result = new Database();

                    catalog.getDbNames();
                    minTimes = 0;
                    result = Lists.newArrayList("testDb");
                }
            };

            CatalogMgr dsMgr = new CatalogMgr();
            new Expectations(dsMgr) {
                {
                    dsMgr.getCatalog((String) any);
                    minTimes = 0;
                    result = catalog;

                    dsMgr.getCatalogOrException((String) any, (Function) any);
                    minTimes = 0;
                    result = catalog;

                    dsMgr.getCatalogOrAnalysisException((String) any);
                    minTimes = 0;
                    result = catalog;
                }
            };

            new Expectations(env, catalog) {
                {
                    env.getAccessManager();
                    minTimes = 0;
                    result = accessManager;

                    env.getCurrentCatalog();
                    minTimes = 0;
                    result = catalog;

                    env.getInternalCatalog();
                    minTimes = 0;
                    result = catalog;

                    env.getEditLog();
                    minTimes = 0;
                    result = editLog;

                    env.getLoadInstance();
                    minTimes = 0;
                    result = new Load();

                    env.changeDb((ConnectContext) any, "blockDb");
                    minTimes = 0;
                    result = new DdlException("failed");

                    env.changeDb((ConnectContext) any, anyString);
                    minTimes = 0;

                    env.getBrokerMgr();
                    minTimes = 0;
                    result = new BrokerMgr();

                    env.getCatalogMgr();
                    minTimes = 0;
                    result = dsMgr;

                    env.isCheckpointThread();
                    minTimes = 0;
                    result = false;
                }
            };
            return env;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    public static AccessControllerManager fetchBlockAccess() {
        Auth auth = new Auth();
        AccessControllerManager accessManager = new AccessControllerManager(auth);
        new Expectations(accessManager) {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                accessManager.checkDbPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = false;
            }
        };
        return accessManager;
    }

    public static OlapTable mockTable(String name) {
        Column column1 = new Column("col1", PrimitiveType.BIGINT);
        Column column2 = new Column("col2", PrimitiveType.DOUBLE);
        Column column3 = new Column("k1", PrimitiveType.VARCHAR);
        Column column4 = new Column("k2", PrimitiveType.VARCHAR);
        Column column5 = new Column("k3", PrimitiveType.VARCHAR);
        Column column6 = new Column("k4", PrimitiveType.BIGINT);

        MaterializedIndex index = new MaterializedIndex();
        new Expectations(index) {
            {
                index.getId();
                minTimes = 0;
                result = 30000L;
            }
        };

        Partition partition = Deencapsulation.newInstance(Partition.class);
        new Expectations(partition) {
            {
                partition.getBaseIndex();
                minTimes = 0;
                result = index;

                partition.getIndex(30000L);
                minTimes = 0;
                result = index;
            }
        };

        OlapTable table = new OlapTable();
        new Expectations(table) {
            {
                table.getBaseSchema();
                minTimes = 0;
                result = Lists.newArrayList(column1, column2);

                table.getPartition(40000L);
                minTimes = 0;
                result = partition;

                table.getColumn("k1");
                minTimes = 0;
                result = column3;

                table.getColumn("k2");
                minTimes = 0;
                result = column4;

                table.getColumn("k3");
                minTimes = 0;
                result = column5;

                table.getColumn("k4");
                minTimes = 0;
                result = column6;
            }
        };
        return table;
    }

    public static Database mockDb(String name) {
        Database db = new Database();
        OlapTable olapTable = mockTable("testTable");

        new Expectations(db) {
            {
                db.getTableNullable("testTable");
                minTimes = 0;
                result = olapTable;

                db.getTableNullable("t");
                minTimes = 0;
                result = olapTable;

                db.getTableNullable("emptyTable");
                minTimes = 0;
                result = null;

                db.getTableNamesWithLock();
                minTimes = 0;
                result = Sets.newHashSet("testTable");

                db.getTables();
                minTimes = 0;
                result = Lists.newArrayList(olapTable);

                db.readLock();
                minTimes = 0;

                db.readUnlock();
                minTimes = 0;

                db.getFullName();
                minTimes = 0;
                result = name;
            }
        };
        return db;
    }

    public static Env fetchBlockCatalog() {
        try {
            Env env = Deencapsulation.newInstance(Env.class);

            AccessControllerManager accessManager = fetchBlockAccess();
            Database db = mockDb("testDb");

            InternalCatalog catalog = Deencapsulation.newInstance(InternalCatalog.class);
            new Expectations(catalog) {
                {
                    catalog.getDbNullable("testDb");
                    minTimes = 0;
                    result = db;

                    catalog.getDbNullable("testdb");
                    minTimes = 0;
                    result = db;

                    catalog.getDbOrAnalysisException("testdb");
                    minTimes = 0;
                    result = db;

                    catalog.getDbNullable("emptyDb");
                    minTimes = 0;
                    result = null;

                    catalog.getDbNullable(anyString);
                    minTimes = 0;
                    result = new Database();

                    catalog.getDbNames();
                    minTimes = 0;
                    result = Lists.newArrayList("testDb");

                    catalog.getDbNullable("emptyCluster");
                    minTimes = 0;
                    result = null;
                }
            };

            CatalogMgr ctlMgr = new CatalogMgr();
            new Expectations(ctlMgr) {
                {
                    ctlMgr.getCatalog((String) any);
                    minTimes = 0;
                    result = catalog;

                    ctlMgr.getCatalogOrException((String) any, (Function) any);
                    minTimes = 0;
                    result = catalog;

                    ctlMgr.getCatalogOrAnalysisException((String) any);
                    minTimes = 0;
                    result = catalog;
                }
            };

            new Expectations(env) {
                {
                    env.getAccessManager();
                    minTimes = 0;
                    result = accessManager;

                    env.changeDb((ConnectContext) any, anyString);
                    minTimes = 0;
                    result = new DdlException("failed");

                    env.getInternalCatalog();
                    minTimes = 0;
                    result = catalog;

                    env.getCurrentCatalog();
                    minTimes = 0;
                    result = catalog;

                    env.getCatalogMgr();
                    minTimes = 0;
                    result = ctlMgr;

                    env.isCheckpointThread();
                    minTimes = 0;
                    result = false;
                }
            };
            return env;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    public static Analyzer fetchAdminAnalyzer(boolean withCluster) {
        final String prefix = "";

        Analyzer analyzer = new Analyzer(fetchAdminCatalog(), new ConnectContext());
        new Expectations(analyzer) {
            {
                analyzer.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;

                analyzer.getDefaultDb();
                minTimes = 0;
                result = withCluster ? prefix + "testDb" : "testDb";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = withCluster ? prefix + "testUser" : "testUser";

                analyzer.incrementCallDepth();
                minTimes = 0;
                result = 1;

                analyzer.decrementCallDepth();
                minTimes = 0;
                result = 0;

                analyzer.getCallDepth();
                minTimes = 0;
                result = 1;
            }
        };
        return analyzer;
    }

    public static Analyzer fetchBlockAnalyzer() throws AnalysisException {
        Analyzer analyzer = new Analyzer(fetchBlockCatalog(), new ConnectContext());
        new Expectations(analyzer) {
            {
                analyzer.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;

                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testUser";
            }
        };
        return analyzer;
    }

    public static Analyzer fetchEmptyDbAnalyzer() {
        Analyzer analyzer = new Analyzer(fetchBlockCatalog(), new ConnectContext());
        new Expectations(analyzer) {
            {
                analyzer.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;

                analyzer.getDefaultDb();
                minTimes = 0;
                result = "";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testUser";
            }
        };
        return analyzer;
    }

    public static Analyzer fetchTableAnalyzer() throws AnalysisException {
        Column column1 = new Column("k1", PrimitiveType.VARCHAR);
        Column column2 = new Column("k2", PrimitiveType.VARCHAR);
        Column column3 = new Column("k3", PrimitiveType.VARCHAR);
        Column column4 = new Column("k4", PrimitiveType.BIGINT);

        MaterializedIndex index = new MaterializedIndex();
        new Expectations(index) {
            {
                index.getId();
                minTimes = 0;
                result = 30000L;
            }
        };

        Partition partition = Deencapsulation.newInstance(Partition.class);
        new Expectations(partition) {
            {
                partition.getBaseIndex();
                minTimes = 0;
                result = index;

                partition.getIndex(30000L);
                minTimes = 0;
                result = index;
            }
        };

        OlapTable table = new OlapTable();
        new Expectations(table) {
            {
                table.getBaseSchema();
                minTimes = 0;
                result = Lists.newArrayList(column1, column2, column3, column4);

                table.getPartition(40000L);
                minTimes = 0;
                result = partition;

                table.getColumn("k1");
                minTimes = 0;
                result = column1;

                table.getColumn("k2");
                minTimes = 0;
                result = column2;

                table.getColumn("k3");
                minTimes = 0;
                result = column3;

                table.getColumn("k4");
                minTimes = 0;
                result = column4;
            }
        };

        Database db = new Database();

        new Expectations(db) {
            {
                db.getTableNullable("t");
                minTimes = 0;
                result = table;

                db.getTableNullable("emptyTable");
                minTimes = 0;
                result = null;

                db.getTableNamesWithLock();
                minTimes = 0;
                result = Sets.newHashSet("t");

                db.getTables();
                minTimes = 0;
                result = Lists.newArrayList(table);

                db.readLock();
                minTimes = 0;

                db.readUnlock();
                minTimes = 0;

                db.getFullName();
                minTimes = 0;
                result = "testDb";
            }
        };

        Env env = fetchBlockCatalog();
        Analyzer analyzer = new Analyzer(env, new ConnectContext());
        new Expectations(analyzer) {
            {
                analyzer.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;

                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testUser";

                analyzer.getEnv();
                minTimes = 0;
                result = env;

                analyzer.incrementCallDepth();
                minTimes = 0;
                result = 1;

                analyzer.decrementCallDepth();
                minTimes = 0;
                result = 0;

                analyzer.getCallDepth();
                minTimes = 0;
                result = 1;

                analyzer.getContext();
                minTimes = 0;
                result = new ConnectContext();

            }
        };
        return analyzer;
    }
}
