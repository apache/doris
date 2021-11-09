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

import mockit.Expectations;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
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
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


import java.util.LinkedList;
import java.util.List;

public class AccessTestUtil {
    private static FakeEditLog fakeEditLog;

    public static SystemInfoService fetchSystemInfoService() {
        SystemInfoService clusterInfo = new SystemInfoService();
        return clusterInfo;
    }
    
    public static PaloAuth fetchAdminAccess() {
        PaloAuth auth = new PaloAuth();
        try {
            new Expectations(auth) {
                {
                    auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                    minTimes = 0;
                    result = true;

                    auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                    minTimes = 0;
                    result = true;

                    auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                    minTimes = 0;
                    result = true;

                    auth.setPassword((SetPassVar) any);
                    minTimes = 0;
                }
            };
        } catch (DdlException e) {
            e.printStackTrace();
        }
        return auth;
    }

    public static Catalog fetchAdminCatalog() {
        try {
            Catalog catalog = Deencapsulation.newInstance(Catalog.class);

            PaloAuth paloAuth = fetchAdminAccess();

            fakeEditLog = new FakeEditLog();
            EditLog editLog = new EditLog("name");
            catalog.setEditLog(editLog);

            Database db = new Database(50000L, "testCluster:testDb");
            MaterializedIndex baseIndex = new MaterializedIndex(30001, IndexState.NORMAL);
            RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
            Partition partition = new Partition(20000L, "testTbl", baseIndex, distributionInfo);
            List<Column> baseSchema = new LinkedList<Column>();
            Column column = new Column();
            baseSchema.add(column);
            OlapTable table = new OlapTable(30000, "testTbl", baseSchema,
                    KeysType.AGG_KEYS, new SinglePartitionInfo(), distributionInfo);
            table.setIndexMeta(baseIndex.getId(), "testTbl", baseSchema, 0, 1, (short) 1,
                    TStorageType.COLUMN, KeysType.AGG_KEYS);
            table.addPartition(partition);
            table.setBaseIndexId(baseIndex.getId());
            db.createTable(table);

            new Expectations(catalog) {
                {
                    catalog.getAuth();
                    minTimes = 0;
                    result = paloAuth;

                    catalog.getDbNullable(50000L);
                    minTimes = 0;
                    result = db;

                    catalog.getDbNullable("testCluster:testDb");
                    minTimes = 0;
                    result = db;

                    catalog.getDbNullable("testCluster:emptyDb");
                    minTimes = 0;
                    result = null;

                    catalog.getDbNullable(anyString);
                    minTimes = 0;
                    result = new Database();

                    catalog.getDbNames();
                    minTimes = 0;
                    result = Lists.newArrayList("testCluster:testDb");

                    catalog.getEditLog();
                    minTimes = 0;
                    result = editLog;

                    catalog.getLoadInstance();
                    minTimes = 0;
                    result = new Load();

                    catalog.getClusterDbNames("testCluster");
                    minTimes = 0;
                    result = Lists.newArrayList("testCluster:testDb");

                    catalog.changeDb((ConnectContext) any, "blockDb");
                    minTimes = 0;
                    result = new DdlException("failed");

                    catalog.changeDb((ConnectContext) any, anyString);
                    minTimes = 0;

                    catalog.getBrokerMgr();
                    minTimes = 0;
                    result = new BrokerMgr();
                }
            };
            return catalog;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    public static PaloAuth fetchBlockAccess() {
        PaloAuth auth = new PaloAuth();
        new Expectations(auth) {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = false;
            }
        };
        return auth;
    }

    public static OlapTable mockTable(String name) {
        Column column1 = new Column("col1", PrimitiveType.BIGINT);
        Column column2 = new Column("col2", PrimitiveType.DOUBLE);

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

    public static Catalog fetchBlockCatalog() {
        try {
            Catalog catalog = Deencapsulation.newInstance(Catalog.class);

            PaloAuth paloAuth = fetchBlockAccess();
            Database db = mockDb("testCluster:testDb");

            new Expectations(catalog) {
                {
                    catalog.getAuth();
                    minTimes = 0;
                    result = paloAuth;

                    catalog.changeDb((ConnectContext) any, anyString);
                    minTimes = 0;
                    result = new DdlException("failed");

                    catalog.getDbNullable("testCluster:testDb");
                    minTimes = 0;
                    result = db;

                    catalog.getDbNullable("testCluster:emptyDb");
                    minTimes = 0;
                    result = null;

                    catalog.getDbNullable(anyString);
                    minTimes = 0;
                    result = new Database();

                    catalog.getDbNames();
                    minTimes = 0;
                    result = Lists.newArrayList("testCluster:testDb");

                    catalog.getClusterDbNames("testCluster");
                    minTimes = 0;
                    result = Lists.newArrayList("testCluster:testDb");

                    catalog.getDbNullable("emptyCluster");
                    minTimes = 0;
                    result = null;
                }
            };
            return catalog;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    public static Analyzer fetchAdminAnalyzer(boolean withCluster) {
        final String prefix = "testCluster:";

        Analyzer analyzer = new Analyzer(fetchAdminCatalog(), new ConnectContext(null));
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = withCluster? prefix + "testDb" : "testDb";

                analyzer.getQualifiedUser();
                minTimes = 0 ;
                result = withCluster? prefix + "testUser" : "testUser";

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";

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
        Analyzer analyzer = new Analyzer(fetchBlockCatalog(), new ConnectContext(null));
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testCluster:testDb";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testCluster:testUser";

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";
            }
        };
        return analyzer;
    }

    public static Analyzer fetchEmptyDbAnalyzer() {
        Analyzer analyzer = new Analyzer(fetchBlockCatalog(), new ConnectContext(null));
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testCluster:testUser";

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";
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
        Catalog catalog = fetchBlockCatalog();
        Analyzer analyzer = new Analyzer(catalog, new ConnectContext(null));
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getTableOrAnalysisException((TableName) any);
                minTimes = 0;
                result = table;

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testUser";

                analyzer.getCatalog();
                minTimes = 0;
                result = catalog;

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";

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
                result = new ConnectContext(null);

            }
        };
        return analyzer;
    }
}

