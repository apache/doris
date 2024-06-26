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

package org.apache.doris.backup;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import mockit.Expectations;

import java.util.List;
import java.util.Map;

public class CatalogMocker {
    // user
    public static final String ROOTUSER = "root";
    public static final String SUPERUSER = "superuser";
    public static final String TESTUSER = "testuser";
    public static final String BLOCKUSER = "blockuser";

    // backend
    public static final long BACKEND1_ID = 10000;
    public static final long BACKEND2_ID = 10001;
    public static final long BACKEND3_ID = 10002;

    // db
    public static final String TEST_DB_NAME = "test_db";
    public static final long TEST_DB_ID = 20000;

    // single partition olap table
    public static final String TEST_TBL_NAME = "test_tbl";
    public static final long TEST_TBL_ID = 30000;

    public static final String TEST_SINGLE_PARTITION_NAME = TEST_TBL_NAME;
    public static final long TEST_SINGLE_PARTITION_ID = 40000;
    public static final long TEST_TABLET0_ID = 60000;
    public static final long TEST_REPLICA0_ID = 70000;
    public static final long TEST_REPLICA1_ID = 70001;
    public static final long TEST_REPLICA2_ID = 70002;

    // mysql table
    public static final long TEST_MYSQL_TABLE_ID = 30001;
    public static final String MYSQL_TABLE_NAME = "test_mysql";
    public static final String MYSQL_HOST = "mysql-host";
    public static final long MYSQL_PORT = 8321;
    public static final String MYSQL_USER = "mysql-user";
    public static final String MYSQL_PWD = "mysql-pwd";
    public static final String MYSQL_DB = "mysql-db";
    public static final String MYSQL_TBL = "mysql-tbl";

    // partition olap table with a rollup
    public static final String TEST_TBL2_NAME = "test_tbl2";
    public static final long TEST_TBL2_ID = 30002;

    public static final String TEST_PARTITION1_NAME = "p1";
    public static final long TEST_PARTITION1_ID = 40001;
    public static final String TEST_PARTITION2_NAME = "p2";
    public static final long TEST_PARTITION2_ID = 40002;
    public static final long TEST_BASE_TABLET_P1_ID = 60001;
    public static final long TEST_REPLICA3_ID = 70003;
    public static final long TEST_REPLICA4_ID = 70004;
    public static final long TEST_REPLICA5_ID = 70005;

    public static final long TEST_BASE_TABLET_P2_ID = 60002;
    public static final long TEST_REPLICA6_ID = 70006;
    public static final long TEST_REPLICA7_ID = 70007;
    public static final long TEST_REPLICA8_ID = 70008;

    public static final String TEST_ROLLUP_NAME = "test_rollup";
    public static final long TEST_ROLLUP_ID = 50000;

    public static final long TEST_ROLLUP_TABLET_P1_ID = 60003;
    public static final long TEST_REPLICA9_ID = 70009;
    public static final long TEST_REPLICA10_ID = 70010;
    public static final long TEST_REPLICA11_ID = 70011;

    public static final long TEST_ROLLUP_TABLET_P2_ID = 60004;
    public static final long TEST_REPLICA12_ID = 70012;
    public static final long TEST_REPLICA13_ID = 70013;
    public static final long TEST_REPLICA14_ID = 70014;

    public static final String WRONG_DB = "wrong_db";

    // schema
    public static List<Column> TEST_TBL_BASE_SCHEMA = Lists.newArrayList();
    public static int SCHEMA_HASH;
    public static int ROLLUP_SCHEMA_HASH;

    public static List<Column> TEST_MYSQL_SCHEMA = Lists.newArrayList();
    public static List<Column> TEST_ROLLUP_SCHEMA = Lists.newArrayList();

    static {
        Column k1 = new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", "key1");
        Column k2 = new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", "key2");
        Column k3 = new Column("k3", ScalarType.createType(PrimitiveType.INT), true, null, "", "key3");
        Column k4 = new Column("k4", ScalarType.createType(PrimitiveType.BIGINT), true, null, "", "key4");
        Column k5 = new Column("k5", ScalarType.createType(PrimitiveType.LARGEINT), true, null, "", "key5");
        Column k6 = new Column("k6", ScalarType.createType(PrimitiveType.DATE), true, null, "", "key6");
        Column k7 = new Column("k7", ScalarType.createType(PrimitiveType.DATETIME), true, null, "", "key7");
        Column k8 = new Column("k8", ScalarType.createDecimalType(10, 3), true, null, "", "key8");
        k1.setIsKey(true);
        k2.setIsKey(true);
        k3.setIsKey(true);
        k4.setIsKey(true);
        k5.setIsKey(true);
        k6.setIsKey(true);
        k7.setIsKey(true);
        k8.setIsKey(true);

        Column v1 = new Column("v1",
                ScalarType.createChar(10), false, AggregateType.REPLACE, "none", " value1");
        Column v2 = new Column("v2",
                ScalarType.createType(PrimitiveType.FLOAT), false, AggregateType.MAX, "none", " value2");
        Column v3 = new Column("v3",
                ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.MIN, "none", " value3");
        Column v4 = new Column("v4",
                ScalarType.createType(PrimitiveType.INT), false, AggregateType.SUM, "", " value4");

        TEST_TBL_BASE_SCHEMA.add(k1);
        TEST_TBL_BASE_SCHEMA.add(k2);
        TEST_TBL_BASE_SCHEMA.add(k3);
        TEST_TBL_BASE_SCHEMA.add(k4);
        TEST_TBL_BASE_SCHEMA.add(k5);
        TEST_TBL_BASE_SCHEMA.add(k6);
        TEST_TBL_BASE_SCHEMA.add(k7);
        TEST_TBL_BASE_SCHEMA.add(k8);

        TEST_TBL_BASE_SCHEMA.add(v1);
        TEST_TBL_BASE_SCHEMA.add(v2);
        TEST_TBL_BASE_SCHEMA.add(v3);
        TEST_TBL_BASE_SCHEMA.add(v4);

        TEST_MYSQL_SCHEMA.add(k1);
        TEST_MYSQL_SCHEMA.add(k2);
        TEST_MYSQL_SCHEMA.add(k3);
        TEST_MYSQL_SCHEMA.add(k4);
        TEST_MYSQL_SCHEMA.add(k5);
        TEST_MYSQL_SCHEMA.add(k6);
        TEST_MYSQL_SCHEMA.add(k7);
        TEST_MYSQL_SCHEMA.add(k8);

        TEST_ROLLUP_SCHEMA.add(k1);
        TEST_ROLLUP_SCHEMA.add(k2);
        TEST_ROLLUP_SCHEMA.add(v1);

        SCHEMA_HASH = Util.generateSchemaHash();
        ROLLUP_SCHEMA_HASH = Util.generateSchemaHash();
    }

    private static AccessControllerManager fetchAdminAccess() {
        AccessControllerManager accessManager = new AccessControllerManager(new Auth());
        new Expectations(accessManager) {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                accessManager.checkDbPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };
        return accessManager;
    }

    public static SystemInfoService fetchSystemInfoService() {
        SystemInfoService clusterInfo = new SystemInfoService();
        return clusterInfo;
    }

    public static Database mockDb() throws UserException {
        // mock all meta obj
        Database db = new Database(TEST_DB_ID, TEST_DB_NAME);

        // 1. single partition olap table
        MaterializedIndex baseIndex = new MaterializedIndex(TEST_TBL_ID, IndexState.NORMAL);
        DistributionInfo distributionInfo = new RandomDistributionInfo(32);
        Partition partition =
                new Partition(TEST_SINGLE_PARTITION_ID, TEST_SINGLE_PARTITION_NAME, baseIndex, distributionInfo);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicaAllocation(TEST_SINGLE_PARTITION_ID, new ReplicaAllocation((short) 3));
        partitionInfo.setIsInMemory(TEST_SINGLE_PARTITION_ID, false);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        partitionInfo.setDataProperty(TEST_SINGLE_PARTITION_ID, dataProperty);
        OlapTable olapTable = new OlapTable(TEST_TBL_ID, TEST_TBL_NAME, TEST_TBL_BASE_SCHEMA,
                                            KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(olapTable, "baseIndexId", TEST_TBL_ID);

        Tablet tablet0 = new Tablet(TEST_TABLET0_ID);
        TabletMeta tabletMeta = new TabletMeta(TEST_DB_ID, TEST_TBL_ID, TEST_SINGLE_PARTITION_ID,
                                               TEST_TBL_ID, SCHEMA_HASH, TStorageMedium.HDD);
        baseIndex.addTablet(tablet0, tabletMeta);
        Replica replica0 = new Replica(TEST_REPLICA0_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica1 = new Replica(TEST_REPLICA1_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica2 = new Replica(TEST_REPLICA2_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        tablet0.addReplica(replica0);
        tablet0.addReplica(replica1);
        tablet0.addReplica(replica2);

        olapTable.setIndexMeta(TEST_TBL_ID, TEST_TBL_NAME, TEST_TBL_BASE_SCHEMA, 0, SCHEMA_HASH, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        olapTable.addPartition(partition);
        db.registerTable(olapTable);

        // 2. mysql table
        Map<String, String> mysqlProp = Maps.newHashMap();
        mysqlProp.put("host", MYSQL_HOST);
        mysqlProp.put("port", String.valueOf(MYSQL_PORT));
        mysqlProp.put("user", MYSQL_USER);
        mysqlProp.put("password", MYSQL_PWD);
        mysqlProp.put("database", MYSQL_DB);
        mysqlProp.put("table", MYSQL_TBL);
        MysqlTable mysqlTable = null;
        try {
            mysqlTable = new MysqlTable(TEST_MYSQL_TABLE_ID, MYSQL_TABLE_NAME, TEST_MYSQL_SCHEMA, mysqlProp);
        } catch (DdlException e) {
            e.printStackTrace();
        }
        db.registerTable(mysqlTable);

        // 3. range partition olap table
        MaterializedIndex baseIndexP1 = new MaterializedIndex(TEST_TBL2_ID, IndexState.NORMAL);
        MaterializedIndex baseIndexP2 = new MaterializedIndex(TEST_TBL2_ID, IndexState.NORMAL);
        DistributionInfo distributionInfo2 =
                new HashDistributionInfo(32, Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(1)));
        Partition partition1 =
                new Partition(TEST_PARTITION1_ID, TEST_PARTITION1_NAME, baseIndexP1, distributionInfo2);
        Partition partition2 =
                new Partition(TEST_PARTITION2_ID, TEST_PARTITION2_NAME, baseIndexP2, distributionInfo2);
        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));

        PartitionKey rangeP1Lower =
                PartitionKey.createInfinityPartitionKey(Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)), false);
        PartitionKey rangeP1Upper =
                PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                                                Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
        Range<PartitionKey> rangeP1 = Range.closedOpen(rangeP1Lower, rangeP1Upper);
        PartitionItem item1 = new RangePartitionItem(rangeP1);
        rangePartitionInfo.setItem(TEST_PARTITION1_ID, false, item1);

        PartitionKey rangeP2Lower =
                PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                        Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
        PartitionKey rangeP2Upper =
                PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("20")),
                        Lists.newArrayList(TEST_TBL_BASE_SCHEMA.get(0)));
        Range<PartitionKey> rangeP2 = Range.closedOpen(rangeP2Lower, rangeP2Upper);
        PartitionItem item2 = new RangePartitionItem(rangeP2);
        rangePartitionInfo.setItem(TEST_PARTITION1_ID, false, item2);

        rangePartitionInfo.setReplicaAllocation(TEST_PARTITION1_ID, new ReplicaAllocation((short) 3));
        rangePartitionInfo.setReplicaAllocation(TEST_PARTITION2_ID, new ReplicaAllocation((short) 3));
        DataProperty dataPropertyP1 = new DataProperty(TStorageMedium.HDD);
        DataProperty dataPropertyP2 = new DataProperty(TStorageMedium.HDD);
        rangePartitionInfo.setDataProperty(TEST_PARTITION1_ID, dataPropertyP1);
        rangePartitionInfo.setDataProperty(TEST_PARTITION2_ID, dataPropertyP2);

        OlapTable olapTable2 = new OlapTable(TEST_TBL2_ID, TEST_TBL2_NAME, TEST_TBL_BASE_SCHEMA,
                KeysType.AGG_KEYS, rangePartitionInfo, distributionInfo2);
        Deencapsulation.setField(olapTable2, "baseIndexId", TEST_TBL2_ID);

        Tablet baseTabletP1 = new Tablet(TEST_BASE_TABLET_P1_ID);
        TabletMeta tabletMetaBaseTabletP1 = new TabletMeta(TEST_DB_ID, TEST_TBL2_ID, TEST_PARTITION1_ID,
                                                           TEST_TBL2_ID, SCHEMA_HASH, TStorageMedium.HDD);
        baseIndexP1.addTablet(baseTabletP1, tabletMetaBaseTabletP1);
        Replica replica3 = new Replica(TEST_REPLICA3_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica4 = new Replica(TEST_REPLICA4_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica5 = new Replica(TEST_REPLICA5_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        baseTabletP1.addReplica(replica3);
        baseTabletP1.addReplica(replica4);
        baseTabletP1.addReplica(replica5);

        Tablet baseTabletP2 = new Tablet(TEST_BASE_TABLET_P2_ID);
        TabletMeta tabletMetaBaseTabletP2 = new TabletMeta(TEST_DB_ID, TEST_TBL2_ID, TEST_PARTITION2_ID,
                                                           TEST_TBL2_ID, SCHEMA_HASH, TStorageMedium.HDD);
        baseIndexP2.addTablet(baseTabletP2, tabletMetaBaseTabletP2);
        Replica replica6 = new Replica(TEST_REPLICA6_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica7 = new Replica(TEST_REPLICA7_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica8 = new Replica(TEST_REPLICA8_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        baseTabletP2.addReplica(replica6);
        baseTabletP2.addReplica(replica7);
        baseTabletP2.addReplica(replica8);


        olapTable2.setIndexMeta(TEST_TBL2_ID, TEST_TBL2_NAME, TEST_TBL_BASE_SCHEMA, 0, SCHEMA_HASH, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        olapTable2.addPartition(partition1);
        olapTable2.addPartition(partition2);

        // rollup index p1
        MaterializedIndex rollupIndexP1 = new MaterializedIndex(TEST_ROLLUP_ID, IndexState.NORMAL);
        Tablet rollupTabletP1 = new Tablet(TEST_ROLLUP_TABLET_P1_ID);
        TabletMeta tabletMetaRollupTabletP1 = new TabletMeta(TEST_DB_ID, TEST_TBL2_ID, TEST_PARTITION1_ID,
                                                             TEST_ROLLUP_TABLET_P1_ID, ROLLUP_SCHEMA_HASH,
                                                             TStorageMedium.HDD);
        rollupIndexP1.addTablet(rollupTabletP1, tabletMetaRollupTabletP1);
        Replica replica9 = new Replica(TEST_REPLICA9_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica10 = new Replica(TEST_REPLICA10_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica11 = new Replica(TEST_REPLICA11_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);

        rollupTabletP1.addReplica(replica9);
        rollupTabletP1.addReplica(replica10);
        rollupTabletP1.addReplica(replica11);

        partition1.createRollupIndex(rollupIndexP1);

        // rollup index p2
        MaterializedIndex rollupIndexP2 = new MaterializedIndex(TEST_ROLLUP_ID, IndexState.NORMAL);
        Tablet rollupTabletP2 = new Tablet(TEST_ROLLUP_TABLET_P2_ID);
        TabletMeta tabletMetaRollupTabletP2 = new TabletMeta(TEST_DB_ID, TEST_TBL2_ID, TEST_PARTITION1_ID,
                                                             TEST_ROLLUP_TABLET_P2_ID, ROLLUP_SCHEMA_HASH,
                                                             TStorageMedium.HDD);
        rollupIndexP2.addTablet(rollupTabletP2, tabletMetaRollupTabletP2);
        Replica replica12 = new Replica(TEST_REPLICA12_ID, BACKEND1_ID, 0, ReplicaState.NORMAL);
        Replica replica13 = new Replica(TEST_REPLICA13_ID, BACKEND2_ID, 0, ReplicaState.NORMAL);
        Replica replica14 = new Replica(TEST_REPLICA14_ID, BACKEND3_ID, 0, ReplicaState.NORMAL);
        rollupTabletP2.addReplica(replica12);
        rollupTabletP2.addReplica(replica13);
        rollupTabletP2.addReplica(replica14);

        partition2.createRollupIndex(rollupIndexP2);

        olapTable2.setIndexMeta(TEST_ROLLUP_ID, TEST_ROLLUP_NAME, TEST_ROLLUP_SCHEMA, 0, ROLLUP_SCHEMA_HASH,
                                      (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS);
        db.registerTable(olapTable2);

        return db;
    }

    public static Env fetchAdminCatalog() {
        try {
            FakeEditLog fakeEditLog = new FakeEditLog(); // CHECKSTYLE IGNORE THIS LINE

            Env env = Deencapsulation.newInstance(Env.class);
            InternalCatalog catalog = Deencapsulation.newInstance(InternalCatalog.class);

            Database db = new Database();
            AccessControllerManager accessManager = fetchAdminAccess();

            new Expectations(env, catalog) {
                {
                    env.getAccessManager();
                    minTimes = 0;
                    result = accessManager;

                    env.getInternalCatalog();
                    minTimes = 0;
                    result = catalog;

                    env.getCurrentCatalog();
                    minTimes = 0;
                    result = catalog;

                    catalog.getDbNullable(TEST_DB_NAME);
                    minTimes = 0;
                    result = db;

                    catalog.getDbNullable(WRONG_DB);
                    minTimes = 0;
                    result = null;

                    catalog.getDbNullable(TEST_DB_ID);
                    minTimes = 0;
                    result = db;

                    catalog.getDbNullable(anyString);
                    minTimes = 0;
                    result = new Database();

                    catalog.getDbNames();
                    minTimes = 0;
                    result = Lists.newArrayList(TEST_DB_NAME);

                    env.getLoadInstance();
                    minTimes = 0;
                    result = new Load();

                    env.getEditLog();
                    minTimes = 0;
                    result = new EditLog("name");

                    env.changeDb((ConnectContext) any, WRONG_DB);
                    minTimes = 0;
                    result = new DdlException("failed");

                    env.changeDb((ConnectContext) any, anyString);
                    minTimes = 0;
                }
            };
            return env;
        } catch (DdlException e) {
            return null;
        }
    }
}
