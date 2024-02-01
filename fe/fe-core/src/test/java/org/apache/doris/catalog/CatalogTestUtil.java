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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CatalogTestUtil {

    public static String testDb1 = "testDb1";
    public static long testDbId1 = 1;
    public static String testTable1 = "testTable1";
    public static String testTable2 = "testTable2";

    public static long testTableId1 = 2;
    public static long testTableId2 = 15;

    public static String testPartition1 = "testPartition1";
    public static String testPartition2 = "testPartition2";

    public static long testPartitionId1 = 3;
    public static long testPartitionId2 = 16;

    public static String testIndex1 = "testIndex1";
    public static String testIndex2 = "testIndex2";

    public static long testIndexId1 = 2; // the base indexId == tableId
    public static long testIndexId2 = 15; // the base indexId == tableId

    public static int testSchemaHash1 = 93423942;
    public static long testBackendId1 = 5;
    public static long testBackendId2 = 6;
    public static long testBackendId3 = 7;
    public static long testReplicaId1 = 8;
    public static long testReplicaId2 = 9;
    public static long testReplicaId3 = 10;
    public static long testReplicaId4 = 17;

    public static long testTabletId1 = 11;
    public static long testTabletId2 = 18;

    public static long testStartVersion = 12;
    public static long testRollupIndexId2 = 13;
    public static String testRollupIndex2 = "newRollupIndex";
    public static String testRollupIndex3 = "newRollupIndex2";
    public static String testTxnLabel1 = "testTxnLabel1";
    public static String testTxnLabel2 = "testTxnLabel2";
    public static String testTxnLabel3 = "testTxnLabel3";
    public static String testTxnLabel4 = "testTxnLabel4";
    public static String testTxnLabel5 = "testTxnLabel5";
    public static String testTxnLabel6 = "testTxnLabel6";
    public static String testTxnLabel7 = "testTxnLabel7";
    public static String testTxnLabel8 = "testTxnLabel8";
    public static String testTxnLabel9 = "testTxnLabel9";
    public static String testTxnLabel10 = "testTxnLabel10";
    public static String testTxnLabel11 = "testTxnLabel11";
    public static String testTxnLabel12 = "testTxnLabel12";
    public static String testEsTable1 = "partitionedEsTable1";
    public static long testEsTableId1 = 14;

    public static Env createTestCatalog()
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        Constructor<Env> constructor = Env.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        Env env = constructor.newInstance();
        env.setEditLog(new EditLog("name"));
        FakeEnv.setEnv(env);
        Backend backend1 = createBackend(testBackendId1, "host1", 123, 124, 125);
        Backend backend2 = createBackend(testBackendId2, "host2", 123, 124, 125);
        Backend backend3 = createBackend(testBackendId3, "host3", 123, 124, 125);
        DiskInfo diskInfo = new DiskInfo("/path/to/disk1/");
        diskInfo.setAvailableCapacityB(2 << 40); // 1TB
        diskInfo.setTotalCapacityB(2 << 40);
        diskInfo.setDataUsedCapacityB(2 << 10);
        backend1.setDisks(ImmutableMap.of("disk1", diskInfo));
        backend2.setDisks(ImmutableMap.of("disk1", diskInfo));
        backend3.setDisks(ImmutableMap.of("disk1", diskInfo));
        Env.getCurrentSystemInfo().addBackend(backend1);
        Env.getCurrentSystemInfo().addBackend(backend2);
        Env.getCurrentSystemInfo().addBackend(backend3);

        Database db = createSimpleDb(testDbId1, testTableId1, testPartitionId1, testIndexId1, testTabletId1,
                testStartVersion);
        env.unprotectCreateDb(db);
        return env;
    }

    public static boolean compareCatalog(Env masterEnv, Env slaveEnv) {
        try {
            Database masterDb = masterEnv.getInternalCatalog().getDbOrMetaException(testDb1);
            Database slaveDb = slaveEnv.getInternalCatalog().getDbOrMetaException(testDb1);
            List<Table> tables = masterDb.getTables();
            for (Table table : tables) {
                Table slaveTable = slaveDb.getTableOrMetaException(table.getId());
                Partition masterPartition = table.getPartition(testPartition1);
                Partition slavePartition = slaveTable.getPartition(testPartition1);
                if (masterPartition == null && slavePartition == null) {
                    return true;
                }
                if (masterPartition.getId() != slavePartition.getId()) {
                    return false;
                }
                if (masterPartition.getVisibleVersion() != slavePartition.getVisibleVersion()
                        || masterPartition.getNextVersion() != slavePartition.getNextVersion()) {
                    return false;
                }
                List<MaterializedIndex> allMaterializedIndices = masterPartition.getMaterializedIndices(IndexExtState.ALL);
                for (MaterializedIndex masterIndex : allMaterializedIndices) {
                    MaterializedIndex slaveIndex = slavePartition.getIndex(masterIndex.getId());
                    if (slaveIndex == null) {
                        return false;
                    }
                    List<Tablet> allTablets = masterIndex.getTablets();
                    for (Tablet masterTablet : allTablets) {
                        Tablet slaveTablet = slaveIndex.getTablet(masterTablet.getId());
                        if (slaveTablet == null) {
                            return false;
                        }
                        List<Replica> allReplicas = masterTablet.getReplicas();
                        for (Replica masterReplica : allReplicas) {
                            Replica slaveReplica = slaveTablet.getReplicaById(masterReplica.getId());
                            if (slaveReplica.getBackendId() != masterReplica.getBackendId()
                                    || slaveReplica.getVersion() != masterReplica.getVersion()
                                    || slaveReplica.getLastFailedVersion() != masterReplica.getLastFailedVersion()
                                    || slaveReplica.getLastSuccessVersion() != masterReplica.getLastSuccessVersion()) {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        } catch (MetaNotFoundException e) {
            return false;
        }
    }

    public static Database createSimpleDb(long dbId, long tableId, long partitionId, long indexId, long tabletId,
            long version) {
        Env.getCurrentInvertedIndex().clear();

        // replica
        Replica replica1 = new Replica(testReplicaId1, testBackendId1, version, 0, 0L, 0L, 0L,
                ReplicaState.NORMAL, -1, 0);
        Replica replica2 = new Replica(testReplicaId2, testBackendId2, version, 0, 0L, 0L, 0L,
                ReplicaState.NORMAL, -1, 0);
        Replica replica3 = new Replica(testReplicaId3, testBackendId3, version, 0, 0L, 0L, 0L,
                ReplicaState.NORMAL, -1, 0);

        // tablet
        Tablet tablet = new Tablet(tabletId);

        // index
        MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta);

        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // columns
        List<Column> columns = new ArrayList<Column>();
        Column temp = new Column("k1", PrimitiveType.INT);
        temp.setIsKey(true);
        columns.add(temp);
        temp = new Column("k2", PrimitiveType.INT);
        temp.setIsKey(true);
        columns.add(temp);
        columns.add(new Column("v", ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.SUM, "0", ""));

        List<Column> keysColumn = new ArrayList<Column>();
        temp = new Column("k1", PrimitiveType.INT);
        temp.setIsKey(true);
        keysColumn.add(temp);
        temp = new Column("k2", PrimitiveType.INT);
        temp.setIsKey(true);
        keysColumn.add(temp);

        HashDistributionInfo distributionInfo = new HashDistributionInfo(10, keysColumn);
        Partition partition = new Partition(partitionId, testPartition1, index, distributionInfo);
        partition.updateVisibleVersion(testStartVersion);
        partition.setNextVersion(testStartVersion + 1);

        // table
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
        partitionInfo.setReplicaAllocation(partitionId, new ReplicaAllocation((short) 3));
        OlapTable table = new OlapTable(tableId, testTable1, columns, KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(indexId, testIndex1, columns, 0, testSchemaHash1, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.setBaseIndexId(indexId);
        // db
        Database db = new Database(dbId, testDb1);
        db.createTable(table);

        // add a es table to catalog
        try {
            createEsTable(db);
            createDupTable(db);
        } catch (DdlException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
        }
        return db;
    }

    public static void createDupTable(Database db) {

        // replica
        Replica replica = new Replica(testReplicaId4, testBackendId1, testStartVersion, 0, 0L, 0L, 0L,
                ReplicaState.NORMAL, -1, 0);

        // tablet
        Tablet tablet = new Tablet(testTabletId2);

        // index
        MaterializedIndex index = new MaterializedIndex(testIndexId2, IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(testDbId1, testTableId2, testPartitionId2, testIndexId2, 0,
                TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta);

        tablet.addReplica(replica);

        // partition
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(1);
        Partition partition = new Partition(testPartitionId2, testPartition2, index, distributionInfo);
        partition.updateVisibleVersion(testStartVersion);
        partition.setNextVersion(testStartVersion + 1);

        // columns
        List<Column> columns = new ArrayList<Column>();
        Column temp = new Column("k1", PrimitiveType.INT);
        temp.setIsKey(true);
        columns.add(temp);
        temp = new Column("k2", PrimitiveType.INT);
        temp.setIsKey(true);
        columns.add(temp);
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.VARCHAR), false, AggregateType.NONE, "0", ""));
        columns.add(new Column("v2", ScalarType.createType(PrimitiveType.VARCHAR), false, AggregateType.NONE, "0", ""));

        // table
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(testPartitionId2, new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
        partitionInfo.setReplicaAllocation(testPartitionId2, new ReplicaAllocation((short) 1));
        OlapTable table = new OlapTable(testTableId2, testTable2, columns, KeysType.DUP_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(testIndexId2, testIndex2, columns, 0, testSchemaHash1, (short) 3,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(testIndexId2);
        // db
        db.createTable(table);
    }

    public static void createEsTable(Database db) throws DdlException {
        // columns
        List<Column> columns = new ArrayList<>();
        Column userId = new Column("userId", PrimitiveType.VARCHAR);
        columns.add(userId);
        columns.add(new Column("time", PrimitiveType.BIGINT));
        columns.add(new Column("type", PrimitiveType.VARCHAR));

        // table
        List<Column> partitionColumns = Lists.newArrayList();
        List<SinglePartitionDesc> singlePartitionDescs = Lists.newArrayList();
        partitionColumns.add(userId);

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createLessThan(Lists.newArrayList(new PartitionValue("100"))),
                null));

        RangePartitionInfo partitionInfo = new RangePartitionInfo(partitionColumns);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(EsResource.HOSTS, "xxx");
        properties.put(EsResource.INDEX, "doe");
        properties.put(EsResource.TYPE, "doc");
        properties.put(EsResource.PASSWORD, "");
        properties.put(EsResource.USER, "root");
        properties.put(EsResource.DOC_VALUE_SCAN, "true");
        properties.put(EsResource.KEYWORD_SNIFF, "true");
        EsTable esTable = new EsTable(testEsTableId1, testEsTable1,
                columns, properties, partitionInfo);
        db.createTable(esTable);
    }

    public static Backend createBackend(long id, String host, int heartPort, int bePort, int httpPort) {
        Backend backend = new Backend(id, host, heartPort);
        // backend.updateOnce(bePort, httpPort, 10000);
        backend.setAlive(true);
        return backend;
    }

    public static long getTabletDataSize(long tabletId) {
        Env env = Env.getCurrentEnv();
        TabletInvertedIndex invertedIndex = env.getTabletInvertedIndex();
        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
        if (tabletMeta == null) {
            return -1L;
        }

        long dbId = tabletMeta.getDbId();
        long tableId = tabletMeta.getTableId();
        long partitionId = tabletMeta.getPartitionId();
        long indexId = tabletMeta.getIndexId();
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            return -1L;
        }
        Table table = db.getTableNullable(tableId);
        if (table == null) {
            return -1L;
        }
        if (table.getType() != Table.TableType.OLAP) {
            return -1L;
        }
        OlapTable olapTable = (OlapTable) table;
        olapTable.readLock();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                return -1L;
            }
            MaterializedIndex materializedIndex = partition.getIndex(indexId);
            if (materializedIndex == null) {
                return -1L;
            }
            Tablet tablet = materializedIndex.getTablet(tabletId);
            if (tablet == null) {
                return -1L;
            }
            return tablet.getDataSize(true);
        } finally {
            olapTable.readUnlock();
        }
    }

    public static long getReplicaPathHash(long tabletId, long backendId) {
        Env env = Env.getCurrentEnv();
        TabletInvertedIndex invertedIndex = env.getTabletInvertedIndex();
        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
        if (tabletMeta == null) {
            return -1L;
        }

        long dbId = tabletMeta.getDbId();
        long tableId = tabletMeta.getTableId();
        long partitionId = tabletMeta.getPartitionId();
        long indexId = tabletMeta.getIndexId();
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            return -1L;
        }
        Table table = db.getTableNullable(tableId);
        if (table == null) {
            return -1L;
        }
        if (table.getType() != Table.TableType.OLAP) {
            return -1L;
        }
        OlapTable olapTable = (OlapTable) table;
        olapTable.readLock();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                return -1L;
            }
            MaterializedIndex materializedIndex = partition.getIndex(indexId);
            if (materializedIndex == null) {
                return -1L;
            }
            Tablet tablet = materializedIndex.getTablet(tabletId);
            for (Replica replica : tablet.getReplicas()) {
                if (replica.getBackendId() == backendId) {
                    return replica.getPathHash();
                }
            }
        } finally {
            olapTable.readUnlock();
        }
        return -1;
    }
}
