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
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CatalogTestUtil {

    public static String testDb1 = "testDb1";
    public static long testDbId1 = 1;
    public static String testTable1 = "testTable1";
    public static long testTableId1 = 2;
    public static String testPartition1 = "testPartition1";
    public static long testPartitionId1 = 3;
    public static String testIndex1 = "testIndex1";
    public static String testIndex2 = "testIndex2";
    public static long testIndexId1 = 2; // the base indexId == tableId
    public static int testSchemaHash1 = 93423942;
    public static long testBackendId1 = 5;
    public static long testBackendId2 = 6;
    public static long testBackendId3 = 7;
    public static long testReplicaId1 = 8;
    public static long testReplicaId2 = 9;
    public static long testReplicaId3 = 10;
    public static long testTabletId1 = 11;
    public static long testStartVersion = 12;
    public static long testStartVersionHash = 12312;
    public static long testPartitionCurrentVersionHash = 12312;
    public static long testPartitionNextVersionHash = 123123123;
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

    public static Catalog createTestCatalog() throws InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        Constructor<Catalog> constructor = Catalog.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        Catalog catalog = constructor.newInstance();
        catalog.setEditLog(new EditLog("name"));
        FakeCatalog.setCatalog(catalog);
        Backend backend1 = createBackend(testBackendId1, "host1", 123, 124, 125);
        Backend backend2 = createBackend(testBackendId2, "host2", 123, 124, 125);
        Backend backend3 = createBackend(testBackendId3, "host3", 123, 124, 125);
        backend1.setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);
        backend2.setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);
        backend3.setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);
        Catalog.getCurrentSystemInfo().addBackend(backend1);
        Catalog.getCurrentSystemInfo().addBackend(backend2);
        Catalog.getCurrentSystemInfo().addBackend(backend3);
        catalog.initDefaultCluster();
        Database db = createSimpleDb(testDbId1, testTableId1, testPartitionId1, testIndexId1, testTabletId1,
                testStartVersion, testStartVersionHash);
        catalog.unprotectCreateDb(db);
        return catalog;
    }

    public static boolean compareCatalog(Catalog masterCatalog, Catalog slaveCatalog) {
        Database masterDb = masterCatalog.getDb(testDb1);
        Database slaveDb = slaveCatalog.getDb(testDb1);
        List<Table> tables = masterDb.getTables();
        for (Table table : tables) {
            Table slaveTable = slaveDb.getTable(table.getId());
            if (slaveTable == null) {
                return false;
            }
            Partition masterPartition = table.getPartition(testPartition1);
            Partition slavePartition = slaveTable.getPartition(testPartition1);
            if (masterPartition == null && slavePartition == null) {
                return true;
            }
            if (masterPartition.getId() != slavePartition.getId()) {
                return false;
            }
            if (masterPartition.getVisibleVersion() != slavePartition.getVisibleVersion()
                    || masterPartition.getVisibleVersionHash() != slavePartition.getVisibleVersionHash()
                    || masterPartition.getNextVersion() != slavePartition.getNextVersion()
                    || masterPartition.getCommittedVersionHash() != slavePartition.getCommittedVersionHash()) {
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
                                || slaveReplica.getVersionHash() != masterReplica.getVersionHash()
                                || slaveReplica.getLastFailedVersion() != masterReplica.getLastFailedVersion()
                                || slaveReplica.getLastFailedVersionHash() != masterReplica.getLastFailedVersionHash()
                                || slaveReplica.getLastSuccessVersion() != slaveReplica.getLastSuccessVersion()
                                || slaveReplica.getLastSuccessVersionHash() != slaveReplica
                                        .getLastSuccessVersionHash()) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    public static Database createSimpleDb(long dbId, long tableId, long partitionId, long indexId, long tabletId,
            long version, long versionHash) {
        Catalog.getCurrentInvertedIndex().clear();

        // replica
        long replicaId = 0;
        Replica replica1 = new Replica(testReplicaId1, testBackendId1, version, versionHash, 0, 0L, 0L,
                ReplicaState.NORMAL, -1, 0, 0, 0);
        Replica replica2 = new Replica(testReplicaId2, testBackendId2, version, versionHash, 0, 0L, 0L,
                ReplicaState.NORMAL, -1, 0, 0, 0);
        Replica replica3 = new Replica(testReplicaId3, testBackendId3, version, versionHash, 0, 0L, 0L,
                ReplicaState.NORMAL, -1, 0, 0, 0);

        // tablet
        Tablet tablet = new Tablet(tabletId);

        // index
        MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta);

        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // partition
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition partition = new Partition(partitionId, testPartition1, index, distributionInfo);
        partition.updateVisibleVersionAndVersionHash(testStartVersion, testStartVersionHash);
        partition.setNextVersion(testStartVersion + 1);
        partition.setNextVersionHash(testPartitionNextVersionHash, testPartitionCurrentVersionHash);

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

        // table
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
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
        db.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        
        // add a es table to catalog
        try {
            createEsTable(db);
        } catch (DdlException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
        }
        return db;
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
        properties.put(EsTable.HOSTS, "xxx");
        properties.put(EsTable.INDEX, "doe");
        properties.put(EsTable.TYPE, "doc");
        properties.put(EsTable.PASSWORD, "");
        properties.put(EsTable.USER, "root");
        properties.put(EsTable.DOC_VALUE_SCAN, "true");
        properties.put(EsTable.KEYWORD_SNIFF, "true");
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

    public static Backend createBackend(long id, String host, int heartPort, int bePort, int httpPort,
            long totalCapacityB, long avaiLabelCapacityB) {
        Backend backend = createBackend(id, host, heartPort, bePort, httpPort);
        Map<String, TDisk> backendDisks = new HashMap<String, TDisk>();
        String rootPath = "root_path";
        TDisk disk = new TDisk(rootPath, totalCapacityB, avaiLabelCapacityB, true);
        backendDisks.put(rootPath, disk);
        backend.updateDisks(backendDisks);
        backend.setAlive(true);
        return backend;
    }
}
