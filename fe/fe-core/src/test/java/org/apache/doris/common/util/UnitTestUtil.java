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

package org.apache.doris.common.util;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.DppConfig;
import org.apache.doris.load.Load;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.collect.Maps;

import org.junit.Assert;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// for unit test
public class UnitTestUtil {
    public static final String DB_NAME = "testDb";
    public static final String TABLE_NAME = "testTable";
    public static final String PARTITION_NAME = "testTable";
    public static final int SCHEMA_HASH = 0;

    public static Database createDb(long dbId, long tableId, long partitionId, long indexId,
                                    long tabletId, long backendId, long version, long versionHash) {
        // Catalog.getCurrentInvertedIndex().clear();

        // replica
        long replicaId = 0;
        Replica replica1 = new Replica(replicaId, backendId, ReplicaState.NORMAL, version, versionHash, 0);
        Replica replica2 = new Replica(replicaId + 1, backendId + 1, ReplicaState.NORMAL, version, versionHash, 0);
        Replica replica3 = new Replica(replicaId + 2, backendId + 2, ReplicaState.NORMAL, version, versionHash, 0);
        
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
        Partition partition = new Partition(partitionId, PARTITION_NAME, index, distributionInfo);

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
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        OlapTable table = new OlapTable(tableId, TABLE_NAME, columns,
                                        KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, TABLE_NAME, columns, 0, SCHEMA_HASH, (short) 1, TStorageType.COLUMN,
                KeysType.AGG_KEYS);

        // db
        Database db = new Database(dbId, DB_NAME);
        db.createTable(table);
        return db;
    }
    
    public static Backend createBackend(long id, String host, int heartPort, int bePort, int httpPort) {
        Backend backend = new Backend(id, host, heartPort);
        backend.updateOnce(bePort, httpPort, 10000);
        return backend;
    }

    public static Backend createBackend(long id, String host, int heartPort, int bePort, int httpPort,
                                        long totalCapacityB, long availableCapacityB) {
        Backend backend = createBackend(id, host, heartPort, bePort, httpPort);
        Map<String, TDisk> backendDisks = new HashMap<String, TDisk>();
        String rootPath = "root_path";
        TDisk disk = new TDisk(rootPath, totalCapacityB, availableCapacityB, true);
        backendDisks.put(rootPath, disk);
        backend.updateDisks(backendDisks);
        return backend;
    }
    
    public static Method getPrivateMethod(Class c, String methodName, Class[] params) {
        Method method = null;
        try {
            method = c.getDeclaredMethod(methodName, params);
            method.setAccessible(true);
        } catch (NoSuchMethodException e) {
            Assert.fail(e.getMessage());
        }
        return method;
    }
    
    public static Class getInnerClass(Class c, String className) {
        Class innerClass = null;
        for (Class tmpClass : c.getDeclaredClasses()) {
            if (tmpClass.getName().equals(className)) {
                innerClass = tmpClass;
                break;
            }
        }
        return innerClass;
    }
    
    public static void initDppConfig() {
        Map<String, String> defaultConfigs = Maps.newHashMap();
        defaultConfigs.put("hadoop_palo_path", "/user/palo2");
        defaultConfigs.put("hadoop_http_port", "1234");
        defaultConfigs.put("hadoop_configs",
                "mapred.job.tracker=host:111;fs.default.name=hdfs://host:112;hadoop.job.ugi=user,password");

        try {
            Load.dppDefaultConfig = DppConfig.create(defaultConfigs);
            Load.clusterToDppConfig.put(Config.dpp_default_cluster, Load.dppDefaultConfig.getCopiedDppConfig());
        } catch (LoadException e) {
            e.printStackTrace();
        }
    }

}
