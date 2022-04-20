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

package org.apache.doris.alter;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import mockit.Expectations;
import mockit.Injectable;

public class MigrationHandlerTest {

    private static Collection<Long> partitionIds = new ArrayList();

    @BeforeClass
    public static void setup() throws Exception {
        partitionIds.add(1001L);
    }

    @Test
    public void testCreateJobRollup(@Injectable OlapTable olapTable) {
        MigrationHandler migrationHandler = new MigrationHandler();
        new Expectations() {
            {
                olapTable.getState();
                result = OlapTable.OlapTableState.ROLLUP;
                olapTable.getName();
                result = "tableName";
            }
        };

        TStorageMedium destStorageMedium = TStorageMedium.S3;
        try {
            Deencapsulation.invoke(migrationHandler, "createJob",
                    new Long(1), olapTable, partitionIds, destStorageMedium);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    @Test
    public void testCreateJobSuc(@Injectable OlapTable olapTable, @Injectable Partition partition,
                                 @Injectable MaterializedIndex originIndex, @Injectable Tablet originTablet,
                                 @Injectable Replica originReplica, @Injectable PartitionInfo partitionInfo,
                                 @Injectable Catalog catalog, @Injectable EditLog editlog) {
        long orignIndexId = 30L;
        long partitionId = 1001L;
        long backendId = 301L;
        MigrationHandler migrationHandler = new MigrationHandler();
        Map<Long, List<Column>> columnMap = new HashMap<>();
        columnMap.put(orignIndexId, new ArrayList<>());
        columnMap.get(orignIndexId).add(new Column());
        Collection<Partition> partitions = new ArrayList<>();
        partitions.add(partition);
        List<Tablet> tablets = new ArrayList<>();
        tablets.add(originTablet);
        List<Replica> originReplicas = new ArrayList<>();
        originReplicas.add(originReplica);
        DataProperty dataProperty = new DataProperty(TStorageMedium.SSD);
        dataProperty.setMigrationState(DataProperty.MigrationState.RUNNING);
        Catalog.getCurrentCatalog().setEditLog(editlog);
        new Expectations() {
            {
                olapTable.getState();
                result = OlapTable.OlapTableState.NORMAL;
                olapTable.getName();
                result = "tableName";
                olapTable.getId();
                result = 10L;
                olapTable.getIndexIdToSchema(anyBoolean);
                result = columnMap;
                olapTable.getPartitions();
                result = partitions;
                partition.getId();
                result = partitionId;
                olapTable.getPartitionInfo();
                result = partitionInfo;
                partitionInfo.getDataProperty(partitionId);
                result = dataProperty;
                partition.getIndex(orignIndexId);
                result = originIndex;
                originIndex.getTablets();
                result = tablets;
                originTablet.getReplicas();
                result = originReplicas;
                originReplica.getBackendId();
                result = backendId;
                originReplica.getState();
                result = Replica.ReplicaState.NORMAL;
            }
        };

        Collection<Long> partitionIds = new ArrayList();
        partitionIds.add(partitionId);
        TStorageMedium destStorageMedium = TStorageMedium.S3;
        Deencapsulation.invoke(migrationHandler, "createJob",
                new Long(1), olapTable, partitionIds, destStorageMedium);
    }

    @Test
    public void testCreateMigrationJobs(@Injectable Database database, @Injectable OlapTable table,
                                        @Injectable PartitionInfo partitionInfo, @Injectable Partition partition,
                                        @Injectable Cluster cluster, @Injectable EditLog editlog) {
        long clusterId = 10L;
        String clusterName = "clusterName";
        long dbId = 100L;
        long tableId = 2000L;
        long partitionId = 30000L;
        String partitionName = "partName";
        List<Table> tableList = new ArrayList<>();
        tableList.add(table);
        Collection<Partition> partitions = new ArrayList<>();
        partitions.add(partition);
        DataProperty dataProperty = new DataProperty(
                TStorageMedium.SSD, 0L, "", 0L);
        dataProperty.setMigrationState(DataProperty.MigrationState.NONE);
        Catalog.getCurrentCatalog().setEditLog(editlog);
        new Expectations() {
            {
                cluster.getId();
                result = clusterId;
                cluster.getName();
                result = clusterName;
                database.getId();
                result = dbId;
                database.getFullName();
                result = "dbName";
                database.getTables();
                result = tableList;
                database.getClusterName();
                result = clusterName;
                table.getId();
                result = tableId;
                table.getType();
                result = Table.TableType.OLAP;
                table.getPartitionInfo();
                result = partitionInfo;
                partitionInfo.getDataProperty(partitionId);
                result = dataProperty;
                table.getAllPartitions();
                result = partitions;
                partition.getId();
                result = partitionId;
                partition.getName();
                result = partitionName;
                database.getTableNullable(tableId);
                result = table;
                table.tryWriteLock(anyLong, TimeUnit.MILLISECONDS);
                result = true;
                table.isWriteLockHeldByCurrentThread();
                result = true;
                table.getState();
                result = OlapTable.OlapTableState.NORMAL;
            }
        };
        Catalog.getCurrentCatalog().addCluster(cluster);
        Catalog.getCurrentCatalog().unprotectCreateDb(database);
        MigrationHandler migrationHandler = new MigrationHandler();
        Deencapsulation.invoke(migrationHandler, "createMigrationJobs");
    }
}