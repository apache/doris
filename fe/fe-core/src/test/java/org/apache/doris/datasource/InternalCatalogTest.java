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

package org.apache.doris.datasource;

import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.DdlException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class InternalCatalogTest {
    private static final long TABLE_ID = 1000L;
    private static final long INDEX_ID = 1000L;
    private static final long PARTITION_ID = 1001L;
    private static final long TABLET_ID = 1002L;
    private static final long BACKEND_ID = 1003L;
    private static final long REPLICA_ID = 1004L;
    private static final long BACKEND_ID_2 = 1005L;
    private static final long BACKEND_ID_3 = 1006L;
    private static final String TABLE_NAME = "range_table";
    private static final String PARTITION_NAME = "p0";
    private static final String NEW_PARTITION_NAME = "p_commit_failed";

    private Database db;
    private FailingCommitInternalCatalog catalog;
    private FakeEnv fakeEnv;

    @Before
    public void setUp() throws Exception {
        fakeEnv = new FakeEnv();
        Env env = new TestingEnv();
        FakeEnv.setEnv(env);
        FakeEnv.setSystemInfo(createSystemInfoService());
        db = createRangePartitionDb();
        catalog = new FailingCommitInternalCatalog();
    }

    @Test
    public void testAddPartitionRollbackPartitionInfoOnCommitFailure() throws Exception {
        AddPartitionClause addPartitionClause = createAddPartitionClause();
        DdlException exception = Assert.assertThrows(DdlException.class,
                () -> catalog.addPartition(db, TABLE_NAME, addPartitionClause, false, 0, true, null));
        Assert.assertTrue(exception.getMessage().contains("injected commit failure"));
        long newPartitionId = catalog.getCommittedPartitionId();

        OlapTable table = (OlapTable) db.getTableOrDdlException(TABLE_NAME);
        Assert.assertNull(table.getPartition(NEW_PARTITION_NAME));
        Assert.assertNull(table.getPartition(newPartitionId));

        PartitionInfo partitionInfo = table.getPartitionInfo();
        Assert.assertNull(partitionInfo.getItem(newPartitionId));
        Assert.assertNull(partitionInfo.getDataProperty(newPartitionId));
        Assert.assertEquals(ReplicaAllocation.DEFAULT_ALLOCATION, partitionInfo.getReplicaAllocation(newPartitionId));
    }

    private AddPartitionClause createAddPartitionClause() {
        SinglePartitionDesc singlePartitionDesc = new SinglePartitionDesc(false, NEW_PARTITION_NAME,
                PartitionKeyDesc.createLessThan(Lists.newArrayList(new PartitionValue("20"))), Maps.newHashMap());
        return new AddPartitionClause(singlePartitionDesc, null, Maps.newHashMap(), false);
    }

    private SystemInfoService createSystemInfoService() {
        SystemInfoService systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(createBackend(BACKEND_ID, "host1"));
        systemInfoService.addBackend(createBackend(BACKEND_ID_2, "host2"));
        systemInfoService.addBackend(createBackend(BACKEND_ID_3, "host3"));
        return systemInfoService;
    }

    private Backend createBackend(long backendId, String host) {
        Backend backend = new Backend(backendId, host, 9050);
        DiskInfo diskInfo = new DiskInfo("/path/to/disk1/");
        diskInfo.setAvailableCapacityB(2L << 40);
        diskInfo.setTotalCapacityB(2L << 40);
        backend.setDisks(ImmutableMap.of("disk1", diskInfo));
        backend.setAlive(true);
        return backend;
    }

    private Database createRangePartitionDb() throws Exception {
        Column keyColumn = new Column("k1", ScalarType.INT);
        keyColumn.setIsKey(true);
        Column valueColumn = new Column("v1", ScalarType.INT);
        List<Column> columns = Lists.newArrayList(keyColumn, valueColumn);

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(CatalogTestUtil.testDbId1, TABLE_ID, PARTITION_ID, INDEX_ID, 0,
                TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);
        tablet.addReplica(new LocalReplica(REPLICA_ID, BACKEND_ID, 0, ReplicaState.NORMAL));

        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList(keyColumn));
        Partition partition = new Partition(PARTITION_ID, PARTITION_NAME, baseIndex, distributionInfo);
        partition.updateVisibleVersion(1L);

        RangePartitionInfo partitionInfo = new RangePartitionInfo(Lists.newArrayList(keyColumn));
        PartitionKey lower = PartitionKey.createInfinityPartitionKey(Lists.newArrayList(keyColumn), false);
        PartitionKey upper = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                Lists.newArrayList(keyColumn));
        PartitionItem partitionItem = new RangePartitionItem(Range.closedOpen(lower, upper));
        partitionInfo.setItem(PARTITION_ID, false, partitionItem);
        partitionInfo.setDataProperty(PARTITION_ID, new DataProperty(TStorageMedium.HDD));
        partitionInfo.setReplicaAllocation(PARTITION_ID, ReplicaAllocation.DEFAULT_ALLOCATION);

        OlapTable table = new OlapTable(TABLE_ID, TABLE_NAME, columns, KeysType.DUP_KEYS, partitionInfo,
                distributionInfo);
        table.setTableProperty(new TableProperty(Maps.newHashMap()));
        table.setIndexMeta(INDEX_ID, TABLE_NAME, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(INDEX_ID);
        table.addPartition(partition);

        Database database = new Database(CatalogTestUtil.testDbId1, CatalogTestUtil.testDb1);
        database.registerTable(table);
        return database;
    }

    private static class FailingCommitInternalCatalog extends InternalCatalog {
        private long committedPartitionId;

        @Override
        protected Partition createPartitionWithIndices(long dbId, OlapTable tbl, long partitionId,
                String partitionName, Map<Long, MaterializedIndexMeta> indexIdToMeta,
                DistributionInfo distributionInfo, DataProperty dataProperty,
                ReplicaAllocation replicaAlloc, Long versionInfo, Set<String> bfColumns, Set<Long> tabletIdSet,
                boolean isInMemory, TTabletType tabletType, String storagePolicy,
                IdGeneratorBuffer idGeneratorBuffer, BinlogConfig binlogConfig, boolean isStorageMediumSpecified)
                throws DdlException {
            MaterializedIndex baseIndex = new MaterializedIndex(tbl.getBaseIndexId(), IndexState.NORMAL);
            Partition partition = new Partition(partitionId, partitionName, baseIndex, distributionInfo);
            if (versionInfo != null) {
                partition.updateVisibleVersion(versionInfo);
                partition.setNextVersion(versionInfo + 1);
            }
            return partition;
        }

        @Override
        public void afterCreatePartitions(long dbId, long tableId, List<Long> partitionIds, List<Long> indexIds,
                boolean isCreateTable, boolean isBatchCommit, OlapTable olapTable) throws DdlException {
            Assert.assertEquals(TABLE_ID, tableId);
            Assert.assertEquals(1, partitionIds.size());
            committedPartitionId = partitionIds.get(0);
            throw new DdlException("injected commit failure");
        }

        public long getCommittedPartitionId() {
            return committedPartitionId;
        }
    }

    private static class TestingEnv extends Env {
        private final HostInfo selfNode = new HostInfo("127.0.0.1", 9010);

        private TestingEnv() throws Exception {
            super(false);
        }

        @Override
        public HostInfo getSelfNode() {
            return selfNode;
        }
    }
}
