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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CloudProcVersionDisplayTest {
    private static final long DB_ID = 10001L;
    private static final long TABLE_ID = 20001L;
    private static final long PARTITION_ID = 30001L;
    private static final long INDEX_ID = 40001L;
    private static final long TABLET_ID = 50001L;
    private static final long REPLICA_ID = 60001L;

    private static final long PARTITION_VISIBLE_VERSION = 88L;
    private static final long STALE_REPLICA_VERSION = 1L;
    private static final int SCHEMA_HASH = 123456789;

    @Mocked
    private Env env;
    @Mocked
    private SystemInfoService systemInfoService;
    @Mocked
    private TabletInvertedIndex invertedIndex;
    @Mocked
    private InternalCatalog internalCatalog;

    private String originDeployMode;
    private String originCloudUniqueId;
    private boolean originEnableQueryHitStats;

    @Before
    public void setUp() throws AnalysisException {
        originDeployMode = Config.deploy_mode;
        originCloudUniqueId = Config.cloud_unique_id;
        originEnableQueryHitStats = Config.enable_query_hit_stats;
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        Config.enable_query_hit_stats = false;

        new Expectations(env) {
            {
                Env.getServingEnv();
                minTimes = 0;
                result = env;

                env.isReady();
                minTimes = 0;
                result = true;

                Env.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.getAllBackendsByAllCluster();
                minTimes = 0;
                result = ImmutableMap.of();
            }
        };
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originDeployMode;
        Config.cloud_unique_id = originCloudUniqueId;
        Config.enable_query_hit_stats = originEnableQueryHitStats;
    }

    @Test
    public void testIndicesLookupPropagatesPartitionCachedVersionToTabletsProc() throws AnalysisException {
        ProcTestContext context = createProcTestContext();

        IndicesProcDir indicesProcDir = new IndicesProcDir(context.db, context.table, context.partition);
        ProcNodeInterface procNode = indicesProcDir.lookup(String.valueOf(INDEX_ID));
        Assert.assertTrue(procNode instanceof TabletsProcDir);

        ProcResult result = procNode.fetchResult();
        Assert.assertEquals(1, result.getRows().size());
        assertVersionColumns(result, PARTITION_VISIBLE_VERSION);
    }

    @Test
    public void testReplicasProcNodeShowsPartitionCachedVersionInCloudMode() throws AnalysisException {
        ProcTestContext context = createProcTestContext();

        new Expectations(env) {
            {
                Env.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                invertedIndex.getTabletMeta(TABLET_ID);
                minTimes = 0;
                result = context.tabletMeta;

                Env.getCurrentInternalCatalog();
                minTimes = 0;
                result = internalCatalog;

                internalCatalog.getDbNullable(DB_ID);
                minTimes = 0;
                result = context.db;
            }
        };

        ReplicasProcNode procNode = new ReplicasProcNode(TABLET_ID, context.tablet.getReplicas());
        ProcResult result = procNode.fetchResult();
        Assert.assertEquals(1, result.getRows().size());
        assertVersionColumns(result, PARTITION_VISIBLE_VERSION);
    }

    private void assertVersionColumns(ProcResult result, long expectedVersion) {
        int versionIndex = result.getColumnNames().indexOf("Version");
        int lastSuccessVersionIndex = result.getColumnNames().indexOf("LstSuccessVersion");
        Assert.assertTrue(versionIndex >= 0);
        Assert.assertTrue(lastSuccessVersionIndex >= 0);

        String expected = String.valueOf(expectedVersion);
        Assert.assertEquals(expected, result.getRows().get(0).get(versionIndex));
        Assert.assertEquals(expected, result.getRows().get(0).get(lastSuccessVersionIndex));
    }

    private ProcTestContext createProcTestContext() {
        Column keyColumn = new Column("k1", PrimitiveType.INT);
        keyColumn.setIsKey(true);
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(1);

        MaterializedIndex index = new MaterializedIndex(INDEX_ID, IndexState.NORMAL);
        Tablet tablet = new LocalTablet(TABLET_ID);
        CloudReplica replica = new CloudReplica(REPLICA_ID, 1L, Replica.ReplicaState.NORMAL,
                STALE_REPLICA_VERSION, SCHEMA_HASH, DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, 0L);
        tablet.addReplica(replica, true);

        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, SCHEMA_HASH,
                TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta, true);

        CloudPartition partition = new CloudPartition(PARTITION_ID, "p1", index, distributionInfo, DB_ID, TABLE_ID);
        partition.setCachedVisibleVersion(PARTITION_VISIBLE_VERSION, System.currentTimeMillis());

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
        partitionInfo.setReplicaAllocation(PARTITION_ID, new ReplicaAllocation((short) 1));

        OlapTable table = new OlapTable(TABLE_ID, "tbl_cloud_proc", Lists.newArrayList(keyColumn),
                KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(INDEX_ID, "base_index", Lists.newArrayList(keyColumn), 0, SCHEMA_HASH, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(INDEX_ID);

        Database db = new Database(DB_ID, "db_cloud_proc");
        db.registerTable(table);

        return new ProcTestContext(db, table, partition, tablet, tabletMeta);
    }

    private static class ProcTestContext {
        private final Database db;
        private final OlapTable table;
        private final CloudPartition partition;
        private final Tablet tablet;
        private final TabletMeta tabletMeta;

        private ProcTestContext(Database db, OlapTable table, CloudPartition partition, Tablet tablet,
                TabletMeta tabletMeta) {
            this.db = db;
            this.table = table;
            this.partition = partition;
            this.tablet = tablet;
            this.tabletMeta = tabletMeta;
        }
    }
}
