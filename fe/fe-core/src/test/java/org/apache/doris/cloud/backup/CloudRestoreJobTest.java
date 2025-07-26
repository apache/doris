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

package org.apache.doris.cloud.backup;

import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.backup.BackupJobInfo;
import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.backup.Repository;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.StorageVaultMgr;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TSortType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class CloudRestoreJobTest {

    public static final String TEST_DB_NAME = "test_db";
    public static final long TEST_DB_ID = 10000;
    public static final String TEST_TBL_NAME = "test_tbl";
    public static final long TEST_TBL_ID = 20000;
    public static final long TEST_PARTITION_ID = 30000;
    public static final long TEST_TABLET_ID = 40000;
    public static final long TEST_VERSION = 10;
    public static final String TEST_LABEL = "test_label";
    private Database db;
    private BackupJobInfo jobInfo;
    private CloudRestoreJob job;
    private OlapTable expectedRestoreTbl;
    private long repoId = 20000;

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static Env cloudEnv;
    private SystemInfoService cloudSystemInfoService;
    private ConnectContext ctx;
    @Mocked
    private StorageVaultMgr storageVaultMgr;

    @Injectable
    private Repository repo = new Repository(repoId, "repo", false, "bos://my_repo",
            FileSystemFactory.get(BrokerProperties.of("broker", Maps.newHashMap())));

    @Before
    public void setUp() throws Exception {
        Config.cloud_unique_id = "test_unique_id";
        Config.meta_service_endpoint = "127.0.0.1:11111";
        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        EnvFactory envFactory = EnvFactory.getInstance();
        cloudEnv = envFactory.createEnv(false);
        Deencapsulation.setField(cloudEnv, "enableStorageVault", true);
        Deencapsulation.setField(cloudEnv, "storageVaultMgr", storageVaultMgr);
        FakeEnv.setEnv(cloudEnv);
        cloudSystemInfoService = cloudEnv.getClusterInfo();
        FakeEnv.setSystemInfo(cloudSystemInfoService);
        ctx = new ConnectContext();
        ctx.setEnv(cloudEnv);
        ctx.setThreadLocalInfo();
        ctx.setCloudCluster("test_compute_group");

        Assert.assertTrue(cloudEnv instanceof CloudEnv);
        Assert.assertTrue(cloudSystemInfoService instanceof CloudSystemInfoService);

        new Expectations() {
            {
                storageVaultMgr.getVaultNameById(anyString);
                minTimes = 0;
                result = "test_vault";
            }
        };

        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.CreateTabletsResponse createTablets(Cloud.CreateTabletsRequest request) {
                Cloud.CreateTabletsResponse.Builder responseBuilder = Cloud.CreateTabletsResponse.newBuilder();
                responseBuilder.setStatus(
                        Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK).setMsg("OK"));
                return responseBuilder.build();
            }

            @Mock
            public Cloud.PartitionResponse preparePartition(Cloud.PartitionRequest request) {
                Cloud.PartitionResponse.Builder builder = Cloud.PartitionResponse.newBuilder();
                builder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"));
                return builder.build();
            }

            @Mock
            public Cloud.PartitionResponse commitPartition(Cloud.PartitionRequest request) {
                Cloud.PartitionResponse.Builder builder = Cloud.PartitionResponse.newBuilder();
                builder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"));
                return builder.build();
            }

            @Mock
            public Cloud.PartitionResponse dropPartition(Cloud.PartitionRequest request) {
                Cloud.PartitionResponse.Builder builder = Cloud.PartitionResponse.newBuilder();
                builder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"));
                return builder.build();
            }
        };

        db = CatalogTestUtil.createSimpleDb(TEST_DB_ID, TEST_TBL_ID, TEST_TBL_ID, TEST_PARTITION_ID, TEST_TABLET_ID, TEST_VERSION);
        cloudEnv.unprotectCreateDb(db);

        // gen BackupJobInfo
        jobInfo = new BackupJobInfo();
        jobInfo.backupTime = System.currentTimeMillis();
        jobInfo.dbId = TEST_DB_ID;
        jobInfo.dbName = TEST_DB_NAME;
        jobInfo.name = TEST_LABEL;
        jobInfo.success = true;

        expectedRestoreTbl = (OlapTable) db.getTableNullable(TEST_TBL_ID);
        Map<String, String> properties = Maps.newHashMap();
        properties.put("storage_vault_id", "test_vault_id");
        TableProperty tableProperty = new TableProperty(properties);
        expectedRestoreTbl.setTableProperty(tableProperty);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        expectedRestoreTbl.setDataSortInfo(dataSortInfo);

        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = TEST_TBL_ID;
        jobInfo.backupOlapTableObjects.put(TEST_TBL_NAME, tblInfo);

        for (Partition partition : expectedRestoreTbl.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = partition.getId();
            tblInfo.partitions.put(partition.getName(), partInfo);

            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.schemaHash = expectedRestoreTbl.getSchemaHashByIndexId(index.getId());
                partInfo.indexes.put(expectedRestoreTbl.getIndexNameById(index.getId()), idxInfo);

                for (Tablet tablet : index.getTablets()) {
                    List<String> files = Lists.newArrayList(tablet.getId() + ".dat",
                            tablet.getId() + ".idx", tablet.getId() + ".hdr");
                    BackupTabletInfo tabletInfo = new BackupTabletInfo(tablet.getId(), files);
                    idxInfo.sortedTabletInfoList.add(tabletInfo);
                }
            }
        }

        // drop this table, cause we want to try restoring this table
        db.unregisterTable(expectedRestoreTbl.getName());

        job = new CloudRestoreJob(TEST_LABEL, "2025-07-01 01:01:01", db.getId(), db.getFullName(), jobInfo,
                false, new ReplicaAllocation((short) 1), 100000, -1, false,
                false, false, false, false,
                false, false, cloudEnv, repo.getId(), "test_vault");
    }

    @Test
    public void testStorageVaultCheck() throws UserException {
        // Case 1: Storage vault exists
        job.checkStorageVault(expectedRestoreTbl);
        Assert.assertTrue(job.getStatus().ok());

        // Case 2: Storage vault does not exist
        Map<String, String> properties = Maps.newHashMap();
        properties.put("storage_vault_id", "");
        TableProperty tableProperty = new TableProperty(properties);
        expectedRestoreTbl.setTableProperty(tableProperty);
        job.checkStorageVault(expectedRestoreTbl);
        Assert.assertFalse(job.getStatus().ok());
    }

    @Test
    public void testCloudClusterCheck() throws UserException {
        // Case 1: Cloud cluster exists
        new MockUp<CloudSystemInfoService>() {
            @Mock
            public String getCloudClusterIdByName(String clusterName) {
                return "test_cluster_id";
            }
        };
        job.checkIfNeedCancel();
        Assert.assertTrue(job.getStatus().ok());

        // Case 2: Cloud cluster not exists
        new MockUp<CloudSystemInfoService>() {
            @Mock
            public String getCloudClusterIdByName(String clusterName) {
                return null;
            }
        };
        job.checkIfNeedCancel();
        Assert.assertFalse(job.getStatus().ok());
    }

    @Test
    public void testCreateReplicas() throws UserException {
        for (Partition expectedRestorePart : expectedRestoreTbl.getPartitions()) {
            job.createReplicas(db, expectedRestoreTbl, expectedRestorePart, null);
        }
        Assert.assertTrue(job.getStatus().ok());
        job.doCreateReplicas();
        Assert.assertTrue(job.getStatus().ok());
    }

}

