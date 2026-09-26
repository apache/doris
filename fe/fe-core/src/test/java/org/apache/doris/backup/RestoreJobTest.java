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
import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo.HashType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand.BackupContent;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;

public class RestoreJobTest {

    private Database db;
    private BackupJobInfo jobInfo;
    private RestoreJob job;
    private String label = "test_label";

    private AtomicLong id = new AtomicLong(50000);

    private OlapTable expectedRestoreTbl;

    private long repoId = 20000;

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    public RestoreJobTest() throws UserException {
    }

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockBackupHandler extends BackupHandler {
        public MockBackupHandler(Env env) {
            super(env);
        }

        @Override
        public RepositoryMgr getRepoMgr() {
            return repoMgr;
        }
    }

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockRepositoryMgr extends RepositoryMgr {
        public MockRepositoryMgr() {
            super();
        }

        @Override
        public Repository getRepo(long repoId) {
            return repo;
        }
    }

    private EditLog editLog = Mockito.mock(EditLog.class);
    private SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);

    private Repository repo = Mockito.spy(new Repository(repoId, "repo", false, "bos://my_repo",
            StorageAdapter.ofBroker("broker", Maps.newHashMap())));

    private BackupMeta backupMeta;

    private MockedStatic<Env> mockedEnvStatic;
    @SuppressWarnings("rawtypes")
    private MockedConstruction<MarkedCountDownLatch> mockedMarkedCountDownLatch;

    @BeforeEach
    public void setUp() throws Exception {
        db = CatalogMocker.mockDb();
        backupHandler = new MockBackupHandler(env);
        repoMgr = new MockRepositoryMgr();

        Deencapsulation.setField(env, "backupHandler", backupHandler);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
        Mockito.when(env.getNextId()).thenAnswer(inv -> id.getAndIncrement());
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        Mockito.doAnswer(inv -> {
            List<Long> beIds = Lists.newArrayList();
            beIds.add(CatalogMocker.BACKEND1_ID);
            beIds.add(CatalogMocker.BACKEND2_ID);
            beIds.add(CatalogMocker.BACKEND3_ID);
            return beIds;
        }).when(systemInfoService).selectBackendIdsForReplicaCreation(
                Mockito.any(ReplicaAllocation.class),
                Mockito.anyMap(),
                Mockito.any(TStorageMedium.class),
                Mockito.eq(false),
                Mockito.eq(true));

        Mockito.doAnswer(inv -> {
            BackupJob job = inv.getArgument(0);
            System.out.println("log backup job: " + job);
            return null;
        }).when(editLog).logBackupJob(Mockito.any(BackupJob.class));

        Mockito.doReturn(Status.OK).when(repo).upload(Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(inv -> {
            List<BackupMeta> metas = inv.getArgument(1);
            metas.add(backupMeta);
            return Status.OK;
        }).when(repo).getSnapshotMetaFile(Mockito.eq(label), Mockito.anyList(), Mockito.eq(-1));

        mockedMarkedCountDownLatch = Mockito.mockConstruction(MarkedCountDownLatch.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (mock, context) -> {
                    Mockito.doReturn(true).when(mock).await(Mockito.anyLong(), Mockito.any(TimeUnit.class));
                });

        // gen BackupJobInfo
        jobInfo = new BackupJobInfo();
        jobInfo.backupTime = System.currentTimeMillis();
        jobInfo.dbId = CatalogMocker.TEST_DB_ID;
        jobInfo.dbName = CatalogMocker.TEST_DB_NAME;
        jobInfo.name = label;
        jobInfo.success = true;

        expectedRestoreTbl = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_ID);
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = CatalogMocker.TEST_TBL2_ID;
        jobInfo.backupOlapTableObjects.put(CatalogMocker.TEST_TBL2_NAME, tblInfo);

        for (Partition partition : expectedRestoreTbl.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = partition.getId();
            tblInfo.partitions.put(partition.getName(), partInfo);

            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
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

        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(), jobInfo, false,
                new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false, false, false,
                env, repo.getId());

        List<Table> tbls = Lists.newArrayList();
        List<Resource> resources = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls, resources);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        if (mockedMarkedCountDownLatch != null) {
            mockedMarkedCountDownLatch.close();
        }
    }

    @Test
    public void testSignature() throws AnalysisException {
        Adler32 sig1 = new Adler32();
        sig1.update("name1".getBytes());
        sig1.update("name2".getBytes());
        System.out.println("sig1: " + Math.abs((int) sig1.getValue()));

        Adler32 sig2 = new Adler32();
        sig2.update("name2".getBytes());
        sig2.update("name1".getBytes());
        System.out.println("sig2: " + Math.abs((int) sig2.getValue()));

        OlapTable tbl = db.getOlapTableOrAnalysisException(CatalogMocker.TEST_TBL_NAME);
        List<String> partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println(partNames);
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
        tbl.setName("newName");
        partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
    }

    @ParameterizedTest
    @EnumSource(HashType.class)
    public void testRestoreDisjointPartitionWithSameHash(HashType hashType) throws Exception {
        // Bucket counts may differ between partitions; only the table-wide hash must match.
        checkRestoreDisjointPartition(hashType, hashType, 5);
    }

    @ParameterizedTest
    @EnumSource(HashType.class)
    public void testRestoreDisjointPartitionWithDifferentHash(HashType localHash) throws Exception {
        HashType remoteHash = localHash == HashType.CRC32 ? HashType.IDENTITY : HashType.CRC32;
        checkRestoreDisjointPartition(localHash, remoteHash, 8);
    }

    private void checkRestoreDisjointPartition(HashType localHash, HashType remoteHash, int remoteBuckets)
            throws Exception {
        OlapTable local = createHashPartitionedTable(30003L, "p1", 40003L, 0, 10, localHash, 8);
        OlapTable remote = createHashPartitionedTable(30004L, "p2", 40004L, 10, 20, remoteHash, remoteBuckets);
        db.registerTable(local);
        List<String> intersectPartNames = Lists.newArrayList();
        Assertions.assertTrue(local.getIntersectPartNamesWith(remote, intersectPartNames).ok());
        Assertions.assertTrue(intersectPartNames.isEmpty());

        jobInfo.backupOlapTableObjects.clear();
        jobInfo.content = BackupContent.METADATA_ONLY;
        BackupOlapTableInfo tableInfo = new BackupOlapTableInfo();
        tableInfo.id = remote.getId();
        BackupPartitionInfo partitionInfo = new BackupPartitionInfo();
        partitionInfo.id = remote.getPartition("p2").getId();
        tableInfo.partitions.put("p2", partitionInfo);
        jobInfo.backupOlapTableObjects.put(remote.getName(), tableInfo);
        BackupMeta meta = new BackupMeta(Lists.newArrayList(remote), Lists.newArrayList());
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        RestoreJob restore = Mockito.spy(new RestoreJob(label, "2018-01-01 01:01:01",
                db.getId(), db.getFullName(), jobInfo, false,
                new ReplicaAllocation((short) 1), 100000, -1,
                false, false, false, false, false, false, false, false,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, meta));
        // Exercise real metadata validation, partition resetting and attachment. Only skip BE tasks
        // and file mappings: this test has no physical tablets to create or restore.
        Mockito.doNothing().when(restore).createReplicas(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doNothing().when(restore).genFileMapping(Mockito.any(), Mockito.any(),
                Mockito.anyLong(), Mockito.any(), Mockito.anyBoolean());
        Mockito.doNothing().when(restore).doCreateReplicas();
        Deencapsulation.invoke(restore, "checkAndPrepareMeta");
        if (localHash == remoteHash) {
            Assertions.assertTrue(restore.getStatus().ok(), restore.getStatus().toString());
            Assertions.assertEquals(RestoreJob.RestoreJobState.CREATING, restore.getState());
            Assertions.assertEquals(1, restore.restoredPartitions.size());
            restore.allReplicasCreated();
            Assertions.assertSame(remote.getPartition("p2"), local.getPartition("p2"));
            HashDistributionInfo restoredDistribution =
                    (HashDistributionInfo) local.getPartition("p2").getDistributionInfo();
            Assertions.assertEquals(remoteHash, restoredDistribution.getHashType());
            Assertions.assertEquals(remoteBuckets, restoredDistribution.getBucketNum());
        } else {
            Assertions.assertFalse(restore.getStatus().ok(),
                    "Mixed hash restore passed metadata validation: " + localHash + " <- " + remoteHash);
            Assertions.assertTrue(restore.getStatus().getErrMsg().contains("different schema"));
            Assertions.assertTrue(restore.restoredPartitions.isEmpty());
            Assertions.assertNull(local.getPartition("p2"));
        }
    }

    private OlapTable createHashPartitionedTable(long tableId, String partitionName, long partitionId,
            int lower, int upper, HashType hashType, int buckets) throws AnalysisException {
        Column key = new Column("id", PrimitiveType.BIGINT, true);
        Column date = new Column("dt", PrimitiveType.INT, true);
        List<Column> partitionColumns = Lists.newArrayList(date);
        RangePartitionInfo partitionInfo = new RangePartitionInfo(partitionColumns);
        PartitionKey lowerKey = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(Integer.toString(lower))), partitionColumns);
        PartitionKey upperKey = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(Integer.toString(upper))), partitionColumns);
        partitionInfo.addPartition(partitionId, false, new RangePartitionItem(Range.closedOpen(lowerKey, upperKey)),
                new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 1), false, true);
        HashDistributionInfo distribution = new HashDistributionInfo(
                buckets, false, Lists.newArrayList(key), hashType);
        OlapTable table = new OlapTable(tableId, "restore_hash_table", Lists.newArrayList(key, date), KeysType.DUP_KEYS,
                partitionInfo, distribution);
        table.addPartition(new Partition(partitionId, partitionName,
                new MaterializedIndex(tableId, MaterializedIndex.IndexState.NORMAL), distribution));
        return table;
    }

    @Test
    public void testSerialization() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("restoreJob", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        job.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        RestoreJob job2 = RestoreJob.read(in);

        Assertions.assertEquals(job.getJobId(), job2.getJobId());
        Assertions.assertEquals(job.getDbId(), job2.getDbId());
        Assertions.assertEquals(job.getCreateTime(), job2.getCreateTime());
        Assertions.assertEquals(job.getType(), job2.getType());

        // 3. delete files
        in.close();
        Files.delete(path);
    }

    @Test
    public void testResetPartitionVisibleAndNextVersionForRestore() throws Exception {
        long visibleVersion = 1234;
        long remotePartId = 123;
        String partName = "p20240723";
        MaterializedIndex index = new MaterializedIndex();
        Partition remotePart = new Partition(remotePartId, partName, index, new HashDistributionInfo());
        remotePart.setVisibleVersionAndTime(visibleVersion, 0);
        remotePart.setNextVersion(visibleVersion + 10);

        OlapTable localTbl = new OlapTable();
        localTbl.setPartitionInfo(new PartitionInfo(PartitionType.RANGE));
        OlapTable remoteTbl = new OlapTable();
        remoteTbl.addPartition(remotePart);
        remoteTbl.setPartitionInfo(new PartitionInfo(PartitionType.RANGE));

        ReplicaAllocation alloc = new ReplicaAllocation();
        job.resetPartitionForRestore(localTbl, remoteTbl, partName, alloc);

        Partition localPart = remoteTbl.getPartition(partName);
        Assertions.assertEquals(localPart.getVisibleVersion(), visibleVersion);
        Assertions.assertEquals(localPart.getNextVersion(), visibleVersion + 1);
    }
}
