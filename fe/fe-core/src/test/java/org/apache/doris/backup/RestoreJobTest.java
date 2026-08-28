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

import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.DistributionMappingConstraint;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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

    @Before
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
        }).when(repo).getSnapshotMetaFile(Mockito.eq(label), Mockito.anyList(), Mockito.anyInt());

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

    @After
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

        Assert.assertEquals(job.getJobId(), job2.getJobId());
        Assert.assertEquals(job.getDbId(), job2.getDbId());
        Assert.assertEquals(job.getCreateTime(), job2.getCreateTime());
        Assert.assertEquals(job.getType(), job2.getType());

        // 3. delete files
        in.close();
        Files.delete(path);
    }

    @Test
    public void testRestoreMappingRejectsMixedFrontendVersionsWhenTargetExists() {
        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("k1"), List.of("k1"));
        expectedRestoreTbl.getTableAttributes().getDistributionMappingConstraints()
                .put(mapping.getName(), mapping);
        Assert.assertTrue(db.registerTable(expectedRestoreTbl));
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(constraintManager.getDistributionMappingConstraints(expectedRestoreTbl))
                .thenReturn(ImmutableList.of(mapping));
        Mockito.doThrow(new org.apache.doris.nereids.exceptions.AnalysisException("mixed versions"))
                .when(constraintManager).validateDistributionMappingFeatureCompatibility();
        Deencapsulation.setField(job, "repo", repo);

        Deencapsulation.invoke(job, "checkAndPrepareMeta");

        Assert.assertFalse(job.getStatus().ok());
        Assert.assertTrue(job.getStatus().getErrMsg().contains("Cannot restore table"));
        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, expectedRestoreTbl.getState());
        Mockito.verify(constraintManager).validateDistributionMappingFeatureCompatibility();
        Mockito.verify(constraintManager, Mockito.never())
                .validateDistributionMappingConstraints(expectedRestoreTbl);
    }

    @Test
    public void testRestoreMappingRejectsIncompatibleSchema() {
        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("k1"), List.of("k1"));
        expectedRestoreTbl.getTableAttributes().getDistributionMappingConstraints()
                .put(mapping.getName(), mapping);
        Assert.assertTrue(db.registerTable(expectedRestoreTbl));
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(constraintManager.getDistributionMappingConstraints(expectedRestoreTbl))
                .thenReturn(ImmutableList.of(mapping));
        Mockito.doThrow(new org.apache.doris.nereids.exceptions.AnalysisException("incompatible schema"))
                .when(constraintManager).validateDistributionMappingConstraints(expectedRestoreTbl);
        Deencapsulation.setField(job, "repo", repo);

        Deencapsulation.invoke(job, "checkAndPrepareMeta");

        Assert.assertFalse(job.getStatus().ok());
        Assert.assertTrue(job.getStatus().getErrMsg().contains("Cannot restore table"));
        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, expectedRestoreTbl.getState());
        Mockito.verify(constraintManager).validateDistributionMappingFeatureCompatibility();
        Mockito.verify(constraintManager).validateDistributionMappingConstraints(expectedRestoreTbl);
    }

    @Test
    public void testRestoreMappingValidatesEveryBackupTable() {
        DistributionMappingConstraint firstMapping = new DistributionMappingConstraint(
                "first_mapping", "mapping_id", List.of("k1"), List.of("k1"));
        DistributionMappingConstraint secondMapping = new DistributionMappingConstraint(
                "second_mapping", "mapping_id", List.of("k1"), List.of("k1"));
        OlapTable secondRestoreTable = Mockito.mock(OlapTable.class);
        Mockito.when(secondRestoreTable.getName()).thenReturn("second_restore_table");
        Mockito.when(secondRestoreTable.getId()).thenReturn(60000L);

        jobInfo.backupOlapTableObjects = Maps.newLinkedHashMap();
        jobInfo.backupOlapTableObjects.put(expectedRestoreTbl.getName(), new BackupOlapTableInfo());
        jobInfo.backupOlapTableObjects.put(secondRestoreTable.getName(), new BackupOlapTableInfo());
        backupMeta = new BackupMeta(
                Lists.newArrayList(expectedRestoreTbl, secondRestoreTable), Lists.newArrayList());
        Deencapsulation.setField(job, "backupMeta", backupMeta);

        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(constraintManager.getDistributionMappingConstraints(expectedRestoreTbl))
                .thenReturn(ImmutableList.of(firstMapping));
        Mockito.when(constraintManager.getDistributionMappingConstraints(secondRestoreTable))
                .thenReturn(ImmutableList.of(secondMapping));
        Mockito.doThrow(new org.apache.doris.nereids.exceptions.AnalysisException("incompatible schema"))
                .when(constraintManager).validateDistributionMappingConstraints(secondRestoreTable);

        boolean valid = Deencapsulation.invoke(job, "validateDistributionMappingConstraintsForRestore");

        Assert.assertFalse(valid);
        Assert.assertTrue(job.getStatus().getErrMsg().contains("second_restore_table"));
        Mockito.verify(constraintManager).validateDistributionMappingFeatureCompatibility();
        Mockito.verify(constraintManager).validateDistributionMappingConstraints(expectedRestoreTbl);
        Mockito.verify(constraintManager).validateDistributionMappingConstraints(secondRestoreTable);
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
        Assert.assertEquals(localPart.getVisibleVersion(), visibleVersion);
        Assert.assertEquals(localPart.getNextVersion(), visibleVersion + 1);
    }
}
