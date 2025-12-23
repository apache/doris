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
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.DataProperty.MediumAllocationMode;
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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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

    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;

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

    @Mocked
    private EditLog editLog;
    @Mocked
    private SystemInfoService systemInfoService;

    @Injectable
    private Repository repo = new Repository(repoId, "repo", false, "bos://my_repo",
            FileSystemFactory.get(BrokerProperties.of("broker", Maps.newHashMap())));

    private BackupMeta backupMeta;

    @Before
    public void setUp() throws Exception {
        db = CatalogMocker.mockDb();
        backupHandler = new MockBackupHandler(env);
        repoMgr = new MockRepositoryMgr();

        Deencapsulation.setField(env, "backupHandler", backupHandler);

        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = db;

                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;

                env.getNextId();
                minTimes = 0;
                result = id.getAndIncrement();

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                Env.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        new Expectations() {
            {
                systemInfoService.selectBackendIdsForReplicaCreation((ReplicaAllocation) any,
                        Maps.newHashMap(), (TStorageMedium) any, MediumAllocationMode.ADAPTIVE, true);
                minTimes = 0;
                result = new Delegate() {
                    public synchronized List<Long> selectBackendIdsForReplicaCreation(
                            ReplicaAllocation replicaAlloc, Map<Tag, Integer> nextIndexs,
                            TStorageMedium medium, MediumAllocationMode mediumAllocationMode,
                            boolean isOnlyForCheck) {
                        List<Long> beIds = Lists.newArrayList();
                        beIds.add(CatalogMocker.BACKEND1_ID);
                        beIds.add(CatalogMocker.BACKEND2_ID);
                        beIds.add(CatalogMocker.BACKEND3_ID);
                        return beIds;
                    }
                };
            }
        };

        new Expectations() {
            {
                editLog.logBackupJob((BackupJob) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logBackupJob(BackupJob job) {
                        System.out.println("log backup job: " + job);
                    }
                };
            }
        };

        new Expectations() {
            {
                repo.upload(anyString, anyString);
                result = Status.OK;
                minTimes = 0;

                List<BackupMeta> backupMetas = Lists.newArrayList();
                repo.getSnapshotMetaFile(label, backupMetas, -1);
                minTimes = 0;
                result = new Delegate() {
                    public Status getSnapshotMetaFile(String label, List<BackupMeta> backupMetas) {
                        backupMetas.add(backupMeta);
                        return Status.OK;
                    }
                };
            }
        };

        new MockUp<MarkedCountDownLatch>() {
            @Mock
            boolean await(long timeout, TimeUnit unit) {
                return true;
            }
        };

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
                "hdd", "strict", env, repo.getId());

        List<Table> tbls = Lists.newArrayList();
        List<Resource> resources = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls, resources);
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

    @Test
    public void testRestoreJobWithHddMode() {
        RestoreJob hddJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "hdd", "strict", env, repo.getId());

        // Verify storage medium and allocation mode
        Assert.assertEquals("hdd", hddJob.getStorageMedium());
        Assert.assertEquals("strict", hddJob.getMediumAllocationMode());
        Assert.assertFalse(hddJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobWithSsdMode() {
        RestoreJob ssdJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "strict", env, repo.getId());

        // Verify storage medium and allocation mode
        Assert.assertEquals("ssd", ssdJob.getStorageMedium());
        Assert.assertEquals("strict", ssdJob.getMediumAllocationMode());
        Assert.assertFalse(ssdJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobWithSameWithUpstreamMode() {
        RestoreJob sameWithUpstreamJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "same_with_upstream", "strict", env, repo.getId());

        // Verify storage medium and allocation mode
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, sameWithUpstreamJob.getStorageMedium());
        Assert.assertEquals("strict", sameWithUpstreamJob.getMediumAllocationMode());
        Assert.assertTrue(sameWithUpstreamJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobWithDefaultMode() {
        RestoreJob defaultJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, null, "strict", env, repo.getId());

        // Default mode should be same_with_upstream when storage_medium is null
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, defaultJob.getStorageMedium());
        Assert.assertEquals("strict", defaultJob.getMediumAllocationMode());
        Assert.assertTrue(defaultJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobPersistence() throws IOException {
        // Create a job with same_with_upstream mode
        RestoreJob originalJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "same_with_upstream", "adaptive", env, repo.getId());

        // Serialize
        final Path path = Files.createTempFile("restoreJobMedium", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));
        originalJob.write(out);
        out.flush();
        out.close();

        // Deserialize
        DataInputStream in = new DataInputStream(Files.newInputStream(path));
        RestoreJob deserializedJob = RestoreJob.read(in);

        // Verify storage_medium is preserved
        Assert.assertTrue(deserializedJob.isSameWithUpstream());
        Assert.assertEquals(originalJob.getJobId(), deserializedJob.getJobId());

        // Cleanup
        in.close();
        Files.delete(path);
    }

    @Test
    public void testRestoreJobWithEmptyStorageMedium() {
        // Empty string should default to same_with_upstream
        RestoreJob emptyJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "", "strict", env, repo.getId());

        // Verify empty string is normalized to same_with_upstream
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, emptyJob.getStorageMedium());
        Assert.assertEquals("strict", emptyJob.getMediumAllocationMode());
        Assert.assertTrue(emptyJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobWithWhitespaceStorageMedium() {
        // Whitespace-only string should default to same_with_upstream
        RestoreJob whitespaceJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "   ", "strict", env, repo.getId());

        // Verify whitespace is normalized to same_with_upstream
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, whitespaceJob.getStorageMedium());
        Assert.assertEquals("strict", whitespaceJob.getMediumAllocationMode());
        Assert.assertTrue(whitespaceJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobWithEmptyMediumAllocationMode() {
        // Empty medium_allocation_mode should default to strict
        // Test verifies that the job can be created without exception
        new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "hdd", "", env, repo.getId());
    }

    @Test
    public void testRestoreJobWithNullMediumAllocationMode() {
        // Null medium_allocation_mode should default to strict
        RestoreJob job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", null, env, repo.getId());

        // Verify it defaults to strict
        Assert.assertEquals(RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT, job.getMediumAllocationMode());
    }

    @Test
    public void testRestoreJobWithAdaptiveMode() {
        RestoreJob adaptiveJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "adaptive", env, repo.getId());

        Assert.assertEquals("adaptive", adaptiveJob.getMediumAllocationMode());
        Assert.assertFalse(adaptiveJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobWithStrictMode() {
        RestoreJob strictJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "hdd", "strict", env, repo.getId());

        Assert.assertEquals("strict", strictJob.getMediumAllocationMode());
        Assert.assertFalse(strictJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobPersistenceWithHddStrictMode() throws IOException {
        RestoreJob originalJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "hdd", "strict", env, repo.getId());

        final Path path = Files.createTempFile("restoreJobHddStrict", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));
        originalJob.write(out);
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(Files.newInputStream(path));
        RestoreJob deserializedJob = RestoreJob.read(in);

        Assert.assertFalse(deserializedJob.isSameWithUpstream());
        Assert.assertEquals(originalJob.getJobId(), deserializedJob.getJobId());

        in.close();
        Files.delete(path);
    }

    @Test
    public void testRestoreJobPersistenceWithSsdAdaptiveMode() throws IOException {
        RestoreJob originalJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "adaptive", env, repo.getId());

        final Path path = Files.createTempFile("restoreJobSsdAdaptive", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));
        originalJob.write(out);
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(Files.newInputStream(path));
        RestoreJob deserializedJob = RestoreJob.read(in);

        Assert.assertFalse(deserializedJob.isSameWithUpstream());
        Assert.assertEquals(originalJob.getJobId(), deserializedJob.getJobId());

        in.close();
        Files.delete(path);
    }

    @Test
    public void testRestoreJobWithAtomicRestoreAndStorageMedium() {
        // Test atomic restore with storage medium settings
        RestoreJob atomicJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, true /* isAtomicRestore */, false, "ssd", "adaptive", env, repo.getId());

        // Verify storage medium and allocation mode are set correctly
        Assert.assertEquals("ssd", atomicJob.getStorageMedium());
        Assert.assertEquals("adaptive", atomicJob.getMediumAllocationMode());
        Assert.assertFalse(atomicJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobWithAtomicRestoreAndSameWithUpstream() {
        // Test atomic restore with same_with_upstream
        RestoreJob atomicJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, true /* isAtomicRestore */, false, "same_with_upstream", "strict", env, repo.getId());

        // Verify storage medium and allocation mode
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, atomicJob.getStorageMedium());
        Assert.assertEquals("strict", atomicJob.getMediumAllocationMode());
        Assert.assertTrue(atomicJob.isSameWithUpstream());
    }

    @Test
    public void testRestoreJobCombinationsOfMediumAndMode() {
        // Test various combinations to ensure coverage

        // hdd + strict
        RestoreJob job1 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "hdd", "strict", env, repo.getId());
        Assert.assertEquals("hdd", job1.getStorageMedium());
        Assert.assertEquals("strict", job1.getMediumAllocationMode());
        Assert.assertFalse(job1.isSameWithUpstream());

        // hdd + adaptive
        RestoreJob job2 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "hdd", "adaptive", env, repo.getId());
        Assert.assertEquals("hdd", job2.getStorageMedium());
        Assert.assertEquals("adaptive", job2.getMediumAllocationMode());
        Assert.assertFalse(job2.isSameWithUpstream());

        // ssd + strict
        RestoreJob job3 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "strict", env, repo.getId());
        Assert.assertEquals("ssd", job3.getStorageMedium());
        Assert.assertEquals("strict", job3.getMediumAllocationMode());
        Assert.assertFalse(job3.isSameWithUpstream());

        // ssd + adaptive
        RestoreJob job4 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "adaptive", env, repo.getId());
        Assert.assertEquals("ssd", job4.getStorageMedium());
        Assert.assertEquals("adaptive", job4.getMediumAllocationMode());
        Assert.assertFalse(job4.isSameWithUpstream());

        // same_with_upstream + strict
        RestoreJob job5 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "same_with_upstream", "strict", env, repo.getId());
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, job5.getStorageMedium());
        Assert.assertEquals("strict", job5.getMediumAllocationMode());
        Assert.assertTrue(job5.isSameWithUpstream());

        // same_with_upstream + adaptive
        RestoreJob job6 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "same_with_upstream", "adaptive", env, repo.getId());
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, job6.getStorageMedium());
        Assert.assertEquals("adaptive", job6.getMediumAllocationMode());
        Assert.assertTrue(job6.isSameWithUpstream());
    }

    @Test
    public void testGetTargetAllocationModeStrict() {
        // Test getTargetAllocationMode() returns STRICT for "strict"
        RestoreJob strictJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "strict", env, repo.getId());

        // Use reflection to call private method
        try {
            java.lang.reflect.Method method = RestoreJob.class.getDeclaredMethod("getTargetAllocationMode");
            method.setAccessible(true);
            DataProperty.MediumAllocationMode result =
                    (DataProperty.MediumAllocationMode) method.invoke(strictJob);
            Assert.assertEquals(DataProperty.MediumAllocationMode.STRICT, result);
        } catch (Exception e) {
            Assert.fail("Failed to test getTargetAllocationMode: " + e.getMessage());
        }
    }

    @Test
    public void testGetTargetAllocationModeAdaptive() {
        // Test getTargetAllocationMode() returns ADAPTIVE for "adaptive"
        RestoreJob adaptiveJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "adaptive", env, repo.getId());

        try {
            java.lang.reflect.Method method = RestoreJob.class.getDeclaredMethod("getTargetAllocationMode");
            method.setAccessible(true);
            DataProperty.MediumAllocationMode result =
                    (DataProperty.MediumAllocationMode) method.invoke(adaptiveJob);
            Assert.assertEquals(DataProperty.MediumAllocationMode.ADAPTIVE, result);
        } catch (Exception e) {
            Assert.fail("Failed to test getTargetAllocationMode: " + e.getMessage());
        }
    }

    @Test
    public void testGetTargetAllocationModeInvalidDefaultsToStrict() {
        // Test getTargetAllocationMode() defaults to STRICT for invalid value
        RestoreJob job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "invalid_mode", env, repo.getId());

        try {
            java.lang.reflect.Method method = RestoreJob.class.getDeclaredMethod("getTargetAllocationMode");
            method.setAccessible(true);
            DataProperty.MediumAllocationMode result =
                    (DataProperty.MediumAllocationMode) method.invoke(job);
            // Should default to STRICT for invalid value
            Assert.assertEquals(DataProperty.MediumAllocationMode.STRICT, result);
        } catch (Exception e) {
            Assert.fail("Failed to test getTargetAllocationMode: " + e.getMessage());
        }
    }

    @Test
    public void testGsonPostProcess() throws IOException {
        // Test gsonPostProcess() correctly deserializes properties
        RestoreJob testJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, true, true, true, false,
                true, true, true, true, "hdd", "adaptive", env, repo.getId());

        // Simulate serialization/deserialization by calling gsonPostProcess
        testJob.gsonPostProcess();

        // Verify all properties are correctly set
        Assert.assertEquals("hdd", testJob.getStorageMedium());
        Assert.assertEquals("adaptive", testJob.getMediumAllocationMode());
    }

    @Test
    public void testTableAliasWithAtomicRestore() {
        // Test the static helper method for atomic restore table naming
        String originalName = "my_table";
        String aliasName = RestoreJob.tableAliasWithAtomicRestore(originalName);

        Assert.assertTrue(aliasName.contains(originalName));
        Assert.assertTrue(aliasName.startsWith("__doris_atomic_restore_prefix__"));
        Assert.assertEquals("__doris_atomic_restore_prefix__my_table", aliasName);
    }

    @Test
    public void testToStringContainsMediumInfo() {
        // Test toString() includes storage medium and allocation mode information
        RestoreJob job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "adaptive", env, repo.getId());

        String str = job.toString();
        Assert.assertTrue(str.contains("backup ts"));
        Assert.assertTrue(str.contains("state"));
        Assert.assertTrue(str.contains("2018-01-01 01:01:01"));
    }

    @Test
    public void testStorageMediumNormalization() {
        // Test that various storage medium inputs are normalized correctly

        // Empty string should normalize to same_with_upstream
        RestoreJob job1 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "", "strict", env, repo.getId());
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, job1.getStorageMedium());

        // Whitespace-only string should normalize to same_with_upstream
        RestoreJob job2 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "   ", "strict", env, repo.getId());
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, job2.getStorageMedium());

        // null should normalize to same_with_upstream
        RestoreJob job3 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, null, "strict", env, repo.getId());
        Assert.assertEquals(RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM, job3.getStorageMedium());
    }

    @Test
    public void testMediumAllocationModeNormalization() {
        // Test that various medium_allocation_mode inputs are normalized correctly

        // Empty string should normalize to strict
        RestoreJob job1 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "", env, repo.getId());
        Assert.assertEquals(RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT, job1.getMediumAllocationMode());

        // Whitespace-only string should normalize to strict
        RestoreJob job2 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "   ", env, repo.getId());
        Assert.assertEquals(RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT, job2.getMediumAllocationMode());

        // null should normalize to strict
        RestoreJob job3 = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", null, env, repo.getId());
        Assert.assertEquals(RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT, job3.getMediumAllocationMode());
    }

    @Test
    public void testIsFromLocalSnapshot() {
        // Test isFromLocalSnapshot() with local repo ID
        RestoreJob localJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "strict", env, Repository.KEEP_ON_LOCAL_REPO_ID);
        Assert.assertTrue("Local snapshot job should return true", localJob.isFromLocalSnapshot());

        // Test isFromLocalSnapshot() with remote repo ID
        RestoreJob remoteJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, "ssd", "strict", env, repo.getId());
        Assert.assertFalse("Remote snapshot job should return false", remoteJob.isFromLocalSnapshot());
    }
}
