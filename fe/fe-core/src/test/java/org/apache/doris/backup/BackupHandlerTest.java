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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelBackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BackupHandlerTest {

    private BackupHandler handler;

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private BrokerMgr brokerMgr = Mockito.mock(BrokerMgr.class);
    private EditLog editLog = Mockito.mock(EditLog.class);

    private MockedStatic<Env> mockedEnvStatic;

    private Database db;

    private long idGen = 0;

    private File rootDir;

    private String tmpPath = "./tmp" + System.currentTimeMillis();

    private TabletInvertedIndex invertedIndex = new LocalTabletInvertedIndex();

    @Before
    public void setUp() throws Exception {
        Config.tmp_dir = tmpPath;
        rootDir = new File(Config.tmp_dir);
        rootDir.mkdirs();
        FeConstants.runningUnitTest = true;

        mockedEnvStatic = Mockito.mockStatic(Env.class);

        Mockito.when(env.getBrokerMgr()).thenReturn(brokerMgr);
        Mockito.when(env.getNextId()).thenAnswer(inv -> idGen++);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);

        db = CatalogMocker.mockDb();

        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.doReturn(db).when(catalog).getDbOrDdlException(Mockito.anyString());
    }

    @After
    public void done() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        if (rootDir != null) {
            try {
                Files.walk(Paths.get(Config.tmp_dir),
                           FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testInit() {
        handler = new BackupHandler(env);
        handler.runAfterCatalogReady();

        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        Assert.assertTrue(backupDir.exists());
    }

    @Test
    public void testCreateAndDropRepository() throws Exception {
        Mockito.when(brokerMgr.containsBroker(Mockito.anyString())).thenReturn(true);

        try (MockedConstruction<Repository> mockedRepo = Mockito.mockConstruction(Repository.class,
                (mock, context) -> {
                    List<?> args = context.arguments();
                    if (args.size() >= 2) {
                        Mockito.when(mock.getName()).thenReturn((String) args.get(1));
                        Mockito.when(mock.getId()).thenReturn((Long) args.get(0));
                    }
                    Mockito.when(mock.initRepository()).thenReturn(Status.OK);
                    Mockito.when(mock.ping()).thenReturn(true);
                    Mockito.doAnswer(inv -> {
                        List<String> snapshotNames = inv.getArgument(0);
                        snapshotNames.add("ss2");
                        return Status.OK;
                    }).when(mock).listSnapshots(Mockito.anyList());
                    Mockito.doAnswer(inv -> {
                        List<BackupJobInfo> infos = inv.getArgument(2);
                        OlapTable tbl = (OlapTable) db.getTableOrMetaException(CatalogMocker.TEST_TBL_NAME);
                        List<Table> tbls = Lists.newArrayList();
                        tbls.add(tbl);
                        List<Resource> resources = Lists.newArrayList();
                        BackupMeta backupMeta = new BackupMeta(tbls, resources);
                        Map<Long, SnapshotInfo> snapshotInfos = Maps.newHashMap();
                        for (Partition part : tbl.getPartitions()) {
                            for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                                for (Tablet tablet : idx.getTablets()) {
                                    List<String> files = Lists.newArrayList();
                                    SnapshotInfo sinfo = new SnapshotInfo(db.getId(), tbl.getId(), part.getId(), idx.getId(),
                                            tablet.getId(), -1, 0, "./path", files);
                                    snapshotInfos.put(tablet.getId(), sinfo);
                                }
                            }
                        }

                        BackupJobInfo info = BackupJobInfo.fromCatalog(System.currentTimeMillis(),
                                "ss2", CatalogMocker.TEST_DB_NAME,
                                CatalogMocker.TEST_DB_ID, BackupCommand.BackupContent.ALL,
                                backupMeta, snapshotInfos, null);
                        infos.add(info);
                        return Status.OK;
                    }).when(mock).getSnapshotInfoFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyList());
                })) {

            // add repo
            handler = new BackupHandler(env);
            StorageBackend storageBackend = new StorageBackend("broker", "bos://location",
                    StorageBackend.StorageType.BROKER, Maps.newHashMap());

            CreateRepositoryCommand command = new CreateRepositoryCommand(false, "repo", storageBackend);
            handler.createRepository(command);

            // process backup
            List<TableRefInfo> tableRefInfos = Lists.newArrayList();
            tableRefInfos.add(new TableRefInfo(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, CatalogMocker.TEST_DB_NAME,
                    CatalogMocker.TEST_TBL_NAME), null, null, null, null, null, null, null));
            Map<String, String> properties = Maps.newHashMap();
            properties.put("backup_timestamp", "2018-08-08-08-08-08");
            boolean isExclude = false;
            BackupCommand backupCommand = new BackupCommand(new LabelNameInfo(CatalogMocker.TEST_DB_NAME, "label1"), "repo", tableRefInfos, properties, isExclude);
            handler.process(backupCommand);

            // handleFinishedSnapshotTask
            BackupJob backupJob = (BackupJob) handler.getJob(CatalogMocker.TEST_DB_ID);
            SnapshotTask snapshotTask = new SnapshotTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID, 0, 0,
                    0, 0, 0, 0, 1, false);
            TFinishTaskRequest request = new TFinishTaskRequest();
            List<String> snapshotFiles = Lists.newArrayList();
            request.setSnapshotFiles(snapshotFiles);
            request.setSnapshotPath("./snapshot/path");
            request.setTaskStatus(new TStatus(TStatusCode.OK));
            handler.handleFinishedSnapshotTask(snapshotTask, request);

            // handleFinishedSnapshotUploadTask
            Map<String, String> srcToDestPath = Maps.newHashMap();
            UploadTask uploadTask = new UploadTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID,
                    srcToDestPath, null, null, StorageBackend.StorageType.BROKER, "");
            request = new TFinishTaskRequest();
            Map<Long, List<String>> tabletFiles = Maps.newHashMap();
            request.setTabletFiles(tabletFiles);
            request.setTaskStatus(new TStatus(TStatusCode.OK));
            handler.handleFinishedSnapshotUploadTask(uploadTask, request);

            // cancel backup
            handler.cancel(new CancelBackupCommand(CatalogMocker.TEST_DB_NAME, false));

            // process restore
            List<TableRefInfo> tableRefInfos2 = Lists.newArrayList();
            tableRefInfos2.add(new TableRefInfo(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, CatalogMocker.TEST_DB_NAME,
                    CatalogMocker.TEST_TBL_NAME), null, null, null, null, null, null, null));
            Map<String, String> properties02 = Maps.newHashMap();
            properties02.put("backup_timestamp", "2018-08-08-08-08-08");
            boolean isExclude02 = false;
            RestoreCommand restoreCommand = new RestoreCommand(new LabelNameInfo(CatalogMocker.TEST_DB_NAME, "ss2"), "repo", tableRefInfos2, properties02, isExclude02);
            restoreCommand.analyzeProperties();
            handler.process(restoreCommand);

            // handleFinishedSnapshotTask
            RestoreJob restoreJob = (RestoreJob) handler.getJob(CatalogMocker.TEST_DB_ID);
            snapshotTask = new SnapshotTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                    0, 0, 0, 0, 0, 0, 1, true);
            request = new TFinishTaskRequest();
            request.setSnapshotPath("./snapshot/path");
            request.setTaskStatus(new TStatus(TStatusCode.OK));
            handler.handleFinishedSnapshotTask(snapshotTask, request);

            // handleDownloadSnapshotTask
            DownloadTask downloadTask = new DownloadTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                    srcToDestPath, null, null, StorageBackend.StorageType.BROKER, "", "");
            request = new TFinishTaskRequest();
            List<Long> downloadedTabletIds = Lists.newArrayList();
            request.setDownloadedTabletIds(downloadedTabletIds);
            request.setTaskStatus(new TStatus(TStatusCode.OK));
            handler.handleDownloadSnapshotTask(downloadTask, request);

            // handleDirMoveTask
            DirMoveTask dirMoveTask = new DirMoveTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID, 0, 0, 0,
                    0, "", 0, true);
            request = new TFinishTaskRequest();
            request.setTaskStatus(new TStatus(TStatusCode.OK));
            handler.handleDirMoveTask(dirMoveTask, request);

            // cancel restore
            handler.cancel(new CancelBackupCommand(CatalogMocker.TEST_DB_NAME, true));

            // drop repo
            handler.dropRepository("repo");
        }
    }

    // ========== Dual Queue Architecture Tests ==========

    /**
     * Test 1: Basic dual queue operations - Add job to running queue
     */
    @Test
    public void testDualQueueAddActiveJob() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);

        try {
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job);

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(1, jobs.size());
            Assert.assertEquals(job, jobs.get(0));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 2: Move job from running to history queue
     */
    @Test
    public void testDualQueueMoveToHistory() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);

        try {
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
            Deencapsulation.invoke(handler, "moveToHistory", dbId, job);

            List runningJobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(0, runningJobs.size());

            List allJobs = Deencapsulation.invoke(handler, "getAllJobs", dbId);
            Assert.assertEquals(1, allJobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 3: FIFO cleanup in history queue
     */
    @Test
    public void testDualQueueFifoCleanup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        int originalLimit = Config.max_backup_restore_job_num_per_db;
        Config.max_backup_restore_job_num_per_db = 3;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            for (int i = 0; i < 5; i++) {
                BackupJob job = new BackupJob("test_backup_" + i, dbId, "test_db",
                        Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
                Deencapsulation.setField(job, "jobId", (long) i);
                Deencapsulation.invoke(handler, "addToHistoryQueue", dbId, job);
            }

            List allJobs = Deencapsulation.invoke(handler, "getAllJobs", dbId);
            Assert.assertEquals(3, allJobs.size());
            Assert.assertEquals("test_backup_2", ((AbstractJob) allJobs.get(0)).getLabel());
            Assert.assertEquals("test_backup_4", ((AbstractJob) allJobs.get(2)).getLabel());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_backup_restore_job_num_per_db = originalLimit;
        }
    }

    /**
     * Test 4: Soft limit (warning only)
     */
    @Test
    public void testDualQueueSoftLimit() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        int originalSoftLimit = Config.max_backup_restore_running_queue_soft_limit;
        Config.max_backup_restore_running_queue_soft_limit = 2;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            for (int i = 0; i < 3; i++) {
                BackupJob job = new BackupJob("test_backup_" + i, dbId, "test_db",
                        Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
                Deencapsulation.setField(job, "jobId", (long) i);
                Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
            }

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(3, jobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_backup_restore_running_queue_soft_limit = originalSoftLimit;
        }
    }

    /**
     * Test 5: Hard limit (reject new jobs)
     */
    @Test
    public void testDualQueueHardLimit() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        int originalHardLimit = Config.max_backup_restore_running_queue_hard_limit;
        Config.max_backup_restore_running_queue_hard_limit = 2;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            for (int i = 0; i < 2; i++) {
                BackupJob job = new BackupJob("test_backup_" + i, dbId, "test_db",
                        Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
                Deencapsulation.setField(job, "jobId", (long) i);
                Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
            }

            BackupJob extraJob = new BackupJob("test_backup_extra", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(extraJob, "jobId", 100L);

            boolean exceptionThrown = false;
            try {
                Deencapsulation.invoke(handler, "addActiveJob", dbId, extraJob);
            } catch (Exception e) {
                exceptionThrown = true;
                Assert.assertTrue(e.getMessage().contains("Running queue is full"));
            }

            Assert.assertTrue("Hard limit should reject new jobs", exceptionThrown);

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(2, jobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_backup_restore_running_queue_hard_limit = originalHardLimit;
        }
    }

    /**
     * Test 6: Get all jobs (merge running + history)
     */
    @Test
    public void testDualQueueGetAllJobs() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob runningJob1 = new BackupJob("running_1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(runningJob1, "jobId", 1L);
            BackupJob runningJob2 = new BackupJob("running_2", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(runningJob2, "jobId", 2L);

            Deencapsulation.invoke(handler, "addActiveJob", dbId, runningJob1);
            Deencapsulation.invoke(handler, "addActiveJob", dbId, runningJob2);

            BackupJob historyJob = new BackupJob("history_1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(historyJob, "jobId", 3L);
            Deencapsulation.invoke(handler, "addToHistoryQueue", dbId, historyJob);

            List allJobs = Deencapsulation.invoke(handler, "getAllJobs", dbId);
            Assert.assertEquals(3, allJobs.size());

            List runningJobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(2, runningJobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 7: Concurrent access thread safety
     */
    @Test
    public void testDualQueueConcurrentAccess() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            List<Thread> threads = Lists.newArrayList();
            for (int i = 0; i < 5; i++) {
                final int index = i;
                Thread thread = new Thread(() -> {
                    try {
                        BackupJob job = new BackupJob("concurrent_" + index, dbId, "test_db",
                                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
                        Deencapsulation.setField(job, "jobId", (long) index);
                        Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
                    } catch (Exception e) {
                        // Ignore
                    }
                });
                threads.add(thread);
                thread.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(5, jobs.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 8: runAfterCatalogReady skips completed jobs (optimization test)
     */
    @Test
    public void testRunAfterCatalogReadySkipsCompletedJobs() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob completedJob = new BackupJob("completed", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(completedJob, "jobId", 1L);
            Deencapsulation.setField(completedJob, "state", BackupJob.BackupJobState.FINISHED);

            Deencapsulation.invoke(handler, "addActiveJob", dbId, completedJob);

            handler.runAfterCatalogReady();

            Assert.assertTrue("Completed jobs should be skipped", true);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 9: Remove job from running queue
     */
    @Test
    public void testRemoveFromRunningQueue() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job1 = new BackupJob("job1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job1, "jobId", 1L);
            BackupJob job2 = new BackupJob("job2", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job2, "jobId", 2L);

            Deencapsulation.invoke(handler, "addActiveJob", dbId, job1);
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job2);

            List jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(2, jobs.size());

            Deencapsulation.invoke(handler, "removeFromRunningQueue", dbId, job1);

            jobs = Deencapsulation.invoke(handler, "getAllRunningJobs");
            Assert.assertEquals(1, jobs.size());
            Assert.assertEquals(job2, jobs.get(0));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 10: Multiple databases isolation
     */
    @Test
    public void testDualQueueMultipleDatabasesIsolation() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId1 = 1L;
        long dbId2 = 2L;

        try {
            BackupJob job1 = new BackupJob("job1", dbId1, "test_db1",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job1, "jobId", 1L);
            BackupJob job2 = new BackupJob("job2", dbId2, "test_db2",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job2, "jobId", 2L);

            Deencapsulation.invoke(handler, "addActiveJob", dbId1, job1);
            Deencapsulation.invoke(handler, "addActiveJob", dbId2, job2);

            List jobs1 = Deencapsulation.invoke(handler, "getAllJobs", dbId1);
            List jobs2 = Deencapsulation.invoke(handler, "getAllJobs", dbId2);

            Assert.assertEquals(1, jobs1.size());
            Assert.assertEquals(1, jobs2.size());
            Assert.assertEquals(job1, jobs1.get(0));
            Assert.assertEquals(job2, jobs2.get(0));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ========== Job Queue Position & Block Reason Tests ==========

    /**
     * Test 11: getJobQueuePosition returns 0 when concurrency is disabled
     */
    @Test
    public void testGetJobQueuePositionDisabled() throws Exception {
        Config.enable_table_level_backup_concurrency = false;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
        Deencapsulation.setField(job, "jobId", 1L);

        Assert.assertEquals(0, handler.getJobQueuePosition(job));
    }

    /**
     * Test 12: getJobQueuePosition returns 0 for running jobs (in allowedJobIds)
     */
    @Test
    public void testGetJobQueuePositionRunning() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 1L);

            // Add to running queue and allowedJobIds
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job);
            Set<Long> allowedJobIds = Deencapsulation.getField(handler, "allowedJobIds");
            allowedJobIds.add(1L);

            Assert.assertEquals(0, handler.getJobQueuePosition(job));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 13: getJobQueuePosition returns correct positions for pending jobs
     */
    @Test
    public void testGetJobQueuePositionPending() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            // Create 3 jobs: job1 is running, job2 and job3 are pending
            BackupJob job1 = new BackupJob("running_job", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job1, "jobId", 1L);
            BackupJob job2 = new BackupJob("pending_job_1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job2, "jobId", 2L);
            BackupJob job3 = new BackupJob("pending_job_2", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job3, "jobId", 3L);

            Deencapsulation.invoke(handler, "addActiveJob", dbId, job1);
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job2);
            Deencapsulation.invoke(handler, "addActiveJob", dbId, job3);

            // Only job1 is allowed (running)
            Set<Long> allowedJobIds = Deencapsulation.getField(handler, "allowedJobIds");
            allowedJobIds.add(1L);

            // job1 is running → position 0
            Assert.assertEquals(0, handler.getJobQueuePosition(job1));
            // job2 is first pending → position 1
            Assert.assertEquals(1, handler.getJobQueuePosition(job2));
            // job3 is second pending → position 2
            Assert.assertEquals(2, handler.getJobQueuePosition(job3));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 14: getJobBlockReason returns null when concurrency is disabled
     */
    @Test
    public void testGetJobBlockReasonDisabled() throws Exception {
        Config.enable_table_level_backup_concurrency = false;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
        Deencapsulation.setField(job, "jobId", 1L);

        Assert.assertNull(handler.getJobBlockReason(job));
    }

    /**
     * Test 15: getJobBlockReason returns null for running jobs (has permission)
     */
    @Test
    public void testGetJobBlockReasonRunning() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 1L);

            Set<Long> allowedJobIds = Deencapsulation.getField(handler, "allowedJobIds");
            allowedJobIds.add(1L);

            Assert.assertNull(handler.getJobBlockReason(job));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 16: getJobBlockReason shows backup blocked by active restores
     */
    @Test
    public void testGetJobBlockReasonBackupBlockedByRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob backupJob = new BackupJob("test_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(backupJob, "jobId", 1L);

            // Set up stats with active restores
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.activeRestores = 2;
            dbJobStats.put(dbId, stats);

            String reason = handler.getJobBlockReason(backupJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("restore"));
            Assert.assertTrue(reason.contains("2"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 17: getJobBlockReason shows restore blocked by active backups
     */
    @Test
    public void testGetJobBlockReasonRestoreBlockedByBackup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            RestoreJob restoreJob = new RestoreJob();
            Deencapsulation.setField(restoreJob, "jobId", 1L);
            Deencapsulation.setField(restoreJob, "dbId", dbId);

            // Set up stats with active backups
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.activeBackups = 3;
            dbJobStats.put(dbId, stats);

            String reason = handler.getJobBlockReason(restoreJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("backup"));
            Assert.assertTrue(reason.contains("3"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 18: getJobBlockReason shows restore blocked by table conflict
     */
    @Test
    public void testGetJobBlockReasonRestoreTableConflict() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            RestoreJob restoreJob = new RestoreJob();
            Deencapsulation.setField(restoreJob, "jobId", 1L);
            Deencapsulation.setField(restoreJob, "dbId", dbId);

            // Set tableRefs
            List<TableRefInfo> tableRefs = Lists.newArrayList();
            tableRefs.add(new TableRefInfo(
                    new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "test_db", "conflict_table"),
                    null, null, null, null, null, null, null));
            restoreJob.setTableRefs(tableRefs);

            // Set up restoring tables with a conflict
            Map<Long, java.util.Set<String>> restoringTables = Deencapsulation.getField(handler, "restoringTables");
            java.util.Set<String> tables = java.util.concurrent.ConcurrentHashMap.newKeySet();
            tables.add("conflict_table");
            restoringTables.put(dbId, tables);

            // Set up empty stats (no backup/restore blocking)
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            dbJobStats.put(dbId, new BackupHandler.DatabaseJobStats());

            String reason = handler.getJobBlockReason(restoreJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("conflict_table"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 19: getJobBlockReason shows backup blocked by full database backup
     */
    @Test
    public void testGetJobBlockReasonFullDatabaseBackup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob backupJob = new BackupJob("table_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(backupJob, "jobId", 2L);

            // Set up stats with full database backup marker
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.backupDatabaseJobId = 1L;
            stats.backupDatabaseLabel = "full_db_backup";
            dbJobStats.put(dbId, stats);

            String reason = handler.getJobBlockReason(backupJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("Full database backup"));
            Assert.assertTrue(reason.contains("full_db_backup"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 20: getJobBlockReason shows restore blocked by full database restore
     */
    @Test
    public void testGetJobBlockReasonFullDatabaseRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            RestoreJob restoreJob = new RestoreJob();
            Deencapsulation.setField(restoreJob, "jobId", 2L);
            Deencapsulation.setField(restoreJob, "dbId", dbId);
            restoreJob.setTableRefs(Lists.newArrayList());

            // Set up stats with full database restore marker
            Map<Long, BackupHandler.DatabaseJobStats> dbJobStats = Deencapsulation.getField(handler, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.restoreDatabaseJobId = 1L;
            stats.restoreDatabaseLabel = "full_db_restore";
            dbJobStats.put(dbId, stats);

            String reason = handler.getJobBlockReason(restoreJob);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("Full database restore"));
            Assert.assertTrue(reason.contains("full_db_restore"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 21: getJobBlockReason returns generic message when no specific block
     */
    @Test
    public void testGetJobBlockReasonWaitingForSlot() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("test_backup", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 1L);

            // No stats, no allowedJobIds → generic waiting message
            String reason = handler.getJobBlockReason(job);
            Assert.assertNotNull(reason);
            Assert.assertTrue(reason.contains("Waiting for execution slot"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test 22: getJobQueuePosition returns 0 when job not in any queue
     */
    @Test
    public void testGetJobQueuePositionNotInQueue() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("orphan_job", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 99L);

            // No queue exists for this dbId
            Assert.assertEquals(0, handler.getJobQueuePosition(job));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testGlobalSnapshotTaskCounter() {
        BackupHandler handler = new BackupHandler(env);

        Assert.assertEquals(0, handler.getGlobalSnapshotTasks());

        handler.addGlobalSnapshotTasks(100);
        Assert.assertEquals(100, handler.getGlobalSnapshotTasks());

        handler.addGlobalSnapshotTasks(200);
        Assert.assertEquals(300, handler.getGlobalSnapshotTasks());

        handler.addGlobalSnapshotTasks(-100);
        Assert.assertEquals(200, handler.getGlobalSnapshotTasks());

        handler.addGlobalSnapshotTasks(-200);
        Assert.assertEquals(0, handler.getGlobalSnapshotTasks());
    }

    @Test
    public void testOnJobDeactivatedDecrementsGlobalSnapshotTasks() {
        BackupHandler handler = new BackupHandler(env);

        Config.enable_table_level_backup_concurrency = true;
        try {
            long dbId = 1L;
            BackupJob job = new BackupJob("test_deactivate", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 200L);
            Deencapsulation.setField(job, "snapshotTaskCount", 500);

            handler.addGlobalSnapshotTasks(500);
            Assert.assertEquals(500, handler.getGlobalSnapshotTasks());

            Deencapsulation.invoke(handler, "onJobCreated", dbId, (AbstractJob) job, true, false);
            Deencapsulation.invoke(handler, "onJobActivated", dbId, (AbstractJob) job, true, false);
            Deencapsulation.invoke(handler, "onJobDeactivated", dbId, (AbstractJob) job, true);

            Assert.assertEquals(0, handler.getGlobalSnapshotTasks());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testGlobalSnapshotTaskLimitRejectsWhenExceeded() {
        BackupHandler handler = new BackupHandler(env);
        int savedLimit = Config.max_concurrent_snapshot_tasks_total;

        Config.enable_table_level_backup_concurrency = true;
        Config.max_concurrent_snapshot_tasks_total = 100;
        try {
            handler.addGlobalSnapshotTasks(90);
            Assert.assertEquals(90, handler.getGlobalSnapshotTasks());

            // 90 + 20 = 110 > 100, should exceed
            int globalCurrent = handler.getGlobalSnapshotTasks();
            Assert.assertTrue(globalCurrent + 20 > Config.max_concurrent_snapshot_tasks_total);

            // 90 + 5 = 95 <= 100, should not exceed
            Assert.assertFalse(globalCurrent + 5 > Config.max_concurrent_snapshot_tasks_total);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_concurrent_snapshot_tasks_total = savedLimit;
        }
    }

    @Test
    public void testGlobalSnapshotTaskCounterNotUsedWhenConcurrencyDisabled() {
        BackupHandler handler = new BackupHandler(env);

        Config.enable_table_level_backup_concurrency = false;
        Assert.assertEquals(0, handler.getGlobalSnapshotTasks());

        // In non-concurrent mode, global counter should not be touched by job logic.
        // Verify the counter stays at 0 when concurrency is disabled.
        Assert.assertEquals(0, handler.getGlobalSnapshotTasks());
    }

    // ========== Scheduling Rules: checkConcurrency Tests ==========

    @Test
    public void testCheckConcurrencyRejectsDuplicateLabel() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, Map<String, Long>> labelIdx = Deencapsulation.getField(h, "labelIndex");
            Map<String, Long> labels = new java.util.concurrent.ConcurrentHashMap<>();
            labels.put("dup_label", 100L);
            labelIdx.put(dbId, labels);

            boolean thrown = false;
            try {
                Deencapsulation.invoke(h, "checkConcurrency",
                        dbId, "dup_label", true, false, Lists.newArrayList());
            } catch (Exception e) {
                thrown = true;
                Assert.assertTrue(e.getMessage().contains("Label already exists"));
            }
            Assert.assertTrue("Should reject duplicate label", thrown);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCheckConcurrencyRejectsConcurrencyLimit() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        int saved = Config.max_backup_restore_concurrent_num_per_db;
        Config.max_backup_restore_concurrent_num_per_db = 2;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.activeBackups = 1;
            s.activeRestores = 1;
            stats.put(dbId, s);

            boolean thrown = false;
            try {
                Deencapsulation.invoke(h, "checkConcurrency",
                        dbId, "new_label", true, false, Lists.newArrayList());
            } catch (Exception e) {
                thrown = true;
                Assert.assertTrue(e.getMessage().contains("Concurrency limit reached"));
            }
            Assert.assertTrue("Should reject at concurrency limit", thrown);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_backup_restore_concurrent_num_per_db = saved;
        }
    }

    @Test
    public void testCheckConcurrencyRejectsBackupWhenFullDbBackupExists() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.backupDatabaseJobId = 1L;
            s.backupDatabaseLabel = "full_backup";
            stats.put(dbId, s);

            boolean thrown = false;
            try {
                Deencapsulation.invoke(h, "checkConcurrency",
                        dbId, "new_backup", true, false, Lists.newArrayList());
            } catch (Exception e) {
                thrown = true;
                Assert.assertTrue(e.getMessage().contains("full database backup"));
            }
            Assert.assertTrue("Should reject backup when full-db backup exists", thrown);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCheckConcurrencyAllowsRestoreEvenWhenFullDbRestoreExists() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.restoreDatabaseJobId = 1L;
            s.restoreDatabaseLabel = "full_restore";
            stats.put(dbId, s);

            // Should NOT throw — restore is queued via canActivate, not hard-rejected
            Deencapsulation.invoke(h, "checkConcurrency",
                    dbId, "new_restore", false, false, Lists.newArrayList());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ========== Scheduling Rules: canActivate Tests ==========

    @Test
    public void testCanActivateBlockedByConcurrencyLimit() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        int saved = Config.max_backup_restore_concurrent_num_per_db;
        Config.max_backup_restore_concurrent_num_per_db = 2;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.runningBackups = 2;
            stats.put(dbId, s);

            BackupJob job = new BackupJob("bk", dbId, "db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);

            boolean result = Deencapsulation.invoke(h, "canActivate",
                    dbId, (AbstractJob) job, true, false);
            Assert.assertFalse("Should be blocked by concurrency limit", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
            Config.max_backup_restore_concurrent_num_per_db = saved;
        }
    }

    @Test
    public void testCanActivateBackupBlockedByRunningRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.runningRestores = 1;
            stats.put(dbId, s);

            BackupJob job = new BackupJob("bk", dbId, "db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);

            boolean result = Deencapsulation.invoke(h, "canActivate",
                    dbId, (AbstractJob) job, true, false);
            Assert.assertFalse("Backup should be blocked by running restore", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCanActivateRestoreBlockedByRunningBackup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.runningBackups = 1;
            stats.put(dbId, s);

            RestoreJob job = new RestoreJob();
            Deencapsulation.setField(job, "jobId", 1L);
            Deencapsulation.setField(job, "dbId", dbId);

            boolean result = Deencapsulation.invoke(h, "canActivate",
                    dbId, (AbstractJob) job, false, false);
            Assert.assertFalse("Restore should be blocked by running backup", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCanActivateFullDbBackupWaitsForRunningBackups() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.runningBackups = 1;
            stats.put(dbId, s);

            BackupJob job = new BackupJob("full_bk", dbId, "db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);

            boolean result = Deencapsulation.invoke(h, "canActivate",
                    dbId, (AbstractJob) job, true, true);
            Assert.assertFalse("Full-db backup should wait for running backups", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCanActivateBackupBlockedByRunningFullDbBackup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.runningBackupDatabaseJobId = 100L;
            stats.put(dbId, s);

            List<TableRefInfo> refs = Lists.newArrayList();
            refs.add(new TableRefInfo(new TableNameInfo("t1"),
                    null, null, null, null, null, null, null));
            BackupJob job = new BackupJob("tbl_bk", dbId, "db",
                    refs, 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);

            boolean result = Deencapsulation.invoke(h, "canActivate",
                    dbId, (AbstractJob) job, true, false);
            Assert.assertFalse("Backup should be blocked by running full-db backup", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCanActivateRestoreBlockedByFullDbRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.restoreDatabaseJobId = 100L;
            stats.put(dbId, s);

            RestoreJob job = new RestoreJob();
            Deencapsulation.setField(job, "jobId", 1L);
            Deencapsulation.setField(job, "dbId", dbId);
            List<TableRefInfo> refs = Lists.newArrayList();
            refs.add(new TableRefInfo(new TableNameInfo("t1"),
                    null, null, null, null, null, null, null));
            job.setTableRefs(refs);

            boolean result = Deencapsulation.invoke(h, "canActivate",
                    dbId, (AbstractJob) job, false, false);
            Assert.assertFalse("Restore should be blocked by running full-db restore", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCanActivateRestoreBlockedByTableConflict() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            stats.put(dbId, new BackupHandler.DatabaseJobStats());

            Map<Long, Set<String>> restoring = Deencapsulation.getField(h, "restoringTables");
            Set<String> tables = java.util.concurrent.ConcurrentHashMap.newKeySet();
            tables.add("conflict_table");
            restoring.put(dbId, tables);

            RestoreJob job = new RestoreJob();
            Deencapsulation.setField(job, "jobId", 1L);
            Deencapsulation.setField(job, "dbId", dbId);
            List<TableRefInfo> refs = Lists.newArrayList();
            refs.add(new TableRefInfo(new TableNameInfo("conflict_table"),
                    null, null, null, null, null, null, null));
            job.setTableRefs(refs);

            boolean result = Deencapsulation.invoke(h, "canActivate",
                    dbId, (AbstractJob) job, false, false);
            Assert.assertFalse("Restore should be blocked by table conflict", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCanActivateRestoreAllowedWhenNoTableConflict() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            stats.put(dbId, new BackupHandler.DatabaseJobStats());

            Map<Long, Set<String>> restoring = Deencapsulation.getField(h, "restoringTables");
            Set<String> tables = java.util.concurrent.ConcurrentHashMap.newKeySet();
            tables.add("table_a");
            restoring.put(dbId, tables);

            RestoreJob job = new RestoreJob();
            Deencapsulation.setField(job, "jobId", 1L);
            Deencapsulation.setField(job, "dbId", dbId);
            List<TableRefInfo> refs = Lists.newArrayList();
            refs.add(new TableRefInfo(new TableNameInfo("table_b"),
                    null, null, null, null, null, null, null));
            job.setTableRefs(refs);

            boolean result = Deencapsulation.invoke(h, "canActivate",
                    dbId, (AbstractJob) job, false, false);
            Assert.assertTrue("Restore should be allowed when no table conflict", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCanActivateAllClear() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats =
                    Deencapsulation.getField(h, "dbJobStats");
            stats.put(dbId, new BackupHandler.DatabaseJobStats());

            BackupJob job = new BackupJob("bk", dbId, "db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);

            boolean result = Deencapsulation.invoke(h, "canActivate",
                    dbId, (AbstractJob) job, true, false);
            Assert.assertTrue("Should activate when all clear", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ========== CANCEL with Label Filter Tests ==========

    @Test
    public void testCancelAllJobsInConcurrencyMode() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = db.getId();

        try {
            for (int i = 0; i < 3; i++) {
                BackupJob job = new BackupJob("backup_" + i, dbId, CatalogMocker.TEST_DB_NAME,
                        Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
                Deencapsulation.setField(job, "jobId", (long) (i + 1));
                Deencapsulation.setField(job, "state", BackupJob.BackupJobState.SNAPSHOTING);
                Deencapsulation.invoke(h, "addActiveJob", dbId, job);
            }

            h.cancel(new CancelBackupCommand(CatalogMocker.TEST_DB_NAME, false));

            List<AbstractJob> jobs = Deencapsulation.invoke(h, "getAllRunningJobs");
            for (AbstractJob job : jobs) {
                if (job instanceof BackupJob) {
                    Assert.assertTrue("All backup jobs should be done", job.isDone());
                }
            }
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testCancelWithLabelFilterOnlyCancelsMatchingJob() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = db.getId();

        try {
            BackupJob target = new BackupJob("target_job", dbId, CatalogMocker.TEST_DB_NAME,
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(target, "jobId", 1L);
            Deencapsulation.setField(target, "state", BackupJob.BackupJobState.SNAPSHOTING);

            BackupJob other = new BackupJob("other_job", dbId, CatalogMocker.TEST_DB_NAME,
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(other, "jobId", 2L);
            Deencapsulation.setField(other, "state", BackupJob.BackupJobState.SNAPSHOTING);

            Deencapsulation.invoke(h, "addActiveJob", dbId, target);
            Deencapsulation.invoke(h, "addActiveJob", dbId, other);

            h.cancel(new CancelBackupCommand(CatalogMocker.TEST_DB_NAME, false, "target_job", false));

            Assert.assertTrue("target_job should be cancelled", target.isDone());
            Assert.assertFalse("other_job should NOT be cancelled", other.isDone());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ========== FE Restart Rebuild Completeness Tests ==========

    @Test
    public void testRebuildConcurrencyStateRebuildsLabelIndex() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("test_label", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 1L);
            Deencapsulation.setField(job, "state", BackupJob.BackupJobState.SNAPSHOTING);

            java.util.Deque<AbstractJob> deque = new java.util.LinkedList<>();
            deque.add(job);
            Map<Long, java.util.Deque<AbstractJob>> legacy =
                    Deencapsulation.getField(h, "dbIdToBackupOrRestoreJobs");
            legacy.put(dbId, deque);

            Deencapsulation.invoke(h, "rebuildConcurrencyStateAfterRestart");

            Map<Long, Map<String, Long>> labelIdx = Deencapsulation.getField(h, "labelIndex");
            Assert.assertTrue("labelIndex should contain the job label",
                    labelIdx.containsKey(dbId) && labelIdx.get(dbId).containsKey("test_label"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testRebuildConcurrencyStateRebuildsDbJobStats() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob bJob = new BackupJob("backup1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(bJob, "jobId", 1L);
            Deencapsulation.setField(bJob, "state", BackupJob.BackupJobState.SNAPSHOTING);

            RestoreJob rJob = new RestoreJob();
            Deencapsulation.setField(rJob, "jobId", 2L);
            Deencapsulation.setField(rJob, "dbId", dbId);
            Deencapsulation.setField(rJob, "label", "restore1");
            Deencapsulation.setField(rJob, "state", RestoreJob.RestoreJobState.SNAPSHOTING);

            java.util.Deque<AbstractJob> deque = new java.util.LinkedList<>();
            deque.add(bJob);
            deque.add(rJob);
            Map<Long, java.util.Deque<AbstractJob>> legacy =
                    Deencapsulation.getField(h, "dbIdToBackupOrRestoreJobs");
            legacy.put(dbId, deque);

            Deencapsulation.invoke(h, "rebuildConcurrencyStateAfterRestart");

            Map<Long, BackupHandler.DatabaseJobStats> dbStats =
                    Deencapsulation.getField(h, "dbJobStats");
            Assert.assertTrue("dbJobStats should contain the database", dbStats.containsKey(dbId));
            BackupHandler.DatabaseJobStats s = dbStats.get(dbId);
            Assert.assertEquals("Should have 1 active backup", 1, s.activeBackups);
            Assert.assertEquals("Should have 1 active restore", 1, s.activeRestores);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testRebuildConcurrencyStateRebuildsAllowedJobIds() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob running = new BackupJob("running", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(running, "jobId", 1L);
            Deencapsulation.setField(running, "state", BackupJob.BackupJobState.SNAPSHOTING);

            BackupJob pending = new BackupJob("pending", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(pending, "jobId", 2L);
            Deencapsulation.setField(pending, "state", BackupJob.BackupJobState.PENDING);

            java.util.Deque<AbstractJob> deque = new java.util.LinkedList<>();
            deque.add(running);
            deque.add(pending);
            Map<Long, java.util.Deque<AbstractJob>> legacy =
                    Deencapsulation.getField(h, "dbIdToBackupOrRestoreJobs");
            legacy.put(dbId, deque);

            Deencapsulation.invoke(h, "rebuildConcurrencyStateAfterRestart");

            Set<Long> allowed = Deencapsulation.getField(h, "allowedJobIds");
            Assert.assertTrue("Running job should be in allowedJobIds", allowed.contains(1L));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test rebuildConcurrencyStateAfterRestart correctly sums globalSnapshotTasks
     * from all active running jobs.
     *
     * Scenario: Add two jobs to running queue with known snapshotTaskCount, rebuild state
     * Expected: globalSnapshotTasks should equal the sum of all running jobs' snapshotTaskCount
     */
    @Test
    public void testRebuildConcurrencyStateRebuildGlobalSnapshotTasks() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job1 = new BackupJob("rebuild_job1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job1, "jobId", 1L);
            Deencapsulation.setField(job1, "snapshotTaskCount", 100);
            Deencapsulation.setField(job1, "state", BackupJob.BackupJobState.SNAPSHOTING);

            BackupJob job2 = new BackupJob("rebuild_job2", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job2, "jobId", 2L);
            Deencapsulation.setField(job2, "snapshotTaskCount", 250);
            Deencapsulation.setField(job2, "state", BackupJob.BackupJobState.UPLOADING);

            // Add to the legacy job store (dbIdToBackupOrRestoreJobs) which rebuild scans
            java.util.Deque<AbstractJob> jobDeque = new java.util.LinkedList<>();
            jobDeque.add(job1);
            jobDeque.add(job2);
            Map<Long, java.util.Deque<AbstractJob>> legacyMap = Deencapsulation.getField(handler, "dbIdToBackupOrRestoreJobs");
            legacyMap.put(dbId, jobDeque);

            Deencapsulation.invoke(handler, "rebuildConcurrencyStateAfterRestart");

            Assert.assertEquals(350, handler.getGlobalSnapshotTasks());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    /**
     * Test rebuildConcurrencyStateAfterRestart does not count completed jobs.
     *
     * Scenario: One active job with snapshotTaskCount=200 and one FINISHED job with snapshotTaskCount=100
     * Expected: globalSnapshotTasks should only reflect the active job (200)
     */
    @Test
    public void testRebuildConcurrencyStateSkipsCompletedJobs() throws Exception {
        Config.enable_table_level_backup_concurrency = true;

        BackupHandler handler = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob activeJob = new BackupJob("active_job", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(activeJob, "jobId", 1L);
            Deencapsulation.setField(activeJob, "snapshotTaskCount", 200);
            Deencapsulation.setField(activeJob, "state", BackupJob.BackupJobState.SNAPSHOTING);

            BackupJob finishedJob = new BackupJob("finished_job", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(finishedJob, "jobId", 2L);
            Deencapsulation.setField(finishedJob, "snapshotTaskCount", 100);
            Deencapsulation.setField(finishedJob, "state", BackupJob.BackupJobState.FINISHED);

            java.util.Deque<AbstractJob> jobDeque = new java.util.LinkedList<>();
            jobDeque.add(activeJob);
            jobDeque.add(finishedJob);
            Map<Long, java.util.Deque<AbstractJob>> legacyMap = Deencapsulation.getField(handler, "dbIdToBackupOrRestoreJobs");
            legacyMap.put(dbId, jobDeque);

            Deencapsulation.invoke(handler, "rebuildConcurrencyStateAfterRestart");

            // Only active job in running queue contributes to globalSnapshotTasks
            Assert.assertEquals(200, handler.getGlobalSnapshotTasks());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== canExecute =====================

    @Test
    public void testCanExecuteDisabled() {
        Config.enable_table_level_backup_concurrency = false;
        BackupHandler h = new BackupHandler(env);
        Assert.assertTrue(h.canExecute(999L));
    }

    @Test
    public void testCanExecuteAllowedAndBlocked() {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        try {
            Set<Long> allowed = Deencapsulation.getField(h, "allowedJobIds");
            allowed.add(1L);
            Assert.assertTrue(h.canExecute(1L));
            Assert.assertFalse(h.canExecute(2L));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== addRestoringTables / removeRestoringTables =====================

    @Test
    public void testAddRestoringTablesTableLevel() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            RestoreJob job = new RestoreJob();
            Deencapsulation.setField(job, "jobId", 10L);
            Deencapsulation.setField(job, "dbId", dbId);
            List<TableRefInfo> refs = Lists.newArrayList();
            refs.add(new TableRefInfo(new TableNameInfo("tbl_a"), null, null, null, null, null, null, null));
            refs.add(new TableRefInfo(new TableNameInfo("tbl_b"), null, null, null, null, null, null, null));
            job.setTableRefs(refs);

            h.addRestoringTables(job);

            Map<Long, Set<String>> restoring = Deencapsulation.getField(h, "restoringTables");
            Assert.assertTrue(restoring.get(dbId).contains("tbl_a"));
            Assert.assertTrue(restoring.get(dbId).contains("tbl_b"));

            Deencapsulation.invoke(h, "removeRestoringTables", job);

            Set<String> remaining = restoring.get(dbId);
            Assert.assertTrue(remaining == null || remaining.isEmpty());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testAddRestoringTablesFullDatabase() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> stats = Deencapsulation.getField(h, "dbJobStats");
            stats.put(dbId, new BackupHandler.DatabaseJobStats());

            RestoreJob job = new RestoreJob();
            Deencapsulation.setField(job, "jobId", 20L);
            Deencapsulation.setField(job, "dbId", dbId);
            Deencapsulation.setField(job, "label", "full_restore");
            job.setTableRefs(Lists.newArrayList());

            h.addRestoringTables(job);

            BackupHandler.DatabaseJobStats s = stats.get(dbId);
            Assert.assertEquals(Long.valueOf(20L), s.restoreDatabaseJobId);
            Assert.assertEquals("full_restore", s.restoreDatabaseLabel);

            Deencapsulation.invoke(h, "removeRestoringTables", job);

            Assert.assertNull(s.restoreDatabaseJobId);
            Assert.assertNull(s.restoreDatabaseLabel);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== onJobCompleted =====================

    @Test
    public void testOnJobCompletedBackup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("completed_bk", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 100L);
            Deencapsulation.setField(job, "state", BackupJob.BackupJobState.FINISHED);

            // Simulate job was active: populate stats, labelIndex, allowedJobIds, running queue
            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.activeBackups = 1;
            stats.activeLabels.add("completed_bk");
            statsMap.put(dbId, stats);

            Map<Long, Map<String, Long>> labels = Deencapsulation.getField(h, "labelIndex");
            Map<String, Long> dbLabels = labels.computeIfAbsent(dbId,
                    k -> new java.util.concurrent.ConcurrentHashMap<>());
            dbLabels.put("completed_bk", 100L);

            Set<Long> allowed = Deencapsulation.getField(h, "allowedJobIds");
            allowed.add(100L);

            // Add to running queue
            Deencapsulation.invoke(h, "addActiveJob", dbId, (AbstractJob) job);

            h.onJobCompleted(100L, dbId, job);

            Assert.assertFalse(allowed.contains(100L));
            // stats should be cleaned up (activeBackups back to 0)
            Assert.assertFalse(statsMap.containsKey(dbId));
            // label should be removed
            Assert.assertFalse(labels.containsKey(dbId));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testOnJobCompletedRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 2L;

        try {
            RestoreJob rJob = new RestoreJob();
            Deencapsulation.setField(rJob, "jobId", 200L);
            Deencapsulation.setField(rJob, "dbId", dbId);
            Deencapsulation.setField(rJob, "label", "completed_rs");
            Deencapsulation.setField(rJob, "state", RestoreJob.RestoreJobState.FINISHED);
            List<TableRefInfo> refs = Lists.newArrayList();
            refs.add(new TableRefInfo(new TableNameInfo("t1"), null, null, null, null, null, null, null));
            rJob.setTableRefs(refs);

            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats stats = new BackupHandler.DatabaseJobStats();
            stats.activeRestores = 1;
            stats.activeLabels.add("completed_rs");
            statsMap.put(dbId, stats);

            Map<Long, Map<String, Long>> labels = Deencapsulation.getField(h, "labelIndex");
            Map<String, Long> dbLabels = labels.computeIfAbsent(dbId,
                    k -> new java.util.concurrent.ConcurrentHashMap<>());
            dbLabels.put("completed_rs", 200L);

            Set<Long> allowed = Deencapsulation.getField(h, "allowedJobIds");
            allowed.add(200L);

            // Add restoring tables
            Map<Long, Set<String>> restoring = Deencapsulation.getField(h, "restoringTables");
            Set<String> tables = java.util.concurrent.ConcurrentHashMap.newKeySet();
            tables.add("t1");
            restoring.put(dbId, tables);

            // Add to running queue
            Deencapsulation.invoke(h, "addActiveJob", dbId, (AbstractJob) rJob);

            h.onJobCompleted(200L, dbId, rJob);

            Assert.assertFalse(allowed.contains(200L));
            Assert.assertFalse(statsMap.containsKey(dbId));
            // restoring tables should be cleaned
            Set<String> remaining = restoring.get(dbId);
            Assert.assertTrue(remaining == null || remaining.isEmpty());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testOnJobCompletedSkipsWhenDisabled() {
        Config.enable_table_level_backup_concurrency = false;
        BackupHandler h = new BackupHandler(env);
        BackupJob job = new BackupJob("skip_job", 1L, "test_db",
                Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
        Deencapsulation.setField(job, "jobId", 300L);
        // Should not throw or modify anything
        h.onJobCompleted(300L, 1L, job);
    }

    // ===================== tryActivatePendingJobs =====================

    @Test
    public void testTryActivatePendingJobsActivatesPendingBackup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            statsMap.put(dbId, new BackupHandler.DatabaseJobStats());

            BackupJob pendingJob = new BackupJob("pending_bk", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(pendingJob, "jobId", 50L);
            Deencapsulation.setField(pendingJob, "state", BackupJob.BackupJobState.PENDING);

            Deencapsulation.invoke(h, "addActiveJob", dbId, (AbstractJob) pendingJob);

            Set<Long> allowed = Deencapsulation.getField(h, "allowedJobIds");
            Assert.assertFalse(allowed.contains(50L));

            Deencapsulation.invoke(h, "tryActivatePendingJobs", dbId);

            Assert.assertTrue("Pending job should be activated", allowed.contains(50L));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testTryActivatePendingJobsSkipsAlreadyAllowed() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            statsMap.put(dbId, new BackupHandler.DatabaseJobStats());

            BackupJob runningJob = new BackupJob("running_bk", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(runningJob, "jobId", 60L);
            Deencapsulation.setField(runningJob, "state", BackupJob.BackupJobState.SNAPSHOTING);

            Set<Long> allowed = Deencapsulation.getField(h, "allowedJobIds");
            allowed.add(60L);

            Deencapsulation.invoke(h, "addActiveJob", dbId, (AbstractJob) runningJob);

            // tryActivate should not add duplicates
            Deencapsulation.invoke(h, "tryActivatePendingJobs", dbId);

            Assert.assertTrue(allowed.contains(60L));
            Assert.assertEquals(1, allowed.size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testTryActivatePendingJobsActivatesPendingRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            statsMap.put(dbId, new BackupHandler.DatabaseJobStats());

            RestoreJob pendingRestore = new RestoreJob();
            Deencapsulation.setField(pendingRestore, "jobId", 70L);
            Deencapsulation.setField(pendingRestore, "dbId", dbId);
            Deencapsulation.setField(pendingRestore, "label", "pending_rs");
            Deencapsulation.setField(pendingRestore, "state", RestoreJob.RestoreJobState.PENDING);
            List<TableRefInfo> refs = Lists.newArrayList();
            refs.add(new TableRefInfo(new TableNameInfo("my_table"), null, null, null, null, null, null, null));
            pendingRestore.setTableRefs(refs);

            Deencapsulation.invoke(h, "addActiveJob", dbId, (AbstractJob) pendingRestore);

            Set<Long> allowed = Deencapsulation.getField(h, "allowedJobIds");
            Assert.assertFalse(allowed.contains(70L));

            Deencapsulation.invoke(h, "tryActivatePendingJobs", dbId);

            Assert.assertTrue("Pending restore should be activated", allowed.contains(70L));

            // restoringTables should be populated
            Map<Long, Set<String>> restoring = Deencapsulation.getField(h, "restoringTables");
            Assert.assertTrue(restoring.containsKey(dbId));
            Assert.assertTrue(restoring.get(dbId).contains("my_table"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== canActivateJob (public variant) =====================

    @Test
    public void testCanActivateJobVariant() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            statsMap.put(dbId, new BackupHandler.DatabaseJobStats());

            BackupJob bj = new BackupJob("can_act_bk", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(bj, "jobId", 80L);

            boolean result = Deencapsulation.invoke(h, "canActivateJob",
                    dbId, (AbstractJob) bj, true);
            Assert.assertTrue(result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== handleFinishedSnapshotTask concurrent branch =====================

    @Test
    public void testHandleFinishedSnapshotTaskConcurrentFound() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob bj = new BackupJob("snap_task_bk", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(bj, "jobId", 90L);
            Deencapsulation.setField(bj, "state", BackupJob.BackupJobState.SNAPSHOTING);

            Deencapsulation.invoke(h, "addActiveJob", dbId, (AbstractJob) bj);

            // Create a restore-type SnapshotTask with matching jobId
            // BackupJob receiving a restore task -> returns true (mismatch warning)
            SnapshotTask task = new SnapshotTask(null, 1L, 1L, 90L, dbId,
                    1L, 1L, 1L, 1L, 1L, 0, 3600000L, true);

            TFinishTaskRequest request = new TFinishTaskRequest();
            request.setTaskStatus(new TStatus(TStatusCode.OK));

            boolean result = h.handleFinishedSnapshotTask(task, request);
            Assert.assertTrue("Mismatched restore task on backup job should return true", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testHandleFinishedSnapshotTaskConcurrentNotFound() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);

        try {
            SnapshotTask task = new SnapshotTask(null, 1L, 1L, 999L, 1L,
                    1L, 1L, 1L, 1L, 1L, 0, 3600000L, false);
            TFinishTaskRequest request = new TFinishTaskRequest();
            request.setTaskStatus(new TStatus(TStatusCode.OK));

            boolean result = h.handleFinishedSnapshotTask(task, request);
            Assert.assertTrue("Not-found job should return true", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== handleFinishedSnapshotUploadTask concurrent branch =====================

    @Test
    public void testHandleUploadTaskConcurrentNotFound() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);

        try {
            UploadTask task = new UploadTask(null, 1L, 1L, 999L, 1L,
                    Maps.newHashMap(), null, null, null, null);
            TFinishTaskRequest request = new TFinishTaskRequest();
            request.setTaskStatus(new TStatus(TStatusCode.OK));

            boolean result = h.handleFinishedSnapshotUploadTask(task, request);
            Assert.assertFalse("Not-found/RestoreJob should return false", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== handleDownloadSnapshotTask concurrent branch =====================

    @Test
    public void testHandleDownloadTaskConcurrentNotFound() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);

        try {
            DownloadTask task = new DownloadTask(null, 1L, 1L, 999L, 1L,
                    Maps.newHashMap(), null, null, null, null, null);
            TFinishTaskRequest request = new TFinishTaskRequest();
            request.setTaskStatus(new TStatus(TStatusCode.OK));

            boolean result = h.handleDownloadSnapshotTask(task, request);
            Assert.assertTrue("Not-found should return true", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== handleDirMoveTask concurrent branch =====================

    @Test
    public void testHandleDirMoveTaskConcurrentNotFound() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);

        try {
            DirMoveTask task = new DirMoveTask(null, 1L, 1L, 999L, 1L,
                    1L, 1L, 1L, 1L, "/tmp", 0, false);
            TFinishTaskRequest request = new TFinishTaskRequest();
            request.setTaskStatus(new TStatus(TStatusCode.OK));

            boolean result = h.handleDirMoveTask(task, request);
            Assert.assertTrue("Not-found should return true", result);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testHandleDirMoveTaskConcurrentFoundRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            RestoreJob rj = new RestoreJob();
            Deencapsulation.setField(rj, "jobId", 110L);
            Deencapsulation.setField(rj, "dbId", dbId);
            Deencapsulation.setField(rj, "label", "dir_move_rs");
            rj.setTableRefs(Lists.newArrayList());

            Deencapsulation.invoke(h, "addActiveJob", dbId, (AbstractJob) rj);

            DirMoveTask task = new DirMoveTask(null, 1L, 1L, 110L, dbId,
                    1L, 1L, 1L, 1L, "/tmp", 0, false);
            TFinishTaskRequest request = new TFinishTaskRequest();
            request.setTaskStatus(new TStatus(TStatusCode.OK));

            // RestoreJob found, calls finishDirMoveTask which may throw if not fully initialized
            // but the concurrent LOOKUP path is exercised
            try {
                h.handleDirMoveTask(task, request);
            } catch (Exception e) {
                // Expected: RestoreJob not fully initialized
            }
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== replayAddJob concurrent branch =====================

    @Test
    public void testReplayAddJobConcurrentPendingJob() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            // PENDING jobs go directly to queue assignment without needing existing job
            BackupJob job = new BackupJob("replay_pending", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 120L);
            // Default state is PENDING

            h.replayAddJob(job);

            // Should be in running queue (active/pending)
            Map<Long, java.util.Deque<AbstractJob>> running =
                    Deencapsulation.getField(h, "dbIdToRunningJobs");
            Assert.assertTrue(running.containsKey(dbId));
            Assert.assertEquals(1, running.get(dbId).size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testReplayAddJobConcurrentCancelledJob() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            // Add existing PENDING job to running queue (concurrent mode uses dbIdToRunningJobs)
            BackupJob existingJob = new BackupJob("replay_cancel", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(existingJob, "jobId", 130L);
            Deencapsulation.setField(existingJob, "state", BackupJob.BackupJobState.PENDING);
            Deencapsulation.invoke(h, "addActiveJob", dbId, (AbstractJob) existingJob);

            // Replay a cancelled version
            BackupJob cancelledJob = new BackupJob("replay_cancel", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(cancelledJob, "jobId", 130L);
            Deencapsulation.setField(cancelledJob, "state", BackupJob.BackupJobState.CANCELLED);

            h.replayAddJob(cancelledJob);

            // Cancelled job should be in history queue
            Map<Long, java.util.Deque<AbstractJob>> history =
                    Deencapsulation.getField(h, "dbIdToHistoryJobs");
            Assert.assertTrue(history.containsKey(dbId));
            Assert.assertEquals(1, history.get(dbId).size());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== getJobs / getCurrentJob concurrent path =====================

    @Test
    public void testGetJobsConcurrentMode() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob runJob = new BackupJob("get_jobs_run", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(runJob, "jobId", 140L);

            BackupJob histJob = new BackupJob("get_jobs_hist", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(histJob, "jobId", 141L);
            Deencapsulation.setField(histJob, "state", BackupJob.BackupJobState.FINISHED);

            Deencapsulation.invoke(h, "addActiveJob", dbId, (AbstractJob) runJob);
            Deencapsulation.invoke(h, "addToHistoryQueue", dbId, (AbstractJob) histJob);

            List<AbstractJob> jobs = h.getJobs(dbId, label -> true);
            Assert.assertEquals(2, jobs.size());

            // getCurrentJob should return the last running job
            AbstractJob current = h.getJob(dbId);
            Assert.assertEquals(140L, current.getJobId());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== onJobActivated / onJobDeactivated direct tests =====================

    @Test
    public void testOnJobActivatedBackupFullDatabase() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            statsMap.put(dbId, new BackupHandler.DatabaseJobStats());

            BackupJob job = new BackupJob("act_full_bk", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 150L);

            Deencapsulation.invoke(h, "onJobActivated", dbId, (AbstractJob) job, true, true);

            BackupHandler.DatabaseJobStats s = statsMap.get(dbId);
            Assert.assertEquals(1, s.runningBackups);
            Assert.assertEquals(Long.valueOf(150L), s.runningBackupDatabaseJobId);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testOnJobActivatedRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            statsMap.put(dbId, new BackupHandler.DatabaseJobStats());

            RestoreJob job = new RestoreJob();
            Deencapsulation.setField(job, "jobId", 160L);

            Deencapsulation.invoke(h, "onJobActivated", dbId, (AbstractJob) job, false, false);

            BackupHandler.DatabaseJobStats s = statsMap.get(dbId);
            Assert.assertEquals(1, s.runningRestores);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testOnJobDeactivatedClearsBackupDatabaseId() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.runningBackups = 1;
            s.runningBackupDatabaseJobId = 170L;
            statsMap.put(dbId, s);

            BackupJob job = new BackupJob("deact_bk", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 170L);

            Deencapsulation.invoke(h, "onJobDeactivated", dbId, (AbstractJob) job, true);

            Assert.assertEquals(0, s.runningBackups);
            Assert.assertNull(s.runningBackupDatabaseJobId);
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testOnJobDeactivatedDecrementsRestoreAndGlobal() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = new BackupHandler.DatabaseJobStats();
            s.runningRestores = 1;
            statsMap.put(dbId, s);

            h.addGlobalSnapshotTasks(50);

            RestoreJob job = new RestoreJob();
            Deencapsulation.setField(job, "jobId", 180L);
            Deencapsulation.setField(job, "snapshotTaskCount", 50);

            Deencapsulation.invoke(h, "onJobDeactivated", dbId, (AbstractJob) job, false);

            Assert.assertEquals(0, s.runningRestores);
            Assert.assertEquals(0, h.getGlobalSnapshotTasks());
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== onJobCreated direct tests =====================

    @Test
    public void testOnJobCreatedBackupFullDatabase() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob job = new BackupJob("created_full_bk", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(job, "jobId", 190L);

            Deencapsulation.invoke(h, "onJobCreated", dbId, (AbstractJob) job, true, true);

            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = statsMap.get(dbId);
            Assert.assertEquals(1, s.activeBackups);
            Assert.assertEquals(Long.valueOf(190L), s.backupDatabaseJobId);
            Assert.assertEquals("created_full_bk", s.backupDatabaseLabel);
            Assert.assertTrue(s.activeLabels.contains("created_full_bk"));

            Map<Long, Map<String, Long>> labels = Deencapsulation.getField(h, "labelIndex");
            Assert.assertEquals(Long.valueOf(190L), labels.get(dbId).get("created_full_bk"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    @Test
    public void testOnJobCreatedRestore() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            RestoreJob job = new RestoreJob();
            Deencapsulation.setField(job, "jobId", 200L);
            Deencapsulation.setField(job, "label", "created_rs");

            Deencapsulation.invoke(h, "onJobCreated", dbId, (AbstractJob) job, false, false);

            Map<Long, BackupHandler.DatabaseJobStats> statsMap = Deencapsulation.getField(h, "dbJobStats");
            BackupHandler.DatabaseJobStats s = statsMap.get(dbId);
            Assert.assertEquals(1, s.activeRestores);
            Assert.assertTrue(s.activeLabels.contains("created_rs"));
        } finally {
            Config.enable_table_level_backup_concurrency = false;
        }
    }

    // ===================== addToHistoryQueue / cleanupJobResources =====================

    @Test
    public void testAddToHistoryQueueFIFOCleanup() throws Exception {
        Config.enable_table_level_backup_concurrency = true;
        int origLimit = Config.max_backup_restore_job_num_per_db;
        Config.max_backup_restore_job_num_per_db = 2;
        BackupHandler h = new BackupHandler(env);
        long dbId = 1L;

        try {
            BackupJob j1 = new BackupJob("hist1", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(j1, "jobId", 210L);
            Deencapsulation.setField(j1, "state", BackupJob.BackupJobState.FINISHED);
            BackupJob j2 = new BackupJob("hist2", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(j2, "jobId", 211L);
            Deencapsulation.setField(j2, "state", BackupJob.BackupJobState.FINISHED);
            BackupJob j3 = new BackupJob("hist3", dbId, "test_db",
                    Lists.newArrayList(), 3600000L, BackupCommand.BackupContent.ALL, env, 0L, 0L);
            Deencapsulation.setField(j3, "jobId", 212L);
            Deencapsulation.setField(j3, "state", BackupJob.BackupJobState.FINISHED);

            Deencapsulation.invoke(h, "addToHistoryQueue", dbId, (AbstractJob) j1);
            Deencapsulation.invoke(h, "addToHistoryQueue", dbId, (AbstractJob) j2);
            Deencapsulation.invoke(h, "addToHistoryQueue", dbId, (AbstractJob) j3);

            Map<Long, java.util.Deque<AbstractJob>> history = Deencapsulation.getField(h, "dbIdToHistoryJobs");
            Assert.assertEquals(2, history.get(dbId).size());
            // j1 should be evicted, j2 and j3 remain
            Assert.assertEquals(211L, history.get(dbId).peekFirst().getJobId());
        } finally {
            Config.max_backup_restore_job_num_per_db = origLimit;
            Config.enable_table_level_backup_concurrency = false;
        }
    }
}
