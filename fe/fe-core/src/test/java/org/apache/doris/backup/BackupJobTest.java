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

import org.apache.doris.analysis.BackupStmt;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.backup.BackupJob.BackupJobState;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackupJobTest {

    private BackupJob job;
    private Database db;

    private long dbId = 1;
    private long tblId = 2;
    private long partId = 3;
    private long idxId = 4;
    private long tabletId = 5;
    private long backendId = 10000;
    private long version = 6;
    private long versionHash = 7;

    private long repoId = 20000;
    private AtomicLong id = new AtomicLong(50000);

    @Mocked
    private Catalog catalog;

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockBackupHandler extends BackupHandler {
        public MockBackupHandler(Catalog catalog) {
            super(catalog);
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

    private Repository repo = new Repository(repoId, "repo", false, "my_repo",
            BlobStorage.create("broker", StorageBackend.StorageType.BROKER, Maps.newHashMap()));

    @BeforeClass
    public static void start() {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        backupDir.mkdirs();
    }
    
    @AfterClass
    public static void end() throws IOException {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        if (backupDir.exists()) {
            Files.walk(BackupHandler.BACKUP_ROOT_DIR,
                       FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    @Before
    public void setUp() {

        repoMgr = new MockRepositoryMgr();
        backupHandler = new MockBackupHandler(catalog);

        // Thread is unmockable after Jmockit version 1.48, so use reflection to set field instead.
        Deencapsulation.setField(catalog, "backupHandler", backupHandler);

        db = UnitTestUtil.createDb(dbId, tblId, partId, idxId, tabletId, backendId, version, versionHash);

        new Expectations(catalog) {
            {
                catalog.getDb(anyLong);
                minTimes = 0;
                result = db;

                Catalog.getCurrentCatalogJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;

                catalog.getNextId();
                minTimes = 0;
                result = id.getAndIncrement();

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
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

        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {

            }
        };

        new MockUp<Repository>() {
            @Mock
            Status upload(String localFilePath, String remoteFilePath) {
                return Status.OK;
            }

            @Mock
            Status getBrokerAddress(Long beId, Catalog catalog, List<FsBroker> brokerAddrs) {
                brokerAddrs.add(new FsBroker());
                return Status.OK;
            }
        };

        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME), null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
                catalog, repo.getId());
    }

    @Test
    public void testRunNormal() {
        // 1.pending
        Assert.assertEquals(BackupJobState.PENDING, job.getState());
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
        
        BackupMeta backupMeta = job.getBackupMeta();
        Assert.assertEquals(1, backupMeta.getTables().size());
        OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.TABLE_NAME);
        List<String> partNames = Lists.newArrayList(backupTbl.getPartitionNames());
        Assert.assertNotNull(backupTbl);
        Assert.assertEquals(backupTbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames),
                            ((OlapTable) db.getTable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, tabletId);
        Assert.assertTrue(task instanceof SnapshotTask);
        SnapshotTask snapshotTask = (SnapshotTask) task;
        
        // 2. snapshoting
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
        
        // 3. snapshot finished
        String snapshotPath = "/path/to/snapshot";
        List<String> snapshotFiles = Lists.newArrayList();
        snapshotFiles.add("1.dat");
        snapshotFiles.add("1.idx");
        snapshotFiles.add("1.hdr");
        TStatus task_status = new TStatus(TStatusCode.OK);
        TBackend tBackend = new TBackend("", 0, 1);
        TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                snapshotTask.getSignature(), task_status);
        request.setSnapshotFiles(snapshotFiles);
        request.setSnapshotPath(snapshotPath);
        Assert.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

        // 4. upload snapshots
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        task = AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
        Assert.assertTrue(task instanceof UploadTask);
        UploadTask upTask = (UploadTask) task;
        
        Assert.assertEquals(job.getJobId(), upTask.getJobId());
        Map<String, String> srcToDest = upTask.getSrcToDestPath();
        Assert.assertEquals(1, srcToDest.size());
        System.out.println(srcToDest);
        String dest = srcToDest.get(snapshotPath + "/" + tabletId + "/" + 0);
        Assert.assertNotNull(dest);

        // 5. uploading
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        request = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD,
                upTask.getSignature(), task_status);
        request.setTabletFiles(tabletFileMap);

        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        List<String> tabletFiles = Lists.newArrayList();
        tabletFileMap.put(tabletId, tabletFiles);
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.idx.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.hdr.4f158689243a3d6030352fec3cfd3798");
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.clear();
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("1.idx.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("1.hdr.4f158689243a3d6030352fec3cfd3798");
        Assert.assertTrue(job.finishSnapshotUploadTask(upTask, request));
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SAVE_META, job.getState());

        // 6. save meta
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
        File metaInfo = new File(job.getLocalMetaInfoFilePath());
        Assert.assertTrue(metaInfo.exists());
        File jobInfo = new File(job.getLocalJobInfoFilePath());
        Assert.assertTrue(jobInfo.exists());

        BackupMeta restoreMetaInfo = null;
        BackupJobInfo restoreJobInfo = null;
        try {
            restoreMetaInfo = BackupMeta.fromFile(job.getLocalMetaInfoFilePath(), -1);
            Assert.assertEquals(1, restoreMetaInfo.getTables().size());
            OlapTable olapTable = (OlapTable) restoreMetaInfo.getTable(tblId);
            Assert.assertNotNull(olapTable);
            Assert.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.TABLE_NAME));
            List<String> names = Lists.newArrayList(olapTable.getPartitionNames());
            Assert.assertEquals(((OlapTable) db.getTable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, names),
                                olapTable.getSignature(BackupHandler.SIGNATURE_VERSION, names));

            restoreJobInfo = BackupJobInfo.fromFile(job.getLocalJobInfoFilePath());
            Assert.assertEquals(UnitTestUtil.DB_NAME, restoreJobInfo.dbName);
            Assert.assertEquals(job.getLabel(), restoreJobInfo.name);
            Assert.assertEquals(1, restoreJobInfo.backupOlapTableObjects.values().size());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertNull(job.getBackupMeta());
        Assert.assertNull(job.getJobInfo());

        // 7. upload_info
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.FINISHED, job.getState());
    }

    @Test
    public void testRunAbnormal() {
        // 1.pending
        AgentTaskQueue.clearAllTasks();

        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, "unknown_tbl"), null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
                catalog, repo.getId());
        job.run();
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assert.assertEquals(BackupJobState.CANCELLED, job.getState());
    }
}
