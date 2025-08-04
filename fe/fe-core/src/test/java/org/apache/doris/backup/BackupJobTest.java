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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.fs.FileSystemFactory;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BackupJobTest {

    private BackupJob job;
    private Database db;
    private OlapTable table2;

    private long dbId = 1;
    private long tblId = 2;
    private long partId = 3;
    private long idxId = 4;
    private long tabletId = 5;
    private long backendId = 10000;
    private long version = 6;

    private long tblId2 = 3;
    private long partId2 = 4;
    private long idxId2 = 5;
    private long tabletId2 = 6;
    private String table2Name = "testTable2";

    private long repoId = 20000;
    private AtomicLong id = new AtomicLong(50000);

    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

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

    private Repository repo = new Repository(repoId, "repo", false, "my_repo",
            FileSystemFactory.get("broker", StorageBackend.StorageType.BROKER, Maps.newHashMap()));

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
        backupHandler = new MockBackupHandler(env);

        // Thread is unmockable after Jmockit version 1.48, so use reflection to set field instead.
        Deencapsulation.setField(env, "backupHandler", backupHandler);

        db = UnitTestUtil.createDb(dbId, tblId, partId, idxId, tabletId, backendId, version);

        // Create second table in setUp to avoid Env initialization issues
        table2 = UnitTestUtil.createTable(db, tblId2, table2Name, partId2, idxId2, tabletId2, backendId, version);

        catalog = Deencapsulation.newInstance(InternalCatalog.class);
        new Expectations(env) {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = db;

                catalog.getTableByTableId(anyLong);
                minTimes = 0;
                result = new Delegate<Table>() {
                    public Table getTableByTableId(Long tableId) {
                        // Check if table exists in the database
                        return db.getTableNullable(tableId);
                    }
                };

                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;

                env.getNextId();
                minTimes = 0;
                result = new Delegate<Long>() {
                    public Long getNextId() {
                        return id.getAndIncrement();
                    }
                };

                env.getEditLog();
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
            Status getBrokerAddress(Long beId, Env env, List<FsBroker> brokerAddrs) {
                brokerAddrs.add(new FsBroker());
                return Status.OK;
            }
        };

        // Only include first table to ensure other tests are not affected
        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRef(
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
                env, repo.getId(), 0);
    }

    /**
     * Test normal backup job execution flow
     *
     * Scenario: Backup a single table with all content
     * Expected Results:
     * 1. Job should progress through all states: PENDING -> SNAPSHOTING -> UPLOAD_SNAPSHOT -> UPLOADING -> SAVE_META -> UPLOAD_INFO -> FINISHED
     * 2. Backup meta should contain the correct table information
     * 3. Snapshot and upload tasks should be created and executed successfully
     * 4. Meta files should be saved and uploaded correctly
     * 5. Job should complete successfully with OK status
     */
    @Test
    public void testRunNormal() {
        // 1. pending
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
                            ((OlapTable) db.getTableNullable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
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
        TStatus taskStatus = new TStatus(TStatusCode.OK);
        TBackend tBackend = new TBackend("", 0, 1);
        TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                snapshotTask.getSignature(), taskStatus);
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
        String dest = srcToDest.get(snapshotPath + "/" + tabletId + "/" + 0);
        Assert.assertNotNull(dest);

        // 5. uploading
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        request = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD,
                upTask.getSignature(), taskStatus);
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
            restoreMetaInfo = BackupMeta.fromFile(job.getLocalMetaInfoFilePath(), FeConstants.meta_version);
            Assert.assertEquals(1, restoreMetaInfo.getTables().size());
            OlapTable olapTable = (OlapTable) restoreMetaInfo.getTable(tblId);
            Assert.assertNotNull(olapTable);
            Assert.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.TABLE_NAME));
            List<String> names = Lists.newArrayList(olapTable.getPartitionNames());
            Assert.assertEquals(((OlapTable) db.getTableNullable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, names),
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

    /**
     * Test backup job execution with non-existent table
     *
     * Scenario: Attempt to backup a table that does not exist
     * Expected Results:
     * 1. Job should fail with NOT_FOUND error code
     * 2. Job state should be CANCELLED
     * 3. No backup tasks should be created
     */
    @Test
    public void testRunAbnormal() {
        // 1. pending
        AgentTaskQueue.clearAllTasks();

        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(
                new TableRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, "unknown_tbl"),
                        null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
                env, repo.getId(), 0);
        job.run();
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assert.assertEquals(BackupJobState.CANCELLED, job.getState());
    }

    /**
     * Test backup job execution with mixed existing and non-existent tables
     *
     * Scenario: Backup two tables - one existing table and one non-existent table
     * Expected Results:
     * 1. Job should succeed and proceed to SNAPSHOTING state
     * 2. Backup meta should only contain the existing table
     * 3. Only snapshot tasks for the existing table should be created
     * 4. Non-existent table should be skipped without causing job failure
     */
    @Test
    public void testRunAbnormalWithMixedTables() {
        // Test backup two tables: one normal table and one non-existent table
        // Verify backup succeeds, backs up the normal table, and skips the non-existent table
        AgentTaskQueue.clearAllTasks();

        List<TableRef> tableRefs = Lists.newArrayList();
        // Add normal table
        tableRefs.add(new TableRef(
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null));
        // Add non-existent table
        tableRefs.add(
                new TableRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, "unknown_tbl"),
                        null));

        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
                env, repo.getId(), 0);

        // 1. pending
        Assert.assertEquals(BackupJobState.PENDING, job.getState());
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        // Verify backup meta only contains the normal table
        BackupMeta backupMeta = job.getBackupMeta();
        Assert.assertEquals(1, backupMeta.getTables().size());
        OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.TABLE_NAME);
        Assert.assertNotNull(backupTbl);
        Assert.assertNull(backupMeta.getTable("unknown_tbl"));

        // Verify only snapshot tasks for the normal table are created
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
        Assert.assertTrue(task instanceof SnapshotTask);
        SnapshotTask snapshotTask = (SnapshotTask) task;
        Assert.assertEquals(tblId, snapshotTask.getTableId());
        Assert.assertEquals(dbId, snapshotTask.getDbId());
        Assert.assertEquals(partId, snapshotTask.getPartitionId());
        Assert.assertEquals(idxId, snapshotTask.getIndexId());
        Assert.assertEquals(tabletId, snapshotTask.getTabletId());
    }

    /**
     * Test backup job execution when a table is dropped during SNAPSHOTING phase
     *
     * Scenario: Start backup with two normal tables, then drop one table during SNAPSHOTING phase
     * Expected Results:
     * 1. Job should start with two tables and create snapshot tasks for both
     * 2. When one table is dropped during SNAPSHOTING, the dropped table should be marked as dropped
     * 3. Backup should continue successfully with only the remaining table
     * 4. Final backup meta should only contain the non-dropped table
     * 5. Job should complete successfully with FINISHED state
     */
    @Test
    public void testRunWithTableDroppedDuringSnapshoting() {
        try {
            AgentTaskQueue.clearAllTasks();

            List<TableRef> tableRefs = Lists.newArrayList();
            tableRefs.add(new TableRef(
                    new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                    null));
            tableRefs.add(new TableRef(
                    new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, table2Name),
                    null));

            job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
                    env, repo.getId(), 0);

            // 1. pending - should create snapshot tasks for both tables
            Assert.assertEquals(BackupJobState.PENDING, job.getState());
            job.run();
            Assert.assertEquals(Status.OK, job.getStatus());
            Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

            // Verify backup meta contains both tables initially
            BackupMeta backupMeta = job.getBackupMeta();
            Assert.assertEquals(2, backupMeta.getTables().size());
            Assert.assertNotNull(backupMeta.getTable(UnitTestUtil.TABLE_NAME));
            Assert.assertNotNull(backupMeta.getTable(table2Name));

            // Verify snapshot tasks are created for both tables
            Assert.assertEquals(2, AgentTaskQueue.getTaskNum());

            // 2. Simulate dropping the second table during SNAPSHOTING phase
            db.unregisterTable(table2Name);

            // 3. Finish snapshot tasks
            SnapshotTask taskForDroppedTable = null;
            SnapshotTask taskForExistingTable = null;
            long taskTabletId1 = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 2).getTabletId();
            if (taskTabletId1 == tabletId) {
                taskForExistingTable = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 2);
                taskForDroppedTable = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
            } else {
                taskForDroppedTable = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 2);
                taskForExistingTable = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
            }

            TBackend tBackend = new TBackend("", 0, 1);

            // Finish task for dropped table
            TStatus taskStatusMissing = new TStatus(TStatusCode.TABLET_MISSING);
            taskStatusMissing.setErrorMsgs(Lists.newArrayList("Tablet missing"));
            TFinishTaskRequest requestMissing = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    taskForDroppedTable.getSignature(), taskStatusMissing);
            Assert.assertTrue(job.finishTabletSnapshotTask(taskForDroppedTable, requestMissing));

            // Finish task for existing table
            String snapshotPath = "/path/to/snapshot";
            List<String> snapshotFiles = Lists.newArrayList("1.dat", "1.idx", "1.hdr");
            TStatus taskStatusOK = new TStatus(TStatusCode.OK);
            TFinishTaskRequest requestOK = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    taskForExistingTable.getSignature(), taskStatusOK);
            requestOK.setSnapshotFiles(snapshotFiles);
            requestOK.setSnapshotPath(snapshotPath);
            Assert.assertTrue(job.finishTabletSnapshotTask(taskForExistingTable, requestOK));

            // 4. Continue the backup process
            job.run();
            Assert.assertEquals(Status.OK, job.getStatus());
            Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

            AgentTaskQueue.clearAllTasks();
            job.run(); // UPLOAD_SNAPSHOT -> UPLOADING
            Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
            UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);

            // Finish upload task
            Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
            List<String> tabletFiles = Lists.newArrayList();
            tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
            tabletFiles.add("1.idx.4f158689243a3d6030352fec3cfd3798");
            tabletFiles.add("1.hdr.4f158689243a3d6030352fec3cfd3798");
            tabletFileMap.put(taskForExistingTable.getTabletId(), tabletFiles);
            TFinishTaskRequest requestUpload = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD,
                    upTask.getSignature(), taskStatusOK);
            requestUpload.setTabletFiles(tabletFileMap);
            Assert.assertTrue(job.finishSnapshotUploadTask(upTask, requestUpload));

            job.run(); // UPLOADING -> SAVE_META
            Assert.assertEquals(BackupJobState.SAVE_META, job.getState());

            job.run(); // SAVE_META -> UPLOAD_INFO
            Assert.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());

            job.run(); // UPLOAD_INFO -> FINISHED
            Assert.assertEquals(BackupJobState.FINISHED, job.getState());

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            // Clean up: re-register the second table if it was removed
            if (db.getTableNullable(table2Name) == null && table2 != null) {
                db.registerTable(table2);
            }
        }
    }

    /**
     * Test backup job serialization and deserialization
     *
     * Scenario: Write backup job to file and read it back
     * Expected Results:
     * 1. Backup job should be successfully written to file
     * 2. Backup job should be successfully read from file
     * 3. All job properties should be preserved during serialization/deserialization
     * 4. Temporary files should be cleaned up
     */
    @Test
    public void testSerialization() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("backupJob", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(
                new TableRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                        null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
            env, repo.getId(), 123);

        job.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        BackupJob job2 = BackupJob.read(in);

        Assert.assertEquals(job.getJobId(), job2.getJobId());
        Assert.assertEquals(job.getDbId(), job2.getDbId());
        Assert.assertEquals(job.getCreateTime(), job2.getCreateTime());
        Assert.assertEquals(job.getType(), job2.getType());
        Assert.assertEquals(job.getCommitSeq(), job2.getCommitSeq());

        // 3. delete files
        in.close();
        Files.delete(path);
    }
}
