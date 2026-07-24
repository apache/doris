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
import org.apache.doris.backup.BackupJob.BackupJobState;
import org.apache.doris.backup.BackupJob.LocalJobDirCleanupResult;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GZIPUtils;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.fs.FileSystemDescriptor;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.persist.EditLog;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);

    private BackupHandler backupHandler;

    private RepositoryMgr repoMgr;

    public BackupJobTest() throws UserException {
    }

    private EditLog editLog = Mockito.mock(EditLog.class);

    private Repository repo = Mockito.spy(new Repository(repoId, "repo", false, "my_repo",
            BrokerProperties.of("broker", Maps.newHashMap())));

    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<AgentTaskExecutor> mockedAgentTaskExecutor;
    private MockedConstruction<FileSystemDescriptor> mockedFsDescriptor;

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
        repoMgr = Mockito.mock(RepositoryMgr.class);
        backupHandler = Mockito.mock(BackupHandler.class);

        db = UnitTestUtil.createDb(dbId, tblId, partId, idxId, tabletId, backendId, version);

        // Create second table in setUp to avoid Env initialization issues
        table2 = UnitTestUtil.createTable(db, tblId2, table2Name, partId2, idxId2, tabletId2, backendId, version);

        // Mock Env static methods
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);

        // Mock instance methods on env
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getNextId()).thenAnswer(inv -> id.getAndIncrement());
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getBackupHandler()).thenReturn(backupHandler);

        // Mock repository lookup through Env.getCurrentEnv().getBackupHandler().getRepoMgr()
        Mockito.when(backupHandler.getRepoMgr()).thenReturn(repoMgr);
        Mockito.when(repoMgr.getRepo(Mockito.anyLong())).thenReturn(repo);

        // Mock instance methods on catalog
        Mockito.when(catalog.getDbNullable(ArgumentMatchers.anyLong())).thenReturn(db);
        Mockito.when(catalog.getTableByTableId(ArgumentMatchers.anyLong())).thenAnswer(inv -> {
            Long tableId = inv.getArgument(0);
            return db.getTableNullable(tableId);
        });

        // Mock editLog
        Mockito.doAnswer(inv -> {
            BackupJob job = inv.getArgument(0);
            System.out.println("log backup job: " + job);
            return null;
        }).when(editLog).logBackupJob(ArgumentMatchers.any(BackupJob.class));

        // Mock AgentTaskExecutor static method
        mockedAgentTaskExecutor = Mockito.mockStatic(AgentTaskExecutor.class);

        // Mock Repository instance methods via spy
        Mockito.doReturn(Status.OK).when(repo).upload(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        Mockito.doAnswer(inv -> {
            List<FsBroker> brokerAddrs = inv.getArgument(2);
            brokerAddrs.add(new FsBroker());
            return Status.OK;
        }).when(repo).getBrokerAddress(ArgumentMatchers.any(Long.class), ArgumentMatchers.any(Env.class), ArgumentMatchers.anyList());
        FileSystemDescriptor fileSystemDescriptor = Mockito.mock(FileSystemDescriptor.class);
        Mockito.when(fileSystemDescriptor.getBackendConfigProperties()).thenReturn(Maps.newHashMap());
        Mockito.when(fileSystemDescriptor.getThriftStorageType()).thenReturn(StorageBackend.StorageType.BROKER);
        Mockito.doReturn(fileSystemDescriptor).when(repo).getFileSystemDescriptor();

        // Mock FileSystemDescriptor construction
        mockedFsDescriptor = Mockito.mockConstruction(FileSystemDescriptor.class, (mock, ctx) -> {
            Mockito.when(mock.getBackendConfigProperties()).thenReturn(Maps.newHashMap());
        });

        // Only include first table to ensure other tests are not affected
        List<TableRefInfo> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null,
                null,
                null,
                new ArrayList<>(),
                null,
                null,
                new ArrayList<>()));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
                env, repo.getId(), 0);
    }

    @After
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        if (mockedAgentTaskExecutor != null) {
            mockedAgentTaskExecutor.close();
        }
        if (mockedFsDescriptor != null) {
            mockedFsDescriptor.close();
        }
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
        File localJobDir = jobInfo.getParentFile();

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
        Assert.assertTrue(localJobDir.exists());

        BackupHandler handler = new BackupHandler(env);
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId, job);
        Deencapsulation.invoke(handler, "cleanupBackupJobLocalJobDirs", System.currentTimeMillis());
        Assert.assertFalse(localJobDir.exists());
    }

    @Test
    public void testBackupCopyTableWithDirtyDynamicPartitionStorageMedium() {
        Map<String, String> dirtyProperties = Maps.newHashMap();
        dirtyProperties.put(DynamicPartitionProperty.STORAGE_MEDIUM, "hdd");
        table2.setTableProperty(new TableProperty(dirtyProperties));

        Assert.assertFalse(table2.dynamicPartitionExists());
        OlapTable copied = table2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assert.assertNotNull(copied);
        Assert.assertFalse(copied.dynamicPartitionExists());
        Assert.assertTrue(copied.getTableProperty().hasInvalidDynamicPartition());
    }

    @Test
    public void testBackupCopyTableWithDirtyDynamicPartitionStoragePolicy() {
        Map<String, String> dirtyProperties = Maps.newHashMap();
        dirtyProperties.put(DynamicPartitionProperty.STORAGE_POLICY, "test_policy");
        table2.setTableProperty(new TableProperty(dirtyProperties));

        Assert.assertFalse(table2.dynamicPartitionExists());
        OlapTable copied = table2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assert.assertNotNull(copied);
        Assert.assertFalse(copied.dynamicPartitionExists());
        Assert.assertTrue(copied.getTableProperty().hasInvalidDynamicPartition());
    }

    @Test
    public void testCleanupFinishedRemoteBackupJobDirFromPersistedFilePaths() throws IOException {
        JobDirFixture fixture = createJobDirFixture(job, "remote_cleanup", BackupJobState.FINISHED);

        Assert.assertEquals(LocalJobDirCleanupResult.DELETED,
                job.cleanupLocalJobDirIfNecessary(System.currentTimeMillis()));

        Assert.assertFalse(fixture.jobDir.exists());
    }

    @Test
    public void testCleanupLocalSnapshotJobDirOnlyAfterExpired() throws IOException {
        JobDirFixture fixture = createLocalJobDirFixture(
                "local_snapshot_cleanup", 1000, BackupJobState.FINISHED);

        Assert.assertEquals(LocalJobDirCleanupResult.NOT_DUE,
                fixture.job.cleanupLocalJobDirIfNecessary(fixture.createTime + 999));
        Assert.assertTrue(fixture.jobDir.exists());

        Assert.assertEquals(LocalJobDirCleanupResult.DELETED,
                fixture.job.cleanupLocalJobDirIfNecessary(fixture.createTime + 1000));
        Assert.assertFalse(fixture.jobDir.exists());
    }

    @Test
    public void testLocalSnapshotDataRemainsReadableAfterJobDirCleanup() throws IOException {
        byte[] metaBytes = new byte[] {1, 2, 3};
        byte[] jobInfoBytes = new byte[] {4, 5, 6};
        JobDirFixture fixture = createLocalJobDirFixture("local_snapshot_read_cleanup", 1000,
                BackupJobState.FINISHED, metaBytes, jobInfoBytes);

        Snapshot snapshot = fixture.job.getSnapshot(false);
        Assert.assertEquals(LocalJobDirCleanupResult.DELETED, fixture.job.cleanupLocalJobDirAfterRemoved());

        Assert.assertFalse(fixture.jobDir.exists());
        Assert.assertFalse(snapshot.isCompressed());
        Assert.assertEquals(metaBytes.length, snapshot.getMetaSize());
        Assert.assertEquals(jobInfoBytes.length, snapshot.getJobInfoSize());
        Assert.assertArrayEquals(metaBytes, snapshot.getMeta());
        Assert.assertArrayEquals(jobInfoBytes, snapshot.getJobInfo());
    }

    /**
     * Oversized local snapshot must not be fully read into FE heap when compression is off.
     * Sparse files provide a logical size &gt;= Integer.MAX_VALUE without allocating disk/RAM.
     */
    @Test
    public void testLocalSnapshotOversizedWithoutCompressDoesNotReadFiles() throws IOException {
        JobDirFixture fixture = createLocalJobDirFixture(
                "local_snapshot_oversized", 3600 * 1000, BackupJobState.FINISHED);

        long metaLogicalSize = (long) Integer.MAX_VALUE - 100L;
        long jobInfoLogicalSize = 100L;
        createSparseFile(fixture.metaInfo, metaLogicalSize);
        createSparseFile(fixture.jobInfo, jobInfoLogicalSize);
        Assert.assertEquals(metaLogicalSize, fixture.metaInfo.length());
        Assert.assertEquals(jobInfoLogicalSize, fixture.jobInfo.length());

        Snapshot snapshot = fixture.job.getSnapshot(false);

        Assert.assertNotNull(snapshot);
        Assert.assertFalse(snapshot.isExpired());
        Assert.assertFalse(snapshot.isCompressed());
        Assert.assertEquals(metaLogicalSize, snapshot.getMetaSize());
        Assert.assertEquals(jobInfoLogicalSize, snapshot.getJobInfoSize());
        Assert.assertTrue(snapshot.getMetaSize() + snapshot.getJobInfoSize() >= Integer.MAX_VALUE);
        // Content must not be materialized for oversized uncompressed responses.
        Assert.assertNull(snapshot.getMeta());
        Assert.assertNull(snapshot.getJobInfo());
        // Job dir must still exist; size guard rejected before any cleanup side-effect.
        Assert.assertTrue(fixture.jobDir.exists());
    }

    @Test
    public void testLocalSnapshotCompressStreamsFilesAndSurvivesCleanup() throws IOException {
        // Slightly compressible payload; enough bytes to exercise streaming path.
        byte[] metaBytes = new byte[64 * 1024];
        byte[] jobInfoBytes = new byte[32 * 1024];
        for (int i = 0; i < metaBytes.length; i++) {
            metaBytes[i] = (byte) (i % 7);
        }
        for (int i = 0; i < jobInfoBytes.length; i++) {
            jobInfoBytes[i] = (byte) (i % 11);
        }
        JobDirFixture fixture = createLocalJobDirFixture("local_snapshot_compress", 3600 * 1000,
                BackupJobState.FINISHED, metaBytes, jobInfoBytes);

        Snapshot snapshot = fixture.job.getSnapshot(true);
        Assert.assertNotNull(snapshot);
        Assert.assertTrue(snapshot.isCompressed());
        Assert.assertEquals(metaBytes.length, snapshot.getMetaSize());
        Assert.assertEquals(jobInfoBytes.length, snapshot.getJobInfoSize());
        Assert.assertTrue(GZIPUtils.isGZIPCompressed(snapshot.getMeta()));
        Assert.assertTrue(GZIPUtils.isGZIPCompressed(snapshot.getJobInfo()));
        Assert.assertArrayEquals(metaBytes, GZIPUtils.decompress(snapshot.getMeta()));
        Assert.assertArrayEquals(jobInfoBytes, GZIPUtils.decompress(snapshot.getJobInfo()));

        Assert.assertEquals(LocalJobDirCleanupResult.DELETED, fixture.job.cleanupLocalJobDirAfterRemoved());
        Assert.assertFalse(fixture.jobDir.exists());
        // Materialized compressed payload remains usable after job dir is deleted.
        Assert.assertArrayEquals(metaBytes, GZIPUtils.decompress(snapshot.getMeta()));
        Assert.assertArrayEquals(jobInfoBytes, GZIPUtils.decompress(snapshot.getJobInfo()));
    }

    /**
     * Active getSnapshot readers pin the staging dir via refcount; cleanup is deferred
     * to a later handler cleanup cycle after the last reader releases, so neither
     * mid-read deletion nor directory IO on the RPC thread can occur.
     */
    @Test
    public void testLocalSnapshotCleanupDefersWhileGetSnapshotReaderPinned() throws Exception {
        byte[] metaBytes = new byte[] {10, 20, 30, 40};
        byte[] jobInfoBytes = new byte[] {50, 60, 70};
        JobDirFixture fixture = createLocalJobDirFixture("local_snapshot_lock", 3600 * 1000,
                BackupJobState.FINISHED, metaBytes, jobInfoBytes);

        // Simulate an in-flight getSnapshot reader pin.
        Deencapsulation.setField(fixture.job, "localJobDirReaders", 1);
        Assert.assertEquals(LocalJobDirCleanupResult.DEFERRED, fixture.job.cleanupLocalJobDirAfterRemoved());
        Assert.assertTrue("cleanup must defer while a reader is pinned", fixture.jobDir.exists());

        // Release the pin (readers 1 -> 0); the RPC reader must not run directory IO.
        Deencapsulation.invoke(fixture.job, "releaseLocalJobDirRead");
        Assert.assertTrue(fixture.jobDir.exists());
        Assert.assertFalse(Deencapsulation.getField(fixture.job, "localJobDirCleaned"));

        // A later backup handler cleanup cycle performs the deferred deletion.
        Assert.assertEquals(LocalJobDirCleanupResult.DELETED, fixture.job.cleanupLocalJobDirAfterRemoved());
        Assert.assertFalse(fixture.jobDir.exists());
        Assert.assertTrue(Deencapsulation.getField(fixture.job, "localJobDirCleaned"));
    }

    @Test
    public void testCleanupRetriesAfterDeleteFailure() throws IOException {
        boolean[] failDelete = {true};
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupCommand.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0) {
            @Override
            void deleteLocalJobDir(Path jobDirPath) throws IOException {
                if (failDelete[0]) {
                    failDelete[0] = false;
                    throw new IOException("injected delete failure");
                }
                super.deleteLocalJobDir(jobDirPath);
            }
        };
        JobDirFixture fixture = createJobDirFixture(
                localSnapshotJob, "local_snapshot_delete_retry", BackupJobState.CANCELLED);

        Assert.assertEquals(LocalJobDirCleanupResult.FAILED,
                localSnapshotJob.cleanupLocalJobDirIfNecessary(fixture.createTime));
        Assert.assertTrue(fixture.jobDir.exists());
        Assert.assertFalse(Deencapsulation.getField(localSnapshotJob, "localJobDirCleaned"));

        Assert.assertEquals(LocalJobDirCleanupResult.DELETED,
                localSnapshotJob.cleanupLocalJobDirIfNecessary(fixture.createTime));
        Assert.assertFalse(fixture.jobDir.exists());
        Assert.assertTrue(Deencapsulation.getField(localSnapshotJob, "localJobDirCleaned"));
    }

    @Test
    public void testLocalSnapshotIoFailurePropagatesAsIOException() throws IOException {
        JobDirFixture fixture = createLocalJobDirFixture(
                "local_snapshot_io_fail", 3600 * 1000, BackupJobState.FINISHED);

        Assert.assertTrue(fixture.metaInfo.delete());
        try {
            fixture.job.getSnapshot(false);
            Assert.fail("expected IOException when meta file is missing");
        } catch (IOException e) {
            // Must not be swallowed as null / SNAPSHOT_NOT_EXIST.
            Assert.assertNotNull(e.getMessage());
        }
        // Failed materialization still releases the pin so later cleanup can proceed.
        Assert.assertEquals(LocalJobDirCleanupResult.DELETED, fixture.job.cleanupLocalJobDirAfterRemoved());
        Assert.assertFalse(fixture.jobDir.exists());
    }

    @Test
    public void testGetSnapshotReturnsNullWhenUnavailable() throws IOException {
        Assert.assertNull(job.getSnapshot(false));

        JobDirFixture remoteFixture = createJobDirFixture(
                job, "remote_snapshot_unavailable", BackupJobState.FINISHED);
        Assert.assertNull(remoteFixture.job.getSnapshot(false));

        JobDirFixture missingPathFixture = createLocalJobDirFixture(
                "local_snapshot_missing_path", 3600 * 1000, BackupJobState.FINISHED);
        Deencapsulation.setField(missingPathFixture.job, "localMetaInfoFilePath", null);
        Assert.assertNull(missingPathFixture.job.getSnapshot(false));

        JobDirFixture cleanedFixture = createLocalJobDirFixture(
                "local_snapshot_cleaned", 3600 * 1000, BackupJobState.FINISHED);
        Deencapsulation.setField(cleanedFixture.job, "localJobDirCleaned", true);
        Assert.assertNull(cleanedFixture.job.getSnapshot(false));

        BackupJob pathWithoutParentJob = new BackupJob("path_without_parent", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupCommand.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        Deencapsulation.setField(pathWithoutParentJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(pathWithoutParentJob, "localMetaInfoFilePath", Repository.FILE_META_INFO);
        Deencapsulation.setField(pathWithoutParentJob, "localJobInfoFilePath", Repository.PREFIX_JOB_INFO + "test");
        Assert.assertNull(pathWithoutParentJob.getSnapshot(false));
    }

    @Test
    public void testCleanupHandlesMissingPersistedAndInvalidPaths() throws IOException {
        BackupJob pendingJob = new BackupJob("pending_cleanup", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupCommand.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        Assert.assertEquals(LocalJobDirCleanupResult.NOT_DUE, pendingJob.cleanupLocalJobDirAfterRemoved());

        BackupJob missingPathJob = new BackupJob("missing_cleanup_path", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupCommand.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        Deencapsulation.setField(missingPathJob, "state", BackupJobState.FINISHED);
        Assert.assertEquals(LocalJobDirCleanupResult.ALREADY_CLEANED,
                missingPathJob.cleanupLocalJobDirAfterRemoved());
        Assert.assertEquals(LocalJobDirCleanupResult.ALREADY_CLEANED,
                missingPathJob.cleanupLocalJobDirAfterRemoved());

        Path invalidJobDir = Files.createTempDirectory("invalid_backup_job_dir");
        try {
            BackupJob invalidPathJob = new BackupJob("invalid_cleanup_path", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 3600 * 1000, BackupCommand.BackupContent.ALL,
                    env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
            Deencapsulation.setField(invalidPathJob, "state", BackupJobState.FINISHED);
            Deencapsulation.setField(invalidPathJob, "localJobDirPath", invalidJobDir);

            Assert.assertEquals(LocalJobDirCleanupResult.INVALID_PATH,
                    invalidPathJob.cleanupLocalJobDirAfterRemoved());
            Assert.assertTrue(Files.exists(invalidJobDir));
        } finally {
            Files.deleteIfExists(invalidJobDir);
        }

        File persistedJobDir = createLocalJobDir("job_info_only_cleanup");
        File persistedJobInfo = new File(persistedJobDir, Repository.PREFIX_JOB_INFO + "persisted");
        Files.write(persistedJobInfo.toPath(), new byte[0]);
        BackupJob jobInfoOnlyJob = new BackupJob("job_info_only", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupCommand.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        Deencapsulation.setField(jobInfoOnlyJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(jobInfoOnlyJob, "localJobInfoFilePath", persistedJobInfo.getAbsolutePath());

        Assert.assertEquals(LocalJobDirCleanupResult.DELETED, jobInfoOnlyJob.cleanupLocalJobDirAfterRemoved());
        Assert.assertFalse(persistedJobDir.exists());
    }

    @Test
    public void testCleanupStatsDistinguishKnownJobResults() throws Exception {
        BackupHandler handler = new BackupHandler(env);

        BackupJob deletedJob = new BackupJob("deleted", dbId + 10, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupCommand.BackupContent.ALL, env, repoId, 0);
        createJobDirFixture(deletedJob, "stats_deleted", BackupJobState.FINISHED);
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId + 10, deletedJob);

        BackupJob absentJob = new BackupJob("absent", dbId + 11, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupCommand.BackupContent.ALL, env, repoId, 0);
        JobDirFixture absentFixture = createJobDirFixture(absentJob, "stats_absent", BackupJobState.FINISHED);
        Files.delete(absentFixture.metaInfo.toPath());
        Files.delete(absentFixture.jobInfo.toPath());
        Files.delete(absentFixture.jobDir.toPath());
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId + 11, absentJob);

        BackupJob deferredJob = new BackupJob("deferred", dbId + 12, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupCommand.BackupContent.ALL, env, repoId, 0);
        createJobDirFixture(deferredJob, "stats_deferred", BackupJobState.FINISHED);
        Deencapsulation.setField(deferredJob, "localJobDirReaders", 1);
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId + 12, deferredJob);

        BackupJob failedJob = new BackupJob("failed", dbId + 13, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupCommand.BackupContent.ALL, env, repoId, 0) {
            @Override
            void deleteLocalJobDir(Path jobDirPath) throws IOException {
                throw new IOException("injected delete failure");
            }
        };
        createJobDirFixture(failedJob, "stats_failed", BackupJobState.FINISHED);
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId + 13, failedJob);

        BackupJob notDueJob = new BackupJob("not_due", dbId + 14, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupCommand.BackupContent.ALL, env, repoId, 0);
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId + 14, notDueJob);

        Object stats = Deencapsulation.invoke(handler, "cleanupBackupJobLocalJobDirs",
                System.currentTimeMillis());
        Assert.assertEquals(5, (int) Deencapsulation.getField(stats, "itemsScanned"));
        Assert.assertEquals(1, (int) Deencapsulation.getField(stats, "directoriesDeleted"));
        Assert.assertEquals(1, (int) Deencapsulation.getField(stats, "directoriesAlreadyAbsent"));
        Assert.assertEquals(1, (int) Deencapsulation.getField(stats, "jobsDeferred"));
        Assert.assertEquals(1, (int) Deencapsulation.getField(stats, "failures"));
        Assert.assertEquals(1, (int) Deencapsulation.getField(stats, "notDue"));
    }

    @Test
    public void testCleanupCancelledLocalSnapshotJobDirFromPersistedFilePaths() throws IOException {
        JobDirFixture fixture = createLocalJobDirFixture(
                "local_cancel_cleanup", 1000, BackupJobState.CANCELLED);

        fixture.job.cleanupLocalJobDirIfNecessary(fixture.createTime);
        Assert.assertFalse(fixture.jobDir.exists());
    }

    @Test
    public void testExpiredGetSnapshotDoesNotDeleteJobDir() throws IOException {
        JobDirFixture fixture = createLocalJobDirFixture(
                "expired_snapshot_rpc", 1000, BackupJobState.FINISHED);
        long expiredCreateTime = fixture.createTime - 1000;
        Deencapsulation.setField(fixture.job, "createTime", expiredCreateTime);

        Snapshot snapshot = fixture.job.getSnapshot(false);
        Assert.assertTrue(snapshot.isExpired());
        Assert.assertTrue(fixture.jobDir.exists());

        fixture.job.cleanupLocalJobDirIfNecessary(expiredCreateTime + 1000);
        Assert.assertFalse(fixture.jobDir.exists());
    }

    @Test
    public void testEvictedJobCleanupWaitsForPinnedReader() throws Exception {
        int oldMaxJobNum = Config.max_backup_restore_job_num_per_db;
        Config.max_backup_restore_job_num_per_db = 1;
        try {
            BackupHandler handler = new BackupHandler(env);
            BackupJob evictedJob = new BackupJob("evicted_label", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 1000, BackupCommand.BackupContent.ALL, env, repoId, 0);
            JobDirFixture fixture = createJobDirFixture(
                    evictedJob, "evicted_snapshot", BackupJobState.FINISHED);
            Deencapsulation.setField(evictedJob, "localJobDirReaders", 1);

            BackupJob replacementJob = new BackupJob("replacement_label", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 1000, BackupCommand.BackupContent.ALL, env, repoId, 0);
            Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId, evictedJob);
            Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId, replacementJob);

            Deque<BackupJob> pendingCleanupJobs = Deencapsulation.getField(handler, "pendingCleanupJobs");
            Assert.assertEquals(1, pendingCleanupJobs.size());
            Deencapsulation.invoke(handler, "cleanupPendingBackupJobLocalJobDirs");
            Assert.assertTrue(fixture.jobDir.exists());
            Assert.assertEquals(1, pendingCleanupJobs.size());

            Deencapsulation.invoke(evictedJob, "releaseLocalJobDirRead");
            Assert.assertTrue(fixture.jobDir.exists());
            Deencapsulation.invoke(handler, "cleanupPendingBackupJobLocalJobDirs");
            Assert.assertFalse(fixture.jobDir.exists());
            Assert.assertTrue(pendingCleanupJobs.isEmpty());
        } finally {
            Config.max_backup_restore_job_num_per_db = oldMaxJobNum;
        }
    }

    @Test
    public void testEvictedLocalSnapshotIsRemovedAndCleaned() throws Exception {
        int oldMaxJobNum = Config.max_backup_restore_job_num_per_db;
        Config.max_backup_restore_job_num_per_db = 1;
        try {
            BackupHandler handler = new BackupHandler(env);
            JobDirFixture fixture = createLocalJobDirFixture(
                    "evicted_local_snapshot", 3600 * 1000, BackupJobState.FINISHED,
                    new byte[] {1, 2}, new byte[] {3, 4});
            Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId, fixture.job);

            Assert.assertNotNull(handler.getSnapshot(fixture.job.getLabel(), false));

            BackupJob replacementJob = new BackupJob("replacement_label", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 3600 * 1000, BackupCommand.BackupContent.ALL, env, repoId, 0);
            Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId, replacementJob);

            Assert.assertNull(handler.getSnapshot(fixture.job.getLabel(), false));
            Assert.assertTrue(fixture.jobDir.exists());

            Deencapsulation.invoke(handler, "cleanupPendingBackupJobLocalJobDirs");
            Assert.assertFalse(fixture.jobDir.exists());
        } finally {
            Config.max_backup_restore_job_num_per_db = oldMaxJobNum;
        }
    }

    @Test
    public void testCheckpointReplayAndEvictionDoNotTouchJobDirs() throws Exception {
        int oldMaxJobNum = Config.max_backup_restore_job_num_per_db;
        Config.max_backup_restore_job_num_per_db = 1;
        mockedEnvStatic.when(Env::isCheckpointThread).thenReturn(true);
        try {
            Deencapsulation.setField(job, "state", BackupJobState.SAVE_META);
            job.replayRun();
            Assert.assertNull(job.getLocalMetaInfoFilePath());
            Assert.assertNull(job.getLocalJobInfoFilePath());

            BackupHandler handler = new BackupHandler(env);
            BackupJob finishedJob = new BackupJob("checkpoint_finished", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 1000, BackupCommand.BackupContent.ALL, env, repoId, 0);
            File localJobDir = createLocalJobDir("checkpoint_eviction");
            Deencapsulation.setField(finishedJob, "state", BackupJobState.FINISHED);
            Deencapsulation.setField(finishedJob, "localJobDirPath", localJobDir.toPath());
            BackupJob replacementJob = new BackupJob("checkpoint_replacement", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 1000, BackupCommand.BackupContent.ALL, env, repoId, 0);

            Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId, finishedJob);
            Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId, replacementJob);

            Deque<BackupJob> pendingCleanupJobs = Deencapsulation.getField(handler, "pendingCleanupJobs");
            Assert.assertTrue(pendingCleanupJobs.isEmpty());
            Assert.assertTrue(localJobDir.exists());
        } finally {
            Config.max_backup_restore_job_num_per_db = oldMaxJobNum;
            mockedEnvStatic.when(Env::isCheckpointThread).thenReturn(false);
        }
    }

    @Test
    public void testOrphanJobDirCleanupUsesReferencesAndGracePeriod() throws Exception {
        int oldOrphanKeepSecond = Config.backup_orphan_dir_keep_max_second;
        Config.backup_orphan_dir_keep_max_second = 2 * 24 * 3600;
        Path externalDir = Files.createTempDirectory("backup_orphan_external");
        Path symlink = null;
        try {
            long nowMs = System.currentTimeMillis();
            File referencedDir = createRepoJobDir(repoId, "referenced");
            File oldOrphanDir = createRepoJobDir(repoId, "old_orphan");
            File newOrphanDir = createRepoJobDir(repoId, "new_orphan");
            Files.setLastModifiedTime(referencedDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
            Files.setLastModifiedTime(oldOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));

            BackupHandler handler = new BackupHandler(env);
            BackupJob referencedJob = new BackupJob("referenced_label", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), TimeUnit.DAYS.toMillis(7), BackupCommand.BackupContent.ALL,
                    env, repoId, 0);
            Deencapsulation.setField(referencedJob, "state", BackupJobState.FINISHED);
            Deencapsulation.setField(referencedJob, "localJobDirPath", referencedDir.toPath());
            Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId, referencedJob);

            Path repoDir = referencedDir.toPath().getParent();
            symlink = repoDir.resolve("orphan_symlink_" + id.getAndIncrement());
            Files.createSymbolicLink(symlink, externalDir);

            Deencapsulation.invoke(handler, "cleanupOrphanBackupJobLocalJobDirs", nowMs);

            Assert.assertTrue(referencedDir.exists());
            Assert.assertFalse(oldOrphanDir.exists());
            Assert.assertTrue(newOrphanDir.exists());
            Assert.assertTrue(Files.exists(symlink, LinkOption.NOFOLLOW_LINKS));
            Assert.assertTrue(Files.exists(externalDir));

            File disabledCleanupOrphanDir = createRepoJobDir(repoId, "disabled_cleanup_orphan");
            Files.setLastModifiedTime(disabledCleanupOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
            Config.backup_orphan_dir_keep_max_second = 0;
            Deencapsulation.invoke(handler, "cleanupOrphanBackupJobLocalJobDirs", nowMs);
            Assert.assertTrue(disabledCleanupOrphanDir.exists());
        } finally {
            if (symlink != null) {
                Files.deleteIfExists(symlink);
            }
            Files.deleteIfExists(externalDir);
            Config.backup_orphan_dir_keep_max_second = oldOrphanKeepSecond;
        }
    }

    @Test
    public void testOrphanJobDirCleanupUsesIndependentInterval() throws Exception {
        long oldCleanupIntervalSecond = Config.backup_orphan_dir_cleanup_interval_second;
        Config.backup_orphan_dir_cleanup_interval_second = 3600;
        try {
            long nowMs = System.currentTimeMillis();
            BackupHandler handler = new BackupHandler(env);

            File firstOldOrphanDir = createRepoJobDir(repoId, "first_interval_orphan");
            Files.setLastModifiedTime(firstOldOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
            Deencapsulation.invoke(handler, "cleanupOrphanBackupJobLocalJobDirsIfNecessary", nowMs);
            Assert.assertFalse(firstOldOrphanDir.exists());

            File secondOldOrphanDir = createRepoJobDir(repoId, "second_interval_orphan");
            Files.setLastModifiedTime(secondOldOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
            Deencapsulation.invoke(handler, "cleanupOrphanBackupJobLocalJobDirsIfNecessary",
                    nowMs + Config.backup_handler_update_interval_millis);
            Assert.assertTrue(secondOldOrphanDir.exists());

            Deencapsulation.invoke(handler, "cleanupOrphanBackupJobLocalJobDirsIfNecessary",
                    nowMs + TimeUnit.HOURS.toMillis(1));
            Assert.assertFalse(secondOldOrphanDir.exists());
        } finally {
            Config.backup_orphan_dir_cleanup_interval_second = oldCleanupIntervalSecond;
        }
    }

    @Test
    public void testOrphanJobDirCleanupCanBeDisabled() throws Exception {
        long oldCleanupIntervalSecond = Config.backup_orphan_dir_cleanup_interval_second;
        Config.backup_orphan_dir_cleanup_interval_second = 0;
        try {
            BackupHandler handler = new BackupHandler(env);
            Deencapsulation.invoke(handler, "cleanupOrphanBackupJobLocalJobDirsIfNecessary",
                    System.currentTimeMillis());
            long lastCleanupTimeMs = Deencapsulation.getField(handler, "lastOrphanJobDirCleanupTimeMs");
            Assert.assertEquals(0L, lastCleanupTimeMs);
        } finally {
            Config.backup_orphan_dir_cleanup_interval_second = oldCleanupIntervalSecond;
        }
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

        List<TableRefInfo> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, "unknown_tbl"),
                null,
                null,
                null,
                new ArrayList<>(),
                null,
                null,
                new ArrayList<>()));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
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

        List<TableRefInfo> tableRefs = Lists.newArrayList();
        // Add normal table
        tableRefs.add(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null,
                null,
                null,
                new ArrayList<>(),
                null,
                null,
                new ArrayList<>()));
        // Add non-existent table
        tableRefs.add(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, "unknown_tbl"),
                null,
                null,
                null,
                new ArrayList<>(),
                null,
                null,
                new ArrayList<>()));

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

            List<TableRefInfo> tableRefs = Lists.newArrayList();
            tableRefs.add(new TableRefInfo(
                    new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                    null,
                    null,
                    null,
                    new ArrayList<>(),
                    null,
                    null,
                    new ArrayList<>()));
            tableRefs.add(new TableRefInfo(
                    new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, table2Name),
                    null,
                    null,
                    null,
                    new ArrayList<>(),
                    null,
                    null,
                    new ArrayList<>()));

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

        List<TableRefInfo> tableRefs = Lists.newArrayList();
        tableRefs.add(
                new TableRefInfo(
                        new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                        null,
                        null,
                        null,
                        new ArrayList<>(),
                        null,
                        null,
                        new ArrayList<>()));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
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

    private File createLocalJobDir(String name) throws IOException {
        Path jobDirPath = BackupHandler.BACKUP_ROOT_DIR.resolve(name + "_" + id.getAndIncrement());
        Files.createDirectories(jobDirPath);
        return jobDirPath.toFile();
    }

    private File createRepoJobDir(long jobRepoId, String name) throws IOException {
        Path jobDirPath = BackupHandler.BACKUP_ROOT_DIR.resolve("repo__" + jobRepoId)
                .resolve(name + "_" + id.getAndIncrement());
        Files.createDirectories(jobDirPath);
        return jobDirPath.toFile();
    }

    private JobDirFixture createLocalJobDirFixture(
            String name, long timeoutMs, BackupJobState state) throws IOException {
        return createLocalJobDirFixture(name, timeoutMs, state, new byte[0], new byte[0]);
    }

    private JobDirFixture createLocalJobDirFixture(
            String name, long timeoutMs, BackupJobState state, byte[] metaBytes, byte[] jobInfoBytes)
            throws IOException {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), timeoutMs, BackupCommand.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        return createJobDirFixture(localSnapshotJob, name, state, metaBytes, jobInfoBytes);
    }

    private JobDirFixture createJobDirFixture(
            BackupJob targetJob, String name, BackupJobState state) throws IOException {
        return createJobDirFixture(targetJob, name, state, new byte[0], new byte[0]);
    }

    private JobDirFixture createJobDirFixture(
            BackupJob targetJob, String name, BackupJobState state, byte[] metaBytes, byte[] jobInfoBytes)
            throws IOException {
        File jobDir = createLocalJobDir(name);
        File metaInfo = new File(jobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(jobDir, Repository.PREFIX_JOB_INFO + "test");
        Files.write(metaInfo.toPath(), metaBytes);
        Files.write(jobInfo.toPath(), jobInfoBytes);

        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(targetJob, "state", state);
        Deencapsulation.setField(targetJob, "createTime", createTime);
        Deencapsulation.setField(targetJob, "localJobDirPath", null);
        Deencapsulation.setField(targetJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(targetJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());
        return new JobDirFixture(targetJob, jobDir, metaInfo, jobInfo, createTime);
    }

    private void createSparseFile(File file, long logicalSize) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(logicalSize);
        }
    }

    private static class JobDirFixture {
        private final BackupJob job;
        private final File jobDir;
        private final File metaInfo;
        private final File jobInfo;
        private final long createTime;

        private JobDirFixture(BackupJob job, File jobDir, File metaInfo, File jobInfo, long createTime) {
            this.job = job;
            this.jobDir = jobDir;
            this.metaInfo = metaInfo;
            this.jobInfo = jobInfo;
            this.createTime = createTime;
        }
    }
}
