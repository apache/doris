package com.baidu.palo.backup;

import com.baidu.palo.backup.BackupJobInfo.BackupIndexInfo;
import com.baidu.palo.backup.BackupJobInfo.BackupPartitionInfo;
import com.baidu.palo.backup.BackupJobInfo.BackupTableInfo;
import com.baidu.palo.backup.BackupJobInfo.BackupTabletInfo;
import com.baidu.palo.backup.RestoreJob.RestoreJobState;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.FeConstants;
import com.baidu.palo.common.MarkedCountDownLatch;
import com.baidu.palo.persist.EditLog;
import com.baidu.palo.system.SystemInfoService;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.task.DirMoveTask;
import com.baidu.palo.task.DownloadTask;
import com.baidu.palo.task.SnapshotTask;
import com.baidu.palo.thrift.TBackend;
import com.baidu.palo.thrift.TFinishTaskRequest;
import com.baidu.palo.thrift.TStatus;
import com.baidu.palo.thrift.TStatusCode;
import com.baidu.palo.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;

import mockit.Delegate;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.internal.startup.Startup;

public class RestoreJobTest {

    private Database db;
    private BackupJobInfo jobInfo;
    private RestoreJob job;
    private String label = "test_label";

    private AtomicLong id = new AtomicLong(50000);

    private OlapTable expectedRestoreTbl;

    private long repoId = 20000;

    @Mocked
    private Catalog catalog;
    @Mocked
    private BackupHandler backupHandler;
    @Mocked
    private RepositoryMgr repoMgr;
    @Mocked
    private EditLog editLog;
    @Mocked
    private SystemInfoService systemInfoService;

    @Injectable
    private Repository repo = new Repository(repoId, "repo", false, "bos://my_repo",
            new BlobStorage("broker", Maps.newHashMap()));

    private BackupMeta backupMeta;

    static {
        Startup.initializeIfPossible();
    }

    @Before
    public void setUp() throws AnalysisException {
        db = CatalogMocker.mockDb();
        
        new NonStrictExpectations() {
            {
                catalog.getBackupHandler();
                result = backupHandler;
        
                catalog.getDb(anyLong);
                result = db;
        
                Catalog.getCurrentCatalogJournalVersion();
                result = FeConstants.meta_version;
        
                catalog.getNextId();
                result = id.getAndIncrement();
        
                catalog.getEditLog();
                result = editLog;
        
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;
            }
        };
        
        new NonStrictExpectations() {
            {
                systemInfoService.seqChooseBackendIds(anyInt, anyBoolean, anyBoolean, anyString);
                result = new Delegate() {
                    public synchronized List<Long> seqChooseBackendIds(int backendNum, boolean needAlive,
                            boolean isCreate, String clusterName) {
                        List<Long> beIds = Lists.newArrayList();
                        beIds.add(CatalogMocker.BACKEND1_ID);
                        beIds.add(CatalogMocker.BACKEND2_ID);
                        beIds.add(CatalogMocker.BACKEND3_ID);
                        return beIds;
                    }
                };
            }
        };
        
        new NonStrictExpectations() {
            {
                backupHandler.getRepoMgr();
                result = repoMgr;
            }
        };
        
        new NonStrictExpectations() {
            {
                repoMgr.getRepo(anyInt);
                result = repo;
                minTimes = 0;
            }
        };
        
        new NonStrictExpectations() {
            {
                editLog.logBackupJob((BackupJob) any);
                result = new Delegate() {
                    public void logBackupJob(BackupJob job) {
                        System.out.println("log backup job: " + job);
                    }
                };
            }
        };
        
        new NonStrictExpectations() {
            {
                repo.upload(anyString, anyString);
                result = Status.OK;
                minTimes = 0;

                List<BackupMeta> backupMetas = Lists.newArrayList();
                repo.getSnapshotMetaFile(label, backupMetas);
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
        
        expectedRestoreTbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL2_ID);
        BackupTableInfo tblInfo = new BackupTableInfo();
        tblInfo.id = CatalogMocker.TEST_TBL2_ID;
        tblInfo.name = CatalogMocker.TEST_TBL2_NAME;
        jobInfo.tables.put(tblInfo.name, tblInfo);
        
        for (Partition partition : expectedRestoreTbl.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = partition.getId();
            partInfo.name = partition.getName();
            tblInfo.partitions.put(partInfo.name, partInfo);
            
            for (MaterializedIndex index : partition.getMaterializedIndices()) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.name = expectedRestoreTbl.getIndexNameById(index.getId());
                idxInfo.schemaHash = expectedRestoreTbl.getSchemaHashByIndexId(index.getId());
                partInfo.indexes.put(idxInfo.name, idxInfo);
                
                for (Tablet tablet : index.getTablets()) {
                    BackupTabletInfo tabletInfo = new BackupTabletInfo();
                    tabletInfo.id = tablet.getId();
                    tabletInfo.files.add(tabletInfo.id + ".dat");
                    tabletInfo.files.add(tabletInfo.id + ".idx");
                    tabletInfo.files.add(tabletInfo.id + ".hdr");
                    idxInfo.tablets.add(tabletInfo);
                }
            }
        }
        
        // drop this table, cause we want to try restoring this table
        db.dropTable(expectedRestoreTbl.getName());
        
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000, catalog, repo.getId());
        
        List<Table> tbls = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls);
    }

    @Ignore
    @Test
    public void testRun() {
        // pending
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(12, job.getFileMapping().getMapping().size());

        // 2. snapshoting
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(12 * 2, AgentTaskQueue.getTaskNum());

        // 3. snapshot finished
        List<AgentTask> agentTasks = Lists.newArrayList();
        Map<TTaskType, Set<Long>> runningTasks = Maps.newHashMap();
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND1_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND2_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND3_ID, runningTasks));
        Assert.assertEquals(12 * 2, agentTasks.size());

        for (AgentTask agentTask : agentTasks) {
            if (agentTask.getTaskType() != TTaskType.MAKE_SNAPSHOT) {
                continue;
            }

            SnapshotTask task = (SnapshotTask) agentTask;
            String snapshotPath = "/path/to/snapshot/" + System.currentTimeMillis();
            TStatus taskStatus = new TStatus(TStatusCode.OK);
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    task.getSignature(), taskStatus);
            request.setSnapshot_path(snapshotPath);
            Assert.assertTrue(job.finishTabletSnapshotTask(task, request));
        }

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOAD, job.getState());

        // download
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOADING, job.getState());
        Assert.assertEquals(9, AgentTaskQueue.getTaskNum());

        // downloading
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOADING, job.getState());

        List<AgentTask> downloadTasks = Lists.newArrayList();
        runningTasks = Maps.newHashMap();
        downloadTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND1_ID, runningTasks));
        downloadTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND2_ID, runningTasks));
        downloadTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND3_ID, runningTasks));
        Assert.assertEquals(9, downloadTasks.size());
        
        List<Long> downloadedTabletIds = Lists.newArrayList();
        for (AgentTask agentTask : downloadTasks) {
            TStatus taskStatus = new TStatus(TStatusCode.OK);
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    agentTask.getSignature(), taskStatus);
            request.setDownloaded_tablet_ids(downloadedTabletIds);
            Assert.assertTrue(job.finishTabletDownloadTask((DownloadTask) agentTask, request));
        }

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMIT, job.getState());

        // commit
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMITTING, job.getState());
        Assert.assertEquals(12, AgentTaskQueue.getTaskNum());

        // committing
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMITTING, job.getState());

        List<AgentTask> dirMoveTasks = Lists.newArrayList();
        runningTasks = Maps.newHashMap();
        dirMoveTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND1_ID, runningTasks));
        dirMoveTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND2_ID, runningTasks));
        dirMoveTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND3_ID, runningTasks));
        Assert.assertEquals(12, dirMoveTasks.size());
        
        for (AgentTask agentTask : dirMoveTasks) {
            TStatus taskStatus = new TStatus(TStatusCode.OK);
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    agentTask.getSignature(), taskStatus);
            job.finishDirMoveTask((DirMoveTask) agentTask, request);
        }

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.FINISHED, job.getState());
    }

    @Test
    public void testSignature() {
        Adler32 sig1 = new Adler32();
        sig1.update("name1".getBytes());
        sig1.update("name2".getBytes());
        System.out.println("sig1: " + Math.abs((int) sig1.getValue()));

        Adler32 sig2 = new Adler32();
        sig2.update("name2".getBytes());
        sig2.update("name1".getBytes());
        System.out.println("sig2: " + Math.abs((int) sig2.getValue()));

        OlapTable tbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL_NAME);
        List<String> partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println(partNames);
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
        tbl.setName("newName");
        partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
    }

}

