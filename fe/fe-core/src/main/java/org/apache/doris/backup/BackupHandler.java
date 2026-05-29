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
import org.apache.doris.backup.AbstractJob.JobType;
import org.apache.doris.backup.BackupJob.BackupJobState;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.cloud.backup.CloudRestoreJob;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelBackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.persist.BarrierLog;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class BackupHandler extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(BackupHandler.class);

    public static final int SIGNATURE_VERSION = 1;
    public static final Path BACKUP_ROOT_DIR = Paths.get(Config.tmp_dir, "backup").normalize();
    public static final Path RESTORE_ROOT_DIR = Paths.get(Config.tmp_dir, "restore").normalize();
    private RepositoryMgr repoMgr = new RepositoryMgr();

    // this lock is used for updating dbIdToBackupOrRestoreJobs
    private final ReentrantLock jobLock = new ReentrantLock();

    // === Dual Queue Architecture ===
    // Running queue: stores all unfinished jobs (PENDING or executing), no size limit
    // These jobs are actively being processed by the scheduler
    private final Map<Long, Deque<AbstractJob>> dbIdToRunningJobs = new ConcurrentHashMap<>();
    private final ReentrantLock runningJobLock = new ReentrantLock();

    // History queue: stores finished jobs (FINISHED, CANCELLED), FIFO with size limit
    // Used for SHOW BACKUP/RESTORE commands to display historical jobs
    private final Map<Long, Deque<AbstractJob>> dbIdToHistoryJobs = new ConcurrentHashMap<>();
    private final ReentrantLock historyJobLock = new ReentrantLock();

    // Legacy single queue (kept for backward compatibility during migration)
    // TODO: Remove this after confirming dual queue works correctly
    private final Map<Long, Deque<AbstractJob>> dbIdToBackupOrRestoreJobs = new HashMap<>();

    // this lock is used for handling one backup or restore request at a time.
    private ReentrantLock seqlock = new ReentrantLock();

    private boolean isInit = false;

    private Env env;

    // map to store backup info, key is label name, value is the BackupJob
    // this map not present in persist && only in fe memory
    // one table only keep one snapshot info, only keep last
    private final Map<String, BackupJob> localSnapshots = new HashMap<>();
    private ReadWriteLock localSnapshotsLock = new ReentrantReadWriteLock();

    // === Concurrency control fields (only used when enable_table_level_backup_concurrency is true) ===

    // Execution gate: only jobs in this set are allowed to execute
    // Jobs in PENDING state but not in this set will skip execution until activated
    private final Set<Long> allowedJobIds = Collections.synchronizedSet(new HashSet<>());

    // Restore table conflict detection: tracks which tables are currently being restored
    // Key: dbId, Value: set of table names being restored
    // Used to detect conflicts between restore jobs operating on the same tables
    private final Map<Long, Set<String>> restoringTables = new ConcurrentHashMap<>();

    // Database job statistics cache for O(1) concurrency checking
    // Key: dbId, Value: statistics for that database
    private final Map<Long, DatabaseJobStats> dbJobStats = new ConcurrentHashMap<>();

    private final AtomicInteger globalSnapshotTasks = new AtomicInteger(0);

    // Label index for O(1) duplicate label detection
    // Key: dbId, Value: (label -> jobId) mapping
    private final Map<Long, Map<String, Long>> labelIndex = new ConcurrentHashMap<>();

    public BackupHandler() {
        // for persist
    }

    public BackupHandler(Env env) {
        super("backupHandler", Config.backup_handler_update_interval_millis);
        this.env = env;
    }

    public void setEnv(Env env) {
        this.env = env;
    }

    @Override
    public synchronized void start() {
        Preconditions.checkNotNull(env);
        super.start();
        repoMgr.start();
    }

    public RepositoryMgr getRepoMgr() {
        return repoMgr;
    }

    public int getGlobalSnapshotTasks() {
        return globalSnapshotTasks.get();
    }

    public void addGlobalSnapshotTasks(int count) {
        globalSnapshotTasks.addAndGet(count);
    }

    private boolean init() {
        // Check and create backup dir if necessarily
        File backupDir = new File(BACKUP_ROOT_DIR.toString());
        if (!backupDir.exists()) {
            if (!backupDir.mkdirs()) {
                LOG.warn("failed to create backup dir: " + BACKUP_ROOT_DIR);
                return false;
            }
        } else {
            if (!backupDir.isDirectory()) {
                LOG.warn("backup dir is not a directory: " + BACKUP_ROOT_DIR);
                return false;
            }
        }

        // Check and create restore dir if necessarily
        File restoreDir = new File(RESTORE_ROOT_DIR.toString());
        if (!restoreDir.exists()) {
            if (!restoreDir.mkdirs()) {
                LOG.warn("failed to create restore dir: " + RESTORE_ROOT_DIR);
                return false;
            }
        } else {
            if (!restoreDir.isDirectory()) {
                LOG.warn("restore dir is not a directory: " + RESTORE_ROOT_DIR);
                return false;
            }
        }

        isInit = true;

        // Rebuild concurrency control state after FE restart
        if (Config.enable_table_level_backup_concurrency) {
            rebuildConcurrencyStateAfterRestart();
        }

        return true;
    }

    /**
     * Rebuild concurrency control state after FE restart.
     * This method scans all jobs and rebuilds the internal data structures including dual queues.
     */
    private void rebuildConcurrencyStateAfterRestart() {
        LOG.info("Rebuilding concurrency control state and dual queues after FE restart...");

        // Clear all caches and queues
        allowedJobIds.clear();
        dbJobStats.clear();
        labelIndex.clear();
        restoringTables.clear();
        dbIdToRunningJobs.clear();
        dbIdToHistoryJobs.clear();

        int totalJobs = 0;
        int runningJobs = 0;
        int historyJobs = 0;
        int activatedJobs = 0;

        // Scan all databases and jobs to rebuild dual queues
        for (Map.Entry<Long, Deque<AbstractJob>> entry : dbIdToBackupOrRestoreJobs.entrySet()) {
            long dbId = entry.getKey();

            for (AbstractJob job : entry.getValue()) {
                totalJobs++;
                boolean isBackup = job instanceof BackupJob;
                boolean isFullDatabase = false;

                // Determine if this is a full database operation
                if (isBackup && job instanceof BackupJob) {
                    isFullDatabase = ((BackupJob) job).getTableRefs().isEmpty();
                } else if (!isBackup && job instanceof RestoreJob) {
                    isFullDatabase = ((RestoreJob) job).getTableRefs().isEmpty();
                }

                // Step 1: Add to appropriate queue (running or history)
                if (job.isDone()) {
                    // Completed job -> history queue
                    addToHistoryQueue(dbId, job);
                    historyJobs++;
                } else {
                    // Active job (PENDING or running) -> running queue
                    try {
                        Deque<AbstractJob> runningQueue = dbIdToRunningJobs.computeIfAbsent(
                                dbId, k -> new LinkedList<>());
                        runningQueue.addLast(job);
                        runningJobs++;
                    } catch (Exception e) {
                        LOG.warn("Failed to add job {} to running queue during rebuild: {}",
                                job.getLabel(), e.getMessage());
                    }

                    // Step 2: Rebuild concurrency control statistics
                    onJobCreated(dbId, job, isBackup, isFullDatabase);

                    // Step 3: For jobs that were already running (not PENDING),
                    // re-add to allowedJobIds and rebuild restoringTables
                    boolean isPending = false;
                    if (isBackup && job instanceof BackupJob) {
                        isPending = ((BackupJob) job).getState() == BackupJob.BackupJobState.PENDING;
                    } else if (!isBackup && job instanceof RestoreJob) {
                        isPending = ((RestoreJob) job).getState() == RestoreJob.RestoreJobState.PENDING;
                    }

                    if (!isPending) {
                        // Job was running, add to allowedJobIds and update running counters
                        allowedJobIds.add(job.getJobId());
                        onJobActivated(dbId, job, isBackup, isFullDatabase);
                        activatedJobs++;

                        // For running restore jobs, rebuild restoringTables
                        if (job instanceof RestoreJob) {
                            addRestoringTables((RestoreJob) job);
                        }
                    }
                }
            }
        }

        // Try to activate PENDING jobs
        for (Long dbId : dbIdToRunningJobs.keySet()) {
            tryActivatePendingJobs(dbId);
        }

        int globalTotal = 0;
        for (Deque<AbstractJob> queue : dbIdToRunningJobs.values()) {
            for (AbstractJob j : queue) {
                globalTotal += j.getSnapshotTaskCount();
            }
        }
        globalSnapshotTasks.set(globalTotal);

        LOG.info("Rebuilt dual queues after restart: {} total jobs ({} running, {} history), "
                + "{} activated, {} databases, globalSnapshotTasks={}",
                totalJobs, runningJobs, historyJobs, activatedJobs,
                Math.max(dbIdToRunningJobs.size(), dbIdToHistoryJobs.size()),
                globalTotal);
    }

    public AbstractJob getJob(long dbId) {
        return getCurrentJob(dbId);
    }

    public List<AbstractJob> getJobs(long dbId, Predicate<String> predicate) {
        if (Config.enable_table_level_backup_concurrency) {
            // In dual queue mode, merge both queues
            return getAllJobs(dbId).stream()
                    .filter(e -> predicate.test(e.getLabel()))
                    .collect(Collectors.toList());
        } else {
            // Legacy mode: use old single queue
            jobLock.lock();
            try {
                return dbIdToBackupOrRestoreJobs.getOrDefault(dbId, new LinkedList<>())
                        .stream()
                        .filter(e -> predicate.test(e.getLabel()))
                        .collect(Collectors.toList());
            } finally {
                jobLock.unlock();
            }
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!isInit) {
            if (!init()) {
                return;
            }
        }

        // Get jobs to process based on concurrency mode
        List<AbstractJob> jobsToProcess;
        if (Config.enable_table_level_backup_concurrency) {
            // In concurrency mode, only process jobs from running queue
            jobsToProcess = getAllRunningJobs();
        } else {
            // In legacy mode, use the old logic
            jobsToProcess = getAllCurrentJobs();
        }

        for (AbstractJob job : jobsToProcess) {
            // Skip completed jobs (optimization: avoid unnecessary run() calls)
            // Although job.run() will return immediately if done, this saves the call overhead
            if (job.isDone()) {
                continue;
            }

            job.setEnv(env);
            job.run();
        }
    }

    // handle create repository command
    public void createRepository(CreateRepositoryCommand command) throws DdlException {
        if (!env.getBrokerMgr().containsBroker(command.getBrokerName())
                && command.getStorageType() == StorageBackend.StorageType.BROKER) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "broker does not exist: " + command.getBrokerName());
        }

        long repoId = env.getNextId();
        Repository repo = new Repository(repoId, command.getName(), command.isReadOnly(), command.getLocation(),
                command.getStorageProperties());

        Status st = repoMgr.addAndInitRepoIfNotExist(repo, false);
        if (!st.ok()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Failed to create repository: " + st.getErrMsg());
        }
        //fixme why ping again? it has pinged in addAndInitRepoIfNotExist
        if (!repo.ping()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Failed to create repository: failed to connect to the repo");
        }
    }

    /**
     * Alters an existing repository by applying the given new properties.
     *
     * @param repoName    The name of the repository to alter.
     * @param newProps    The new properties to apply to the repository.
     * @throws DdlException if the repository does not exist, fails to apply properties, or cannot connect
     * to the updated repository.
     */
    public void alterRepository(String repoName, Map<String, String> newProps)
            throws DdlException {
        tryLock();
        try {
            Repository oldRepo = repoMgr.getRepo(repoName);
            if (oldRepo == null) {
                throw new DdlException("Repository does not exist");
            }
            // Merge new properties with the existing repository's properties
            Map<String, String> mergedProps = mergeProperties(oldRepo, newProps);
            // Create new Repository instance with merged properties
            Repository newRepo = new Repository(
                    oldRepo.getId(), oldRepo.getName(), oldRepo.isReadOnly(),
                    oldRepo.getLocation(), StorageProperties.createPrimary(mergedProps)
            );
            // Verify the repository can be connected with new settings
            if (!newRepo.ping()) {
                LOG.warn("Failed to connect repository {}. msg: {}", repoName, newRepo.getErrorMsg());
                throw new DdlException("Repository ping failed with new properties");
            }
            // Apply the new repository metadata
            Status st = repoMgr.alterRepo(newRepo, false /* not replay */);
            if (!st.ok()) {
                throw new DdlException("Failed to alter repository: " + st.getErrMsg());
            }
            // Update all running jobs that are using this repository
            updateOngoingJobs(oldRepo.getId(), newRepo);
        } finally {
            seqlock.unlock();
        }
    }

    /**
     * Merges new user-provided properties into the existing repository's configuration.
     *
     * @param repo        The existing repository.
     * @param newProps    New user-specified properties.
     * @return A complete set of merged properties.
     */
    private Map<String, String> mergeProperties(Repository repo, Map<String, String> newProps) {
        // General case: just override old props with new ones
        Map<String, String> combined = new HashMap<>(repo.getFileSystemDescriptor().getProperties());
        combined.putAll(newProps);
        return combined;
    }

    /**
     * Updates all currently running jobs associated with the given repository ID.
     * Used to ensure that all jobs operate on the new repository instance after alteration.
     *
     * @param repoId  The ID of the altered repository.
     * @param newRepo The new repository instance.
     */
    private void updateOngoingJobs(long repoId, Repository newRepo) {
        // Get jobs to update based on concurrency mode
        List<AbstractJob> jobsToUpdate;
        if (Config.enable_table_level_backup_concurrency) {
            jobsToUpdate = getAllRunningJobs();
        } else {
            jobsToUpdate = getAllCurrentJobs();
        }

        for (AbstractJob job : jobsToUpdate) {
            if (!job.isDone() && job.getRepoId() == repoId) {
                job.updateRepo(newRepo);
            }
        }
    }

    // handle drop repository command
    public void dropRepository(String repoName) throws DdlException {
        tryLock();
        try {
            Repository repo = repoMgr.getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository does not exist");
            }

            // Get jobs to check based on concurrency mode
            List<AbstractJob> jobsToCheck;
            if (Config.enable_table_level_backup_concurrency) {
                jobsToCheck = getAllRunningJobs();
            } else {
                jobsToCheck = getAllCurrentJobs();
            }

            for (AbstractJob job : jobsToCheck) {
                if (!job.isDone() && job.getRepoId() == repo.getId()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                                   "Backup or restore job is running on this repository."
                                                           + " Can not drop it");
                }
            }

            Status st = repoMgr.removeRepo(repo.getName(), false /* not replay */);
            if (!st.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                               "Failed to drop repository: " + st.getErrMsg());
            }
        } finally {
            seqlock.unlock();
        }
    }

    public void process(BackupCommand command) throws DdlException {
        if (Config.isCloudMode()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "BACKUP are not supported by the cloud mode yet");
        }

        // check if repo exist
        String repoName = command.getRepoName();
        Repository repository = null;
        if (!repoName.equals(Repository.KEEP_ON_LOCAL_REPO_NAME)) {
            repository = repoMgr.getRepo(repoName);
            if (repository == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository " + repoName + " does not exist");
            }
        }

        // check if db exist
        String dbName = command.getDbName();
        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);

        // Try to get sequence lock.
        // We expect at most one operation on a repo at same time.
        // But this operation may take a few seconds with lock held.
        // So we use tryLock() to give up this operation if we can not get lock.
        tryLock();
        try {
            // === Concurrency control ===
            if (!Config.enable_table_level_backup_concurrency) {
                // Original logic: only one job at a time
                AbstractJob currentJob = getCurrentJob(db.getId());
                if (currentJob != null && !currentJob.isDone()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Can only run one backup or restore job of a database at same time "
                            + ", current running: label = " + currentJob.getLabel() + " jobId = "
                            + currentJob.getJobId() + ", to run label = " + command.getLabel());
                }
            }
            // Note: For concurrent mode, actual concurrency checks are performed in backup() method
            // after the job is created, because we need the job ID to add to allowedJobIds
            backup(repository, db, command);
        } finally {
            seqlock.unlock();
        }
    }

    public void process(RestoreCommand command) throws DdlException {
        if (Config.isCloudMode() && !Config.enable_cloud_restore_job) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Restore is an experimental feature in cloud mode. Set config "
                    + "`experimental_enable_cloud_restore_job` = `true` to enable.");
        }
        // check if repo exist
        String repoName = command.getRepoName();
        Repository repository = null;
        if (!repoName.equals(Repository.KEEP_ON_LOCAL_REPO_NAME)) {
            repository = repoMgr.getRepo(repoName);
            if (repository == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository " + repoName + " does not exist");
            }
        }

        // check if db exist
        String dbName = command.getDbName();
        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);

        // Try to get sequence lock.
        // We expect at most one operation on a repo at same time.
        // But this operation may take a few seconds with lock held.
        // So we use tryLock() to give up this operation if we can not get lock.
        tryLock();
        try {
            // === Concurrency control ===
            if (!Config.enable_table_level_backup_concurrency) {
                // Original logic: only one job at a time
                AbstractJob currentJob = getCurrentJob(db.getId());
                if (currentJob != null && !currentJob.isDone()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Can only run one backup or restore job of a database at same time "
                            + ", current running: label = " + currentJob.getLabel() + " jobId = "
                            + currentJob.getJobId() + ", to run label = " + command.getLabel());
                }
            }
            // Note: For concurrent mode, checkConcurrency is called in restore() method
            // after jobInfo is available, because we need to know the actual tables
            restore(repository, db, command);
        } finally {
            seqlock.unlock();
        }
    }

    private void tryLock() throws DdlException {
        try {
            if (!seqlock.tryLock(10, TimeUnit.SECONDS)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Another backup or restore job"
                        + " is being submitted. Please wait and try again");
            }
        } catch (InterruptedException e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Got interrupted exception when "
                    + "try locking. Try again");
        }
    }

    private void backup(Repository repository, Database db, BackupCommand command) throws DdlException {
        if (repository != null && repository.isReadOnly()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository " + repository.getName()
                    + " is read only");
        }

        long commitSeq = 0;
        Set<String> tableNames = Sets.newHashSet();

        List<TableRefInfo> tableRefInfos = command.getTableRefInfos();

        // Obtain the snapshot commit seq, any creating table binlog will be visible.
        db.readLock();
        try {
            BarrierLog log = new BarrierLog(db.getId(), db.getFullName());
            commitSeq = env.getEditLog().logBarrier(log);

            // Determine the tables to be backed up
            if (tableRefInfos.isEmpty()) {
                tableNames = db.getTableNames();
            } else if (command.isExclude()) {
                tableNames = db.getTableNames();
                for (TableRefInfo tableRefInfo : tableRefInfos) {
                    if (!tableNames.remove(tableRefInfo.getTableNameInfo().getTbl())) {
                        LOG.info("exclude table " + tableRefInfo.getTableNameInfo().getTbl()
                                + " of backup stmt is not exists in db " + db.getFullName());
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        List<TableRefInfo> tableRefInfoList = Lists.newArrayList();
        if (!tableRefInfos.isEmpty() && !command.isExclude()) {
            tableRefInfoList.addAll(tableRefInfos);
        } else {
            for (String tableName : tableNames) {
                TableRefInfo tableRefInfo = new TableRefInfo(new TableNameInfo(db.getFullName(), tableName), null);
                tableRefInfoList.add(tableRefInfo);
            }
        }

        // Check if backup objects are valid
        // This is just a pre-check to avoid most of invalid backup requests.
        // Also calculate the signature for incremental backup check.
        List<TableRefInfo> tblRefInfosNotSupport = Lists.newArrayList();
        for (TableRefInfo tableRef : tableRefInfoList) {
            String tblName = tableRef.getTableNameInfo().getTbl();
            Table tbl = db.getTableOrDdlException(tblName);

            // filter the table types which are not supported by local backup.
            if (repository == null && tbl.getType() != TableType.OLAP
                    && tbl.getType() != TableType.VIEW && tbl.getType() != TableType.MATERIALIZED_VIEW) {
                tblRefInfosNotSupport.add(tableRef);
                continue;
            }

            if (tbl.getType() == TableType.VIEW || tbl.getType() == TableType.ODBC
                    || tbl.getType() == TableType.MATERIALIZED_VIEW) {
                continue;
            }
            if (tbl.getType() != TableType.OLAP) {
                if (Config.ignore_backup_not_support_table_type) {
                    LOG.warn("Table '{}' is a {} table, can not backup and ignore it."
                            + "Only OLAP(Doris)/ODBC/VIEW table can be backed up",
                            tblName, tbl.getType().toString());
                    tblRefInfosNotSupport.add(tableRef);
                    continue;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_NOT_OLAP_TABLE, tblName);
                }
            }

            if (tbl.isTemporary()) {
                if (Config.ignore_backup_not_support_table_type || tableRefInfoList.size() > 1) {
                    LOG.warn("Table '{}' is a temporary table, can not backup and ignore it."
                            + "Only OLAP(Doris)/ODBC/VIEW table can be backed up",
                            Util.getTempTableDisplayName(tblName));
                    tblRefInfosNotSupport.add(tableRef);
                    continue;
                } else {
                    ErrorReport.reportDdlException("Table " + Util.getTempTableDisplayName(tblName)
                            + " is a temporary table, do not support backup");
                }
            }

            OlapTable olapTbl = (OlapTable) tbl;
            tbl.readLock();
            try {
                if (olapTbl.needRowBinlog()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support backup table with binlog<Row> enabled: " + olapTbl.getName());
                }
                if (!Config.ignore_backup_tmp_partitions && olapTbl.existTempPartitions()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support backup table " + olapTbl.getName() + " with temp partitions");
                }

                PartitionNamesInfo partitionNames = tableRef.getPartitionNamesInfo();
                if (partitionNames != null) {
                    if (!Config.ignore_backup_tmp_partitions && partitionNames.isTemp()) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                "Do not support backup temp partitions in table "
                                    + tableRef.getTableNameInfo().getTbl());
                    }

                    for (String partName : partitionNames.getPartitionNames()) {
                        Partition partition = olapTbl.getPartition(partName);
                        if (partition == null) {
                            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                    "Unknown partition " + partName + " in table" + tblName);
                        }
                    }
                }
            } finally {
                tbl.readUnlock();
            }
        }

        tableRefInfoList.removeAll(tblRefInfosNotSupport);

        // Check if label already be used
        long repoId = Repository.KEEP_ON_LOCAL_REPO_ID;
        if (repository != null) {
            List<String> existSnapshotNames = Lists.newArrayList();
            Status st = repository.listSnapshots(existSnapshotNames);
            if (!st.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, st.getErrMsg());
            }
            if (existSnapshotNames.contains(command.getLabel())) {
                if (command.getBackupType() == BackupCommand.BackupType.FULL) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Snapshot with name '"
                            + command.getLabel() + "' already exist in repository");
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Currently does not support "
                            + "incremental backup");
                }
            }
            repoId = repository.getId();
        }

        // Create a backup job
        BackupJob backupJob = new BackupJob(command.getLabel(), db.getId(),
                db.getFullName(),
                tableRefInfoList, command.getTimeoutMs(), command.getContent(), env, repoId, commitSeq);
        // === Concurrency control ===
        if (Config.enable_table_level_backup_concurrency) {
            if (backupJob.getJobId() == -1) {
                backupJob.setJobId(env.getNextId());
            }

            // Determine if this is a full database backup
            boolean isFullDatabase = tableRefInfoList.isEmpty()
                    || (command.getTableRefInfos().isEmpty() && !command.isExclude());

            // Collect table names for concurrency check
            List<String> tableNameList = new ArrayList<>();
            for (TableRefInfo tblRef : tableRefInfoList) {
                tableNameList.add(tblRef.getTableNameInfo().getTbl());
            }

            // Check concurrency (throws exception for hard rejection cases)
            checkConcurrency(db.getId(), command.getLabel(), true, isFullDatabase, tableNameList);

            // Update statistics
            onJobCreated(db.getId(), backupJob, true, isFullDatabase);

            // Decide whether to add to allowedJobIds (canActivate checks running jobs)
            if (canActivate(db.getId(), backupJob, true, isFullDatabase)) {
                allowedJobIds.add(backupJob.getJobId());
                onJobActivated(db.getId(), backupJob, true, isFullDatabase);
                LOG.info("Backup job [{}] can execute immediately (jobId={})",
                        backupJob.getLabel(), backupJob.getJobId());
            } else {
                LOG.info("Backup job [{}] will be pending (jobId={})",
                        backupJob.getLabel(), backupJob.getJobId());
            }
        }

        // write log (after jobId is assigned so EditLog records the correct jobId)
        env.getEditLog().logBackupJob(backupJob);

        // Add to appropriate queue based on concurrency mode
        if (Config.enable_table_level_backup_concurrency) {
            addActiveJob(db.getId(), backupJob);
        } else {
            addBackupOrRestoreJob(db.getId(), backupJob);
        }

        LOG.info("finished to submit backup job: {}", backupJob);
    }

    public void restore(Repository repository, Database db, RestoreCommand command) throws DdlException {
        BackupJobInfo jobInfo;
        if ((command.isLocal() || command.isAtomicRestore() || command.reserveColocate() || command.isForceReplace())
                && Config.isCloudMode()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "not supported now.");
        }
        if (command.isLocal()) {
            jobInfo = command.getJobInfo();

            if (jobInfo.extraInfo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Invalid job extra info empty");
            }
            if (jobInfo.extraInfo.beNetworkMap == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Invalid job extra info be network map");
            }
            if (Strings.isNullOrEmpty(jobInfo.extraInfo.token)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Invalid job extra info token");
            }
        } else {
            // Check if snapshot exist in repository
            List<BackupJobInfo> infos = Lists.newArrayList();
            Status status = repository.getSnapshotInfoFile(command.getLabel(), command.getBackupTimestamp(), infos);
            if (!status.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Failed to get info of snapshot '" + command.getLabel() + "' because: "
                        + status.getErrMsg() + ". Maybe specified wrong backup timestamp");
            }

            // Check if all restore objects are exist in this snapshot.
            // Also remove all unrelated objs
            Preconditions.checkState(infos.size() == 1);
            jobInfo = infos.get(0);
        }

        checkAndFilterRestoreObjsExistInSnapshot(jobInfo, command);

        // Create a restore job
        RestoreJob restoreJob;
        if (command.isLocal()) {
            int metaVersion = command.getMetaVersion();
            if (metaVersion == -1) {
                metaVersion = jobInfo.metaVersion;
            }

            BackupMeta backupMeta = command.getMeta();
            String backupTimestamp = TimeUtils.longToTimeString(
                    jobInfo.getBackupTime(), TimeUtils.getDatetimeFormatWithHyphenWithTimeZone());
            restoreJob = new RestoreJob(command.getLabel(), backupTimestamp,
                db.getId(), db.getFullName(), jobInfo, command.allowLoad(), command.getReplicaAlloc(),
                command.getTimeoutMs(), command.getMetaVersion(), command.reserveReplica(), command.reserveColocate(),
                command.reserveDynamicPartitionEnable(), command.isBeingSynced(), command.isCleanTables(),
                command.isCleanPartitions(), command.isAtomicRestore(), command.isForceReplace(),
                env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMeta);
        } else {
            if (Config.isCloudMode()) {
                restoreJob = new CloudRestoreJob(command.getLabel(), command.getBackupTimestamp(),
                    db.getId(), db.getFullName(), jobInfo, command.allowLoad(), command.getReplicaAlloc(),
                    command.getTimeoutMs(), command.getMetaVersion(), command.reserveReplica(),
                    command.reserveDynamicPartitionEnable(), command.isBeingSynced(), command.isCleanTables(),
                    command.isCleanPartitions(), command.isAtomicRestore(), command.isForceReplace(),
                    env, repository.getId(), command.getStorageVaultName());
            } else {
                restoreJob = new RestoreJob(command.getLabel(), command.getBackupTimestamp(),
                    db.getId(), db.getFullName(), jobInfo, command.allowLoad(), command.getReplicaAlloc(),
                    command.getTimeoutMs(), command.getMetaVersion(), command.reserveReplica(),
                    command.reserveColocate(), command.reserveDynamicPartitionEnable(), command.isBeingSynced(),
                    command.isCleanTables(), command.isCleanPartitions(), command.isAtomicRestore(),
                    command.isForceReplace(), env, repository.getId());
            }
        }

        // Set tableRefs for concurrency control
        restoreJob.setTableRefs(command.getTableRefInfos());

        // === Concurrency control ===
        if (Config.enable_table_level_backup_concurrency) {
            if (restoreJob.getJobId() == -1) {
                restoreJob.setJobId(env.getNextId());
            }

            // Determine if this is a full database restore
            boolean isFullDatabase = command.getTableRefInfos().isEmpty();

            // Collect table names for concurrency check
            List<String> tableNameList = new ArrayList<>();
            for (TableRefInfo tblRef : command.getTableRefInfos()) {
                tableNameList.add(tblRef.getTableNameInfo().getTbl());
            }

            // Check concurrency (throws exception for hard rejection cases)
            checkConcurrency(db.getId(), command.getLabel(), false, isFullDatabase, tableNameList);

            // Update statistics
            onJobCreated(db.getId(), restoreJob, false, isFullDatabase);

            // Decide whether to add to allowedJobIds (canActivate checks running jobs)
            if (canActivate(db.getId(), restoreJob, false, isFullDatabase)) {
                allowedJobIds.add(restoreJob.getJobId());
                onJobActivated(db.getId(), restoreJob, false, isFullDatabase);
                // CRITICAL: Add restoring tables atomically with activation
                // to prevent TOCTOU race condition where another restore job
                // targeting the same table could also pass canActivate()
                addRestoringTables(restoreJob);
                LOG.info("Restore job [{}] can execute immediately (jobId={})",
                        restoreJob.getLabel(), restoreJob.getJobId());
            } else {
                LOG.info("Restore job [{}] will be pending (jobId={})",
                        restoreJob.getLabel(), restoreJob.getJobId());
            }
        }

        // write log (after jobId is assigned so EditLog records the correct jobId)
        env.getEditLog().logRestoreJob(restoreJob);

        // Add to appropriate queue based on concurrency mode
        if (Config.enable_table_level_backup_concurrency) {
            addActiveJob(db.getId(), restoreJob);
        } else {
            addBackupOrRestoreJob(db.getId(), restoreJob);
        }

        LOG.info("finished to submit restore job: {}", restoreJob);
    }

    /**
     * Add a job to the running queue (no size limit).
     * This method is used for active jobs that are either PENDING or executing.
     *
     * @param dbId Database ID
     * @param job  The job to add
     * @throws DdlException if the running queue soft limit is exceeded
     */
    private void addActiveJob(long dbId, AbstractJob job) throws DdlException {
        runningJobLock.lock();
        try {
            Deque<AbstractJob> queue = dbIdToRunningJobs.computeIfAbsent(dbId, k -> new LinkedList<>());

            // Check soft limit (warning only)
            if (queue.size() >= Config.max_backup_restore_running_queue_soft_limit) {
                LOG.warn("Running queue for database {} has reached soft limit: {} jobs (limit: {})",
                        dbId, queue.size(), Config.max_backup_restore_running_queue_soft_limit);
            }

            // Check hard limit (reject)
            if (queue.size() >= Config.max_backup_restore_running_queue_hard_limit) {
                throw new DdlException(String.format(
                        "Running queue is full (%d jobs, hard limit: %d). "
                                + "Too many active backup/restore jobs for database %d. "
                                + "Please wait for some jobs to complete or increase "
                                + "max_backup_restore_running_queue_hard_limit.",
                        queue.size(), Config.max_backup_restore_running_queue_hard_limit, dbId));
            }

            // Check if job already exists in queue (skip duplicate)
            // This can happen during FE restart replay
            boolean exists = queue.stream().anyMatch(j -> j.getJobId() == job.getJobId());
            if (exists) {
                LOG.info("Job {} already exists in running queue, skipping duplicate (dbId={})",
                        job.getLabel(), dbId);
                return;
            }

            queue.addLast(job);

            if (Config.enable_backup_concurrency_logging) {
                LOG.info("Added job {} to running queue, dbId={}, queueSize={}",
                        job.getLabel(), dbId, queue.size());
            }
        } finally {
            runningJobLock.unlock();
        }
    }

    /**
     * Move a completed job from running queue to history queue.
     * History queue has FIFO cleanup when it reaches the size limit.
     *
     * @param dbId Database ID
     * @param job  The completed job
     */
    private void moveToHistory(long dbId, AbstractJob job) {
        // Step 1: Remove from running queue
        removeFromRunningQueue(dbId, job);

        // Step 2: Add to history queue with FIFO cleanup
        addToHistoryQueue(dbId, job);
    }

    /**
     * Remove a job from the running queue.
     *
     * @param dbId Database ID
     * @param job  The job to remove
     */
    private void removeFromRunningQueue(long dbId, AbstractJob job) {
        runningJobLock.lock();
        try {
            Deque<AbstractJob> queue = dbIdToRunningJobs.get(dbId);
            if (queue != null) {
                // Need to iterate to find and remove (job might be in the middle of queue)
                Iterator<AbstractJob> it = queue.iterator();
                while (it.hasNext()) {
                    if (it.next().getJobId() == job.getJobId()) {
                        it.remove();
                        if (Config.enable_backup_concurrency_logging) {
                            LOG.info("Removed job {} from running queue, dbId={}, remainingSize={}",
                                    job.getLabel(), dbId, queue.size());
                        }
                        break;
                    }
                }

                // If running queue is empty, remove the key
                if (queue.isEmpty()) {
                    dbIdToRunningJobs.remove(dbId);
                }
            }
        } finally {
            runningJobLock.unlock();
        }
    }

    /**
     * Add a job to the history queue with FIFO cleanup when size limit is reached.
     *
     * @param dbId Database ID
     * @param job  The finished job to add
     */
    private void addToHistoryQueue(long dbId, AbstractJob job) {
        historyJobLock.lock();
        try {
            Deque<AbstractJob> queue = dbIdToHistoryJobs.computeIfAbsent(dbId, k -> new LinkedList<>());

            // FIFO cleanup: remove oldest if queue is full
            while (queue.size() >= Config.max_backup_restore_job_num_per_db) {
                AbstractJob removed = queue.removeFirst();
                cleanupJobResources(removed);

                if (Config.enable_backup_concurrency_logging) {
                    LOG.info("Removed oldest history job {} from queue (limit reached), dbId={}",
                            removed.getLabel(), dbId);
                }
            }

            queue.addLast(job);

            // Save snapshot to local repo if applicable
            if (job instanceof BackupJob) {
                BackupJob backupJob = (BackupJob) job;
                if (backupJob.isLocalSnapshot()) {
                    addSnapshot(backupJob.getLabel(), backupJob);
                }
            }

            if (Config.enable_backup_concurrency_logging) {
                LOG.info("Added job {} to history queue, dbId={}, queueSize={}",
                        job.getLabel(), dbId, queue.size());
            }
        } finally {
            historyJobLock.unlock();
        }
    }

    /**
     * Clean up resources associated with a job being removed from history.
     *
     * @param job The job to clean up
     */
    private void cleanupJobResources(AbstractJob job) {
        // Remove from local snapshots if it's a local backup
        if (job instanceof BackupJob) {
            BackupJob backupJob = (BackupJob) job;
            if (backupJob.isLocalSnapshot()) {
                removeSnapshot(backupJob.getLabel());
            }
        }
        // Additional cleanup can be added here if needed
    }

    /**
     * Get all jobs for a database (merging running and history queues).
     * Used for SHOW BACKUP/RESTORE commands.
     *
     * @param dbId Database ID
     * @return List of all jobs (running + history)
     */
    private List<AbstractJob> getAllJobs(long dbId) {
        List<AbstractJob> result = new ArrayList<>();

        // Add jobs from running queue
        runningJobLock.lock();
        try {
            Deque<AbstractJob> running = dbIdToRunningJobs.get(dbId);
            if (running != null) {
                result.addAll(running);
            }
        } finally {
            runningJobLock.unlock();
        }

        // Add jobs from history queue
        historyJobLock.lock();
        try {
            Deque<AbstractJob> history = dbIdToHistoryJobs.get(dbId);
            if (history != null) {
                result.addAll(history);
            }
        } finally {
            historyJobLock.unlock();
        }

        return result;
    }

    /**
     * Get all running jobs across all databases.
     * Used by the scheduler in runAfterCatalogReady().
     *
     * @return List of all running jobs
     */
    private List<AbstractJob> getAllRunningJobs() {
        runningJobLock.lock();
        try {
            List<AbstractJob> result = new ArrayList<>();
            for (Deque<AbstractJob> queue : dbIdToRunningJobs.values()) {
                result.addAll(queue);
            }
            return result;
        } finally {
            runningJobLock.unlock();
        }
    }

    private void addBackupOrRestoreJob(long dbId, AbstractJob job) {
        // If there are too many backup/restore jobs, it may cause OOM.  If the job num option is set to 0,
        // skip all backup/restore jobs.
        if (Config.max_backup_restore_job_num_per_db <= 0) {
            return;
        }

        List<String> removedLabels = Lists.newArrayList();
        jobLock.lock();
        try {
            Deque<AbstractJob> jobs = dbIdToBackupOrRestoreJobs.computeIfAbsent(dbId, k -> Lists.newLinkedList());
            while (jobs.size() >= Config.max_backup_restore_job_num_per_db) {
                AbstractJob removedJob = jobs.removeFirst();
                if (removedJob instanceof BackupJob && ((BackupJob) removedJob).isLocalSnapshot()) {
                    removedLabels.add(removedJob.getLabel());
                }
            }
            AbstractJob lastJob = jobs.peekLast();

            // Remove duplicate jobs and keep only the latest status
            // Otherwise, the tasks that have been successfully executed will be repeated when replaying edit log.
            if (lastJob != null && (lastJob.isPending() || lastJob.getJobId() == job.getJobId())) {
                jobs.removeLast();
            }
            jobs.addLast(job);
        } finally {
            jobLock.unlock();
        }

        if (job.isFinished() && job instanceof BackupJob) {
            // Save snapshot to local repo, when reload backupHandler from image.
            BackupJob backupJob = (BackupJob) job;
            if (backupJob.isLocalSnapshot()) {
                addSnapshot(backupJob.getLabel(), backupJob);
            }
        }
        for (String label : removedLabels) {
            removeSnapshot(label);
        }
    }

    private List<AbstractJob> getAllCurrentJobs() {
        jobLock.lock();
        try {
            return dbIdToBackupOrRestoreJobs.values().stream().filter(CollectionUtils::isNotEmpty)
                    .map(Deque::getLast).collect(Collectors.toList());
        } finally {
            jobLock.unlock();
        }
    }

    private AbstractJob getCurrentJob(long dbId) {
        if (Config.enable_table_level_backup_concurrency) {
            // In dual queue mode, get the last job from running queue
            runningJobLock.lock();
            try {
                Deque<AbstractJob> jobs = dbIdToRunningJobs.get(dbId);
                return (jobs != null && !jobs.isEmpty()) ? jobs.getLast() : null;
            } finally {
                runningJobLock.unlock();
            }
        } else {
            // Legacy mode: use old single queue
            jobLock.lock();
            try {
                Deque<AbstractJob> jobs = dbIdToBackupOrRestoreJobs.getOrDefault(dbId, Lists.newLinkedList());
                return jobs.isEmpty() ? null : jobs.getLast();
            } finally {
                jobLock.unlock();
            }
        }
    }

    private void checkAndFilterRestoreObjsExistInSnapshot(BackupJobInfo jobInfo,
                                                          RestoreCommand command)
            throws DdlException {

        // case1: all table in job info
        if (command.getTableRefInfos().isEmpty()) {
            return;
        }

        // case2: exclude table ref
        if (command.isExclude()) {
            for (TableRefInfo tableRefInfo : command.getTableRefInfos()) {
                String tblName = tableRefInfo.getTableNameInfo().getTbl();
                TableType tableType = jobInfo.getTypeByTblName(tblName);
                if (tableType == null) {
                    LOG.info("Ignore error : exclude table " + tblName + " does not exist in snapshot "
                            + jobInfo.name);
                    continue;
                }
                if (tableRefInfo.hasAlias()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "The table alias in exclude clause does not make sense");
                }
                jobInfo.removeTable(tableRefInfo, tableType);
            }
            return;
        }

        // case3: include table ref
        Set<String> olapTableNames = Sets.newHashSet();
        Set<String> viewNames = Sets.newHashSet();
        Set<String> odbcTableNames = Sets.newHashSet();
        for (TableRefInfo tableRefInfo : command.getTableRefInfos()) {
            String tblName = tableRefInfo.getTableNameInfo().getTbl();
            TableType tableType = jobInfo.getTypeByTblName(tblName);
            if (tableType == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Table " + tblName + " does not exist in snapshot " + jobInfo.name);
            }
            switch (tableType) {
                case OLAP:
                    checkAndFilterRestoreOlapTableExistInSnapshot(jobInfo.backupOlapTableObjects, tableRefInfo);
                    olapTableNames.add(tblName);
                    break;
                case VIEW:
                    viewNames.add(tblName);
                    break;
                case ODBC:
                    odbcTableNames.add(tblName);
                    break;
                default:
                    break;
            }

            // set alias
            if (tableRefInfo.hasAlias()) {
                jobInfo.setAlias(tblName, tableRefInfo.getTableAlias());
            }
        }
        jobInfo.retainOlapTables(olapTableNames);
        jobInfo.retainView(viewNames);
        jobInfo.retainOdbcTables(odbcTableNames);
    }

    public void checkAndFilterRestoreOlapTableExistInSnapshot(Map<String, BackupOlapTableInfo> backupOlapTableInfoMap,
                                                              TableRefInfo tableRefInfo) throws DdlException {
        String tblName = tableRefInfo.getTableNameInfo().getTbl();
        BackupOlapTableInfo tblInfo = backupOlapTableInfoMap.get(tblName);
        PartitionNamesInfo partitionNamesInfo = tableRefInfo.getPartitionNamesInfo();
        if (partitionNamesInfo != null) {
            if (partitionNamesInfo.isTemp()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Do not support restoring temporary partitions in table " + tblName);
            }
            // check the selected partitions
            for (String partName : partitionNamesInfo.getPartitionNames()) {
                if (!tblInfo.containsPart(partName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Partition " + partName + " of table " + tblName
                            + " does not exist in snapshot");
                }
            }
        }
        // only retain restore partitions
        tblInfo.retainPartitions(partitionNamesInfo == null ? null : partitionNamesInfo.getPartitionNames());
    }

    public void cancel(CancelBackupCommand command) throws DdlException {
        String dbName = command.getDbName();
        String labelFilter = command.getLabel();  // Get label filter
        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);

        List<AbstractJob> jobsToCancel = new ArrayList<>();

        // In concurrency mode, find all running jobs that match the criteria
        if (Config.enable_table_level_backup_concurrency) {
            runningJobLock.lock();
            try {
                Deque<AbstractJob> jobs = dbIdToRunningJobs.get(db.getId());
                if (jobs != null) {
                    for (AbstractJob job : jobs) {
                        // Check if job type matches
                        boolean typeMatch = (job instanceof BackupJob && !command.isRestore())
                                         || (job instanceof RestoreJob && command.isRestore());

                        // Check if label matches (using CancelBackupCommand.matchesLabel())
                        boolean labelMatch = command.matchesLabel(job.getLabel());

                        if (typeMatch && labelMatch && !job.isDone()) {
                            jobsToCancel.add(job);
                        }
                    }
                }
            } finally {
                runningJobLock.unlock();
            }
        } else {
            // Legacy mode: use getCurrentJob()
            AbstractJob job = getCurrentJob(db.getId());
            if (job != null) {
                boolean typeMatch = (job instanceof BackupJob && !command.isRestore())
                                 || (job instanceof RestoreJob && command.isRestore());
                boolean labelMatch = command.matchesLabel(job.getLabel());

                if (typeMatch && labelMatch) {
                    jobsToCancel.add(job);
                }
            }
        }

        // Error message based on whether label was specified
        if (jobsToCancel.isEmpty()) {
            String errorMsg;
            if (labelFilter != null) {
                errorMsg = String.format("No %s job with label '%s' is currently running",
                                         command.isRestore() ? "restore" : "backup",
                                         labelFilter);
            } else {
                errorMsg = String.format("No %s job is currently running",
                                         command.isRestore() ? "restore" : "backup");
            }
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, errorMsg);
        }

        // Cancel all matching jobs
        List<String> cancelledLabels = new ArrayList<>();
        List<String> failedLabels = new ArrayList<>();

        for (AbstractJob job : jobsToCancel) {
            Status status = job.cancel();
            if (status.ok()) {
                cancelledLabels.add(job.getLabel());
            } else {
                failedLabels.add(job.getLabel() + "(" + status.getErrMsg() + ")");
                LOG.warn("Failed to cancel {} job {}: {}",
                         command.isRestore() ? "restore" : "backup",
                         job.getLabel(), status.getErrMsg());
            }
        }

        if (!failedLabels.isEmpty()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Failed to cancel some jobs: " + String.join(", ", failedLabels));
        }

        // Log with more details
        if (labelFilter != null) {
            LOG.info("Cancelled {} {} job(s) matching label '{}': {}",
                     cancelledLabels.size(),
                     command.isRestore() ? "restore" : "backup",
                     labelFilter,
                     String.join(", ", cancelledLabels));
        } else {
            LOG.info("Cancelled {} {} job(s): {}",
                     cancelledLabels.size(),
                     command.isRestore() ? "restore" : "backup",
                     String.join(", ", cancelledLabels));
        }
    }

    public boolean handleFinishedSnapshotTask(SnapshotTask task, TFinishTaskRequest request) {
        AbstractJob job = null;

        // In concurrency mode, we need to find the job by jobId across all running jobs
        if (Config.enable_table_level_backup_concurrency) {
            runningJobLock.lock();
            try {
                Deque<AbstractJob> jobs = dbIdToRunningJobs.get(task.getDbId());
                if (jobs != null) {
                    // Search for the job with matching jobId
                    for (AbstractJob j : jobs) {
                        if (j.getJobId() == task.getJobId()) {
                            job = j;
                            break;
                        }
                    }
                }
            } finally {
                runningJobLock.unlock();
            }
        } else {
            // Legacy mode: use getCurrentJob()
            job = getCurrentJob(task.getDbId());
            if (job != null && job.getJobId() != task.getJobId()) {
                LOG.warn("invalid snapshot task: {}, job id: {}, task job id: {}",
                        task, job.getJobId(), task.getJobId());
                return true;
            }
        }

        if (job == null) {
            LOG.warn("failed to find backup or restore job for task: {} (jobId={})", task, task.getJobId());
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        if (job instanceof BackupJob) {
            if (task.isRestoreTask()) {
                LOG.warn("expect finding restore job, but get backup job {} for task: {}", job, task);
                // return true to remove this task from AgentTaskQueue
                return true;
            }

            return ((BackupJob) job).finishTabletSnapshotTask(task, request);
        } else {
            if (!task.isRestoreTask()) {
                LOG.warn("expect finding backup job, but get restore job {} for task: {}", job, task);
                // return true to remove this task from AgentTaskQueue
                return true;
            }
            return ((RestoreJob) job).finishTabletSnapshotTask(task, request);
        }
    }

    public boolean handleFinishedSnapshotUploadTask(UploadTask task, TFinishTaskRequest request) {
        AbstractJob job = null;

        // In concurrency mode, find the job by jobId across all running jobs
        if (Config.enable_table_level_backup_concurrency) {
            runningJobLock.lock();
            try {
                Deque<AbstractJob> jobs = dbIdToRunningJobs.get(task.getDbId());
                if (jobs != null) {
                    for (AbstractJob j : jobs) {
                        if (j.getJobId() == task.getJobId()) {
                            job = j;
                            break;
                        }
                    }
                }
            } finally {
                runningJobLock.unlock();
            }
        } else {
            job = getCurrentJob(task.getDbId());
        }

        if (job == null || (job instanceof RestoreJob)) {
            LOG.info("invalid upload task: {}, no backup job is found. db id: {}", task, task.getDbId());
            return false;
        }
        BackupJob backupJob = (BackupJob) job;
        if (backupJob.getJobId() != task.getJobId() || backupJob.getState() != BackupJobState.UPLOADING) {
            LOG.info("invalid upload task: {}, job id: {}, job state: {}",
                     task, backupJob.getJobId(), backupJob.getState().name());
            return false;
        }
        return backupJob.finishSnapshotUploadTask(task, request);
    }

    public boolean handleDownloadSnapshotTask(DownloadTask task, TFinishTaskRequest request) {
        AbstractJob job = null;

        // In concurrency mode, find the job by jobId across all running jobs
        if (Config.enable_table_level_backup_concurrency) {
            runningJobLock.lock();
            try {
                Deque<AbstractJob> jobs = dbIdToRunningJobs.get(task.getDbId());
                if (jobs != null) {
                    for (AbstractJob j : jobs) {
                        if (j.getJobId() == task.getJobId()) {
                            job = j;
                            break;
                        }
                    }
                }
            } finally {
                runningJobLock.unlock();
            }
        } else {
            job = getCurrentJob(task.getDbId());
            if (job != null && job.getJobId() != task.getJobId()) {
                LOG.warn("invalid download task: {}, job id: {}, task job id: {}",
                        task, job.getJobId(), task.getJobId());
                return true;
            }
        }

        if (!(job instanceof RestoreJob)) {
            LOG.warn("failed to find restore job for task: {} (jobId={})", task, task.getJobId());
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        return ((RestoreJob) job).finishTabletDownloadTask(task, request);
    }

    public boolean handleDirMoveTask(DirMoveTask task, TFinishTaskRequest request) {
        AbstractJob job = null;

        // In concurrency mode, find the job by jobId across all running jobs
        if (Config.enable_table_level_backup_concurrency) {
            runningJobLock.lock();
            try {
                Deque<AbstractJob> jobs = dbIdToRunningJobs.get(task.getDbId());
                if (jobs != null) {
                    for (AbstractJob j : jobs) {
                        if (j.getJobId() == task.getJobId()) {
                            job = j;
                            break;
                        }
                    }
                }
            } finally {
                runningJobLock.unlock();
            }
        } else {
            job = getCurrentJob(task.getDbId());
            if (job != null && job.getJobId() != task.getJobId()) {
                LOG.warn("invalid dir move task: {}, job id: {}, task job id: {}",
                        task, job.getJobId(), task.getJobId());
                return true;
            }
        }

        if (!(job instanceof RestoreJob)) {
            LOG.warn("failed to find restore job for task: {} (jobId={})", task, task.getJobId());
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        return ((RestoreJob) job).finishDirMoveTask(task, request);
    }

    public void replayAddJob(AbstractJob job) {
        LOG.info("replay backup/restore job: {}", job);

        if (job.isCancelled()) {
            AbstractJob existingJob = getCurrentJob(job.getDbId());
            if (existingJob == null || existingJob.isDone()) {
                LOG.error("invalid existing job: {}. current replay job is: {}",
                        existingJob, job);
                return;
            }
            existingJob.setEnv(env);
            existingJob.replayCancel();
        } else if (!job.isPending()) {
            AbstractJob existingJob = getCurrentJob(job.getDbId());
            if (existingJob == null || existingJob.isDone()) {
                LOG.error("invalid existing job: {}. current replay job is: {}",
                        existingJob, job);
                return;
            }
            // We use replayed job, not the existing job, to do the replayRun().
            // Because if we use the existing job to run again,
            // for example: In restore job, PENDING will transfer to SNAPSHOTING, not DOWNLOAD.
            job.setEnv(env);
            job.replayRun();
        }

        // Add to appropriate queue based on concurrency mode and job state
        if (Config.enable_table_level_backup_concurrency) {
            // In dual queue mode, decide based on job state
            if (job.isDone()) {
                // Completed job goes to history queue
                addToHistoryQueue(job.getDbId(), job);
            } else {
                // Active job (PENDING or running) goes to running queue
                try {
                    addActiveJob(job.getDbId(), job);
                } catch (DdlException e) {
                    LOG.warn("Failed to add job {} to running queue during replay: {}",
                            job.getLabel(), e.getMessage());
                }
            }
        } else {
            // Legacy mode: use old single queue
            addBackupOrRestoreJob(job.getDbId(), job);
        }
    }

    public boolean report(TTaskType type, long jobId, long taskId, int finishedNum, int totalNum) {
        // Get jobs to report based on concurrency mode
        List<AbstractJob> jobsToCheck;
        if (Config.enable_table_level_backup_concurrency) {
            jobsToCheck = getAllRunningJobs();
        } else {
            jobsToCheck = getAllCurrentJobs();
        }

        for (AbstractJob job : jobsToCheck) {
            if (job.getType() == JobType.BACKUP) {
                if (!job.isDone() && job.getJobId() == jobId && type == TTaskType.UPLOAD) {
                    job.taskProgress.put(taskId, Pair.of(finishedNum, totalNum));
                    return true;
                }
            } else if (job.getType() == JobType.RESTORE) {
                if (!job.isDone() && job.getJobId() == jobId && type == TTaskType.DOWNLOAD) {
                    job.taskProgress.put(taskId, Pair.of(finishedNum, totalNum));
                    return true;
                }
            }
        }
        return false;
    }

    public void addSnapshot(String labelName, BackupJob backupJob) {
        assert backupJob.isFinished();

        LOG.info("add snapshot {} to local repo", labelName);
        localSnapshotsLock.writeLock().lock();
        try {
            localSnapshots.put(labelName, backupJob);
        } finally {
            localSnapshotsLock.writeLock().unlock();
        }
    }

    public void removeSnapshot(String labelName) {
        LOG.info("remove snapshot {} from local repo", labelName);
        localSnapshotsLock.writeLock().lock();
        try {
            localSnapshots.remove(labelName);
        } finally {
            localSnapshotsLock.writeLock().unlock();
        }
    }

    public Snapshot getSnapshot(String labelName) {
        BackupJob backupJob;
        localSnapshotsLock.readLock().lock();
        try {
            backupJob = localSnapshots.get(labelName);
        } finally {
            localSnapshotsLock.readLock().unlock();
        }

        if (backupJob == null) {
            return null;
        }

        return backupJob.getSnapshot();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        repoMgr.write(out);

        // Collect jobs based on concurrency mode
        List<AbstractJob> jobs;
        if (Config.enable_table_level_backup_concurrency) {
            // In dual queue mode, merge running and history queues
            jobs = new ArrayList<>();
            runningJobLock.lock();
            try {
                for (Deque<AbstractJob> queue : dbIdToRunningJobs.values()) {
                    jobs.addAll(queue);
                }
            } finally {
                runningJobLock.unlock();
            }
            historyJobLock.lock();
            try {
                for (Deque<AbstractJob> queue : dbIdToHistoryJobs.values()) {
                    jobs.addAll(queue);
                }
            } finally {
                historyJobLock.unlock();
            }
        } else {
            // Legacy mode: use old single queue
            jobs = dbIdToBackupOrRestoreJobs.values()
                    .stream().flatMap(Deque::stream).collect(Collectors.toList());
        }

        out.writeInt(jobs.size());
        for (AbstractJob job : jobs) {
            job.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        repoMgr = RepositoryMgr.read(in);

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            AbstractJob job = AbstractJob.read(in);
            // Note: addBackupOrRestoreJob will be replaced by replayAddJob during normal operation
            // This is just for initial loading, and the actual queue assignment will happen in
            // rebuildConcurrencyStateAfterRestart()
            addBackupOrRestoreJob(job.getDbId(), job);
        }
    }

    // === Concurrency Control Methods ===

    /**
     * Check if a job has execution permission.
     * Called by BackupJob/RestoreJob to determine if they can proceed with execution.
     *
     * @param jobId the job ID to check
     * @return true if the job is allowed to execute, false otherwise
     */
    public boolean canExecute(long jobId) {
        if (!Config.enable_table_level_backup_concurrency) {
            return true;  // Concurrency not enabled, all jobs can execute
        }
        return allowedJobIds.contains(jobId);
    }

    /**
     * Called when a job is activated (added to allowedJobIds) to update running counters.
     * This enables O(1) canActivate checks.
     *
     * @param dbId database ID
     * @param job the activated job
     * @param isBackup true for backup jobs, false for restore jobs
     * @param isFullDatabase true if this is a full database operation
     */
    private void onJobActivated(long dbId, AbstractJob job, boolean isBackup, boolean isFullDatabase) {
        DatabaseJobStats stats = dbJobStats.computeIfAbsent(dbId, k -> new DatabaseJobStats());

        if (isBackup) {
            stats.runningBackups++;
            if (isFullDatabase) {
                stats.runningBackupDatabaseJobId = job.getJobId();
            }
        } else {
            stats.runningRestores++;
        }

        if (Config.enable_backup_concurrency_logging) {
            LOG.info("Job activated: dbId={}, label={}, runningBackups={}, runningRestores={}",
                     dbId, job.getLabel(), stats.runningBackups, stats.runningRestores);
        }
    }

    /**
     * Called when a job is deactivated (removed from allowedJobIds) to update running counters.
     *
     * @param dbId database ID
     * @param job the deactivated job
     * @param isBackup true for backup jobs, false for restore jobs
     */
    private void onJobDeactivated(long dbId, AbstractJob job, boolean isBackup) {
        DatabaseJobStats stats = dbJobStats.get(dbId);
        if (stats == null) {
            return;
        }

        if (isBackup) {
            stats.runningBackups--;
            if (Long.valueOf(job.getJobId()).equals(stats.runningBackupDatabaseJobId)) {
                stats.runningBackupDatabaseJobId = null;
            }
        } else {
            stats.runningRestores--;
        }

        int taskCount = job.getSnapshotTaskCount();
        if (taskCount > 0) {
            globalSnapshotTasks.addAndGet(-taskCount);
        }

        if (Config.enable_backup_concurrency_logging) {
            LOG.info("Job deactivated: dbId={}, label={}, runningBackups={}, runningRestores={}, "
                     + "globalSnapshotTasks={}",
                     dbId, job.getLabel(), stats.runningBackups, stats.runningRestores,
                     globalSnapshotTasks.get());
        }
    }

    /**
     * Concurrency check for new backup/restore job submissions.
     * This method only performs hard rejection checks. Soft checks (whether to PENDING)
     * are handled by canActivate().
     *
     * @param dbId database ID
     * @param label job label
     * @param isBackup true for backup jobs, false for restore jobs
     * @param isFullDatabase true if this is a full database backup/restore (no specific tables)
     * @param tableNames list of table names involved (empty for full database operations)
     * @throws DdlException if job should be rejected
     */
    private void checkConcurrency(long dbId, String label, boolean isBackup,
                                  boolean isFullDatabase, List<String> tableNames) throws DdlException {
        // === Step 1: O(1) Label duplicate check ===
        Map<String, Long> labels = labelIndex.get(dbId);
        if (labels != null && labels.containsKey(label)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Label already exists: " + label);
        }

        // === Step 2: O(1) Get statistics ===
        DatabaseJobStats stats = dbJobStats.get(dbId);
        if (stats == null) {
            return;  // No jobs, no rejection needed
        }

        int totalActive = stats.activeBackups + stats.activeRestores;

        // === Rule 1: Concurrency limit (hard reject) ===
        if (totalActive >= Config.max_backup_restore_concurrent_num_per_db) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Concurrency limit reached (" + totalActive + " active jobs).\n"
                        + "Active jobs: " + String.join(", ", stats.activeLabels) + "\n"
                        + "Limit: " + Config.max_backup_restore_concurrent_num_per_db);
        }

        // === Rule 2-A: Queue has backup_database -> reject all new backups ===
        if (isBackup && stats.backupDatabaseJobId != null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Cannot submit backup job while full database backup exists.\n"
                        + "Full database backup: " + stats.backupDatabaseLabel + " (running or pending)\n"
                        + "Suggestion: Wait for it to complete, or cancel it first.");
        }

        // Other rules (backup_database waiting, restore conflicts, backup/restore mutual exclusion)
        // are soft rules handled by canActivate() - they cause PENDING, not rejection
    }

    /**
     * Called when a job is created to update statistics.
     *
     * @param dbId database ID
     * @param job the created job
     * @param isBackup true for backup jobs, false for restore jobs
     * @param isFullDatabase true if this is a full database operation
     */
    private void onJobCreated(long dbId, AbstractJob job, boolean isBackup, boolean isFullDatabase) {
        // Update statistics
        DatabaseJobStats stats = dbJobStats.computeIfAbsent(dbId, k -> new DatabaseJobStats());

        if (isBackup) {
            stats.activeBackups++;
            if (isFullDatabase) {
                stats.backupDatabaseJobId = job.getJobId();
                stats.backupDatabaseLabel = job.getLabel();
            }
        } else {
            stats.activeRestores++;
        }

        stats.activeLabels.add(job.getLabel());

        // Update label index
        labelIndex.computeIfAbsent(dbId, k -> new ConcurrentHashMap<>())
                  .put(job.getLabel(), job.getJobId());

        if (Config.enable_backup_concurrency_logging) {
            LOG.info("Job created: dbId={}, label={}, backups={}, restores={}",
                     dbId, job.getLabel(), stats.activeBackups, stats.activeRestores);
        }
    }

    /**
     * Check if a newly created job can be activated immediately (O(1) version).
     * This is called right after job creation to decide whether to add it to allowedJobIds.
     *
     * @param dbId database ID
     * @param job the job to check
     * @param isBackup true for backup jobs, false for restore jobs
     * @param isFullDatabase true if this is a full database operation
     * @return true if job can be activated immediately
     */
    private boolean canActivate(long dbId, AbstractJob job, boolean isBackup, boolean isFullDatabase) {
        DatabaseJobStats stats = dbJobStats.get(dbId);

        // O(1) lookup for running job counts
        int runningBackups = (stats != null) ? stats.runningBackups : 0;
        int runningRestores = (stats != null) ? stats.runningRestores : 0;
        boolean hasRunningBackupDatabase = (stats != null) && (stats.runningBackupDatabaseJobId != null);

        // === Rule 1: Concurrency limit ===
        if (runningBackups + runningRestores >= Config.max_backup_restore_concurrent_num_per_db) {
            return false;
        }

        // === Rule 2-A: Running backup_database -> backup cannot activate ===
        if (hasRunningBackupDatabase && isBackup) {
            return false;
        }

        // === Rule 2-B: Job is backup_database -> wait for all backups to complete ===
        if (isBackup && isFullDatabase && runningBackups > 0) {
            return false;
        }

        // === Rule 3: Backup/Restore mutual exclusion ===
        if (isBackup && runningRestores > 0) {
            return false;
        }
        if (!isBackup && runningBackups > 0) {
            return false;
        }

        // === Rule 4: For restore jobs, check table conflicts ===
        if (!isBackup && job instanceof RestoreJob) {
            RestoreJob restoreJob = (RestoreJob) job;
            // Check full database restore conflict
            if (stats != null && stats.restoreDatabaseJobId != null) {
                return false;  // Full database restore is running
            }

            // Check if this is a full database restore
            if (isFullDatabase) {
                Set<String> currentRestoring = restoringTables.get(dbId);
                if (currentRestoring != null && !currentRestoring.isEmpty()) {
                    return false;  // Other tables are being restored
                }
            } else {
                // Check table conflicts (O(k) where k = number of tables in the job)
                Set<String> currentRestoring = restoringTables.get(dbId);
                if (currentRestoring != null && !currentRestoring.isEmpty()) {
                    for (TableRefInfo tblRef : restoreJob.getTableRefs()) {
                        String tableName = tblRef.getTableNameInfo().getTbl();
                        if (currentRestoring.contains(tableName)) {
                            return false;  // Table conflict
                        }
                    }
                }
            }
        }

        return true;
    }

    /**
     * Check if a PENDING job can be activated (O(1) version).
     * Called during tryActivatePendingJobs() to determine which jobs can start running.
     *
     * @param dbId database ID
     * @param jobToCheck the job to check for activation
     * @param isBackup true for backup jobs, false for restore jobs
     * @return true if job can be activated
     */
    private boolean canActivateJob(long dbId, AbstractJob jobToCheck, boolean isBackup) {
        DatabaseJobStats stats = dbJobStats.get(dbId);

        // Determine job type
        boolean isJobBackupDatabase = false;
        boolean isJobRestore = false;

        if (isBackup && jobToCheck instanceof BackupJob) {
            BackupJob bj = (BackupJob) jobToCheck;
            isJobBackupDatabase = bj.getTableRefs().isEmpty();
        } else {
            isJobRestore = true;
        }

        // O(1) lookup for running job counts
        int runningBackups = (stats != null) ? stats.runningBackups : 0;
        int runningRestores = (stats != null) ? stats.runningRestores : 0;
        boolean hasRunningBackupDatabase = (stats != null) && (stats.runningBackupDatabaseJobId != null);

        // === Rule 1: Concurrency limit ===
        if (runningBackups + runningRestores >= Config.max_backup_restore_concurrent_num_per_db) {
            return false;
        }

        // === Rule 2-A: Running backup_database -> backup cannot activate ===
        if (hasRunningBackupDatabase && isBackup) {
            return false;
        }

        // === Rule 2-B: Job is backup_database -> wait for all backups to complete ===
        if (isJobBackupDatabase && runningBackups > 0) {
            return false;
        }

        // === Rule 3: Backup/Restore mutual exclusion ===
        if (isBackup && runningRestores > 0) {
            return false;
        }
        if (isJobRestore && runningBackups > 0) {
            return false;
        }

        // === Rule 4: For restore jobs, check table conflicts ===
        if (isJobRestore && jobToCheck instanceof RestoreJob) {
            RestoreJob restoreJob = (RestoreJob) jobToCheck;
            // Check full database restore conflict
            if (stats != null && stats.restoreDatabaseJobId != null) {
                return false;  // Full database restore is running
            }

            // Check if this is a full database restore
            if (restoreJob.getTableRefs().isEmpty()) {
                Set<String> currentRestoring = restoringTables.get(dbId);
                if (currentRestoring != null && !currentRestoring.isEmpty()) {
                    return false;  // Other tables are being restored
                }
            } else {
                // Check table conflicts (O(k) where k = number of tables in the job)
                Set<String> currentRestoring = restoringTables.get(dbId);
                if (currentRestoring != null && !currentRestoring.isEmpty()) {
                    for (TableRefInfo tblRef : restoreJob.getTableRefs()) {
                        String tableName = tblRef.getTableNameInfo().getTbl();
                        if (currentRestoring.contains(tableName)) {
                            return false;  // Table conflict
                        }
                    }
                }
            }
        }

        return true;
    }

    /**
     * Called when a job completes (finished or cancelled) to update statistics
     * and try to activate pending jobs.
     *
     * @param jobId the completed job ID
     * @param dbId database ID
     * @param job the completed job
     */
    public void onJobCompleted(long jobId, long dbId, AbstractJob job) {
        if (!Config.enable_table_level_backup_concurrency) {
            return;
        }

        boolean isBackup = (job instanceof BackupJob);

        // === 0. Move from running queue to history queue (dual queue architecture) ===
        moveToHistory(dbId, job);

        // === 1. Update statistics (O(1)) ===
        DatabaseJobStats stats = dbJobStats.get(dbId);
        if (stats != null) {
            if (isBackup) {
                stats.activeBackups--;
                if (Long.valueOf(jobId).equals(stats.backupDatabaseJobId)) {
                    stats.backupDatabaseJobId = null;
                    stats.backupDatabaseLabel = null;
                }
            } else {
                stats.activeRestores--;
                if (Long.valueOf(jobId).equals(stats.restoreDatabaseJobId)) {
                    stats.restoreDatabaseJobId = null;
                    stats.restoreDatabaseLabel = null;
                }
            }

            stats.activeLabels.remove(job.getLabel());

            // Clean up empty stats
            if (stats.activeBackups == 0 && stats.activeRestores == 0) {
                dbJobStats.remove(dbId);
            }
        }

        // === 2. Update label index ===
        Map<String, Long> labels = labelIndex.get(dbId);
        if (labels != null) {
            labels.remove(job.getLabel());
            if (labels.isEmpty()) {
                labelIndex.remove(dbId);
            }
        }

        // === 3. Remove execution permission and update running counters ===
        boolean wasRunning = allowedJobIds.remove(jobId);
        if (wasRunning) {
            onJobDeactivated(dbId, job, isBackup);
        }

        // === 4. Clean up restoringTables for restore jobs ===
        if (job instanceof RestoreJob) {
            removeRestoringTables((RestoreJob) job);
        }

        if (Config.enable_backup_concurrency_logging) {
            LOG.info("Job completed: dbId={}, label={}, activeBackups={}, activeRestores={}, "
                     + "runningBackups={}, runningRestores={}",
                     dbId, job.getLabel(),
                     stats != null ? stats.activeBackups : 0,
                     stats != null ? stats.activeRestores : 0,
                     stats != null ? stats.runningBackups : 0,
                     stats != null ? stats.runningRestores : 0);
        }

        // === 5. Try to activate PENDING jobs ===
        tryActivatePendingJobs(dbId);
    }

    /**
     * Try to activate pending jobs after a job completes.
     * This method scans the job queue and activates jobs that can now run.
     *
     * @param dbId database ID
     */
    private void tryActivatePendingJobs(long dbId) {
        // Get jobs to scan based on concurrency mode
        Deque<AbstractJob> jobs;
        if (Config.enable_table_level_backup_concurrency) {
            // In dual queue mode, scan running queue
            runningJobLock.lock();
            try {
                jobs = dbIdToRunningJobs.get(dbId);
                if (jobs == null || jobs.isEmpty()) {
                    return;
                }
                // Make a copy to avoid concurrent modification
                jobs = new LinkedList<>(jobs);
            } finally {
                runningJobLock.unlock();
            }
        } else {
            // Legacy mode: use old single queue
            jobs = dbIdToBackupOrRestoreJobs.get(dbId);
            if (jobs == null || jobs.isEmpty()) {
                return;
            }
        }

        for (AbstractJob job : jobs) {
            // Only process PENDING jobs
            if (job.isDone()) {
                continue;
            }

            // Already has execution permission, skip
            if (allowedJobIds.contains(job.getJobId())) {
                continue;
            }

            // Check if can activate
            boolean isBackup = job instanceof BackupJob;
            boolean isFullDatabase = false;
            if (isBackup && job instanceof BackupJob) {
                isFullDatabase = ((BackupJob) job).getTableRefs().isEmpty();
            } else if (!isBackup && job instanceof RestoreJob) {
                isFullDatabase = ((RestoreJob) job).getTableRefs().isEmpty();
            }

            if (canActivateJob(dbId, job, isBackup)) {
                allowedJobIds.add(job.getJobId());
                onJobActivated(dbId, job, isBackup, isFullDatabase);
                // CRITICAL: Add restoring tables atomically with activation
                // to prevent TOCTOU race condition
                if (job instanceof RestoreJob) {
                    addRestoringTables((RestoreJob) job);
                }
                LOG.info("Activated pending job: {} (jobId={})",
                        job.getLabel(), job.getJobId());
            }
        }
    }

    /**
     * Add restore job's tables to the restoring set for conflict detection.
     *
     * @param job the restore job
     */
    public void addRestoringTables(RestoreJob job) {
        long dbId = job.getDbId();

        // Check if this is a full database restore
        if (job.getTableRefs().isEmpty()) {
            // Full database restore: mark as exclusive
            DatabaseJobStats stats = dbJobStats.computeIfAbsent(dbId,
                    k -> new DatabaseJobStats());
            stats.restoreDatabaseJobId = job.getJobId();
            stats.restoreDatabaseLabel = job.getLabel();
            LOG.info("Full database restore [{}] started, blocking other restores on db [{}]",
                     job.getLabel(), dbId);
            return;
        }

        // Table-level restore: add table names to set
        Set<String> tables = restoringTables.computeIfAbsent(dbId, k -> ConcurrentHashMap.newKeySet());

        for (TableRefInfo tblRef : job.getTableRefs()) {
            String tableName = tblRef.getTableNameInfo().getTbl();
            tables.add(tableName);

            if (Config.enable_backup_concurrency_logging) {
                LOG.info("Table [{}] added to restoring set, job: [{}]", tableName, job.getLabel());
            }
        }
    }

    /**
     * Remove restore job's tables from the restoring set when job completes.
     *
     * @param job the restore job
     */
    private void removeRestoringTables(RestoreJob job) {
        long dbId = job.getDbId();

        // Check if this is a full database restore
        if (job.getTableRefs().isEmpty()) {
            // Full database restore: clear marker
            DatabaseJobStats stats = dbJobStats.get(dbId);
            if (stats != null && Long.valueOf(job.getJobId()).equals(stats.restoreDatabaseJobId)) {
                stats.restoreDatabaseJobId = null;
                stats.restoreDatabaseLabel = null;
            }
            LOG.info("Full database restore [{}] completed, unblocking other restores",
                     job.getLabel());
            return;
        }

        // Table-level restore: remove table names
        Set<String> tables = restoringTables.get(dbId);
        if (tables != null) {
            for (TableRefInfo tblRef : job.getTableRefs()) {
                String tableName = tblRef.getTableNameInfo().getTbl();
                tables.remove(tableName);

                if (Config.enable_backup_concurrency_logging) {
                    LOG.info("Table [{}] removed from restoring set, job: [{}]",
                             tableName, job.getLabel());
                }
            }

            // Remove key if set is empty
            if (tables.isEmpty()) {
                restoringTables.remove(dbId);
            }
        }
    }

    /**
     * Get the block reason for a job (for SHOW command).
     *
     * @param job the job to check
     * @return block reason string, or null if not blocked
     */
    public String getJobBlockReason(AbstractJob job) {
        if (!Config.enable_table_level_backup_concurrency) {
            return null;
        }

        // Finished/cancelled jobs are not blocked
        if (job.isDone()) {
            return null;
        }

        // If job has execution permission, it's not blocked
        if (allowedJobIds.contains(job.getJobId())) {
            return null;
        }

        long dbId = job.getDbId();
        boolean isBackup = job instanceof BackupJob;

        // Check backup/restore mutual exclusion
        DatabaseJobStats stats = dbJobStats.get(dbId);
        if (stats != null) {
            if (isBackup && stats.activeRestores > 0) {
                return "Waiting for " + stats.activeRestores + " restore(s) to complete";
            }
            if (!isBackup && stats.activeBackups > 0) {
                return "Waiting for " + stats.activeBackups + " backup(s) to complete";
            }

            // Check full database backup blocking
            if (isBackup && stats.backupDatabaseJobId != null) {
                return "Full database backup is running: " + stats.backupDatabaseLabel;
            }
        }

        // Check restore table conflicts
        if (job instanceof RestoreJob) {
            RestoreJob restoreJob = (RestoreJob) job;

            // Check full database restore
            if (stats != null && stats.restoreDatabaseJobId != null) {
                return "Full database restore is running: " + stats.restoreDatabaseLabel;
            }

            // Check table conflicts
            Set<String> restoring = restoringTables.get(dbId);
            if (restoring != null && !restoring.isEmpty()) {
                Set<String> conflicts = new HashSet<>();
                for (TableRefInfo tblRef : restoreJob.getTableRefs()) {
                    String tableName = tblRef.getTableNameInfo().getTbl();
                    if (restoring.contains(tableName)) {
                        conflicts.add(tableName);
                    }
                }

                if (!conflicts.isEmpty()) {
                    return "Table conflict: " + String.join(", ", conflicts);
                }
            }
        }

        return "Waiting for execution slot";
    }

    /**
     * Get the queue position for a job (for SHOW command).
     * Returns 0 if job is running, or the 1-based position in the queue.
     *
     * @param job the job to check
     * @return queue position (0 = running, 1+ = position in queue)
     */
    public int getJobQueuePosition(AbstractJob job) {
        if (!Config.enable_table_level_backup_concurrency) {
            return 0;
        }

        // Finished/cancelled jobs have no queue position
        if (job.isDone()) {
            return 0;
        }

        // If job has execution permission, it's running (position 0)
        if (allowedJobIds.contains(job.getJobId())) {
            return 0;
        }

        // Count position in pending queue (use dbIdToRunningJobs in concurrency mode)
        Deque<AbstractJob> jobs = dbIdToRunningJobs.get(job.getDbId());
        if (jobs == null) {
            return 0;
        }

        int position = 0;
        for (AbstractJob j : jobs) {
            if (j.isDone()) {
                continue;
            }
            if (!allowedJobIds.contains(j.getJobId())) {
                position++;
                if (j.getJobId() == job.getJobId()) {
                    return position;
                }
            }
        }

        return position;
    }

    /**
     * Database job statistics for O(1) concurrency checking.
     * This class maintains counters and markers for each database to avoid
     * traversing the job queue on every submission.
     */
    static class DatabaseJobStats {
        // Active job counts (includes PENDING jobs, not just running)
        int activeBackups = 0;
        int activeRestores = 0;

        // Running job counts (only jobs in allowedJobIds, for O(1) canActivate check)
        int runningBackups = 0;
        int runningRestores = 0;

        // Full database backup marker (in queue, pending or running)
        Long backupDatabaseJobId = null;    // jobId of the full database backup
        String backupDatabaseLabel = null;  // label of the full database backup

        // Running full database backup marker (for canActivate check)
        Long runningBackupDatabaseJobId = null;

        // Full database restore marker (integrated here for unified management)
        Long restoreDatabaseJobId = null;   // jobId of the full database restore
        String restoreDatabaseLabel = null; // label of the full database restore

        // All active job labels (for error messages)
        Set<String> activeLabels = new HashSet<>();

        /**
         * Check if there are any active jobs.
         */
        boolean hasActiveJobs() {
            return activeBackups > 0 || activeRestores > 0;
        }

        /**
         * Get total active job count.
         */
        int getTotalActiveJobs() {
            return activeBackups + activeRestores;
        }

        /**
         * Get total running job count.
         */
        int getTotalRunningJobs() {
            return runningBackups + runningRestores;
        }

        @Override
        public String toString() {
            return "DatabaseJobStats{"
                + "activeBackups=" + activeBackups
                + ", activeRestores=" + activeRestores
                + ", runningBackups=" + runningBackups
                + ", runningRestores=" + runningRestores
                + ", backupDatabaseLabel=" + backupDatabaseLabel
                + ", restoreDatabaseLabel=" + restoreDatabaseLabel
                + '}';
        }
    }
}
