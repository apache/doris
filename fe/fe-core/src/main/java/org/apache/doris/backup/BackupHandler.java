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

import org.apache.doris.analysis.AbstractBackupStmt;
import org.apache.doris.analysis.AbstractBackupTableRefClause;
import org.apache.doris.analysis.AlterRepositoryStmt;
import org.apache.doris.analysis.BackupStmt;
import org.apache.doris.analysis.BackupStmt.BackupType;
import org.apache.doris.analysis.CancelBackupStmt;
import org.apache.doris.analysis.CreateRepositoryStmt;
import org.apache.doris.analysis.DropRepositoryStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.RestoreStmt;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.backup.AbstractJob.JobType;
import org.apache.doris.backup.BackupJob.BackupJobState;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.AzureFileSystem;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.fs.remote.S3FileSystem;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

    // db id ->  last 10(max_backup_restore_job_num_per_db) backup/restore jobs
    // Newly submitted job will replace the current job, only if current job is finished or cancelled.
    // If the last job is finished, user can get the job info from repository. If the last job is cancelled,
    // user can get the error message before submitting the next one.
    private final Map<Long, Deque<AbstractJob>> dbIdToBackupOrRestoreJobs = new HashMap<>();

    // this lock is used for handling one backup or restore request at a time.
    private ReentrantLock seqlock = new ReentrantLock();

    private boolean isInit = false;

    private Env env;

    // map to store backup info, key is label name, value is Pair<meta, info>, meta && info is bytes
    // this map not present in persist && only in fe master memory
    // one table only keep one snapshot info, only keep last
    private final Map<String, Snapshot> localSnapshots = new HashMap<>();
    private ReadWriteLock localSnapshotsLock = new ReentrantReadWriteLock();

    public BackupHandler() {
        // for persist
    }

    public BackupHandler(Env env) {
        super("backupHandler", 3000L);
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
        return true;
    }

    public AbstractJob getJob(long dbId) {
        return getCurrentJob(dbId);
    }

    public List<AbstractJob> getJobs(long dbId, Predicate<String> predicate) {
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

    @Override
    protected void runAfterCatalogReady() {
        if (!isInit) {
            if (!init()) {
                return;
            }
        }

        for (AbstractJob job : getAllCurrentJobs()) {
            job.setEnv(env);
            job.run();
        }
    }

    // handle create repository stmt
    public void createRepository(CreateRepositoryStmt stmt) throws DdlException {
        if (!env.getBrokerMgr().containsBroker(stmt.getBrokerName())
                && stmt.getStorageType() == StorageBackend.StorageType.BROKER) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "broker does not exist: " + stmt.getBrokerName());
        }

        RemoteFileSystem fileSystem = FileSystemFactory.get(stmt.getBrokerName(), stmt.getStorageType(),
                    stmt.getProperties());
        long repoId = env.getNextId();
        Repository repo = new Repository(repoId, stmt.getName(), stmt.isReadOnly(), stmt.getLocation(), fileSystem);

        Status st = repoMgr.addAndInitRepoIfNotExist(repo, false);
        if (!st.ok()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                           "Failed to create repository: " + st.getErrMsg());
        }
        if (!repo.ping()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Failed to create repository: failed to connect to the repo");
        }
    }

    public void alterRepository(AlterRepositoryStmt stmt) throws DdlException {
        tryLock();
        try {
            Repository repo = repoMgr.getRepo(stmt.getName());
            if (repo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository does not exist");
            }

            if (repo.getRemoteFileSystem() instanceof S3FileSystem
                    || repo.getRemoteFileSystem() instanceof AzureFileSystem) {
                Map<String, String> oldProperties = new HashMap<>(stmt.getProperties());
                Status status = repo.alterRepositoryS3Properties(oldProperties);
                if (!status.ok()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, status.getErrMsg());
                }
                RemoteFileSystem fileSystem = null;
                if (repo.getRemoteFileSystem() instanceof S3FileSystem) {
                    fileSystem = FileSystemFactory.get(repo.getRemoteFileSystem().getName(),
                            StorageBackend.StorageType.S3, oldProperties);
                } else if (repo.getRemoteFileSystem() instanceof AzureFileSystem) {
                    fileSystem = FileSystemFactory.get(repo.getRemoteFileSystem().getName(),
                            StorageBackend.StorageType.AZURE, oldProperties);
                }

                Repository newRepo = new Repository(repo.getId(), repo.getName(), repo.isReadOnly(),
                        repo.getLocation(), fileSystem);
                if (!newRepo.ping()) {
                    LOG.warn("Failed to connect repository {}. msg: {}", repo.getName(), repo.getErrorMsg());
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Repo can not ping with new s3 properties");
                }

                Status st = repoMgr.alterRepo(newRepo, false /* not replay */);
                if (!st.ok()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Failed to alter repository: " + st.getErrMsg());
                }
                for (AbstractJob job : getAllCurrentJobs()) {
                    if (!job.isDone() && job.getRepoId() == repo.getId()) {
                        job.updateRepo(newRepo);
                    }
                }
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Only support alter s3 or azure repository");
            }
        } finally {
            seqlock.unlock();
        }
    }

    // handle drop repository stmt
    public void dropRepository(DropRepositoryStmt stmt) throws DdlException {
        tryLock();
        try {
            Repository repo = repoMgr.getRepo(stmt.getRepoName());
            if (repo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository does not exist");
            }

            for (AbstractJob job : getAllCurrentJobs()) {
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

    // the entry method of submitting a backup or restore job
    public void process(AbstractBackupStmt stmt) throws DdlException {
        if (Config.isCloudMode()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "BACKUP and RESTORE are not supported by the cloud mode yet");
        }

        // check if repo exist
        String repoName = stmt.getRepoName();
        Repository repository = null;
        if (!repoName.equals(Repository.KEEP_ON_LOCAL_REPO_NAME)) {
            repository = repoMgr.getRepo(repoName);
            if (repository == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository " + repoName + " does not exist");
            }
        }

        // check if db exist
        String dbName = stmt.getDbName();
        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);

        // Try to get sequence lock.
        // We expect at most one operation on a repo at same time.
        // But this operation may take a few seconds with lock held.
        // So we use tryLock() to give up this operation if we can not get lock.
        tryLock();
        try {
            // Check if there is backup or restore job running on this database
            AbstractJob currentJob = getCurrentJob(db.getId());
            if (currentJob != null && !currentJob.isDone()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                               "Can only run one backup or restore job of a database at same time");
            }

            if (stmt instanceof BackupStmt) {
                backup(repository, db, (BackupStmt) stmt);
            } else if (stmt instanceof RestoreStmt) {
                restore(repository, db, (RestoreStmt) stmt);
            }
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

    private void backup(Repository repository, Database db, BackupStmt stmt) throws DdlException {
        if (repository != null && repository.isReadOnly()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository " + repository.getName()
                    + " is read only");
        }

        // Determine the tables to be backed up
        Set<String> tableNames = Sets.newHashSet();
        AbstractBackupTableRefClause abstractBackupTableRefClause = stmt.getAbstractBackupTableRefClause();
        if (abstractBackupTableRefClause == null) {
            tableNames = db.getTableNamesWithLock();
        } else if (abstractBackupTableRefClause.isExclude()) {
            tableNames = db.getTableNamesWithLock();
            for (TableRef tableRef : abstractBackupTableRefClause.getTableRefList()) {
                if (!tableNames.remove(tableRef.getName().getTbl())) {
                    LOG.info("exclude table " + tableRef.getName().getTbl()
                            + " of backup stmt is not exists in db " + db.getFullName());
                }
            }
        }
        List<TableRef> tblRefs = Lists.newArrayList();
        if (abstractBackupTableRefClause != null && !abstractBackupTableRefClause.isExclude()) {
            tblRefs = abstractBackupTableRefClause.getTableRefList();
        } else {
            for (String tableName : tableNames) {
                TableRef tableRef = new TableRef(new TableName(null, db.getFullName(), tableName), null);
                tblRefs.add(tableRef);
            }
        }

        // Check if backup objects are valid
        // This is just a pre-check to avoid most of invalid backup requests.
        // Also calculate the signature for incremental backup check.
        List<TableRef> tblRefsNotSupport = Lists.newArrayList();
        for (TableRef tblRef : tblRefs) {
            String tblName = tblRef.getName().getTbl();
            Table tbl = db.getTableOrDdlException(tblName);
            if (tbl.getType() == TableType.VIEW || tbl.getType() == TableType.ODBC
                    || tbl.getType() == TableType.MATERIALIZED_VIEW) {
                continue;
            }
            if (tbl.getType() != TableType.OLAP) {
                if (Config.ignore_backup_not_support_table_type) {
                    LOG.warn("Table '{}' is a {} table, can not backup and ignore it."
                            + "Only OLAP(Doris)/ODBC/VIEW table can be backed up",
                            tblName, tbl.getType().toString());
                    tblRefsNotSupport.add(tblRef);
                    continue;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_NOT_OLAP_TABLE, tblName);
                }
            }

            OlapTable olapTbl = (OlapTable) tbl;
            tbl.readLock();
            try {
                if (olapTbl.existTempPartitions()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support backup table with temp partitions");
                }

                PartitionNames partitionNames = tblRef.getPartitionNames();
                if (partitionNames != null) {
                    if (partitionNames.isTemp()) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                "Do not support backup temp partitions");
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

        tblRefs.removeAll(tblRefsNotSupport);

        // Check if label already be used
        long repoId = -1;
        if (repository != null) {
            List<String> existSnapshotNames = Lists.newArrayList();
            Status st = repository.listSnapshots(existSnapshotNames);
            if (!st.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, st.getErrMsg());
            }
            if (existSnapshotNames.contains(stmt.getLabel())) {
                if (stmt.getType() == BackupType.FULL) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Snapshot with name '"
                            + stmt.getLabel() + "' already exist in repository");
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Currently does not support "
                            + "incremental backup");
                }
            }
            repoId = repository.getId();
        }

        // Create a backup job
        BackupJob backupJob = new BackupJob(stmt.getLabel(), db.getId(),
                ClusterNamespace.getNameFromFullName(db.getFullName()),
                tblRefs, stmt.getTimeoutMs(), stmt.getContent(), env, repoId);
        // write log
        env.getEditLog().logBackupJob(backupJob);

        // must put to dbIdToBackupOrRestoreJob after edit log, otherwise the state of job may be changed.
        addBackupOrRestoreJob(db.getId(), backupJob);

        LOG.info("finished to submit backup job: {}", backupJob);
    }

    private void restore(Repository repository, Database db, RestoreStmt stmt) throws DdlException {
        BackupJobInfo jobInfo;
        if (stmt.isLocal()) {
            String jobInfoString = new String(stmt.getJobInfo());
            jobInfo = BackupJobInfo.genFromJson(jobInfoString);

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
            Status status = repository.getSnapshotInfoFile(stmt.getLabel(), stmt.getBackupTimestamp(), infos);
            if (!status.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Failed to get info of snapshot '" + stmt.getLabel() + "' because: "
                                + status.getErrMsg() + ". Maybe specified wrong backup timestamp");
            }

            // Check if all restore objects are exist in this snapshot.
            // Also remove all unrelated objs
            Preconditions.checkState(infos.size() == 1);
            jobInfo = infos.get(0);
        }

        checkAndFilterRestoreObjsExistInSnapshot(jobInfo, stmt.getAbstractBackupTableRefClause());

        // Create a restore job
        RestoreJob restoreJob;
        if (stmt.isLocal()) {
            int metaVersion = stmt.getMetaVersion();
            if (metaVersion == -1) {
                metaVersion = jobInfo.metaVersion;
            }

            BackupMeta backupMeta;
            try {
                backupMeta = BackupMeta.fromBytes(stmt.getMeta(), metaVersion);
            } catch (IOException e) {
                LOG.warn("read backup meta failed, current meta version {}", Env.getCurrentEnvJournalVersion(), e);
                throw new DdlException("read backup meta failed", e);
            }
            String backupTimestamp = TimeUtils.longToTimeString(
                    jobInfo.getBackupTime(), TimeUtils.getDatetimeFormatWithHyphenWithTimeZone());
            restoreJob = new RestoreJob(stmt.getLabel(), backupTimestamp,
                    db.getId(), db.getFullName(), jobInfo, stmt.allowLoad(), stmt.getReplicaAlloc(),
                    stmt.getTimeoutMs(), metaVersion, stmt.reserveReplica(),
                    stmt.reserveDynamicPartitionEnable(), stmt.isBeingSynced(),
                    env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMeta);
        } else {
            restoreJob = new RestoreJob(stmt.getLabel(), stmt.getBackupTimestamp(),
                db.getId(), db.getFullName(), jobInfo, stmt.allowLoad(), stmt.getReplicaAlloc(),
                stmt.getTimeoutMs(), stmt.getMetaVersion(), stmt.reserveReplica(), stmt.reserveDynamicPartitionEnable(),
                stmt.isBeingSynced(), env, repository.getId());
        }

        env.getEditLog().logRestoreJob(restoreJob);

        // must put to dbIdToBackupOrRestoreJob after edit log, otherwise the state of job may be changed.
        addBackupOrRestoreJob(db.getId(), restoreJob);

        LOG.info("finished to submit restore job: {}", restoreJob);
    }

    private void addBackupOrRestoreJob(long dbId, AbstractJob job) {
        // If there are too many backup/restore jobs, it may cause OOM.  If the job num option is set to 0,
        // skip all backup/restore jobs.
        if (Config.max_backup_restore_job_num_per_db <= 0) {
            return;
        }

        jobLock.lock();
        try {
            Deque<AbstractJob> jobs = dbIdToBackupOrRestoreJobs.computeIfAbsent(dbId, k -> Lists.newLinkedList());
            while (jobs.size() >= Config.max_backup_restore_job_num_per_db) {
                jobs.removeFirst();
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
        jobLock.lock();
        try {
            Deque<AbstractJob> jobs = dbIdToBackupOrRestoreJobs.getOrDefault(dbId, Lists.newLinkedList());
            return jobs.isEmpty() ? null : jobs.getLast();
        } finally {
            jobLock.unlock();
        }
    }

    private void checkAndFilterRestoreObjsExistInSnapshot(BackupJobInfo jobInfo,
                                                          AbstractBackupTableRefClause backupTableRefClause)
            throws DdlException {
        // case1: all table in job info
        if (backupTableRefClause == null) {
            return;
        }

        // case2: exclude table ref
        if (backupTableRefClause.isExclude()) {
            for (TableRef tblRef : backupTableRefClause.getTableRefList()) {
                String tblName = tblRef.getName().getTbl();
                TableType tableType = jobInfo.getTypeByTblName(tblName);
                if (tableType == null) {
                    LOG.info("Ignore error : exclude table " + tblName + " does not exist in snapshot "
                            + jobInfo.name);
                    continue;
                }
                if (tblRef.hasExplicitAlias()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "The table alias in exclude clause does not make sense");
                }
                jobInfo.removeTable(tblRef, tableType);
            }
            return;
        }
        // case3: include table ref
        Set<String> olapTableNames = Sets.newHashSet();
        Set<String> viewNames = Sets.newHashSet();
        Set<String> odbcTableNames = Sets.newHashSet();
        for (TableRef tblRef : backupTableRefClause.getTableRefList()) {
            String tblName = tblRef.getName().getTbl();
            TableType tableType = jobInfo.getTypeByTblName(tblName);
            if (tableType == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Table " + tblName + " does not exist in snapshot " + jobInfo.name);
            }
            switch (tableType) {
                case OLAP:
                    checkAndFilterRestoreOlapTableExistInSnapshot(jobInfo.backupOlapTableObjects, tblRef);
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
            if (tblRef.hasExplicitAlias()) {
                jobInfo.setAlias(tblName, tblRef.getExplicitAlias());
            }
        }
        jobInfo.retainOlapTables(olapTableNames);
        jobInfo.retainView(viewNames);
        jobInfo.retainOdbcTables(odbcTableNames);
    }



    public void checkAndFilterRestoreOlapTableExistInSnapshot(Map<String, BackupOlapTableInfo> backupOlapTableInfoMap,
                                                              TableRef tableRef) throws DdlException {
        String tblName = tableRef.getName().getTbl();
        BackupOlapTableInfo tblInfo = backupOlapTableInfoMap.get(tblName);
        PartitionNames partitionNames = tableRef.getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.isTemp()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Do not support restoring temporary partitions");
            }
            // check the selected partitions
            for (String partName : partitionNames.getPartitionNames()) {
                if (!tblInfo.containsPart(partName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Partition " + partName + " of table " + tblName
                                    + " does not exist in snapshot");
                }
            }
        }
        // only retain restore partitions
        tblInfo.retainPartitions(partitionNames == null ? null : partitionNames.getPartitionNames());
    }

    public void cancel(CancelBackupStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);

        AbstractJob job = getCurrentJob(db.getId());
        if (job == null || (job instanceof BackupJob && stmt.isRestore())
                || (job instanceof RestoreJob && !stmt.isRestore())) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "No "
                    + (stmt.isRestore() ? "restore" : "backup" + " job")
                    + " is currently running");
        }

        Status status = job.cancel();
        if (!status.ok()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Failed to cancel job: " + status.getErrMsg());
        }

        LOG.info("finished to cancel {} job: {}", (stmt.isRestore() ? "restore" : "backup"), job);
    }

    public boolean handleFinishedSnapshotTask(SnapshotTask task, TFinishTaskRequest request) {
        AbstractJob job = getCurrentJob(task.getDbId());

        if (job == null) {
            LOG.warn("failed to find backup or restore job for task: {}", task);
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
        AbstractJob job = getCurrentJob(task.getDbId());
        if (job == null || (job instanceof RestoreJob)) {
            LOG.info("invalid upload task: {}, no backup job is found. db id: {}", task, task.getDbId());
            return false;
        }
        BackupJob restoreJob = (BackupJob) job;
        if (restoreJob.getJobId() != task.getJobId() || restoreJob.getState() != BackupJobState.UPLOADING) {
            LOG.info("invalid upload task: {}, job id: {}, job state: {}",
                     task, restoreJob.getJobId(), restoreJob.getState().name());
            return false;
        }
        return restoreJob.finishSnapshotUploadTask(task, request);
    }

    public boolean handleDownloadSnapshotTask(DownloadTask task, TFinishTaskRequest request) {
        AbstractJob job = getCurrentJob(task.getDbId());
        if (!(job instanceof RestoreJob)) {
            LOG.warn("failed to find restore job for task: {}", task);
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        return ((RestoreJob) job).finishTabletDownloadTask(task, request);
    }

    public boolean handleDirMoveTask(DirMoveTask task, TFinishTaskRequest request) {
        AbstractJob job = getCurrentJob(task.getDbId());
        if (!(job instanceof RestoreJob)) {
            LOG.warn("failed to find restore job for task: {}", task);
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

        addBackupOrRestoreJob(job.getDbId(), job);
    }

    public boolean report(TTaskType type, long jobId, long taskId, int finishedNum, int totalNum) {
        for (AbstractJob job : getAllCurrentJobs()) {
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

    public void addSnapshot(String labelName, Snapshot snapshot) {
        localSnapshotsLock.writeLock().lock();
        try {
            localSnapshots.put(labelName, snapshot);
        } finally {
            localSnapshotsLock.writeLock().unlock();
        }
    }

    public Snapshot getSnapshot(String labelName) {
        localSnapshotsLock.readLock().lock();
        try {
            return localSnapshots.get(labelName);
        } finally {
            localSnapshotsLock.readLock().unlock();
        }
    }

    public static BackupHandler read(DataInput in) throws IOException {
        BackupHandler backupHandler = new BackupHandler();
        backupHandler.readFields(in);
        return backupHandler;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        repoMgr.write(out);

        List<AbstractJob> jobs = dbIdToBackupOrRestoreJobs.values()
                .stream().flatMap(Deque::stream).collect(Collectors.toList());
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
            addBackupOrRestoreJob(job.getDbId(), job);
        }
    }
}
