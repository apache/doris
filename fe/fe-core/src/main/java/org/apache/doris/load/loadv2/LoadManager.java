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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.CancelLoadStmt;
import org.apache.doris.analysis.CleanLabelStmt;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DataQualityException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.CleanLabelOperationLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The broker and mini load jobs(v2) are included in this class.
 * The lock sequence:
 * Database.lock
 * LoadManager.lock
 * LoadJob.lock
 */
public class LoadManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(LoadManager.class);

    protected Map<Long, LoadJob> idToLoadJob = Maps.newConcurrentMap();
    protected Map<Long, Map<String, List<LoadJob>>> dbIdToLabelToLoadJobs = Maps.newConcurrentMap();
    protected LoadJobScheduler loadJobScheduler;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private MysqlLoadManager mysqlLoadManager;
    private TokenManager tokenManager;

    public LoadManager(LoadJobScheduler loadJobScheduler) {
        this.loadJobScheduler = loadJobScheduler;
        this.tokenManager = new TokenManager();
        this.mysqlLoadManager = new MysqlLoadManager(tokenManager);
    }

    public void start() {
        tokenManager.start();
        mysqlLoadManager.start();
    }

    /**
     * This method will be invoked by the broker load(v2) now.
     */
    public long createLoadJobFromStmt(LoadStmt stmt) throws DdlException, UserException {
        Database database = checkDb(stmt.getLabel().getDbName());
        long dbId = database.getId();
        LoadJob loadJob;
        writeLock();
        try {
            if (stmt.getBrokerDesc() != null && stmt.getBrokerDesc().isMultiLoadBroker()) {
                if (!Env.getCurrentEnv().getLoadInstance()
                        .isUncommittedLabel(dbId, stmt.getLabel().getLabelName())) {
                    throw new DdlException("label: " + stmt.getLabel().getLabelName() + " not found!");
                }
            } else {
                checkLabelUsed(dbId, stmt.getLabel().getLabelName());
                if (stmt.getBrokerDesc() == null && stmt.getResourceDesc() == null) {
                    throw new DdlException("LoadManager only support the broker and spark load.");
                }
                if (unprotectedGetUnfinishedJobNum() >= Config.desired_max_waiting_jobs) {
                    throw new DdlException(
                            "There are more than " + Config.desired_max_waiting_jobs
                                    + " unfinished load jobs, please retry later. "
                                    + "You can use `SHOW LOAD` to view submitted jobs");
                }
            }

            loadJob = BulkLoadJob.fromLoadStmt(stmt);
            createLoadJob(loadJob);
        } finally {
            writeUnlock();
        }

        if (Config.enable_workload_group) {
            loadJob.settWorkloadGroups(
                    Env.getCurrentEnv().getWorkloadGroupMgr().getWorkloadGroup(ConnectContext.get()));
        }

        Env.getCurrentEnv().getEditLog().logCreateLoadJob(loadJob);

        // The job must be submitted after edit log.
        // It guarantee that load job has not been changed before edit log.
        loadJobScheduler.submitJob(loadJob);
        return loadJob.getId();
    }

    private long unprotectedGetUnfinishedJobNum() {
        return idToLoadJob.values().stream()
                .filter(j -> (j.getState() != JobState.FINISHED && j.getState() != JobState.CANCELLED)).count();
    }

    /**
     * MultiLoadMgr use.
     **/
    public void createLoadJobV1FromMultiStart(String fullDbName, String label) throws DdlException {
        Database database = checkDb(fullDbName);
        writeLock();
        try {
            checkLabelUsed(database.getId(), label);
            Env.getCurrentEnv().getLoadInstance()
                    .registerMiniLabel(fullDbName, label, System.currentTimeMillis());
        } finally {
            writeUnlock();
        }
    }

    public MysqlLoadManager getMysqlLoadManager() {
        return mysqlLoadManager;
    }

    public TokenManager getTokenManager() {
        return tokenManager;
    }

    public void replayCreateLoadJob(LoadJob loadJob) {
        createLoadJob(loadJob);
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId()).add("msg", "replay create load job").build());
    }

    // add load job and also add to callback factory
    protected void createLoadJob(LoadJob loadJob) {
        if (loadJob.isExpired(System.currentTimeMillis())) {
            // This can happen in replay logic.
            return;
        }
        addLoadJob(loadJob);
        // add callback before txn if load job is uncompleted,
        // because callback will be performed on replay without txn begin
        // register txn state listener
        if (!loadJob.isCompleted()) {
            Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(loadJob);
        }
    }

    private void addLoadJob(LoadJob loadJob) {
        idToLoadJob.put(loadJob.getId(), loadJob);
        long dbId = loadJob.getDbId();
        if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
            dbIdToLabelToLoadJobs.put(loadJob.getDbId(), new ConcurrentHashMap<>());
        }
        Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
        if (!labelToLoadJobs.containsKey(loadJob.getLabel())) {
            labelToLoadJobs.put(loadJob.getLabel(), new ArrayList<>());
        }
        labelToLoadJobs.get(loadJob.getLabel()).add(loadJob);
    }

    /**
     * Record finished load job by editLog.
     **/
    public void recordFinishedLoadJob(String label, long transactionId, String dbName, long tableId, EtlJobType jobType,
                                      long createTimestamp, String failMsg, String trackingUrl,
                                      UserIdentity userInfo, long jobId) throws MetaNotFoundException {

        // get db id
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbName);

        LoadJob loadJob;
        switch (jobType) {
            case INSERT:
                loadJob = new InsertLoadJob(label, transactionId, db.getId(), tableId, createTimestamp, failMsg,
                        trackingUrl, userInfo);
                break;
            case INSERT_JOB:
                loadJob = new InsertLoadJob(label, transactionId, db.getId(), tableId, createTimestamp, failMsg,
                        trackingUrl, userInfo, jobId);
                break;
            default:
                return;
        }
        addLoadJob(loadJob);
        // persistent
        Env.getCurrentEnv().getEditLog().logCreateLoadJob(loadJob);
    }

    /**
     * Match need cancel loadJob by stmt.
     **/
    @VisibleForTesting
    public static void addNeedCancelLoadJob(CancelLoadStmt stmt, List<LoadJob> loadJobs, List<LoadJob> matchLoadJobs)
            throws AnalysisException {
        String label = stmt.getLabel();
        String state = stmt.getState();
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(label,
                CaseSensibility.LABEL.getCaseSensibility());
        matchLoadJobs.addAll(
                loadJobs.stream()
                        .filter(job -> job.getState() != JobState.CANCELLED)
                        .filter(job -> {
                            if (stmt.getOperator() != null) {
                                // compound
                                boolean labelFilter =
                                        label.contains("%") ? matcher.match(job.getLabel())
                                                : job.getLabel().equalsIgnoreCase(label);
                                boolean stateFilter = job.getState().name().equalsIgnoreCase(state);
                                return Operator.AND.equals(stmt.getOperator()) ? labelFilter && stateFilter :
                                        labelFilter || stateFilter;
                            }
                            if (StringUtils.isNotEmpty(label)) {
                                return label.contains("%") ? matcher.match(job.getLabel())
                                        : job.getLabel().equalsIgnoreCase(label);
                            }
                            if (StringUtils.isNotEmpty(state)) {
                                return job.getState().name().equalsIgnoreCase(state);
                            }
                            return false;
                        }).collect(Collectors.toList())
        );
    }

    /**
     * Cancel load job by stmt.
     **/
    public void cancelLoadJob(CancelLoadStmt stmt) throws DdlException, AnalysisException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(stmt.getDbName());
        // List of load jobs waiting to be cancelled
        List<LoadJob> unfinishedLoadJob;
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("Load job does not exist");
            }
            List<LoadJob> matchLoadJobs = Lists.newArrayList();
            addNeedCancelLoadJob(stmt,
                    labelToLoadJobs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                    matchLoadJobs);
            if (matchLoadJobs.isEmpty()) {
                throw new DdlException("Load job does not exist");
            }
            // check state here
            unfinishedLoadJob =
                    matchLoadJobs.stream().filter(entity -> !entity.isTxnDone()).collect(Collectors.toList());
            if (unfinishedLoadJob.isEmpty()) {
                throw new DdlException("There is no uncompleted job");
            }
        } finally {
            readUnlock();
        }
        for (LoadJob loadJob : unfinishedLoadJob) {
            try {
                loadJob.cancelJob(new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel"));
            } catch (DdlException e) {
                throw new DdlException(
                        "Cancel load job [" + loadJob.getId() + "] fail, " + "label=[" + loadJob.getLabel()
                                +
                                "] failed msg=" + e.getMessage());
            }
        }
    }

    /**
     * Replay end load job.
     **/
    public void replayEndLoadJob(LoadJobFinalOperation operation) {
        LoadJob job = idToLoadJob.get(operation.getId());
        if (job == null) {
            // This should not happen.
            // Last time I found that when user submit a job with already used label, an END_LOAD_JOB edit log
            // will be wrote but the job is not added to 'idToLoadJob', so this job here we got will be null.
            // And this bug has been fixed.
            // Just add a log here to observe.
            LOG.warn("job does not exist when replaying end load job edit log: {}", operation);
            return;
        }
        job.unprotectReadEndOperation(operation);
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, operation.getId()).add("operation", operation)
                .add("msg", "replay end load job").build());

        // When idToLoadJob size increase 10000 roughly, we run removeOldLoadJob to reduce mem used
        if ((idToLoadJob.size() > 0) && (idToLoadJob.size() % 10000 == 0)) {
            removeOldLoadJob();
        }
    }

    /**
     * Replay update load job.
     **/
    public void replayUpdateLoadJobStateInfo(LoadJob.LoadJobStateUpdateInfo info) {
        long jobId = info.getJobId();
        LoadJob job = idToLoadJob.get(jobId);
        if (job == null) {
            LOG.warn("replay update load job state failed. error: job not found, id: {}", jobId);
            return;
        }

        job.replayUpdateStateInfo(info);
    }

    /**
     * Get load job num, used by proc.
     **/
    public int getLoadJobNum(JobState jobState, long dbId) {
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs == null) {
                return 0;
            }
            List<LoadJob> loadJobList =
                    labelToLoadJobs.values().stream().flatMap(entity -> entity.stream()).collect(Collectors.toList());
            return (int) loadJobList.stream().filter(entity -> entity.getState() == jobState).count();
        } finally {
            readUnlock();
        }
    }

    /**
     * Get load job num, used by proc.
     **/
    public int getLoadJobNum(JobState jobState) {
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = new HashMap<>();
            for (Long dbId : dbIdToLabelToLoadJobs.keySet()) {
                if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                        InternalCatalog.INTERNAL_CATALOG_NAME,
                        Env.getCurrentEnv().getCatalogMgr().getDbNullable(dbId).getFullName(),
                        PrivPredicate.LOAD)) {
                    continue;
                }

                labelToLoadJobs.putAll(dbIdToLabelToLoadJobs.get(dbId));
            }

            List<LoadJob> loadJobList =
                    labelToLoadJobs.values().stream().flatMap(entity -> entity.stream()).collect(Collectors.toList());
            return (int) loadJobList.stream().filter(entity -> entity.getState() == jobState).count();
        } finally {
            readUnlock();
        }
    }

    /**
     * Get load job num, used by metric.
     **/
    public Map<Pair<EtlJobType, JobState>, Long> getLoadJobNum() {
        return idToLoadJob.values().stream().collect(Collectors.groupingBy(
                loadJob -> Pair.of(loadJob.getJobType(), loadJob.getState()),
                Collectors.counting()));
    }

    /**
     * Remove old load job.
     **/
    public void removeOldLoadJob() {
        long currentTimeMs = System.currentTimeMillis();
        removeLoadJobIf(job -> job.isExpired(currentTimeMs));
    }

    /**
     * Remove completed jobs if total job num exceed Config.label_num_threshold
     */
    public void removeOverLimitLoadJob() {
        if (Config.label_num_threshold < 0 || idToLoadJob.size() <= Config.label_num_threshold) {
            return;
        }
        writeLock();
        try {
            Deque<LoadJob> finishedJobs = idToLoadJob
                    .values()
                    .stream()
                    .filter(LoadJob::isCompleted)
                    .sorted(Comparator.comparingLong(o -> o.finishTimestamp))
                    .collect(Collectors.toCollection(ArrayDeque::new));
            while (!finishedJobs.isEmpty()
                    && idToLoadJob.size() > Config.label_num_threshold) {
                LoadJob loadJob = finishedJobs.pollFirst();
                idToLoadJob.remove(loadJob.getId());
                jobRemovedTrigger(loadJob);
            }
        } finally {
            writeUnlock();
        }
    }

    private void jobRemovedTrigger(LoadJob job) {
        if (job instanceof SparkLoadJob) {
            ((SparkLoadJob) job).clearSparkLauncherLog();
        }
        if (job instanceof BulkLoadJob) {
            ((BulkLoadJob) job).recycleProgress();
        }
        Map<String, List<LoadJob>> map = dbIdToLabelToLoadJobs.get(job.getDbId());
        if (map == null) {
            return;
        }
        List<LoadJob> list = map.get(job.getLabel());
        if (list == null) {
            return;
        }
        list.remove(job);
        if (list.isEmpty()) {
            map.remove(job.getLabel());
        }
        if (map.isEmpty()) {
            dbIdToLabelToLoadJobs.remove(job.getDbId());
        }
    }

    private void removeLoadJobIf(Predicate<LoadJob> pred) {
        long removeJobNum = 0;
        StopWatch stopWatch = StopWatch.createStarted();
        writeLock();
        try {
            Iterator<Map.Entry<Long, LoadJob>> iter = idToLoadJob.entrySet().iterator();
            while (iter.hasNext()) {
                LoadJob job = iter.next().getValue();
                if (pred.test(job)) {
                    iter.remove();
                    jobRemovedTrigger(job);
                    removeJobNum++;
                }
            }
        } finally {
            writeUnlock();
            stopWatch.stop();
            LOG.info("end to removeOldLoadJob, removeJobNum:{} cost:{} ms",
                    removeJobNum, stopWatch.getTime());
        }
    }

    /**
     * Only for those jobs which have etl state, like SparkLoadJob.
     **/
    public void processEtlStateJobs() {
        idToLoadJob.values().stream().filter(job -> (job.jobType == EtlJobType.SPARK && job.state == JobState.ETL))
                .forEach(job -> {
                    try {
                        ((SparkLoadJob) job).updateEtlStatus();
                    } catch (DataQualityException e) {
                        LOG.info("update load job etl status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED,
                                DataQualityException.QUALITY_FAIL_MSG), true, true);
                    } catch (UserException e) {
                        LOG.warn("update load job etl status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
                    } catch (Exception e) {
                        LOG.warn("update load job etl status failed. job id: {}", job.getId(), e);
                    }
                });
    }

    /**
     * Only for those jobs which load by PushTask.
     **/
    public void processLoadingStateJobs() {
        idToLoadJob.values().stream().filter(job -> (job.jobType == EtlJobType.SPARK && job.state == JobState.LOADING))
                .forEach(job -> {
                    try {
                        ((SparkLoadJob) job).updateLoadingStatus();
                    } catch (UserException e) {
                        LOG.warn("update load job loading status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(CancelType.LOAD_RUN_FAIL, e.getMessage()), true, true);
                    } catch (Exception e) {
                        LOG.warn("update load job loading status failed. job id: {}", job.getId(), e);
                    }
                });
    }

    public List<Pair<Long, String>> getCreateLoadStmt(long dbId, String label) throws DdlException {
        List<Pair<Long, String>> result = new ArrayList<>();
        readLock();
        try {
            if (dbIdToLabelToLoadJobs.containsKey(dbId)) {
                Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
                if (labelToLoadJobs.containsKey(label)) {
                    List<LoadJob> labelLoadJobs = labelToLoadJobs.get(label);
                    for (LoadJob job : labelLoadJobs) {
                        try {
                            Method getOriginStmt = job.getClass().getMethod("getOriginStmt");
                            if (getOriginStmt != null) {
                                result.add(
                                        Pair.of(job.getId(), ((OriginStatement) getOriginStmt.invoke(job)).originStmt));
                            } else {
                                throw new DdlException("Not support load job type: " + job.getClass().getName());
                            }
                        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                            throw new DdlException("Not support load job type: " + job.getClass().getName());
                        }
                    }
                } else {
                    throw new DdlException("Label does not exist: " + label);
                }
            } else {
                throw new DdlException("Database does not exist");
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    /**
     * This method will return the jobs info which can meet the condition of input param.
     *
     * @param dbId          used to filter jobs which belong to this db
     * @param labelValue    used to filter jobs which's label is or like labelValue.
     * @param accurateMatch true: filter jobs which's label is labelValue. false: filter jobs which's label like itself.
     * @param statesValue   used to filter jobs which's state within the statesValue set.
     * @return The result is the list of jobInfo.
     *         JobInfo is a list which includes the comparable object: jobId, label, state etc.
     *         The result is unordered.
     */
    public List<List<Comparable>> getLoadJobInfosByDb(long dbId, String labelValue, boolean accurateMatch,
                                                      Set<String> statesValue) throws AnalysisException {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<List<Comparable>>();
        if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
            return loadJobInfos;
        }

        Set<JobState> states = Sets.newHashSet();
        if (statesValue == null || statesValue.size() == 0) {
            states.addAll(EnumSet.allOf(JobState.class));
        } else {
            for (String stateValue : statesValue) {
                try {
                    states.add(JobState.valueOf(stateValue));
                } catch (IllegalArgumentException e) {
                    // ignore this state
                }
            }
        }

        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            List<LoadJob> loadJobList = Lists.newArrayList();
            if (Strings.isNullOrEmpty(labelValue)) {
                loadJobList.addAll(
                        labelToLoadJobs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
            } else {
                // check label value
                if (accurateMatch) {
                    if (!labelToLoadJobs.containsKey(labelValue)) {
                        return loadJobInfos;
                    }
                    loadJobList.addAll(labelToLoadJobs.get(labelValue));
                } else {
                    // non-accurate match
                    PatternMatcher matcher =
                            PatternMatcherWrapper.createMysqlPattern(labelValue,
                                    CaseSensibility.LABEL.getCaseSensibility());
                    for (Map.Entry<String, List<LoadJob>> entry : labelToLoadJobs.entrySet()) {
                        if (matcher.match(entry.getKey())) {
                            loadJobList.addAll(entry.getValue());
                        }
                    }
                }
            }

            // check state
            for (LoadJob loadJob : loadJobList) {
                try {
                    if (!states.contains(loadJob.getState())) {
                        continue;
                    }
                    // check auth
                    try {
                        checkJobAuth(loadJob.getDb().getCatalog().getName(), loadJob.getDb().getName(),
                                loadJob.getTableNames());
                    } catch (AnalysisException e) {
                        continue;
                    }
                    // add load job info
                    loadJobInfos.add(loadJob.getShowInfo());
                } catch (RuntimeException | DdlException | MetaNotFoundException e) {
                    // ignore this load job
                    LOG.warn("get load job info failed. job id: {}", loadJob.getId(), e);
                }
            }
            return loadJobInfos;
        } finally {
            readUnlock();
        }
    }

    public void checkJobAuth(String ctlName, String dbName, Set<String> tableNames) throws AnalysisException {
        if (tableNames.isEmpty()) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), ctlName, dbName,
                            PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR,
                        PrivPredicate.LOAD.getPrivs().toString(), dbName);
            }
        } else {
            for (String tblName : tableNames) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), ctlName, dbName,
                                tblName, PrivPredicate.LOAD)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                            PrivPredicate.LOAD.getPrivs().toString(), tblName);
                    return;
                }
            }
        }
    }

    public List<List<Comparable>> getAllLoadJobInfos() {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<List<Comparable>>();

        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = new HashMap<>();
            for (Long dbId : dbIdToLabelToLoadJobs.keySet()) {
                if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                        InternalCatalog.INTERNAL_CATALOG_NAME,
                        Env.getCurrentEnv().getCatalogMgr().getDbNullable(dbId).getFullName(),
                        PrivPredicate.LOAD)) {
                    continue;
                }

                labelToLoadJobs.putAll(dbIdToLabelToLoadJobs.get(dbId));
            }
            List<LoadJob> loadJobList = Lists.newArrayList();
            loadJobList.addAll(
                    labelToLoadJobs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));

            // check state
            for (LoadJob loadJob : loadJobList) {
                try {
                    // add load job info
                    loadJobInfos.add(loadJob.getShowInfo());
                } catch (DdlException e) {
                    continue;
                }
            }
            return loadJobInfos;
        } finally {
            readUnlock();
        }
    }

    /**
     * Get load job info.
     **/
    public void getLoadJobInfo(Load.JobInfo info) throws DdlException {
        String fullDbName = info.dbName;
        info.dbName = fullDbName;
        Database database = checkDb(info.dbName);
        readLock();
        try {
            // find the latest load job by info
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(database.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("No jobs belong to database(" + info.dbName + ")");
            }
            List<LoadJob> loadJobList = labelToLoadJobs.get(info.label);
            if (loadJobList == null || loadJobList.isEmpty()) {
                throw new DdlException("Unknown job(" + info.label + ")");
            }

            LoadJob loadJob = loadJobList.get(loadJobList.size() - 1);
            loadJob.getJobInfo(info);
        } finally {
            readUnlock();
        }
    }

    public LoadJob getLoadJob(long jobId) {
        return idToLoadJob.get(jobId);
    }

    public List<LoadJob> queryLoadJobsByJobIds(List<Long> jobIds) {
        if (CollectionUtils.isEmpty(jobIds)) {
            return new ArrayList<>();
        }
        List<LoadJob> jobs = new ArrayList<>();
        jobIds.forEach(id -> {
            if (null != idToLoadJob.get(id)) {
                jobs.add(idToLoadJob.get(id));
            }
        });
        return jobs;
    }

    public void prepareJobs() {
        analyzeLoadJobs();
        submitJobs();
    }

    private void submitJobs() {
        loadJobScheduler.submitJob(idToLoadJob.values().stream().filter(loadJob -> loadJob.state == JobState.PENDING)
                .collect(Collectors.toList()));
    }

    private void analyzeLoadJobs() {
        for (LoadJob loadJob : idToLoadJob.values()) {
            if (loadJob.getState() == JobState.PENDING) {
                loadJob.analyze();
            }
        }
    }

    protected Database checkDb(String dbName) throws DdlException {
        return Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
    }

    /**
     * step1: if label has been used in old load jobs which belong to load class.
     * step2: if label has been used in v2 load jobs.
     * step2.1: if label has been user in v2 load jobs, the create timestamp will be checked.
     *
     * @throws LabelAlreadyUsedException throw exception when label has been used by an unfinished job.
     */
    private void checkLabelUsed(long dbId, String label) throws DdlException {
        // if label has been used in old load jobs
        Env.getCurrentEnv().getLoadInstance().isLabelUsed(dbId, label);
        // if label has been used in v2 of load jobs
        if (dbIdToLabelToLoadJobs.containsKey(dbId)) {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs.containsKey(label)) {
                List<LoadJob> labelLoadJobs = labelToLoadJobs.get(label);
                Optional<LoadJob> loadJobOptional = labelLoadJobs.stream()
                        .filter(entity -> entity.getState() != JobState.CANCELLED).findFirst();
                if (loadJobOptional.isPresent()) {
                    LOG.warn("Failed to add load job when label {} has been used.", label);
                    throw new LabelAlreadyUsedException(label);
                }
            }
        }
    }

    public void cleanLabel(CleanLabelStmt stmt) throws DdlException {
        String dbName = stmt.getDb();
        String label = stmt.getLabel();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        cleanLabelInternal(db.getId(), label, false);
    }

    public void replayCleanLabel(CleanLabelOperationLog log) {
        cleanLabelInternal(log.getDbId(), log.getLabel(), true);
    }

    /**
     * Clean the label with given database and label
     * It will only remove the load jobs which are already done.
     * 1. Remove from LoadManager
     * 2. Remove from DatabaseTransactionMgr
     *
     * @param dbId
     * @param label
     * @param isReplay
     */
    private void cleanLabelInternal(long dbId, String label, boolean isReplay) {
        // 1. Remove from LoadManager
        int counter = 0;
        writeLock();
        try {
            if (dbIdToLabelToLoadJobs.containsKey(dbId)) {
                Map<String, List<LoadJob>> labelToJob = dbIdToLabelToLoadJobs.get(dbId);
                if (Strings.isNullOrEmpty(label)) {
                    // clean all labels in this db
                    Iterator<Map.Entry<String, List<LoadJob>>> iter = labelToJob.entrySet().iterator();
                    while (iter.hasNext()) {
                        List<LoadJob> jobs = iter.next().getValue();
                        Iterator<LoadJob> innerIter = jobs.iterator();
                        while (innerIter.hasNext()) {
                            LoadJob job = innerIter.next();
                            if (!job.isCompleted()) {
                                continue;
                            }
                            if (job instanceof BulkLoadJob) {
                                ((BulkLoadJob) job).recycleProgress();
                            }
                            innerIter.remove();
                            idToLoadJob.remove(job.getId());
                            ++counter;
                        }
                        if (jobs.isEmpty()) {
                            iter.remove();
                        }
                    }
                } else {
                    List<LoadJob> jobs = labelToJob.get(label);
                    if (jobs != null) {
                        // stream load labelToJob is null
                        Iterator<LoadJob> iter = jobs.iterator();
                        while (iter.hasNext()) {
                            LoadJob job = iter.next();
                            if (!job.isCompleted()) {
                                continue;
                            }
                            if (job instanceof BulkLoadJob) {
                                ((BulkLoadJob) job).recycleProgress();
                            }
                            iter.remove();
                            idToLoadJob.remove(job.getId());
                            ++counter;
                        }
                        if (jobs.isEmpty()) {
                            labelToJob.remove(label);
                        }
                    }
                }
            }
        } finally {
            writeUnlock();
        }
        // 2. Remove from DatabaseTransactionMgr
        try {
            Env.getCurrentGlobalTransactionMgr().cleanLabel(dbId, label, isReplay);
        } catch (Exception e) {
            // just ignore, because we don't want to throw any exception here.
            LOG.warn("Exception:", e);
        }

        LOG.info("finished to clean {} labels on db {} with label '{}' in load mgr. is replay: {}",
                counter, dbId, label, isReplay);
    }

    protected void readLock() {
        lock.readLock().lock();
    }

    protected void readUnlock() {
        lock.readLock().unlock();
    }

    protected void writeLock() {
        lock.writeLock().lock();
    }

    protected void writeUnlock() {
        lock.writeLock().unlock();
    }

    /**
     * Init.
     **/
    public void initJobProgress(Long jobId, TUniqueId loadId, Set<TUniqueId> fragmentIds,
                                List<Long> relatedBackendIds) {
        LoadJob job = idToLoadJob.get(jobId);
        if (job != null) {
            job.initLoadProgress(loadId, fragmentIds, relatedBackendIds);
        }
    }

    /**
     * Update.
     **/
    public void updateJobProgress(Long jobId, Long beId, TUniqueId loadId, TUniqueId fragmentId, long scannedRows,
                                  long scannedBytes, boolean isDone) {
        LoadJob job = idToLoadJob.get(jobId);
        if (job != null) {
            job.updateProgress(beId, loadId, fragmentId, scannedRows, scannedBytes, isDone);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        long currentTimeMs = System.currentTimeMillis();
        List<LoadJob> loadJobs =
                idToLoadJob.values().stream().filter(t -> !t.isExpired(currentTimeMs))
                        .filter(t -> !(t instanceof MiniLoadJob)).collect(Collectors.toList());

        LOG.info("write load job size: {}", loadJobs.size());
        out.writeInt(loadJobs.size());
        for (LoadJob loadJob : loadJobs) {
            LOG.info("write load job: {}", loadJob.getId());
            loadJob.write(out);
        }
    }

    /**
     * Read from file.
     **/
    public void readFields(DataInput in) throws IOException {
        long currentTimeMs = System.currentTimeMillis();
        int size = in.readInt();
        LOG.info("load job num {} ", size);
        for (int i = 0; i < size; i++) {
            LoadJob loadJob = LoadJob.read(in);
            if (loadJob.isExpired(currentTimeMs)) {
                continue;
            }

            if (loadJob.getJobType() == EtlJobType.MINI) {
                LOG.warn("skip mini load job {} in db {} as it is no longer supported", loadJob.getId(),
                        loadJob.getDbId());
                continue;
            }
            idToLoadJob.put(loadJob.getId(), loadJob);
            Map<String, List<LoadJob>> map = dbIdToLabelToLoadJobs.get(loadJob.getDbId());
            if (map == null) {
                map = Maps.newConcurrentMap();
                dbIdToLabelToLoadJobs.put(loadJob.getDbId(), map);
            }

            List<LoadJob> jobs = map.get(loadJob.getLabel());
            if (jobs == null) {
                jobs = Lists.newArrayList();
                map.put(loadJob.getLabel(), jobs);
            }
            jobs.add(loadJob);
            // The callback of load job which is replayed by image need to be registered in callback factory.
            // The commit and visible txn will callback the unfinished load job.
            // Otherwise, the load job always does not be completed while the txn is visible.
            if (!loadJob.isCompleted()) {
                Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(loadJob);
            }
        }
    }

    // ------------------------ for load refactor ------------------------
    public long createLoadJobFromStmt(InsertStmt insertStmt) throws DdlException {
        Database database = checkDb(insertStmt.getLoadLabel().getDbName());
        long dbId = database.getId();
        LoadJob loadJob;
        writeLock();
        BrokerDesc brokerDesc = (BrokerDesc) insertStmt.getResourceDesc();
        try {
            if (brokerDesc != null && brokerDesc.isMultiLoadBroker()) {
                if (!Env.getCurrentEnv().getLoadInstance()
                        .isUncommittedLabel(dbId, insertStmt.getLoadLabel().getLabelName())) {
                    throw new DdlException("label: " + insertStmt.getLoadLabel().getLabelName() + " not found!");
                }
            } else {
                checkLabelUsed(dbId, insertStmt.getLoadLabel().getLabelName());
                if (brokerDesc == null && insertStmt.getResourceDesc() == null) {
                    throw new DdlException("LoadManager only support the broker and spark load.");
                }
                if (unprotectedGetUnfinishedJobNum() >= Config.desired_max_waiting_jobs) {
                    throw new DdlException(
                            "There are more than " + Config.desired_max_waiting_jobs
                                    + " unfinished load jobs, please retry later. "
                                    + "You can use `SHOW LOAD` to view submitted jobs");
                }
            }

            loadJob = BulkLoadJob.fromInsertStmt(insertStmt);
            createLoadJob(loadJob);
        } finally {
            writeUnlock();
        }
        Env.getCurrentEnv().getEditLog().logCreateLoadJob(loadJob);

        // The job must be submitted after edit log.
        // It guarantee that load job has not been changed before edit log.
        loadJobScheduler.submitJob(loadJob);
        return loadJob.getId();
    }
}
