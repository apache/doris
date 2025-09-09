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

package org.apache.doris.job.manager;

import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.job.scheduler.JobScheduler;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Log4j2
public class JobManager<T extends AbstractJob<?, C>, C> implements Writable {
    private static final Logger LOG = LogManager.getLogger(JobManager.class);

    private final ConcurrentHashMap<Long, T> jobMap = new ConcurrentHashMap<>(32);

    private JobScheduler<T, C> jobScheduler;

    // lock for job
    // lock is private and must use after db lock
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void start() {
        jobScheduler = new JobScheduler<T, C>(jobMap);
        jobScheduler.start();
    }


    /**
     * get running job
     *
     * @param jobId id
     * @return running job
     */
    public T getJob(long jobId) {
        return jobMap.get(jobId);
    }

    public void registerJob(T job) throws JobException {
        job.initParams();
        createJobInternal(job, false);
        jobScheduler.scheduleOneJob(job);
    }

    public void createJobInternal(T job, boolean isReplay) throws JobException {
        writeLock();
        try {
            if (!isReplay) {
                job.onRegister();
                job.checkJobParams();
                checkJobNameExist(job.getJobName());
                if (jobMap.get(job.getJobId()) != null) {
                    throw new JobException("job id exist, jobId:" + job.getJobId());
                }
            }
            jobMap.put(job.getJobId(), job);
            if (isReplay) {
                job.onReplayCreate();
            }
            if (!isReplay && job.needPersist()) {
                job.logCreateOperation();
            }
        } finally {
            writeUnlock();
        }
        if (!isReplay) {
            jobScheduler.cycleTimerJobScheduler(job);
        }
    }

    private void checkJobNameExist(String jobName) throws JobException {
        if (jobMap.values().stream().anyMatch(a -> a.getJobName().equals(jobName))) {
            throw new JobException("job name exist, jobName:" + jobName);
        }
    }

    /**
     * unregister job by job id,this method will delete job from job map
     * we need to check job status, if job status is running, we need to stop it
     * and cancel all running task
     */
    public void unregisterJob(Long jobId) throws JobException {
        checkJobExist(jobId);
        T dropJob = jobMap.get(jobId);
        dropJob(dropJob, dropJob.getJobName());
    }

    /**
     * unregister job by job name,this method will delete job from job map
     *
     * @param jobName  job name
     * @param ifExists is is true, if job not exist,we will ignore job not exist exception, else throw exception
     */
    public void unregisterJob(String jobName, boolean ifExists) throws JobException {
        try {
            T dropJob = null;
            for (T job : jobMap.values()) {
                if (job.getJobName().equals(jobName)) {
                    dropJob = job;
                }
            }
            if (dropJob == null && ifExists) {
                return;
            }
            dropJob(dropJob, jobName);
        } catch (Exception e) {
            log.error("drop job error, jobName:" + jobName, e);
            throw new JobException("unregister job error, jobName:" + jobName);
        }
    }

    private void dropJob(T dropJob, String jobName) throws JobException {
        if (dropJob == null) {
            throw new JobException("job not exist, jobName:" + jobName);
        }
        dropJobInternal(dropJob, false);
    }

    public void dropJobInternal(T job, boolean isReplay) throws JobException {
        if (!isReplay) {
            // is job status is running, we need to stop it and cancel all running task
            // since job only running in master, we don't need to write update metadata log
            if (job.getJobStatus().equals(JobStatus.RUNNING)) {
                job.updateJobStatus(JobStatus.STOPPED);
            }
        }
        writeLock();
        try {
            jobMap.remove(job.getJobId());
            if (isReplay) {
                job.onReplayEnd(job);
            }
            // write delete log
            if (!isReplay && job.needPersist()) {
                job.logDeleteOperation();
            }
        } finally {
            writeUnlock();
        }
    }

    public void alterJobStatus(Long jobId, JobStatus status) throws JobException {
        checkJobExist(jobId);
        jobMap.get(jobId).updateJobStatus(status);
        if (status.equals(JobStatus.RUNNING)) {
            jobScheduler.cycleTimerJobScheduler(jobMap.get(jobId));
        }
        jobMap.get(jobId).logUpdateOperation();
    }

    public void alterJob(T job) {
        writeLock();
        try {
            jobMap.put(job.getJobId(), job);
            job.logUpdateOperation();
        } finally {
            writeUnlock();
        }
        log.info("update job success, jobId: {}", job.getJobId());
    }

    public void alterJobStatus(String jobName, JobStatus jobStatus) throws JobException {
        for (T a : jobMap.values()) {
            if (a.getJobName().equals(jobName)) {
                try {
                    if (jobStatus.equals(a.getJobStatus())) {
                        throw new JobException("Can't change job status to the same status");
                    }
                    alterJobStatus(a.getJobId(), jobStatus);
                } catch (JobException e) {
                    throw new JobException("Alter job status error, jobName is %s, errorMsg is %s",
                            jobName, e.getMessage());
                }
            }
        }
    }

    private void checkJobExist(Long jobId) throws JobException {
        if (null == jobMap.get(jobId)) {
            throw new JobException("job not exist, jobId:" + jobId);
        }
    }

    public List<T> queryJobs(JobType type) {
        return jobMap.values().stream().filter(a -> a.getJobType().equals(type))
                .collect(java.util.stream.Collectors.toList());
    }

    public List<T> queryJobs(JobType jobType, String jobName) {
        //only query insert job,we just provide insert job
        return jobMap.values().stream().filter(a -> checkItsMatch(jobType, jobName, a))
                .collect(Collectors.toList());
    }

    /**
     * query jobs by job type
     *
     * @param jobTypes @JobType
     * @return List<AbstractJob> job list
     */
    public List<T> queryJobs(List<JobType> jobTypes) {
        return jobMap.values().stream().filter(a -> checkItsMatch(jobTypes, a))
                .collect(Collectors.toList());
    }

    private boolean checkItsMatch(JobType jobType, String jobName, T job) {
        if (null == jobType) {
            throw new IllegalArgumentException("jobType cannot be null");
        }
        if (StringUtils.isBlank(jobName)) {
            return job.getJobType().equals(jobType);
        }
        return job.getJobType().equals(jobType) && job.getJobName().equals(jobName);
    }

    private boolean checkItsMatch(List<JobType> jobTypes, T job) {
        if (null == jobTypes) {
            throw new IllegalArgumentException("jobType cannot be null");
        }
        return jobTypes.contains(job.getJobType());
    }

    /**
     * Actively trigger job execution tasks, tasks type is manual
     *
     * @param jobId   job id
     * @param context Context parameter information required by some tasks executed this time
     */
    public void triggerJob(long jobId, C context) throws JobException {
        log.info("trigger job, job id is {}", jobId);
        checkJobExist(jobId);
        jobScheduler.schedulerInstantJob(jobMap.get(jobId), TaskType.MANUAL, context);
    }

    public void replayCreateJob(T job) throws JobException {
        // mtmv has its own editLog to replay jobs, here it is to ignore the logs already generated by older versions.
        if (!job.needPersist()) {
            return;
        }
        createJobInternal(job, true);
    }

    /**
     * Replay update load job.
     **/
    public void replayUpdateJob(T job) {
        Long jobId = job.getJobId();
        // In previous versions, the job ID in MTMV was not fixed (a new ID would be generated each time the editLog
        // was replayed), but the name was constant and unique. However, since job updates use jobId as the key,
        // it is possible that this jobId no longer exists. Therefore, we now look up the ID based on the name.
        if (!jobMap.containsKey(jobId) && job instanceof MTMVJob) {
            List<T> jobs = queryJobs(JobType.MV, job.getJobName());
            if (CollectionUtils.isEmpty(jobs) || jobs.size() != 1) {
                LOG.warn("jobs by name: {} not normal,should have one job,but job num is: {}", job.getJobName(),
                        jobs.size());
                return;
            }
            jobId = jobs.get(0).getJobId();
            job.setJobId(jobId);
        }

        if (!jobMap.containsKey(jobId)) {
            LOG.warn("replayUpdateJob not normal, job: {}, jobId: {}, jobMap: {}", job, jobId, jobMap);
            return;
        }
        jobMap.put(jobId, job);
        log.info(new LogBuilder(LogKey.SCHEDULER_JOB, jobId)
                .add("msg", "replay update scheduler job").build());
    }

    /**
     * Replay delete load job. we need to remove job from job map
     */
    public void replayDeleteJob(T replayJob) throws JobException {
        // mtmv has its own editLog to replay jobs, here it is to ignore the logs already generated by older version
        if (!replayJob.needPersist()) {
            return;
        }
        dropJobInternal(replayJob, true);
    }

    /**
     * Cancel task by task id, if task is running, cancel it
     * if job not exist, throw JobException exception job not exist
     * if task not exist, throw JobException exception task not exist
     *
     * @param jobName job name
     * @param taskId  task id
     */
    public void cancelTaskById(String jobName, Long taskId) throws JobException {
        for (T job : jobMap.values()) {
            if (job.getJobConfig().getExecuteType().equals(JobExecuteType.STREAMING)) {
                throw new JobException("streaming job not support cancel task by id");
            }
            if (job.getJobName().equals(jobName)) {
                job.cancelTaskById(taskId);
                job.logUpdateOperation();
                return;
            }
        }
        throw new JobException("job not exist, jobName:" + jobName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(jobMap.size());
        jobMap.forEach((jobId, job) -> {
            try {
                job.write(out);
            } catch (IOException e) {
                log.error("write job error, jobId:" + jobId, e);
            }
        });
    }

    /**
     * read job from data input, and init job
     *
     * @param in data input
     * @throws IOException io exception when read data input error
     */
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            AbstractJob job = AbstractJob.readFields(in);
            // for compatible
            if (job instanceof MTMVJob) {
                job.setJobId(((MTMVJob) job).getMtmvId());
            }
            jobMap.putIfAbsent(job.getJobId(), (T) job);
        }
    }

    public T getJob(Long jobId) {
        return jobMap.get(jobId);
    }

    public T getJobByName(String jobName) throws JobException {
        for (T a : jobMap.values()) {
            if (a.getJobName().equals(jobName)) {
                return a;
            }
        }
        throw new JobException("job not exist, jobName:" + jobName);
    }

    /**
     * get load info by db
     *
     * @param dbId          db id
     * @param dbName        db name
     * @param labelValue    label name
     * @param accurateMatch accurate match
     * @param jobState      state
     * @return load infos
     * @throws AnalysisException ex
     */
    public List<List<Comparable>> getLoadJobInfosByDb(long dbId, String dbName,
                                                      String labelValue,
                                                      boolean accurateMatch,
                                                      JobState jobState, String catalogName) throws AnalysisException {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<>();
        if (!Env.getCurrentEnv().getLabelProcessor().existJobs(dbId)) {
            return loadJobInfos;
        }
        readLock();
        try {
            List<InsertJob> loadJobList = Env.getCurrentEnv().getLabelProcessor()
                    .filterJobs(dbId, labelValue, accurateMatch);
            // check state
            for (InsertJob loadJob : loadJobList) {
                try {
                    if (jobState != null && !validState(jobState, loadJob)) {
                        continue;
                    }
                    // check auth
                    try {
                        checkJobAuth(catalogName, dbName, loadJob.getTableNames());
                    } catch (AnalysisException e) {
                        continue;
                    }
                    // add load job info, convert String list to Comparable list
                    loadJobInfos.add(new ArrayList<>(loadJob.getShowInfo()));
                } catch (RuntimeException e) {
                    // ignore this load job
                    log.warn("get load job info failed. job id: {}", loadJob.getJobId(), e);
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

    private static boolean validState(JobState jobState, InsertJob loadJob) {
        JobStatus status = loadJob.getJobStatus();
        switch (status) {
            case RUNNING:
                return jobState == JobState.PENDING || jobState == JobState.ETL
                        || jobState == JobState.LOADING || jobState == JobState.COMMITTED;
            case STOPPED:
                return jobState == JobState.CANCELLED;
            case FINISHED:
                return jobState == JobState.FINISHED;
            default:
                return false;
        }
    }

    private static void addNeedCancelLoadJob(String label, String state,
                                             CompoundPredicate.Operator operator, List<InsertJob> loadJobs,
                                             List<InsertJob> matchLoadJobs)
            throws AnalysisException {
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(label,
                CaseSensibility.LABEL.getCaseSensibility());
        matchLoadJobs.addAll(
                loadJobs.stream()
                        .filter(job -> !job.isCancelled())
                        .filter(job -> {
                            if (operator != null) {
                                // compound
                                boolean labelFilter =
                                        label.contains("%") ? matcher.match(job.getLabelName())
                                                : job.getLabelName().equalsIgnoreCase(label);
                                boolean stateFilter = job.getJobStatus().name().equalsIgnoreCase(state);
                                return CompoundPredicate.Operator.AND.equals(operator) ? labelFilter && stateFilter :
                                        labelFilter || stateFilter;
                            }
                            if (StringUtils.isNotEmpty(label)) {
                                return label.contains("%") ? matcher.match(job.getLabelName())
                                        : job.getLabelName().equalsIgnoreCase(label);
                            }
                            if (StringUtils.isNotEmpty(state)) {
                                return job.getJobStatus().name().equalsIgnoreCase(state);
                            }
                            return false;
                        }).collect(Collectors.toList())
        );
    }
    //    public void updateJobProgress(Long jobId, Long beId, TUniqueId loadId, TUniqueId fragmentId, long scannedRows,
    //                                  long scannedBytes, boolean isDone) {
    //        AbstractJob job = jobMap.get(jobId);
    //        if (job != null) {
    //            job.updateLoadingStatus(beId, loadId, fragmentId, scannedRows, scannedBytes, isDone);
    //        }
    //    }

    /**
     * used for nereids planner
     */
    public void cancelLoadJob(String dbName, String label, String state,
                              Expression operator)
            throws JobException, AnalysisException, DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        // List of load jobs waiting to be cancelled
        List<InsertJob> unfinishedLoadJob;
        readLock();
        try {
            List<InsertJob> loadJobs = Env.getCurrentEnv().getLabelProcessor().getJobs(db);
            List<InsertJob> matchLoadJobs = Lists.newArrayList();
            addNeedCancelLoadJob(label, state, operator, loadJobs, matchLoadJobs);
            if (matchLoadJobs.isEmpty()) {
                throw new JobException("Load job does not exist");
            }
            // check state here
            unfinishedLoadJob =
                matchLoadJobs.stream().filter(InsertJob::isRunning)
                    .collect(Collectors.toList());
            if (unfinishedLoadJob.isEmpty()) {
                throw new JobException("There is no uncompleted job");
            }
        } finally {
            readUnlock();
        }
        // check auth
        if (unfinishedLoadJob.size() > 1 || unfinishedLoadJob.get(0).getTableNames().isEmpty()) {
            if (Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                    PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(), dbName);
            }
        } else {
            for (String tableName : unfinishedLoadJob.get(0).getTableNames()) {
                if (Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                        tableName,
                        PrivPredicate.LOAD)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                            ConnectContext.get().getQualifiedUser(),
                            ConnectContext.get().getRemoteIP(), dbName + ":" + tableName);
                }
            }
        }
        for (InsertJob loadJob : unfinishedLoadJob) {
            try {
                alterJobStatus(loadJob.getJobId(), JobStatus.STOPPED);
            } catch (JobException e) {
                log.warn("Fail to cancel job, its label: {}", loadJob.getLabelName());
            }
        }
    }

    private static void addNeedCancelLoadJob(String label, String state,
                                             Expression operator,
                                             List<InsertJob> loadJobs,
                                             List<InsertJob> matchLoadJobs)
            throws AnalysisException {
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(label,
                CaseSensibility.LABEL.getCaseSensibility());
        matchLoadJobs.addAll(
                loadJobs.stream()
                .filter(job -> !job.isCancelled())
                .filter(job -> {
                    if (operator != null) {
                        // compound
                        boolean labelFilter =
                                label.contains("%") ? matcher.match(job.getLabelName())
                                : job.getLabelName().equalsIgnoreCase(label);
                        boolean stateFilter = job.getJobStatus().name().equalsIgnoreCase(state);
                        return operator instanceof And ? labelFilter && stateFilter :
                            labelFilter || stateFilter;
                    }
                    if (StringUtils.isNotEmpty(label)) {
                        return label.contains("%") ? matcher.match(job.getLabelName())
                            : job.getLabelName().equalsIgnoreCase(label);
                    }
                    if (StringUtils.isNotEmpty(state)) {
                        return job.getJobStatus().name().equalsIgnoreCase(state);
                    }
                    return false;
                }).collect(Collectors.toList())
        );
    }
}
