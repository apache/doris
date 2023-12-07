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

package org.apache.doris.nereids.jobs.load;

import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.load.DataTransFormMgr;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.nereids.jobs.load.replay.ReplayLoadLog;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * load manager
 */
public class LoadMgr extends DataTransFormMgr {
    private static final Logger LOG = LogManager.getLogger(LoadMgr.class);
    private Map<Long, InsertJob> loadIdToJob = new HashMap<>();
    private Map<String, Long> labelToLoadJobId = new HashMap<>();
    private Map<Long, Map<String, List<InsertJob>>> dbIdToLabelToLoadJobs = new ConcurrentHashMap<>();

    private JobManager<InsertJob> getJobManager() {
        return Env.getCurrentEnv().getJobManager();
    }

    /**
     * add load job and add tasks
     * @param loadJob job
     */
    public void addLoadJob(InsertJob loadJob) throws DdlException {
        writeLock();
        try {
            if (labelToLoadJobId.containsKey(loadJob.getLabel())) {
                throw new LabelAlreadyUsedException(loadJob.getLabel());
            }
            unprotectAddJob(loadJob);
            Env.getCurrentEnv().getEditLog().logLoadCreate(ReplayLoadLog.logCreateLoadOperation(loadJob));
        } catch (LabelAlreadyUsedException e) {
            throw new RuntimeException(e);
        } finally {
            writeUnlock();
        }
    }

    private void unprotectAddJob(InsertJob job) throws DdlException {
        loadIdToJob.put(job.getJobId(), job);
        labelToLoadJobId.putIfAbsent(job.getLabel(), job.getJobId());
        try {
            getJobManager().registerJob(job);
            if (!dbIdToLabelToLoadJobs.containsKey(job.getDbId())) {
                dbIdToLabelToLoadJobs.put(job.getDbId(), new ConcurrentHashMap<>());
            }
            Map<String, List<InsertJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(job.getDbId());
            if (!labelToLoadJobs.containsKey(job.getLabel())) {
                labelToLoadJobs.put(job.getLabel(), new ArrayList<>());
            }
            labelToLoadJobs.get(job.getLabel()).add(job);
        } catch (org.apache.doris.job.exception.JobException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    /**
     * replay load job
     * @param replayLoadLog load log
     * @throws DdlException ex
     */
    public void replayLoadJob(ReplayLoadLog replayLoadLog) throws DdlException {
        writeLock();
        try {
            if (replayLoadLog instanceof ReplayLoadLog.ReplayCreateLoadLog) {
                InsertJob loadJob = new InsertJob((ReplayLoadLog.ReplayCreateLoadLog) replayLoadLog);
                addLoadJob(loadJob);
                LOG.info(new LogBuilder(LogKey.LOAD_JOB, loadJob.getJobId()).add("msg", "replay create load job")
                        .build());
            } else if (replayLoadLog instanceof ReplayLoadLog.ReplayEndLoadLog) {
                InsertJob job = loadIdToJob.get(replayLoadLog.getId());
                if (job == null) {
                    // This should not happen.
                    // Last time I found that when user submit a job with already used label, an END_LOAD_JOB edit log
                    // will be written but the job is not added to 'idToLoadJob', so this job here we got will be null.
                    // And this bug has been fixed.
                    // Just add a log here to observe.
                    LOG.warn("job does not exist when replaying end load job edit log: {}", replayLoadLog);
                    return;
                }
                job.unprotectReadEndOperation((ReplayLoadLog.ReplayEndLoadLog) replayLoadLog);
                LOG.info(new LogBuilder(LogKey.LOAD_JOB, replayLoadLog.getId()).add("operation", replayLoadLog)
                        .add("msg", "replay end load job").build());
            } else {
                throw new DdlException("Unsupported replay job type. ");
            }
        } finally {
            writeUnlock();
        }
    }

    //    public void updateJobProgress(Long jobId, Long beId, TUniqueId loadId, TUniqueId fragmentId, long scannedRows,
    //                                  long scannedBytes, boolean isDone) {
    //        LoadJobExecutor job = loadIdToJob.get(jobId);
    //        if (job != null) {
    //            job.updateLoadingStatus(beId, loadId, fragmentId, scannedRows, scannedBytes, isDone);
    //        }
    //    }

    /**
     * cancel job
     *
     * @param dbName   dbName
     * @param label    job label
     * @param state    job state
     * @param operator filter operator, like or equals
     */
    public void cancelLoadJob(String dbName, String label, String state, CompoundPredicate.Operator operator)
            throws DdlException, AnalysisException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        // List of load jobs waiting to be cancelled
        List<InsertJob> uncompletedLoadJob;
        readLock();
        try {
            Map<String, List<InsertJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("Load job does not exist");
            }
            List<InsertJob> matchLoadJobs = Lists.newArrayList();
            addNeedCancelLoadJob(label, state, operator,
                    labelToLoadJobs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                    matchLoadJobs);
            if (matchLoadJobs.isEmpty()) {
                throw new DdlException("Load job does not exist");
            }
            // check state here
            uncompletedLoadJob =
                    matchLoadJobs.stream().filter(InsertJob::isRunning)
                            .collect(Collectors.toList());
            if (uncompletedLoadJob.isEmpty()) {
                throw new DdlException("There is no uncompleted job");
            }
        } finally {
            readUnlock();
        }
        for (InsertJob loadJob : uncompletedLoadJob) {
            // 权限问题，try catch continue
            loadJob.cancelJob(new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel"));
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
                                        label.contains("%") ? matcher.match(job.getLabel())
                                                : job.getLabel().equalsIgnoreCase(label);
                                boolean stateFilter = job.getJobStatus().name().equalsIgnoreCase(state);
                                return CompoundPredicate.Operator.AND.equals(operator) ? labelFilter && stateFilter :
                                        labelFilter || stateFilter;
                            }
                            if (StringUtils.isNotEmpty(label)) {
                                return label.contains("%") ? matcher.match(job.getLabel())
                                        : job.getLabel().equalsIgnoreCase(label);
                            }
                            if (StringUtils.isNotEmpty(state)) {
                                return job.getJobStatus().name().equalsIgnoreCase(state);
                            }
                            return false;
                        }).collect(Collectors.toList())
        );
    }

    /**
     * get running job
     *
     * @param jobId id
     * @return running job
     */
    public InsertJob getJob(long jobId) {
        InsertJob job;
        readLock();
        try {
            job = loadIdToJob.get(jobId);
        } finally {
            readUnlock();
        }
        return job;
    }

    /**
     * get load info by db
     * @param dbId db id
     * @param dbName db name
     * @param labelValue label name
     * @param accurateMatch accurate match
     * @param jobState state
     * @return load infos
     * @throws AnalysisException ex
     */
    public List<List<Comparable>> getLoadJobInfosByDb(long dbId, String dbName,
                                                      String labelValue,
                                                      boolean accurateMatch,
                                                      JobState jobState) throws AnalysisException {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<>();
        if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
            return loadJobInfos;
        }
        readLock();
        try {
            Map<String, List<InsertJob>> labelToLoadJobs = this.dbIdToLabelToLoadJobs.get(dbId);
            List<InsertJob> loadJobList = Lists.newArrayList();
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
                    for (Map.Entry<String, List<InsertJob>> entry : labelToLoadJobs.entrySet()) {
                        if (matcher.match(entry.getKey())) {
                            loadJobList.addAll(entry.getValue());
                        }
                    }
                }
            }
            // check state
            for (InsertJob loadJob : loadJobList) {
                try {
                    if (!validState(jobState, loadJob)) {
                        continue;
                    }
                    // add load job info, convert String list to Comparable list
                    loadJobInfos.add(new ArrayList<>(loadJob.getShowInfo()));
                } catch (RuntimeException e) {
                    // ignore this load job
                    LOG.warn("get load job info failed. job id: {}", loadJob.getJobId(), e);
                }
            }
            return loadJobInfos;
        } finally {
            readUnlock();
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
}
