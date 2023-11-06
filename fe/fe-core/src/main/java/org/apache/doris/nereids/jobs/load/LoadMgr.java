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
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.load.DataTransFormMgr;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.jobs.load.replay.ReplayLoadLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.executor.TVFLoadJob;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * load manager
 */
public class LoadMgr extends DataTransFormMgr {
    private static final Logger log = LogManager.getLogger(LoadMgr.class);
    private Map<Long, TVFLoadJob> loadIdToJob = Maps.newHashMap();
    private Map<String, Long> labelToLoadJobId = Maps.newHashMap();
    private Map<Long, Map<String, List<TVFLoadJob>>> dbIdToLabelToLoadJobs = new ConcurrentHashMap<>();
    private Map<Long, List<TVFLoadJob>> dbToLoadJobs; // db to loadJob list

    public List<TVFLoadJob> getExecutableJobs() {
        return Lists.newArrayList(loadIdToJob.values());
    }

    private JobManager<TVFLoadJob> getJobManager() {
        return Env.getCurrentEnv().getJobManager();
    }

    /**
     * add load job and add tasks
     * @param loadJob job
     */
    public void addLoadJob(TVFLoadJob loadJob) throws DdlException {
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

    private void unprotectAddJob(TVFLoadJob job) throws DdlException {
        loadIdToJob.put(job.getId(), job);
        labelToLoadJobId.putIfAbsent(job.getLabel(), job.getId());
        try {
            getJobManager().registerJob(job);
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
                TVFLoadJob loadJob = new TVFLoadJob(replayLoadLog);
                addLoadJob(loadJob);
                log.info(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId()).add("msg", "replay create load job").build());
            } else if (replayLoadLog instanceof ReplayLoadLog.ReplayEndLoadLog) {
                TVFLoadJob job = loadIdToJob.get(replayLoadLog.getId());
                if (job == null) {
                    // This should not happen.
                    // Last time I found that when user submit a job with already used label, an END_LOAD_JOB edit log
                    // will be written but the job is not added to 'idToLoadJob', so this job here we got will be null.
                    // And this bug has been fixed.
                    // Just add a log here to observe.
                    log.warn("job does not exist when replaying end load job edit log: {}", replayLoadLog);
                    return;
                }
                job.unprotectReadEndOperation((ReplayLoadLog.ReplayEndLoadLog) replayLoadLog);
                log.info(new LogBuilder(LogKey.LOAD_JOB, replayLoadLog.getId()).add("operation", replayLoadLog)
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
        List<TVFLoadJob> uncompletedLoadJob;
        readLock();
        try {
            Map<String, List<TVFLoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("Load job does not exist");
            }
            List<TVFLoadJob> matchLoadJobs = Lists.newArrayList();
            addNeedCancelLoadJob(label, state, operator,
                    labelToLoadJobs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                    matchLoadJobs);
            if (matchLoadJobs.isEmpty()) {
                throw new DdlException("Load job does not exist");
            }
            // check state here
            uncompletedLoadJob =
                    matchLoadJobs.stream().filter(executor -> !executor.isTxnDone()).collect(Collectors.toList());
            if (uncompletedLoadJob.isEmpty()) {
                throw new DdlException("There is no uncompleted job");
            }
        } finally {
            readUnlock();
        }
        for (TVFLoadJob loadJob : uncompletedLoadJob) {
            loadJob.cancelJob(new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel"));
        }
    }

    private static void addNeedCancelLoadJob(String label, String state,
                                            CompoundPredicate.Operator operator, List<TVFLoadJob> loadJobs,
                                            List<TVFLoadJob> matchLoadJobs)
            throws AnalysisException {
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(label,
                CaseSensibility.LABEL.getCaseSensibility());
        matchLoadJobs.addAll(
                loadJobs.stream()
                        .filter(job -> job.getState() != JobState.CANCELLED)
                        .filter(job -> {
                            if (operator != null) {
                                // compound
                                boolean labelFilter =
                                        label.contains("%") ? matcher.match(job.getLabel())
                                                : job.getLabel().equalsIgnoreCase(label);
                                boolean stateFilter = job.getState().name().equalsIgnoreCase(state);
                                return CompoundPredicate.Operator.AND.equals(operator) ? labelFilter && stateFilter :
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
     * get running job
     *
     * @param jobId id
     * @return running job
     */
    public TVFLoadJob getJob(long jobId) {
        TVFLoadJob job;
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
        readLock();
        try {
            List<TVFLoadJob> loadJobs = this.dbToLoadJobs.get(dbId);
            if (loadJobs == null) {
                return new LinkedList<>();
            }

            long start = System.currentTimeMillis();
            log.debug("begin to get load job info, size: {}", loadJobs.size());
            PatternMatcher matcher = null;
            if (labelValue != null && !accurateMatch) {
                matcher = PatternMatcherWrapper.createMysqlPattern(labelValue,
                        CaseSensibility.LABEL.getCaseSensibility());
            }

            // find load job by label.
            for (TVFLoadJob loadJob : loadJobs) {
                String label = loadJob.getLabel();
                JobState state = loadJob.getState();
                if (labelValue != null) {
                    boolean foundLoad = accurateMatch ? label.equals(labelValue) : matcher.match(label);
                    if (!foundLoad) {
                        continue;
                    }
                }
                if (jobState != null && !jobState.equals(state)) {
                    continue;
                }
                // check auth
                Set<String> tableNames = loadJob.getTableNames();
                if (tableNames.isEmpty()) {
                    // forward compatibility
                    if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), dbName,
                            PrivPredicate.LOAD)) {
                        continue;
                    }
                } else {
                    boolean auth = true;
                    for (String tblName : tableNames) {
                        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), dbName,
                                tblName, PrivPredicate.LOAD)) {
                            auth = false;
                            break;
                        }
                    }
                    if (!auth) {
                        continue;
                    }
                }
                loadJobInfos.add(loadJob.getDetail());
            } // end for loadJobs
            log.debug("finished to get load job info, cost: {}", (System.currentTimeMillis() - start));
        } finally {
            readUnlock();
        }
        return loadJobInfos;
    }
}
