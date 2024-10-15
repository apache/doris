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

package org.apache.doris.cloud.load;

import org.apache.doris.analysis.CopyStmt;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.UserException;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadJobScheduler;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CloudLoadManager extends LoadManager {
    private static final Logger LOG = LogManager.getLogger(CloudLoadManager.class);
    private CleanCopyJobScheduler cleanCopyJobScheduler;

    public CloudLoadManager(LoadJobScheduler loadJobScheduler, CleanCopyJobScheduler cleanCopyJobScheduler) {
        super(loadJobScheduler);
        this.cleanCopyJobScheduler = cleanCopyJobScheduler;
    }

    @Override
    public long createLoadJobFromStmt(LoadStmt stmt) throws DdlException, UserException {
        ((CloudSystemInfoService) Env.getCurrentSystemInfo()).waitForAutoStartCurrentCluster();

        return super.createLoadJobFromStmt(stmt);
    }

    @Override
    public long createLoadJobFromStmt(InsertStmt stmt) throws DdlException {
        ((CloudSystemInfoService) Env.getCurrentSystemInfo()).waitForAutoStartCurrentCluster();

        return super.createLoadJobFromStmt(stmt);
    }

    public LoadJob createLoadJobFromStmt(CopyStmt stmt) throws DdlException {
        Database database = super.checkDb(stmt.getDbName());
        long dbId = database.getId();
        BrokerLoadJob loadJob = null;
        ((CloudSystemInfoService) Env.getCurrentSystemInfo()).waitForAutoStartCurrentCluster();

        writeLock();
        try {
            long unfinishedCopyJobNum = unprotectedGetUnfinishedCopyJobNum();
            if (unfinishedCopyJobNum >= Config.cluster_max_waiting_copy_jobs) {
                throw new DdlException(
                        "There are more than " + unfinishedCopyJobNum + " unfinished copy jobs, please retry later.");
            }
            loadJob = new CopyJob(dbId, stmt.getLabel().getLabelName(), ConnectContext.get().queryId(),
                    stmt.getBrokerDesc(), stmt.getOrigStmt(), stmt.getUserInfo(), stmt.getStageId(),
                    stmt.getStageType(), stmt.getStagePrefix(), stmt.getSizeLimit(), stmt.getPattern(),
                    stmt.getObjectInfo(), stmt.isForce(), stmt.getUserName());
            loadJob.setJobProperties(stmt.getProperties());
            loadJob.checkAndSetDataSourceInfo(database, stmt.getDataDescriptions());
            loadJob.setTimeout(ConnectContext.get().getExecTimeout());
            createLoadJob(loadJob);
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        } finally {
            super.writeUnlock();
        }
        Env.getCurrentEnv().getEditLog().logCreateLoadJob(loadJob);

        // The job must be submitted after edit log.
        // It guarantees that load job has not been changed before edit log.
        loadJobScheduler.submitJob(loadJob);
        return loadJob;
    }

    public void createCleanCopyJobTask(CleanCopyJobTask task) throws DdlException {
        cleanCopyJobScheduler.submitJob(task);
    }

    private long unprotectedGetUnfinishedCopyJobNum() {
        return idToLoadJob.values().stream()
                .filter(j -> (j.getState() != JobState.FINISHED && j.getState() != JobState.CANCELLED))
                .filter(j -> j instanceof CopyJob).count();
    }

    /**
     * This method will return the jobs info which can meet the condition of input param.
     *
     * @param dbId          used to filter jobs which belong to this db
     * @param labelValue    used to filter jobs which's label is or like labelValue.
     * @param accurateMatch true: filter jobs which's label is labelValue. false: filter jobs which's label like itself.
     * @param statesValue   used to filter jobs which's state within the statesValue set.
     * @param jobTypes      used to filter jobs which's type within the jobTypes set.
     * @param copyIdValue        used to filter jobs which's copyId is or like copyIdValue.
     * @param copyIdAccurateMatch  true: filter jobs which's copyId is copyIdValue.
     *                             false: filter jobs which's copyId like itself.
     * @return The result is the list of jobInfo.
     *         JobInfo is a list which includes the comparable object: jobId, label, state etc.
     *         The result is unordered.
     */
    public List<List<Comparable>> getLoadJobInfosByDb(long dbId, String labelValue, boolean accurateMatch,
            Set<String> statesValue, Set<EtlJobType> jobTypes, String copyIdValue, boolean copyIdAccurateMatch,
            String tableNameValue, boolean tableNameAccurateMatch, String fileValue, boolean fileAccurateMatch)
            throws AnalysisException {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<List<Comparable>>();
        if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
            return loadJobInfos;
        }

        if (jobTypes == null || jobTypes.isEmpty()) {
            jobTypes = new HashSet<>();
            jobTypes.addAll(EnumSet.allOf(EtlJobType.class));
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

            List<LoadJob> loadJobList2 = new ArrayList<>();
            // check state
            for (LoadJob loadJob : loadJobList) {
                if (!states.contains(loadJob.getState())) {
                    continue;
                }
                if (!jobTypes.contains(loadJob.getJobType())) {
                    continue;
                }
                loadJobList2.add(loadJob);
            }
            loadJobList2 = filterCopyJob(loadJobList2, copyIdValue, copyIdAccurateMatch, c -> c.getCopyId());
            loadJobList2 = filterCopyJob(loadJobList2, tableNameValue, tableNameAccurateMatch, c -> c.getTableName());
            loadJobList2 = filterCopyJob(loadJobList2, fileValue, fileAccurateMatch, c -> c.getFiles());
            for (LoadJob loadJob : loadJobList2) {
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

    private List<LoadJob> filterCopyJob(List<LoadJob> loadJobList, String value, boolean accurateMatch,
            Function<CopyJob, String> func) throws AnalysisException {
        if (Strings.isNullOrEmpty(value)) {
            return loadJobList;
        }
        List<LoadJob> loadJobList2 = Lists.newArrayList();
        for (LoadJob loadJob : loadJobList) {
            if (loadJob.getJobType() != EtlJobType.COPY) {
                continue;
            }
            CopyJob copyJob = (CopyJob) loadJob;
            if (accurateMatch) {
                if (func.apply(copyJob).equalsIgnoreCase(value)) {
                    loadJobList2.add(copyJob);
                }
            } else {
                // non-accurate match
                PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(value, false);
                if (matcher.match(func.apply(copyJob))) {
                    loadJobList2.add(copyJob);
                }
            }
        }
        return loadJobList2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        removeCopyJobs();
    }

    @Override
    public void removeOldLoadJob() {
        super.removeOldLoadJob();
        removeCopyJobs();
    }

    private void removeCopyJobs() {
        if (Config.cloud_max_copy_job_per_table <= 0) {
            return;
        }
        Map<Long, Set<String>> dbToLabels = new HashMap<>();
        readLock();
        long start = System.currentTimeMillis();
        try {
            // group jobs by table
            Map<String, List<LoadJob>> tableToLoadJobs = dbIdToLabelToLoadJobs.values().stream()
                    .flatMap(loadJobsMap -> loadJobsMap.values().stream())
                    .flatMap(loadJobs -> loadJobs.stream())
                    .filter(loadJob -> (loadJob instanceof CopyJob) && StringUtils.isNotEmpty(
                            ((CopyJob) loadJob).getTableName()))
                    .map(copyJob -> Pair.of(copyJob.getDbId() + "#" + ((CopyJob) copyJob).getTableName(), copyJob))
                    .collect(Collectors.groupingBy(v -> v.first,
                            Collectors.mapping(jobPairs -> jobPairs.second, Collectors.toList())));
            // find labels to remove
            for (List<LoadJob> jobs : tableToLoadJobs.values()) {
                if (jobs.size() <= Config.cloud_max_copy_job_per_table) {
                    continue;
                }
                jobs.sort((o1, o2) -> Long.compare(o2.getFinishTimestamp(), o1.getFinishTimestamp()));
                int finishJobCount = 0;
                boolean found = false;
                for (LoadJob job : jobs) {
                    if (!found) {
                        if (job.getState() == JobState.FINISHED) {
                            finishJobCount++;
                            if (finishJobCount >= Config.cloud_max_copy_job_per_table) {
                                found = true;
                            }
                        }
                    } else {
                        if (job.isCompleted()) {
                            dbToLabels.computeIfAbsent(job.getDbId(), (k) -> new HashSet<>()).add(job.getLabel());
                        }
                    }
                }
            }
        } catch (Throwable e) {
            LOG.warn("Failed to remove copy jobs", e);
        } finally {
            readUnlock();
        }
        if (dbToLabels.isEmpty()) {
            return;
        }
        writeLock();
        long copyJobNum = idToLoadJob.size();
        try {
            for (Map.Entry<Long, Set<String>> entry : dbToLabels.entrySet()) {
                long dbId = entry.getKey();
                if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
                    continue;
                }
                Map<String, List<LoadJob>> labelToJob = dbIdToLabelToLoadJobs.get(dbId);
                for (String label : entry.getValue()) {
                    List<LoadJob> jobs = labelToJob.get(label);
                    if (jobs == null) {
                        continue;
                    }
                    Iterator<LoadJob> iter = jobs.iterator();
                    while (iter.hasNext()) {
                        CopyJob job = (CopyJob) iter.next();
                        iter.remove();
                        idToLoadJob.remove(job.getId());
                        job.recycleProgress();
                    }
                    if (jobs.isEmpty()) {
                        labelToJob.remove(label);
                    }
                }
            }
            LOG.info("remove copy jobs from {} to {}, cost={}ms", copyJobNum, idToLoadJob.size(),
                    System.currentTimeMillis() - start);
        } catch (Throwable e) {
            LOG.warn("Failed to remove copy jobs", e);
        } finally {
            writeUnlock();
        }
    }
}

