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

package org.apache.doris.load;

import org.apache.doris.analysis.CancelExportStmt;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.task.MasterTaskExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ExportMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ExportJob.class);

    // lock for export job
    // lock is private and must use after db lock
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private Map<Long, ExportJob> exportIdToJob = Maps.newHashMap(); // exportJobId to exportJob
    private Map<String, Long> labelToExportJobId = Maps.newHashMap();

    private MasterTaskExecutor exportingExecutor;

    public ExportMgr() {
        int poolSize = Config.export_running_job_num_limit == 0 ? 5 : Config.export_running_job_num_limit;
        exportingExecutor = new MasterTaskExecutor("export-exporting-job", poolSize, true);
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    @Override
    public synchronized void start() {
        super.start();
        exportingExecutor.start();
    }

    @Override
    protected void runAfterCatalogReady() {
        // List<ExportJob> pendingJobs = getExportJobs(ExportJobState.PENDING);
        // List<ExportJob> newInQueueJobs = Lists.newArrayList();
        // for (ExportJob job : pendingJobs) {
        //     if (handlePendingJobs(job)) {
        //         newInQueueJobs.add(job);
        //     }
        // }
        // LOG.debug("new IN_QUEUE export job num: {}", newInQueueJobs.size());
        // for (ExportJob job : newInQueueJobs) {
        //     try {
        //         MasterTask task = new ExportExportingTask(job);
        //         job.setTask((ExportExportingTask) task);
        //         if (exportingExecutor.submit(task)) {
        //             LOG.info("success to submit IN_QUEUE export job. job: {}", job);
        //         } else {
        //             LOG.info("fail to submit IN_QUEUE job to executor. job: {}", job);
        //         }
        //     } catch (Exception e) {
        //         LOG.warn("run export exporting job {}.", job, e);
        //     }
        // }
    }

    private boolean handlePendingJobs(ExportJob job) {
        // because maybe this job has been cancelled by user.
        if (job.getState() != ExportJobState.PENDING) {
            return false;
        }

        if (job.isReplayed()) {
            // If the job is created from replay thread, all plan info will be lost.
            // so the job has to be cancelled.
            String failMsg = "FE restarted or Master changed during exporting. Job must be cancelled.";
            job.cancel(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            return false;
        }

        if (job.updateState(ExportJobState.IN_QUEUE)) {
            LOG.info("Exchange pending status to in_queue status success. job: {}", job);
            return true;
        }
        return false;
    }

    public List<ExportJob> getJobs() {
        return Lists.newArrayList(exportIdToJob.values());
    }

    public void addExportJob(ExportStmt stmt) throws Exception {
        long jobId = Env.getCurrentEnv().getNextId();
        ExportJob job = createJob(jobId, stmt);
        writeLock();
        try {
            if (labelToExportJobId.containsKey(job.getLabel())) {
                throw new LabelAlreadyUsedException(job.getLabel());
            }
            unprotectAddJob(job);
            Env.getCurrentEnv().getEditLog().logExportCreate(job);
        } finally {
            writeUnlock();
        }
        LOG.info("add export job. {}", job);
    }

    public void addExportJobAndRegisterTask(ExportJob job) throws Exception {
        long jobId = Env.getCurrentEnv().getNextId();
        job.setId(jobId);
        writeLock();
        try {
            if (labelToExportJobId.containsKey(job.getLabel())) {
                throw new LabelAlreadyUsedException(job.getLabel());
            }
            unprotectAddJob(job);
            job.getJobExecutorList().forEach(executor -> {
                Long taskId = Env.getCurrentEnv().getExportTaskRegister().registerTask(executor);
                executor.setTaskId(taskId);
                job.getTaskIdToExecutor().put(taskId, executor);
            });
            Env.getCurrentEnv().getEditLog().logExportCreate(job);
        } finally {
            writeUnlock();
        }
        LOG.info("add export job. {}", job);
    }

    public void cancelExportJob(CancelExportStmt stmt) throws DdlException, AnalysisException {
        // List of export jobs waiting to be cancelled
        List<ExportJob> matchExportJobs = getWaitingCancelJobs(stmt);
        if (matchExportJobs.isEmpty()) {
            throw new DdlException("Export job(s) do not exist");
        }
        matchExportJobs = matchExportJobs.stream()
                .filter(job -> !job.isFinalState()).collect(Collectors.toList());
        if (matchExportJobs.isEmpty()) {
            throw new DdlException("All export job(s) are at final state (CANCELLED/FINISHED)");
        }
        try {
            for (ExportJob exportJob : matchExportJobs) {
                // exportJob.cancel(ExportFailMsg.CancelType.USER_CANCEL, "user cancel");
                exportJob.updateExportJobState(ExportJobState.CANCELLED, 0L, null,
                        ExportFailMsg.CancelType.USER_CANCEL, "user cancel");
            }
        } catch (JobException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public void unprotectAddJob(ExportJob job) {
        exportIdToJob.put(job.getId(), job);
        labelToExportJobId.putIfAbsent(job.getLabel(), job.getId());
    }

    private List<ExportJob> getWaitingCancelJobs(CancelExportStmt stmt) throws AnalysisException {
        Predicate<ExportJob> jobFilter = buildCancelJobFilter(stmt);
        readLock();
        try {
            return getJobs().stream().filter(jobFilter).collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    @VisibleForTesting
    public static Predicate<ExportJob> buildCancelJobFilter(CancelExportStmt stmt) throws AnalysisException {
        String label = stmt.getLabel();
        String state = stmt.getState();
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(label,
                CaseSensibility.LABEL.getCaseSensibility());

        return job -> {
            boolean labelFilter = true;
            boolean stateFilter = true;
            if (StringUtils.isNotEmpty(label)) {
                labelFilter = label.contains("%") ? matcher.match(job.getLabel()) :
                        job.getLabel().equalsIgnoreCase(label);
            }
            if (StringUtils.isNotEmpty(state)) {
                stateFilter = job.getState().name().equalsIgnoreCase(state);
            }

            if (stmt.getOperator() != null && CompoundPredicate.Operator.OR.equals(stmt.getOperator())) {
                return labelFilter || stateFilter;
            }

            return labelFilter && stateFilter;
        };
    }

    private ExportJob createJob(long jobId, ExportStmt stmt) {
        ExportJob exportJob = stmt.getExportJob();
        exportJob.setId(jobId);
        return exportJob;
    }

    public ExportJob getJob(long jobId) {
        ExportJob job;
        readLock();
        try {
            job = exportIdToJob.get(jobId);
        } finally {
            readUnlock();
        }
        return job;
    }

    public List<ExportJob> getExportJobs(ExportJobState state) {
        List<ExportJob> result = Lists.newArrayList();
        readLock();
        try {
            for (ExportJob job : exportIdToJob.values()) {
                if (job.getState() == state) {
                    result.add(job);
                }
            }
        } finally {
            readUnlock();
        }

        return result;
    }

    // used for `show export` statement
    // NOTE: jobid and states may both specified, or only one of them, or neither
    public List<List<String>> getExportJobInfosByIdOrState(
            long dbId, long jobId, String label, boolean isLabelUseLike, Set<ExportJobState> states,
            ArrayList<OrderByPair> orderByPairs, long limit) throws AnalysisException {

        long resultNum = limit == -1L ? Integer.MAX_VALUE : limit;
        LinkedList<List<Comparable>> exportJobInfos = new LinkedList<List<Comparable>>();
        PatternMatcher matcher = null;
        if (isLabelUseLike) {
            matcher = PatternMatcherWrapper.createMysqlPattern(label, CaseSensibility.LABEL.getCaseSensibility());
        }

        readLock();
        try {
            int counter = 0;
            for (ExportJob job : exportIdToJob.values()) {
                long id = job.getId();
                ExportJobState state = job.getState();
                String jobLabel = job.getLabel();

                if (job.getDbId() != dbId) {
                    continue;
                }

                if (jobId != 0 && id != jobId) {
                    continue;
                }

                if (!Strings.isNullOrEmpty(label)) {
                    if (!isLabelUseLike && !jobLabel.equals(label)) {
                        // use = but does not match
                        continue;
                    } else if (isLabelUseLike && !matcher.match(jobLabel)) {
                        // use like but does not match
                        continue;
                    }
                }

                if (states != null) {
                    if (!states.contains(state)) {
                        continue;
                    }
                }

                // check auth
                if (isJobShowable(job)) {
                    exportJobInfos.add(composeExportJobInfo(job));
                }

                if (++counter >= resultNum && orderByPairs == null) {
                    break;
                }
            }
        } finally {
            readUnlock();
        }

        // order by
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(exportJobInfos, comparator);

        List<List<String>> results = Lists.newArrayList();
        int counter = 0;
        for (List<Comparable> list : exportJobInfos) {
            results.add(list.stream().map(e -> e.toString()).collect(Collectors.toList()));
            if (++counter >= resultNum) {
                break;
            }
        }

        return results;
    }

    public List<List<String>> getExportJobInfos(long limit) {
        long resultNum = limit == -1L ? Integer.MAX_VALUE : limit;
        LinkedList<List<Comparable>> exportJobInfos = new LinkedList<List<Comparable>>();

        readLock();
        try {
            int counter = 0;
            for (ExportJob job : exportIdToJob.values()) {
                // check auth
                if (isJobShowable(job)) {
                    exportJobInfos.add(composeExportJobInfo(job));
                }

                if (++counter >= resultNum) {
                    break;
                }
            }
        } finally {
            readUnlock();
        }

        // order by
        ListComparator<List<Comparable>> comparator = null;
        // sort by id asc
        comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(exportJobInfos, comparator);

        List<List<String>> results = Lists.newArrayList();
        for (List<Comparable> list : exportJobInfos) {
            results.add(list.stream().map(e -> e.toString()).collect(Collectors.toList()));
        }

        return results;
    }

    public boolean isJobShowable(ExportJob job) {
        TableName tableName = job.getTableName();
        if (tableName == null || tableName.getTbl().equals("DUMMY")) {
            // forward compatibility, no table name is saved before
            Database db = Env.getCurrentInternalCatalog().getDbNullable(job.getDbId());
            if (db == null) {
                return false;
            }
            if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                    db.getFullName(), PrivPredicate.SHOW)) {
                return false;
            }
        } else {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
                    tableName.getDb(), tableName.getTbl(),
                    PrivPredicate.SHOW)) {
                return false;
            }
        }

        return true;
    }

    private List<Comparable> composeExportJobInfo(ExportJob job) {
        List<Comparable> jobInfo = new ArrayList<Comparable>();

        jobInfo.add(job.getId());
        jobInfo.add(job.getLabel());
        jobInfo.add(job.getState().name());
        jobInfo.add(job.getProgress() + "%");

        // task infos
        Map<String, Object> infoMap = Maps.newHashMap();
        List<String> partitions = job.getPartitionNames();
        if (partitions == null) {
            partitions = Lists.newArrayList();
            partitions.add("*");
        }
        infoMap.put("db", job.getTableName().getDb());
        infoMap.put("tbl", job.getTableName().getTbl());
        if (job.getWhereExpr() != null) {
            infoMap.put("where expr", job.getWhereExpr().toSql());
        }
        infoMap.put("partitions", partitions);
        infoMap.put("broker", job.getBrokerDesc().getName());
        infoMap.put("column_separator", job.getColumnSeparator());
        infoMap.put("format", job.getFormat());
        infoMap.put("line_delimiter", job.getLineDelimiter());
        infoMap.put("columns", job.getColumns());
        infoMap.put("tablet_num", job.getTabletsNum());
        infoMap.put("max_file_size", job.getMaxFileSize());
        infoMap.put("delete_existing_files", job.getDeleteExistingFiles());
        jobInfo.add(new Gson().toJson(infoMap));
        // path
        jobInfo.add(job.getExportPath());

        jobInfo.add(TimeUtils.longToTimeString(job.getCreateTimeMs()));
        jobInfo.add(TimeUtils.longToTimeString(job.getStartTimeMs()));
        jobInfo.add(TimeUtils.longToTimeString(job.getFinishTimeMs()));
        jobInfo.add(job.getTimeoutSecond());

        // error msg
        if (job.getState() == ExportJobState.CANCELLED) {
            ExportFailMsg failMsg = job.getFailMsg();
            jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
        } else {
            jobInfo.add(FeConstants.null_string);
        }

        // outfileInfo
        if (job.getState() == ExportJobState.FINISHED) {
            jobInfo.add(job.getOutfileInfo());
        } else {
            jobInfo.add(FeConstants.null_string);
        }

        return jobInfo;
    }

    public void removeOldExportJobs() {
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            Iterator<Map.Entry<Long, ExportJob>> iter = exportIdToJob.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, ExportJob> entry = iter.next();
                ExportJob job = entry.getValue();
                if ((currentTimeMs - job.getCreateTimeMs()) / 1000 > Config.history_job_keep_max_second
                        && (job.getState() == ExportJobState.CANCELLED
                            || job.getState() == ExportJobState.FINISHED)) {
                    iter.remove();
                    labelToExportJobId.remove(job.getLabel(), job.getId());
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayCreateExportJob(ExportJob job) {
        writeLock();
        try {
            unprotectAddJob(job);
        } finally {
            writeUnlock();
        }
    }

    public void replayUpdateJobState(ExportJobStateTransfer stateTransfer) {
        readLock();
        try {
            ExportJob job = exportIdToJob.get(stateTransfer.getJobId());
            job.replayExportJobState(stateTransfer.getState());
            job.setStartTimeMs(stateTransfer.getStartTimeMs());
            job.setFinishTimeMs(stateTransfer.getFinishTimeMs());
            job.setFailMsg(stateTransfer.getFailMsg());
            job.setOutfileInfo(stateTransfer.getOutFileInfo());
        } finally {
            readUnlock();
        }
    }

    public long getJobNum(ExportJobState state, long dbId) {
        int size = 0;
        readLock();
        try {
            for (ExportJob job : exportIdToJob.values()) {
                if (job.getState() == state && job.getDbId() == dbId) {
                    ++size;
                }
            }
        } finally {
            readUnlock();
        }
        return size;
    }

    public long getJobNum(ExportJobState state) {
        int size = 0;
        readLock();
        try {
            for (ExportJob job : exportIdToJob.values()) {
                if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                        Env.getCurrentEnv().getCatalogMgr().getDbNullable(job.getDbId()).getFullName(),
                        PrivPredicate.LOAD)) {
                    continue;
                }

                if (job.getState() == state) {
                    ++size;
                }
            }
        } finally {
            readUnlock();
        }
        return size;
    }
}
