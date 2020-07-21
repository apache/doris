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

import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

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
import java.util.stream.Collectors;

public class ExportMgr {
    private static final Logger LOG = LogManager.getLogger(ExportJob.class);

    // lock for export job
    // lock is private and must use after db lock
    private ReentrantReadWriteLock lock;

    private Map<Long, ExportJob> idToJob; // exportJobId to exportJob

    public ExportMgr() {
        idToJob = Maps.newHashMap();
        lock = new ReentrantReadWriteLock(true);
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

    public Map<Long, ExportJob> getIdToJob() {
        return idToJob;
    }

    public void addExportJob(ExportStmt stmt) throws Exception {
        long jobId = Catalog.getCurrentCatalog().getNextId();
        ExportJob job = createJob(jobId, stmt);
        writeLock();
        try {
            unprotectAddJob(job);
            Catalog.getCurrentCatalog().getEditLog().logExportCreate(job);
        } finally {
            writeUnlock();
        }
        LOG.info("add export job. {}", job);
    }

    public void unprotectAddJob(ExportJob job) {
        idToJob.put(job.getId(), job);
    }

    private ExportJob createJob(long jobId, ExportStmt stmt) throws Exception {
        ExportJob job = new ExportJob(jobId);
        job.setJob(stmt);
        return job;
    }

    public List<ExportJob> getExportJobs(ExportJob.JobState state) {
        List<ExportJob> result = Lists.newArrayList();
        readLock();
        try {
            for (ExportJob job : idToJob.values()) {
                if (job.getState() == state) {
                    result.add(job);
                }
            }
        } finally {
            readUnlock();
        }

        return result;
    }

    // NOTE: jobid and states may both specified, or only one of them, or neither
    public List<List<String>> getExportJobInfosByIdOrState(
            long dbId, long jobId, Set<ExportJob.JobState> states,
            ArrayList<OrderByPair> orderByPairs, long limit) {

        long resultNum = limit == -1L ? Integer.MAX_VALUE : limit;
        LinkedList<List<Comparable>> exportJobInfos = new LinkedList<List<Comparable>>();
        readLock();
        try {
            int counter = 0;
            for (ExportJob job : idToJob.values()) {
                long id = job.getId();
                ExportJob.JobState state = job.getState();

                if (job.getDbId() != dbId) {
                    continue;
                }

                if (jobId != 0) {
                    if (id != jobId) {
                        continue;
                    }
                }

                // check auth
                TableName tableName = job.getTableName();
                if (tableName == null || tableName.getTbl().equals("DUMMY")) {
                    // forward compatibility, no table name is saved before
                    Database db = Catalog.getCurrentCatalog().getDb(dbId);
                    if (db == null) {
                        continue;
                    }
                    if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(),
                                                                           db.getFullName(), PrivPredicate.SHOW)) {
                        continue;
                    }
                } else {
                    if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                            tableName.getDb(), tableName.getTbl(),
                                                                            PrivPredicate.SHOW)) {
                        continue;
                    }
                }

                if (states != null) {
                    if (!states.contains(state)) {
                        continue;
                    }
                }

                List<Comparable> jobInfo = new ArrayList<Comparable>();

                jobInfo.add(id);
                jobInfo.add(state.name());
                jobInfo.add(job.getProgress() + "%");

                // task infos
                Map<String, Object> infoMap = Maps.newHashMap();
                List<String> partitions = job.getPartitions();
                if (partitions == null) {
                    partitions = Lists.newArrayList();
                    partitions.add("*");
                }
                infoMap.put("db", job.getTableName().getDb());
                infoMap.put("tbl", job.getTableName().getTbl());
                infoMap.put("partitions", partitions);
                infoMap.put("broker", job.getBrokerDesc().getName());
                infoMap.put("column separator", job.getColumnSeparator());
                infoMap.put("line delimiter", job.getLineDelimiter());
                infoMap.put("exec mem limit", job.getExecMemLimit());
                infoMap.put("coord num", job.getCoordList().size());
                infoMap.put("tablet num", job.getTabletLocations() == null ? -1 : job.getTabletLocations().size());
                jobInfo.add(new Gson().toJson(infoMap));
                // path
                jobInfo.add(job.getExportPath());

                jobInfo.add(TimeUtils.longToTimeString(job.getCreateTimeMs()));
                jobInfo.add(TimeUtils.longToTimeString(job.getStartTimeMs()));
                jobInfo.add(TimeUtils.longToTimeString(job.getFinishTimeMs()));
                jobInfo.add(job.getTimeoutSecond());

                // error msg
                if (job.getState() == ExportJob.JobState.CANCELLED) {
                    ExportFailMsg failMsg = job.getFailMsg();
                    jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
                } else {
                    jobInfo.add(FeConstants.null_string);
                }

                exportJobInfos.add(jobInfo);

                if (counter++ >= resultNum) {
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
        for (List<Comparable> list : exportJobInfos) {
            results.add(list.stream().map(e -> e.toString()).collect(Collectors.toList()));
        }

        return results;
    }

    public void removeOldExportJobs() {
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            Iterator<Map.Entry<Long, ExportJob>> iter = idToJob.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, ExportJob> entry = iter.next();
                ExportJob job = entry.getValue();
                if ((currentTimeMs - job.getCreateTimeMs()) / 1000 > Config.history_job_keep_max_second
                        && (job.getState() == ExportJob.JobState.CANCELLED
                            || job.getState() == ExportJob.JobState.FINISHED)) {
                    iter.remove();
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

    public void replayUpdateJobState(long jobId, ExportJob.JobState newState) {
        writeLock();
        try {
            ExportJob job = idToJob.get(jobId);
            job.updateState(newState, true);
        } finally {
            writeUnlock();
        }
    }

    public long getJobNum(ExportJob.JobState state, long dbId) {
        int size = 0;
        readLock();
        try {
            for (ExportJob job : idToJob.values()) {
                if (job.getState() == state && job.getDbId() == dbId) {
                    ++size;
                }
            }
        } finally {
            readUnlock();
        }
        return size;
    }
}
