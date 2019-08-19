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

package org.apache.doris.task;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;

// A pull load task is used to process one table of this pull load job.
@Deprecated
public class PullLoadTask {
    private static final Logger LOG = LogManager.getLogger(PullLoadTask.class);
    // Input parameter
    public final long jobId;
    public final int taskId;
    public final Database db;
    public final OlapTable table;
    public final BrokerDesc brokerDesc;
    public final List<BrokerFileGroup> fileGroups;
    public final long jobDeadlineMs;

    private PullLoadTaskPlanner planner;

    // Useful things after executed
    private Map<String, Long> fileMap;
    private String trackingUrl;
    private Map<String, String> counters;
    private final long execMemLimit;

    // Runtime variables
    private enum State {
        RUNNING,
        FINISHED,
        FAILED,
        CANCELLED,
    }

    private TUniqueId queryId;
    private Coordinator curCoordinator;
    private State executeState = State.RUNNING;
    private Status executeStatus;
    private Thread curThread;

    public PullLoadTask(
            long jobId, int taskId,
            Database db, OlapTable table,
            BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
            long jobDeadlineMs, long execMemLimit) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.db = db;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.jobDeadlineMs = jobDeadlineMs;
        this.execMemLimit = execMemLimit;
    }

    public void init(List<List<TBrokerFileStatus>> fileStatusList, int fileNum) throws UserException {
        planner = new PullLoadTaskPlanner(this);
        planner.plan(fileStatusList, fileNum);
    }

    public Map<String, Long> getFileMap() {
        return fileMap;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public Map<String, String> getCounters() {
        return counters;
    }

    private long getLeftTimeMs() {
        if (jobDeadlineMs <= 0) {
            return Config.broker_load_default_timeout_second * 1000;
        }
        return jobDeadlineMs - System.currentTimeMillis();
    }

    public synchronized void cancel() {
        if (curCoordinator != null) {
            curCoordinator.cancel();
        }
    }

    public synchronized boolean isFinished() {
        return executeState == State.FINISHED;
    }

    public Status getExecuteStatus() {
        return executeStatus;
    }

    public synchronized void onCancelled(String reason) {
        if (executeState == State.RUNNING) {
            executeState = State.CANCELLED;
            executeStatus = Status.CANCELLED;
            LOG.info("cancel one pull load task({}). task id: {}, query id: {}, job id: {}",
                    reason, taskId, DebugUtil.printId(curCoordinator.getQueryId()), jobId);
        }
    }

    public synchronized void onFinished(Map<String, Long> fileMap,
                                        Map<String, String> counters,
                                        String trackingUrl) {
        if (executeState == State.RUNNING) {
            executeState = State.FINISHED;

            executeStatus = Status.OK;
            this.fileMap = fileMap;
            this.counters = counters;
            this.trackingUrl = trackingUrl;
            LOG.info("finished one pull load task. task id: {}, query id: {}, job id: {}",
                    taskId, DebugUtil.printId(curCoordinator.getQueryId()), jobId);
        }
    }

    public synchronized void onFailed(TUniqueId id, Status failStatus) {
        if (executeState == State.RUNNING) {
            if (id != null && !queryId.equals(id)) {
                return;
            }
            executeState = State.FAILED;
            executeStatus = failStatus;
            LOG.info("failed one pull load task({}). task id: {}, query id: {}, job id: {}",
                    failStatus.getErrorMsg(), taskId, id != null ? DebugUtil.printId(id) : "NaN", jobId);
        }
    }

    private void actualExecute() {
        int waitSecond = (int) (getLeftTimeMs() / 1000);
        if (waitSecond <= 0) {
            onCancelled("waiting timeout");
            return;
        }

        // TODO(zc): to refine coordinator
        try {
            curCoordinator.exec();
        } catch (Exception e) {
            LOG.warn("pull load task exec failed", e);
            onFailed(queryId, new Status(TStatusCode.INTERNAL_ERROR, "Coordinator execute failed: " + e.getMessage()));
            return;
        }

        if (curCoordinator.join(waitSecond)) {
            Status status = curCoordinator.getExecStatus();
            if (status.ok()) {
                Map<String, Long> resultFileMap = Maps.newHashMap();
                for (String file : curCoordinator.getDeltaUrls()) {
                    resultFileMap.put(file, -1L);
                }
                onFinished(resultFileMap, curCoordinator.getLoadCounters(), curCoordinator.getTrackingUrl());
            } else {
                onFailed(queryId, status);
            }
        } else {
            onCancelled("execution timeout");
        }
    }

    public void executeOnce() throws UserException {
        synchronized (this) {
            if (curThread != null) {
                throw new UserException("Task already executing.");
            }
            curThread = Thread.currentThread();
            executeState = State.RUNNING;
            executeStatus = Status.OK;

            // New one query id,
            UUID uuid = UUID.randomUUID();
            queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            curCoordinator = new Coordinator(jobId, queryId, planner.getDescTable(),
                    planner.getFragments(), planner.getScanNodes(), db.getClusterName(), TimeUtils.DEFAULT_TIME_ZONE);
            curCoordinator.setQueryType(TQueryType.LOAD);
            curCoordinator.setExecMemoryLimit(execMemLimit);
            curCoordinator.setTimeout((int) (getLeftTimeMs() / 1000));
        }

        boolean needUnregister = false;
        try {
            QeProcessorImpl.INSTANCE.registerQuery(queryId, curCoordinator);
            actualExecute();
            needUnregister = true;
        } catch (UserException e) {
            onFailed(queryId, new Status(TStatusCode.INTERNAL_ERROR, e.getMessage()));
        } finally {
            if (needUnregister) {
                QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
            }
            synchronized (this) {
                curThread = null;
                curCoordinator = null;
            }
        }
    }
}
