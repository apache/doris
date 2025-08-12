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

package org.apache.doris.cloud;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Triple;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService.Client;
import org.apache.doris.thrift.TDownloadType;
import org.apache.doris.thrift.TJobMeta;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TWarmUpEventType;
import org.apache.doris.thrift.TWarmUpTabletsRequest;
import org.apache.doris.thrift.TWarmUpTabletsRequestType;
import org.apache.doris.thrift.TWarmUpTabletsResponse;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CloudWarmUpJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(CloudWarmUpJob.class);

    public enum JobState {
        PENDING,
        RUNNING,
        FINISHED,
        CANCELLED,
        DELETED;
        public boolean isFinalState() {
            return this == JobState.FINISHED || this == JobState.CANCELLED || this == JobState.DELETED;
        }
    }

    public enum JobType {
        CLUSTER,
        TABLE;
    }

    public enum SyncMode {
        ONCE,
        PERIODIC,
        EVENT_DRIVEN;
    }

    public enum SyncEvent {
        LOAD,
        QUERY
    }

    @SerializedName(value = "jobId")
    protected long jobId;
    @SerializedName(value = "jobState")
    protected JobState jobState;
    @SerializedName(value = "createTimeMs")
    protected long createTimeMs = -1;
    @SerializedName(value = "startTimeMs")
    protected long startTimeMs = -1;

    @SerializedName(value = "errMsg")
    protected String errMsg = "";
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs = -1;

    @SerializedName(value = "srcClusterName")
    protected String srcClusterName = "";

    // the serialized name is kept for compatibility reasons
    @SerializedName(value = "cloudClusterName")
    protected String dstClusterName = "";

    @SerializedName(value = "lastBatchId")
    protected long lastBatchId = -1;

    @SerializedName(value = "beToTabletIdBatches")
    protected Map<Long, List<List<Long>>> beToTabletIdBatches = new HashMap<>();

    @SerializedName(value = "beToThriftAddress")
    protected Map<Long, String> beToThriftAddress = new HashMap<>();

    @SerializedName(value = "JobType")
    protected JobType jobType;

    @SerializedName(value = "tables")
    protected List<Triple<String, String, String>> tables = new ArrayList<>();

    @SerializedName(value = "force")
    protected boolean force = false;

    @SerializedName(value = "syncMode")
    protected SyncMode syncMode = SyncMode.ONCE;

    @SerializedName(value = "syncInterval")
    protected long syncInterval;

    @SerializedName(value = "syncEvent")
    protected SyncEvent syncEvent;

    private Map<Long, Client> beToClient;

    private Map<Long, TNetworkAddress> beToAddr;

    private int maxRetryTime = 3;

    private int retryTime = 0;

    private boolean retry = false;

    private boolean setJobDone = false;

    public static class Builder {
        private long jobId;
        private String srcClusterName;
        private String dstClusterName;
        private JobType jobType = JobType.CLUSTER;
        private SyncMode syncMode = SyncMode.ONCE;
        private SyncEvent syncEvent;
        private long syncInterval;

        public Builder() {}

        public Builder setJobId(long jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder setSrcClusterName(String srcClusterName) {
            this.srcClusterName = srcClusterName;
            return this;
        }

        public Builder setDstClusterName(String dstClusterName) {
            this.dstClusterName = dstClusterName;
            return this;
        }

        public Builder setJobType(JobType jobType) {
            this.jobType = jobType;
            return this;
        }

        public Builder setSyncMode(SyncMode syncMode) {
            this.syncMode = syncMode;
            return this;
        }

        public Builder setSyncEvent(SyncEvent syncEvent) {
            this.syncEvent = syncEvent;
            return this;
        }

        public Builder setSyncInterval(long syncInterval) {
            this.syncInterval = syncInterval;
            return this;
        }

        public CloudWarmUpJob build() {
            if (jobId == 0 || srcClusterName == null || dstClusterName == null || jobType == null || syncMode == null) {
                throw new IllegalStateException("Missing required fields for CloudWarmUpJob");
            }
            return new CloudWarmUpJob(this);
        }
    }

    private CloudWarmUpJob(Builder builder) {
        this.jobId = builder.jobId;
        this.jobState = JobState.PENDING;
        this.srcClusterName = builder.srcClusterName;
        this.dstClusterName = builder.dstClusterName;
        this.jobType = builder.jobType;
        this.syncMode = builder.syncMode;
        this.syncEvent = builder.syncEvent;
        this.syncInterval = builder.syncInterval;
        this.createTimeMs = System.currentTimeMillis();
    }

    private void fetchBeToThriftAddress() {
        String clusterName = isEventDriven() ? srcClusterName : dstClusterName;
        List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getBackendsByClusterName(clusterName);
        for (Backend backend : backends) {
            beToThriftAddress.put(backend.getId(), backend.getHost() + ":" + backend.getBePort());
        }
    }

    public CloudWarmUpJob(long jobId, String srcClusterName, String dstClusterName,
                                Map<Long, List<List<Long>>> beToTabletIdBatches, JobType jobType) {
        this.jobId = jobId;
        this.jobState = JobState.PENDING;
        this.srcClusterName = srcClusterName;
        this.dstClusterName = dstClusterName;
        this.beToTabletIdBatches = beToTabletIdBatches;
        this.createTimeMs = System.currentTimeMillis();
        this.jobType = jobType;
        if (!FeConstants.runningUnitTest) {
            List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                            .getBackendsByClusterName(dstClusterName);
            for (Backend backend : backends) {
                beToThriftAddress.put(backend.getId(), backend.getHost() + ":" + backend.getBePort());
            }
        }
    }

    public void fetchBeToTabletIdBatches() {
        if (FeConstants.runningUnitTest) {
            return;
        }
        if (jobType == JobType.TABLE) {
            // warm up with table will have to set tablets on creation
            return;
        }
        if (syncMode == null) {
            // This job was created by an old FE version.
            // It doesn't have the source cluster name, but tablets were already set.
            // Return for backward compatibility.
            return;
        }
        if (this.isEventDriven()) {
            // Event-driven jobs do not need to calculate tablets
            return;
        }
        CacheHotspotManager manager = ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr();
        Map<Long, List<Tablet>> beToWarmUpTablets =
                manager.warmUpNewClusterByCluster(dstClusterName, srcClusterName);
        long totalTablets = beToWarmUpTablets.values().stream()
                .mapToLong(List::size)
                .sum();
        beToTabletIdBatches = manager.splitBatch(beToWarmUpTablets);
        long totalBatches = beToTabletIdBatches.values().stream()
                .mapToLong(List::size)
                .sum();
        LOG.info("warm up job {} tablet num {}, batch num {}", jobId, totalTablets, totalBatches);
    }

    public boolean shouldWait() {
        if (!this.isPeriodic()) {
            return false;
        }
        if (this.jobState != JobState.PENDING) {
            return false;
        }
        long timeSinceLastStart = System.currentTimeMillis() - this.startTimeMs;
        if (timeSinceLastStart < this.syncInterval * 1000L) {
            return true;
        }
        return false;
    }

    public boolean isOnce() {
        return this.syncMode == SyncMode.ONCE || this.syncMode == null;
    }

    public boolean isPeriodic() {
        return this.syncMode == SyncMode.PERIODIC;
    }

    public boolean isEventDriven() {
        return this.syncMode == SyncMode.EVENT_DRIVEN;
    }

    public long getJobId() {
        return jobId;
    }

    public JobState getJobState() {
        return jobState;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public long getLastBatchId() {
        return lastBatchId;
    }

    public Map<Long, List<List<Long>>> getBeToTabletIdBatches() {
        return beToTabletIdBatches;
    }

    public Map<Long, String> getBeToThriftAddress() {
        return beToThriftAddress;
    }

    public JobType getJobType() {
        return jobType;
    }

    public SyncMode getSyncMode() {
        return syncMode;
    }

    public String getSyncModeString() {
        if (syncMode == null) {
            // For backward compatibility: older FE versions did not set syncMode for jobs,
            // so default to ONCE when syncMode is missing.
            return String.valueOf(SyncMode.ONCE);
        }
        StringBuilder sb = new StringBuilder().append(syncMode);
        switch (syncMode) {
            case PERIODIC:
                sb.append(" (");
                sb.append(syncInterval);
                sb.append("s)");
                break;
            case EVENT_DRIVEN:
                sb.append(" (");
                sb.append(syncEvent);
                sb.append(")");
                break;
            default:
                break;
        }
        return sb.toString();
    }

    public List<String> getJobInfo() {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(jobId));
        info.add(srcClusterName);
        info.add(dstClusterName);
        info.add(String.valueOf(jobState));
        info.add(String.valueOf(jobType));
        info.add(this.getSyncModeString());
        info.add(TimeUtils.longToTimeStringWithms(createTimeMs));
        info.add(TimeUtils.longToTimeStringWithms(startTimeMs));
        info.add(Long.toString(lastBatchId + 1));
        long maxBatchSize = 0;
        if (beToTabletIdBatches != null) {
            maxBatchSize = beToTabletIdBatches.values().stream()
                    .mapToLong(List::size)
                    .max()
                    .orElse(0);
        }
        info.add(Long.toString(maxBatchSize));
        info.add(TimeUtils.longToTimeStringWithms(finishedTimeMs));
        info.add(errMsg);
        info.add(tables == null ? "" : tables.stream()
                .map(t -> StringUtils.isEmpty(t.getRight())
                        ? t.getLeft() + "." + t.getMiddle()
                        : t.getLeft() + "." + t.getMiddle() + "." + t.getRight())
                .collect(Collectors.joining(", ")));
        return info;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public void setCreateTimeMs(long timeMs) {
        this.createTimeMs = timeMs;
    }

    public void setErrMsg(String msg) {
        this.errMsg = msg;
    }

    public void setFinishedTimeMs(long timeMs) {
        this.finishedTimeMs = timeMs;
    }

    public void setCloudClusterName(String name) {
        this.dstClusterName = name;
    }

    public void setLastBatchId(long id) {
        this.lastBatchId = id;
    }

    public void setBeToTabletIdBatches(Map<Long, List<List<Long>>> m) {
        this.beToTabletIdBatches = m;
    }

    public void setBeToThriftAddress(Map<Long, String> m) {
        this.beToThriftAddress = m;
    }

    public void setJobType(JobType t) {
        this.jobType = t;
    }

    public boolean isDone() {
        return jobState.isFinalState();
    }

    public boolean isTimeout() {
        return jobState == JobState.RUNNING
                && (System.currentTimeMillis() - startTimeMs) / 1000 > Config.cloud_warm_up_timeout_second;
    }

    public boolean isExpire() {
        return isDone() && (System.currentTimeMillis() - finishedTimeMs) / 1000
                > Config.history_cloud_warm_up_job_keep_max_second;
    }

    public String getDstClusterName() {
        return dstClusterName;
    }

    public String getSrcClusterName() {
        return srcClusterName;
    }

    public synchronized void run() {
        if (isTimeout()) {
            cancel("Timeout", false);
            return;
        }
        if (Config.isCloudMode()) {
            LOG.debug("set context to job");
            ConnectContext ctx = new ConnectContext();
            ctx.setThreadLocalInfo();
            ctx.setCloudCluster(dstClusterName);
        }
        try {
            switch (jobState) {
                case PENDING:
                    runPendingJob();
                    break;
                case RUNNING:
                    runRunningJob();
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("state {} exception {}", jobState, e.getMessage());
        } finally {
            if (Config.isCloudMode()) {
                LOG.debug("remove context from job");
                ConnectContext.remove();
            }
        }
    }

    public void initClients() throws Exception {
        if (beToThriftAddress == null || beToThriftAddress.isEmpty()) {
            fetchBeToThriftAddress();
        }
        if (beToClient == null) {
            beToClient = new HashMap<>();
            beToAddr = new HashMap<>();
        }
        if (beToClient.isEmpty()) {
            for (Map.Entry<Long, String> entry : beToThriftAddress.entrySet()) {
                boolean ok = false;
                TNetworkAddress address = null;
                Client client = null;
                try {
                    String[] ipPort = entry.getValue().split(":");
                    address = new TNetworkAddress(ipPort[0], Integer.parseInt(ipPort[1]));
                    beToAddr.put(entry.getKey(), address);
                    client = ClientPool.backendPool.borrowObject(address);
                    beToClient.put(entry.getKey(), client);
                    ok = true;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    if (!ok) {
                        ClientPool.backendPool.invalidateObject(address, client);
                        releaseClients();
                    }
                }
            }
        }
    }

    public void releaseClients() {
        if (beToClient != null) {
            for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                ClientPool.backendPool.returnObject(beToAddr.get(entry.getKey()),
                        entry.getValue());
            }
        }
        beToClient = null;
        beToAddr = null;
    }

    private final void clearJobOnBEs() {
        try {
            initClients();
            for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
                request.setType(TWarmUpTabletsRequestType.CLEAR_JOB);
                request.setJobId(jobId);
                if (this.isEventDriven()) {
                    TWarmUpEventType event = getTWarmUpEventType();
                    if (event == null) {
                        throw new IllegalArgumentException("Unknown SyncEvent " + syncEvent);
                    }
                    request.setEvent(event);
                }
                LOG.info("send warm up request to BE {}. job_id={}, request_type=CLEAR_JOB",
                        entry.getKey(), jobId);
                entry.getValue().warmUpTablets(request);
            }
        } catch (Exception e) {
            LOG.warn("send warm up request failed. job_id={}, request_type=CLEAR_JOB, exception={}",
                    jobId, e.getMessage());
        } finally {
            releaseClients();
        }
    }

    public final synchronized boolean cancel(String errMsg, boolean force) {
        if (this.jobState.isFinalState()) {
            return false;
        }
        if (this.jobState == JobState.PENDING) {
            // BE haven't started this job yet, skip RPC
        } else {
            clearJobOnBEs();
        }
        if (this.isOnce() || force) {
            this.jobState = JobState.CANCELLED;
        } else {
            this.jobState = JobState.PENDING;
        }
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        MetricRepo.updateClusterWarmUpJobLastFinishTime(String.valueOf(jobId), srcClusterName,
                dstClusterName, finishedTimeMs);
        LOG.info("cancel cloud warm up job {}, err {}", jobId, errMsg);
        Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
        ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().notifyJobStop(this);
        return true;
    }

    private void runPendingJob() throws DdlException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);

        // make sure only one job runs concurrently for one destination cluster
        if (!((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().tryRegisterRunningJob(this)) {
            return;
        }

        // Todo: nothing to prepare yet
        this.setJobDone = false;
        this.lastBatchId = -1;
        this.startTimeMs = System.currentTimeMillis();
        MetricRepo.updateClusterWarmUpJobLatestStartTime(String.valueOf(jobId), srcClusterName,
                dstClusterName, startTimeMs);
        this.fetchBeToTabletIdBatches();
        long totalTablets = beToTabletIdBatches.values().stream()
                .flatMap(List::stream)
                .mapToLong(List::size)
                .sum();
        MetricRepo.increaseClusterWarmUpJobRequestedTablets(dstClusterName, totalTablets);
        MetricRepo.increaseClusterWarmUpJobExecCount(dstClusterName);
        this.jobState = JobState.RUNNING;
        Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
        LOG.info("transfer cloud warm up job {} state to {}", jobId, this.jobState);
    }

    private List<TJobMeta> buildJobMetas(long beId, long batchId) {
        List<TJobMeta> jobMetas = new ArrayList<>();
        List<List<Long>> tabletIdBatches = beToTabletIdBatches.get(beId);
        if (batchId < tabletIdBatches.size()) {
            List<Long> tabletIds = tabletIdBatches.get((int) batchId);
            TJobMeta jobMeta = new TJobMeta();
            jobMeta.setDownloadType(TDownloadType.S3);
            jobMeta.setTabletIds(tabletIds);
            jobMetas.add(jobMeta);
            MetricRepo.increaseClusterWarmUpJobFinishedTablets(dstClusterName, tabletIds.size());
        }
        return jobMetas;
    }

    private TWarmUpEventType getTWarmUpEventType() {
        switch (syncEvent) {
            case LOAD:
                return TWarmUpEventType.LOAD;
            case QUERY:
                return TWarmUpEventType.QUERY;
            default:
                return null;
        }
    }

    private void runEventDrivenJob() throws Exception {
        try {
            initClients();
            for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
                request.setType(TWarmUpTabletsRequestType.SET_JOB);
                request.setJobId(jobId);
                TWarmUpEventType event = getTWarmUpEventType();
                if (event == null) {
                    throw new IllegalArgumentException("Unknown SyncEvent " + syncEvent);
                }
                request.setEvent(event);
                LOG.debug("send warm up request to BE {}. job_id={}, event={}, request_type=SET_JOB(EVENT)",
                        entry.getKey(), jobId, syncEvent);
                TWarmUpTabletsResponse response = entry.getValue().warmUpTablets(request);
                if (response.getStatus().getStatusCode() != TStatusCode.OK) {
                    if (!response.getStatus().getErrorMsgs().isEmpty()) {
                        errMsg = response.getStatus().getErrorMsgs().get(0);
                    }
                    LOG.warn("send warm up request failed. job_id={}, event={}, err={}",
                            jobId, syncEvent, errMsg);
                }
            }
        } catch (Exception e) {
            LOG.warn("send warm up request job_id={} failed with exception {}",
                    jobId, e);
        } finally {
            releaseClients();
        }
    }

    private void runRunningJob() throws Exception {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);
        if (FeConstants.runningUnitTest) {
            Thread.sleep(1000);
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();
            ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().notifyJobStop(this);
            Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
            return;
        }
        if (this.isEventDriven()) {
            runEventDrivenJob();
            return;
        }
        boolean changeToCancelState = false;
        try {
            initClients();
            // If there is first batch, send SET_JOB RPC
            if (lastBatchId == -1 && !setJobDone) {
                setJobDone = true;
                for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                    TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
                    request.setType(TWarmUpTabletsRequestType.SET_JOB);
                    request.setJobId(jobId);
                    request.setBatchId(lastBatchId + 1);
                    request.setJobMetas(buildJobMetas(entry.getKey(), request.batch_id));
                    LOG.info("send warm up request to BE {}. job_id={}, batch_id={}"
                            + ", job_size={}, request_type=SET_JOB",
                            entry.getKey(), jobId, request.batch_id, request.job_metas.size());
                    TWarmUpTabletsResponse response = entry.getValue().warmUpTablets(request);
                    if (response.getStatus().getStatusCode() != TStatusCode.OK) {
                        if (!response.getStatus().getErrorMsgs().isEmpty()) {
                            errMsg = response.getStatus().getErrorMsgs().get(0);
                        }
                        changeToCancelState = true;
                    }
                }
            } else {
                // Check the batches of all BEs done
                boolean allLastBatchDone = true;
                for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                    TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
                    request.setType(TWarmUpTabletsRequestType.GET_CURRENT_JOB_STATE_AND_LEASE);
                    LOG.info("send warm up request to BE {}. job_id={}, request_type=GET_CURRENT_JOB_STATE_AND_LEASE",
                            entry.getKey(), jobId);
                    TWarmUpTabletsResponse response = entry.getValue().warmUpTablets(request);
                    if (response.getStatus().getStatusCode() != TStatusCode.OK) {
                        if (!response.getStatus().getErrorMsgs().isEmpty()) {
                            errMsg = response.getStatus().getErrorMsgs().get(0);
                        }
                        changeToCancelState = true;
                    }
                    if (!changeToCancelState && response.pending_job_size != 0) {
                        allLastBatchDone = false;
                        break;
                    }
                    // /api/debug_point/add/CloudWarmUpJob.FakeLastBatchNotDone
                    if (DebugPointUtil.isEnable("CloudWarmUpJob.FakeLastBatchNotDone")) {
                        allLastBatchDone = false;
                        LOG.info("DebugPoint:CloudWarmUpJob.FakeLastBatchNotDone, jobID={}", jobId);
                        break;
                    }
                }
                if (!changeToCancelState && allLastBatchDone) {
                    if (retry) {
                        // RPC failed, retry
                        retry = false;
                    } else {
                        // last batch is done, log and do next batch
                        lastBatchId++;
                        Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
                    }
                    boolean allBatchesDone = true;
                    for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                        TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
                        request.setType(TWarmUpTabletsRequestType.SET_BATCH);
                        request.setJobId(jobId);
                        request.setBatchId(lastBatchId + 1);
                        request.setJobMetas(buildJobMetas(entry.getKey(), request.batch_id));
                        if (!request.job_metas.isEmpty()) {
                            // check all batches is done or not
                            allBatchesDone = false;
                            LOG.info("send warm up request to BE {}. job_id={}, batch_id={}"
                                    + ", job_size={}, request_type=SET_BATCH",
                                    entry.getKey(), jobId, request.batch_id, request.job_metas.size());
                            TWarmUpTabletsResponse response = entry.getValue().warmUpTablets(request);
                            if (response.getStatus().getStatusCode() != TStatusCode.OK) {
                                if (!response.getStatus().getErrorMsgs().isEmpty()) {
                                    errMsg = response.getStatus().getErrorMsgs().get(0);
                                }
                                changeToCancelState = true;
                            }
                        }
                    }
                    if (allBatchesDone) {
                        clearJobOnBEs();
                        this.finishedTimeMs = System.currentTimeMillis();
                        if (this.isPeriodic()) {
                            // wait for next schedule
                            this.jobState = JobState.PENDING;
                        } else {
                            // release job
                            this.jobState = JobState.FINISHED;
                        }
                        ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().notifyJobStop(this);
                        Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
                    }
                }
            }
            if (changeToCancelState) {
                // release job
                cancel("job fail", false);
            }
        } catch (Exception e) {
            retryTime++;
            retry = true;
            if (retryTime < maxRetryTime) {
                LOG.warn("warm up job {} exception: {}", jobId, e.getMessage());
            } else {
                // retry three times and release job
                cancel("retry the warm up job until max retry time " + String.valueOf(maxRetryTime), false);
            }
            releaseClients();
        }
    }

    public void replay() throws Exception {
       // No need to replay anything yet
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, CloudWarmUpJob.class);
        Text.writeString(out, json);
    }

    public static CloudWarmUpJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CloudWarmUpJob.class);
    }
}
