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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService.Client;
import org.apache.doris.thrift.TDownloadType;
import org.apache.doris.thrift.TJobMeta;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TWarmUpTabletsRequest;
import org.apache.doris.thrift.TWarmUpTabletsRequestType;
import org.apache.doris.thrift.TWarmUpTabletsResponse;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @SerializedName(value = "jobId")
    protected long jobId;
    @SerializedName(value = "jobState")
    protected JobState jobState;
    @SerializedName(value = "createTimeMs")
    protected long createTimeMs = -1;

    @SerializedName(value = "errMsg")
    protected String errMsg = "";
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs = -1;

    @SerializedName(value = "cloudClusterName")
    protected String cloudClusterName = "";

    @SerializedName(value = "lastBatchId")
    protected long lastBatchId = -1;

    @SerializedName(value = "beToTabletIdBatches")
    protected Map<Long, List<List<Long>>> beToTabletIdBatches;

    @SerializedName(value = "beToThriftAddress")
    protected Map<Long, String> beToThriftAddress = new HashMap<>();

    @SerializedName(value = "JobType")
    protected JobType jobType;

    private Map<Long, Client> beToClient;

    private Map<Long, TNetworkAddress> beToAddr;

    private int maxRetryTime = 3;

    private int retryTime = 0;

    private boolean retry = false;

    private boolean setJobDone = false;

    public CloudWarmUpJob(long jobId, String cloudClusterName,
                                Map<Long, List<List<Long>>> beToTabletIdBatches, JobType jobType) {
        this.jobId = jobId;
        this.jobState = JobState.PENDING;
        this.cloudClusterName = cloudClusterName;
        this.beToTabletIdBatches = beToTabletIdBatches;
        this.createTimeMs = System.currentTimeMillis();
        this.jobType = jobType;
        if (!FeConstants.runningUnitTest) {
            List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                            .getBackendsByClusterName(cloudClusterName);
            for (Backend backend : backends) {
                beToThriftAddress.put(backend.getId(), backend.getHost() + ":" + backend.getBePort());
            }
        }
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

    public List<String> getJobInfo() {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(jobId));
        info.add(cloudClusterName);
        info.add(jobState.name());
        info.add(jobType.name());
        info.add(TimeUtils.longToTimeStringWithms(createTimeMs));
        info.add(Long.toString(lastBatchId + 1));
        long maxBatchSize = 0;
        for (List<List<Long>> list : beToTabletIdBatches.values()) {
            long size = list.size();
            if (size > maxBatchSize) {
                maxBatchSize = size;
            }
        }
        info.add(Long.toString(maxBatchSize));
        info.add(TimeUtils.longToTimeStringWithms(finishedTimeMs));
        info.add(errMsg);
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
        this.cloudClusterName = name;
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
        return (System.currentTimeMillis() - createTimeMs) / 1000 > Config.cloud_warm_up_timeout_second;
    }

    public boolean isExpire() {
        return isDone() && (System.currentTimeMillis() - finishedTimeMs) / 1000
                > Config.history_cloud_warm_up_job_keep_max_second;
    }

    public String getCloudClusterName() {
        return cloudClusterName;
    }

    public synchronized void run() {
        if (isTimeout()) {
            cancel("Timeout");
            return;
        }
        if (Config.isCloudMode()) {
            LOG.debug("set context to job");
            ConnectContext ctx = new ConnectContext();
            ctx.setThreadLocalInfo();
            ctx.setCloudCluster(cloudClusterName);
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

    public final synchronized boolean cancel(String errMsg) {
        if (this.jobState.isFinalState()) {
            return false;
        }
        try {
            initClients();
            for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
                request.setType(TWarmUpTabletsRequestType.CLEAR_JOB);
                request.setJobId(jobId);
                LOG.info("send warm up request. request_type=CLEAR_JOB");
                entry.getValue().warmUpTablets(request);
            }
        } catch (Exception e) {
            LOG.warn("warm up job {} cancel exception: {}", jobId, e.getMessage());
        } finally {
            releaseClients();
        }
        this.jobState = JobState.CANCELLED;
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        LOG.info("cancel cloud warm up job {}, err {}", jobId, errMsg);
        Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
        ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().getRunnableClusterSet().remove(this.cloudClusterName);
        return true;

    }

    private void runPendingJob() throws DdlException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);

        // Todo: nothing to prepare yet

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
        }
        return jobMetas;
    }

    private void runRunningJob() throws Exception {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);
        if (FeConstants.runningUnitTest) {
            Thread.sleep(1000);
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();
            ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().getRunnableClusterSet().remove(this.cloudClusterName);
            Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
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
                    LOG.info("send warm up request. job_id={}, batch_id={}, job_sizes={}, request_type=SET_JOB",
                                        jobId, request.batch_id, request.job_metas.size());
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
                    LOG.info("send warm up request. request_type=GET_CURRENT_JOB_STATE_AND_LEASE");
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
                            LOG.info("send warm up request. job_id={}, batch_id={}"
                                    + "job_sizes={}, request_type=SET_BATCH",
                                    jobId, request.batch_id, request.job_metas.size());
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
                        // release job
                        this.jobState = JobState.FINISHED;
                        for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                            TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
                            request.setType(TWarmUpTabletsRequestType.CLEAR_JOB);
                            request.setJobId(jobId);
                            LOG.info("send warm up request. request_type=CLEAR_JOB");
                            entry.getValue().warmUpTablets(request);
                        }
                        this.finishedTimeMs = System.currentTimeMillis();
                        releaseClients();
                        ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr()
                                .getRunnableClusterSet().remove(this.cloudClusterName);
                        Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
                    }
                }
            }
            if (changeToCancelState) {
                // release job
                this.jobState = JobState.CANCELLED;
                for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                    TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
                    request.setType(TWarmUpTabletsRequestType.CLEAR_JOB);
                    request.setJobId(jobId);
                    LOG.info("send warm up request. request_type=CLEAR_JOB");
                    entry.getValue().warmUpTablets(request);
                }
                this.finishedTimeMs = System.currentTimeMillis();
                releaseClients();
                ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr()
                        .getRunnableClusterSet().remove(this.cloudClusterName);
                Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
            }
        } catch (Exception e) {
            retryTime++;
            retry = true;
            if (retryTime < maxRetryTime) {
                LOG.warn("warm up job {} exception: {}", jobId, e.getMessage());
            } else {
                // retry three times and release job
                this.jobState = JobState.CANCELLED;
                this.finishedTimeMs = System.currentTimeMillis();
                this.errMsg = "retry the warm up job until max retry time " + String.valueOf(maxRetryTime);
                ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr()
                        .getRunnableClusterSet().remove(this.cloudClusterName);
                Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
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
