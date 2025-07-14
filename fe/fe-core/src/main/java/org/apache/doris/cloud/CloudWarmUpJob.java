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
import org.apache.doris.common.Pair;
import org.apache.doris.common.Triple;
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

    @SerializedName("beToTabletIdBatchesWithSize")
    protected Map<Long, List<Pair<List<Long>, Long>>> beToTabletIdBatchesWithSize;

    @SerializedName(value = "beToThriftAddress")
    protected Map<Long, String> beToThriftAddress = new HashMap<>();

    @SerializedName(value = "JobType")
    protected JobType jobType;

    @SerializedName(value = "tables")
    protected List<Triple<String, String, String>> tables = new ArrayList<>();

    @SerializedName(value = "force")
    protected boolean force = false;

    @SerializedName("beToDataSizeFinished")
    protected Map<Long, Long> beToDataSizeFinished = new HashMap<>();

    @SerializedName(value = "beToLastFinishedBatchId")
    private Map<Long, Long> beToLastFinishedBatchId = new HashMap<>();

    @SerializedName(value = "beIsRunning")
    private Map<Long, Boolean> beIsRunning = new HashMap<>();

    private Map<Long, Client> beToClient;

    private Map<Long, TNetworkAddress> beToAddr;

    private int maxRetryTime = 3;

    private int retryTime = 0;

    private boolean retry = false;

    private boolean setJobDone = false;

    public CloudWarmUpJob(long jobId, String cloudClusterName,
                      Map<Long, List<Pair<List<Long>, Long>>> beToTabletIdBatchesWithSize,
                      JobType jobType) {
        this.jobId = jobId;
        this.jobState = JobState.PENDING;
        this.cloudClusterName = cloudClusterName;
        this.beToTabletIdBatchesWithSize = beToTabletIdBatchesWithSize;
        this.createTimeMs = System.currentTimeMillis();
        this.jobType = jobType;

        if (!FeConstants.runningUnitTest) {
            List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                        .getBackendsByClusterName(cloudClusterName);
            for (Backend backend : backends) {
                beToThriftAddress.put(backend.getId(), backend.getHost() + ":" + backend.getBePort());
            }
        }

        for (Long beId : beToTabletIdBatchesWithSize.keySet()) {
            beToLastFinishedBatchId.put(beId, -1L);
            beToDataSizeFinished.put(beId, 0L);
            beIsRunning.put(beId, false);
        }
    }

    public CloudWarmUpJob(long jobId, String cloudClusterName,
                          Map<Long, List<Pair<List<Long>, Long>>> beToTabletIdBatchesWithSize,
                          JobType jobType,
                          List<Triple<String, String, String>> tables, boolean force) {
        this(jobId, cloudClusterName, beToTabletIdBatchesWithSize, jobType);
        this.tables = tables;
        this.force = force;
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

    public Map<Long, List<Pair<List<Long>, Long>>> getBeToTabletIdBatchesWithSize() {
        return beToTabletIdBatchesWithSize;
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

        // Determine maximum batch ID completed across all BEs
        long maxFinishedBatchId = beToLastFinishedBatchId.values().stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(-1);
        info.add(Long.toString(maxFinishedBatchId + 1));

        // Determine maximum total batches across all BEs
        long maxBatchCount = beToTabletIdBatchesWithSize.values().stream()
                .mapToLong(List::size).max().orElse(0);
        info.add(Long.toString(maxBatchCount));

        info.add(TimeUtils.longToTimeStringWithms(finishedTimeMs));

        // Compute progress in human-readable data size
        long totalSizeBytes = 0;
        long finishedSizeBytes = 0;

        for (Map.Entry<Long, List<Pair<List<Long>, Long>>> entry : beToTabletIdBatchesWithSize.entrySet()) {
            long beId = entry.getKey();
            List<Pair<List<Long>, Long>> batches = entry.getValue();
            totalSizeBytes += batches.stream().mapToLong(p -> p.second).sum();

            long finishedIdx = beToLastFinishedBatchId.getOrDefault(beId, -1L);
            for (int i = 0; i <= finishedIdx && i < batches.size(); i++) {
                finishedSizeBytes += batches.get(i).second;
            }
        }

        String progress = String.format("Finished %.2f GB / Total %.2f GB",
                finishedSizeBytes / (1024.0 * 1024 * 1024),
                totalSizeBytes / (1024.0 * 1024 * 1024));
        info.add(progress);

        info.add(errMsg);
        info.add(tables.stream()
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
        this.cloudClusterName = name;
    }

    public void setLastBatchId(long id) {
        this.lastBatchId = id;
    }

    public void setBeToTabletIdBatchesWithSize(Map<Long, List<Pair<List<Long>, Long>>> beToTabletIdBatchesWithSize) {
        this.beToTabletIdBatchesWithSize = beToTabletIdBatchesWithSize;
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

    private List<TJobMeta> buildJobMetasFromPair(long beId, long batchId) {
        List<TJobMeta> jobMetas = new ArrayList<>();
        List<Pair<List<Long>, Long>> tabletBatches = beToTabletIdBatchesWithSize.get(beId);

        if (batchId < tabletBatches.size()) {
            List<Long> tabletIds = tabletBatches.get((int) batchId).first;
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

        try {
            initClients();

            for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
                long beId = entry.getKey();
                Client client = entry.getValue();

                // Step 1: Poll BE status if currently running
                if (beIsRunning.getOrDefault(beId, false)) {
                    TWarmUpTabletsRequest stateReq = new TWarmUpTabletsRequest();
                    stateReq.setType(TWarmUpTabletsRequestType.GET_CURRENT_JOB_STATE_AND_LEASE);
                    TWarmUpTabletsResponse stateResp = client.warmUpTablets(stateReq);

                    if (stateResp.getStatus().getStatusCode() != TStatusCode.OK) {
                        throw new RuntimeException(stateResp.getStatus().getErrorMsgs().toString());
                    }

                    if (stateResp.pending_job_size == 0) {
                        // BE finished its current batch
                        long finishedBatchId = beToLastFinishedBatchId.getOrDefault(beId, -1L) + 1;
                        beToLastFinishedBatchId.put(beId, finishedBatchId);
                        beIsRunning.put(beId, false);

                        // Update finished data size
                        long size = beToTabletIdBatchesWithSize.get(beId).get((int) finishedBatchId).second;
                        beToDataSizeFinished.merge(beId, size, Long::sum);
                        LOG.info("BE {} finished batch {}", beId, finishedBatchId);

                        Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
                    }
                }

                // Step 2: If BE is idle, dispatch next batch if available
                if (!beIsRunning.getOrDefault(beId, false)) {
                    List<Pair<List<Long>, Long>> batches = beToTabletIdBatchesWithSize.get(beId);
                    long nextBatchId = beToLastFinishedBatchId.getOrDefault(beId, -1L) + 1;

                    if (batches != null && nextBatchId < batches.size()) {
                        TWarmUpTabletsRequest setBatchReq = new TWarmUpTabletsRequest();
                        setBatchReq.setType(TWarmUpTabletsRequestType.SET_BATCH);
                        setBatchReq.setJobId(jobId);
                        setBatchReq.setBatchId(nextBatchId);
                        setBatchReq.setJobMetas(buildJobMetasFromPair(beId, nextBatchId));

                        LOG.info("Dispatching batch {} to BE {}", nextBatchId, beId);
                        TWarmUpTabletsResponse resp = client.warmUpTablets(setBatchReq);

                        if (resp.getStatus().getStatusCode() != TStatusCode.OK) {
                            throw new RuntimeException(resp.getStatus().getErrorMsgs().toString());
                        }

                        beIsRunning.put(beId, true);
                    }
                }
            }

            // Step 3: Check if all batches on all BEs are finished
            boolean allFinished = true;
            for (Map.Entry<Long, List<Pair<List<Long>, Long>>> entry : beToTabletIdBatchesWithSize.entrySet()) {
                long beId = entry.getKey();
                int totalBatches = entry.getValue().size();
                long finishedBatches = beToLastFinishedBatchId.getOrDefault(beId, -1L) + 1;

                if (finishedBatches < totalBatches) {
                    allFinished = false;
                    break;
                }
            }

            if (allFinished) {
                LOG.info("All batches completed for job {}", jobId);
                for (Client client : beToClient.values()) {
                    TWarmUpTabletsRequest clearReq = new TWarmUpTabletsRequest();
                    clearReq.setType(TWarmUpTabletsRequestType.CLEAR_JOB);
                    clearReq.setJobId(jobId);
                    client.warmUpTablets(clearReq);
                }

                this.jobState = JobState.FINISHED;
                this.finishedTimeMs = System.currentTimeMillis();
                Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
                ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().getRunnableClusterSet().remove(cloudClusterName);
                releaseClients();
            }

        } catch (Exception e) {
            retryTime++;
            retry = true;

            if (retryTime < maxRetryTime) {
                LOG.warn("Warm-up job {}: exception during run: {}", jobId, e.getMessage());
            } else {
                this.jobState = JobState.CANCELLED;
                this.finishedTimeMs = System.currentTimeMillis();
                this.errMsg = "Exceeded max retry attempts";
                Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(this);
                ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().getRunnableClusterSet().remove(cloudClusterName);
                releaseClients();
            }
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
