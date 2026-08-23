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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.job.base.Job;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.StreamingTaskStatus;
import org.apache.doris.job.cdc.request.CommitOffsetRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.WriteRecordRequest;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.jdbc.JdbcOffset;
import org.apache.doris.job.offset.jdbc.JdbcSourceOffsetProvider;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PRequestCdcClientResult;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * In PostgreSQL/MySQL, multi-table writes are performed by tasks that only make calls.
 * The write logic resides in the `cdc_client` and is implemented via `stream_load`.
 */
@Log4j2
@Getter
public class StreamingMultiTblTask extends AbstractStreamingTask {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private DataSourceType dataSourceType;
    private SourceOffsetProvider offsetProvider;
    private Map<String, String> sourceProperties;
    private Map<String, String> targetProperties;
    private String targetDb;
    private StreamingJobProperties jobProperties;
    private String cloudCluster;
    private long scannedRows = 0L;
    private long loadBytes = 0L;
    private long filteredRows = 0L;
    private long loadedRows = 0L;
    private volatile long runningBackendId;
    long lastScannedRows = -1;
    long lastProgressMs = 0;

    public StreamingMultiTblTask(Long jobId,
            long taskId,
            DataSourceType dataSourceType,
            SourceOffsetProvider offsetProvider,
            Map<String, String> sourceProperties,
            String targetDb,
            Map<String, String> targetProperties,
            StreamingJobProperties jobProperties,
            UserIdentity userIdentity,
            String cloudCluster) {
        super(jobId, taskId, userIdentity);
        this.dataSourceType = dataSourceType;
        this.offsetProvider = offsetProvider;
        this.sourceProperties = sourceProperties;
        this.targetProperties = targetProperties;
        this.jobProperties = jobProperties;
        this.targetDb = targetDb;
        this.cloudCluster = cloudCluster;
    }

    @Override
    public void before() throws Exception {
        if (getIsCanceled().get()) {
            log.info("streaming multi task has been canceled, task id is {}", getTaskId());
            return;
        }
        this.startTimeMs = System.currentTimeMillis();
        this.lastProgressMs = this.startTimeMs;
        this.status = TaskStatus.RUNNING;
        this.runningOffset = offsetProvider.getNextOffset(null, sourceProperties);
        log.info("streaming multi task {} get running offset: {}", taskId, runningOffset.toString());
    }

    @Override
    public void run() throws JobException {
        if (getIsCanceled().get()) {
            log.info("streaming task has been canceled, task id is {}", getTaskId());
            return;
        }
        sendWriteRequest();
    }

    private void sendWriteRequest() throws JobException {
        Backend backend = resolveBackend();
        log.info("start to run streaming multi task {} in backend {}/{}, offset is {}",
                taskId, backend.getId(), backend.getHost(), runningOffset.toString());
        this.runningBackendId = backend.getId();
        WriteRecordRequest params = buildRequestParams();
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/writeRecords")
                .setParams(new Gson().toJson(params)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future = BackendServiceProxy.getInstance()
                    .requestCdcClient(address, request, Config.streaming_cdc_heavy_rpc_timeout_sec);
            result = future.get(Config.streaming_cdc_heavy_rpc_timeout_sec, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.error("Failed to send write records request, {}", result.getStatus().getErrorMsgs(0));
                throw new JobException(
                        "Failed to send write records request," + result.getStatus().getErrorMsgs(0) + ", response: "
                                + result.getResponse());
            }
            String response = result.getResponse();
            try {
                ResponseBody<String> responseObj = objectMapper.readValue(
                        response,
                        new TypeReference<ResponseBody<String>>() {
                        }
                );
                if (responseObj.getCode() == RestApiStatusCode.OK.code) {
                    log.info("Send write records request successfully, response: {}", responseObj.getData());
                    return;
                }
            } catch (JsonProcessingException e) {
                log.warn("Failed to parse write records response: {}", response);
                throw new JobException("Failed to parse write records response: " + response);
            }
            throw new JobException("Failed to send write records request , error message: " + response);
        } catch (TimeoutException te) {
            log.warn("cdc_client RPC timeout api=/api/writeRecords taskId={} jobId={} backend={}:{} timeout_sec={}",
                    taskId, getJobId(), backend.getHost(), backend.getBrpcPort(),
                    Config.streaming_cdc_heavy_rpc_timeout_sec);
            // the request may have been dispatched and still running remotely
            noRetry = true;
            throw new JobException("cdc_client RPC timeout: /api/writeRecords taskId=" + taskId);
        } catch (ExecutionException | InterruptedException ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Send write request failed: ", ex);
            noRetry = true;
            throw new JobException(ex);
        }
    }

    private Backend resolveBackend() throws JobException {
        // Snapshot phase keeps per-round selection; binlog phase binds to a fixed BE for reuse.
        if (((JdbcOffset) runningOffset).snapshotSplit()) {
            return StreamingJobUtils.selectBackend(cloudCluster);
        }
        return getStreamingJob().resolveBoundBackend();
    }

    // Fail loud on a dropped/wrong-type job rather than return null and risk a downstream NPE.
    private StreamingInsertJob getStreamingJob() throws JobException {
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        if (job == null) {
            throw new JobException("Streaming job " + getJobId() + " not found");
        }
        return (StreamingInsertJob) job;
    }

    private String getToken() throws JobException {
        String token = "";
        try {
            // Acquire token from master
            token = Env.getCurrentEnv().getTokenManager().acquireToken();
        } catch (Exception e) {
            log.warn("Failed to get auth token:", e);
            throw new JobException(e.getMessage());
        }
        return token;
    }

    private WriteRecordRequest buildRequestParams() throws JobException {
        JdbcOffset offset = (JdbcOffset) runningOffset;
        WriteRecordRequest request = new WriteRecordRequest();
        request.setJobId(String.valueOf(getJobId()));
        request.setConfig(sourceProperties);

        request.setDataSource(dataSourceType.name());
        request.setTaskId(getTaskId() + "");
        request.setToken(getToken());
        request.setTargetDb(targetDb);

        Map<String, String> props = generateStreamLoadProps();
        request.setStreamLoadProps(props);

        //`meta` refers to the data synchronized by the job in this instance,
        // while `sourceProperties.offset` is the data entered by the user.
        Map<String, Object> splitMeta = offset.generateMeta();
        Preconditions.checkArgument(!splitMeta.isEmpty(), "split meta is empty");
        request.setMeta(splitMeta);
        String feAddr = Env.getCurrentEnv().getMasterHost() + ":" + Env.getCurrentEnv().getMasterHttpPort();
        request.setFrontendAddress(feAddr);
        request.setMaxInterval(jobProperties.getMaxIntervalSecond());
        request.setTaskTimeoutMs(getTaskTimeoutMs());
        request.setRebuildReader(getStreamingJob().isNeedRebuildReader());
        // Reader reuse applies only to the binlog phase (snapshot rebinds/closes per split).
        request.setReuseReader(!offset.snapshotSplit());
        if (offsetProvider instanceof JdbcSourceOffsetProvider) {
            String schemas = ((JdbcSourceOffsetProvider) offsetProvider).getTableSchemas();
            if (schemas != null) {
                request.setTableSchemas(schemas);
            }
        }
        return request;
    }

    private Map<String, String> generateStreamLoadProps() {
        Map<String, String> streamLoadProps = new HashMap<>();
        String maxFilterRatio =
                targetProperties.get(DataSourceConfigKeys.LOAD_PROPERTIES + LoadCommand.MAX_FILTER_RATIO_PROPERTY);

        if (StringUtils.isNotEmpty(maxFilterRatio) && Double.parseDouble(maxFilterRatio) > 0) {
            // If `load.max_filter_ratio` is set, it is calculated on the job side based on a window;
            // the `max_filter_ratio` of the streamload must be 1.
            streamLoadProps.put(LoadCommand.MAX_FILTER_RATIO_PROPERTY, "1");
        }

        String strictMode = targetProperties.get(DataSourceConfigKeys.LOAD_PROPERTIES + LoadCommand.STRICT_MODE);
        if (StringUtils.isNotEmpty(strictMode)) {
            streamLoadProps.put(LoadCommand.STRICT_MODE, strictMode);
        }
        return streamLoadProps;
    }

    @Override
    public boolean onSuccess() throws JobException {
        if (getIsCanceled().get()) {
            return false;
        }
        log.info("streaming multi task {} send write request run successfully.", getTaskId());
        return false;
    }

    /**
     * Callback function for offset commit success.
     */
    public void successCallback(CommitOffsetRequest offsetRequest) throws JobException {
        if (getIsCanceled().get()) {
            return;
        }
        this.status = TaskStatus.SUCCESS;
        this.finishTimeMs = System.currentTimeMillis();
        JdbcOffset runOffset = (JdbcOffset) this.runningOffset;
        if (!isCallable()) {
            return;
        }
        // set end offset to running offset
        // binlogSplit : [{"splitId":"binlog-split"}]  only 1 element
        // snapshotSplit:[{"splitId":"table-0"},...],...}]
        List<Map<String, String>> offsetMeta;
        try {
            offsetMeta = objectMapper.readValue(offsetRequest.getOffset(),
                    new TypeReference<List<Map<String, String>>>() {});
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse offset meta from request: {}", offsetRequest.getOffset(), e);
            throw new RuntimeException(e);
        }

        Preconditions.checkState(offsetMeta.size() == runOffset.getSplits().size(), "offset meta size "
                + offsetMeta.size() + " is not equal to running offset splits size "
                + runOffset.getSplits().size());

        if (runOffset.snapshotSplit()) {
            for (int i = 0; i < runOffset.getSplits().size(); i++) {
                SnapshotSplit split = (SnapshotSplit) runOffset.getSplits().get(i);
                Map<String, String> splitOffsetMeta = offsetMeta.get(i);
                String splitId = splitOffsetMeta.remove(JdbcSourceOffsetProvider.SPLIT_ID);
                Preconditions.checkState(split.getSplitId().equals(splitId),
                        "split id " + split.getSplitId() + " is not equal to offset meta split id " + splitId);
                split.setHighWatermark(splitOffsetMeta);
            }
        } else {
            Map<String, String> offsetMap = offsetMeta.get(0);
            String splitId = offsetMap.remove(JdbcSourceOffsetProvider.SPLIT_ID);
            Preconditions.checkState(BinlogSplit.BINLOG_SPLIT_ID.equals(splitId),
                    "split id is not equal to binlog split id");
            BinlogSplit split = (BinlogSplit) runOffset.getSplits().get(0);
            split.setEndingOffset(offsetMap);
        }

        this.scannedRows = offsetRequest.getScannedRows();
        this.loadBytes = offsetRequest.getLoadBytes();
        this.filteredRows = offsetRequest.getFilteredRows();
        this.loadedRows = offsetRequest.getLoadedRows();
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        if (null == job) {
            log.info("job is null, job id is {}", jobId);
            return;
        }
        StreamingInsertJob streamingInsertJob = (StreamingInsertJob) job;
        streamingInsertJob.onStreamTaskSuccess(this);
    }

    @Override
    protected void onFail(String errMsg) throws JobException {
        // Stop a possibly still-running reader now, so the PG slot frees before auto-resume re-acquires it.
        releaseRemoteReader();
        super.onFail(errMsg);
    }

    @Override
    public void cancel(boolean needWaitCancelComplete) {
        // No release here: drop/stop free via /api/close and manual pause via /api/releaseReader;
        // releasing in cancel would orphan the reused engine.
        super.cancel(needWaitCancelComplete);
    }

    @Override
    public void closeOrReleaseResources() {
        // No-op: the reader is async and reused; releasing here (per-iteration finally) would kill it.
    }

    @Override
    public long getRunningBackendId() {
        return runningBackendId;
    }

    // Best-effort release on runningBackendId (keep slot): on task failure to stop a stuck/zombie
    // reader early, and on manual pause so resume can rebind. Failures swallowed; idle reaper backs up.
    @Override
    public void releaseRemoteReader() {
        if (runningBackendId <= 0) {
            return;
        }
        Backend backend = Env.getCurrentSystemInfo().getBackend(runningBackendId);
        if (backend == null) {
            log.info("Skip releasing remote reader: backend {} not found, job {} task {}",
                    runningBackendId, getJobId(), getTaskId());
            return;
        }
        try {
            JobBaseConfig releaseParams = new JobBaseConfig(
                    String.valueOf(getJobId()), dataSourceType.name(), sourceProperties, getFrontendAddress());
            InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                    .setApi("/api/releaseReader/" + getTaskId())
                    .setParams(new Gson().toJson(releaseParams)).build();
            TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
            // Fire-and-forget: this runs under the job write lock, so never block on the result.
            BackendServiceProxy.getInstance()
                    .requestCdcClient(address, request, Config.streaming_cdc_light_rpc_timeout_sec);
            log.info("Sent release reader request to backend {}:{} for job {} task {}",
                    backend.getHost(), backend.getBrpcPort(), getJobId(), getTaskId());
        } catch (Exception ex) {
            log.warn("Release remote reader request failed for job {} task {}: ", getJobId(), getTaskId(), ex);
        }
    }

    private String getFrontendAddress() {
        return Env.getCurrentEnv().getMasterHost() + ":" + Env.getCurrentEnv().getMasterHttpPort();
    }

    // Local pre-check, no RPC: gates whether to pull real progress this tick.
    boolean isLocalTimeout() {
        if (startTimeMs == null) {
            return false;
        }
        return System.currentTimeMillis() - lastProgressMs > getTaskTimeoutMs();
    }

    boolean isTimeout(StreamingTaskStatus status) {
        if (startTimeMs == null) {
            // It's still pending, waiting for scheduling.
            return false;
        }
        long now = System.currentTimeMillis();
        if (status != null && status.getScannedRows() > lastScannedRows) {
            lastScannedRows = status.getScannedRows();
            lastProgressMs = now;
        }
        long timeoutMs = getTaskTimeoutMs();
        long elapsed = now - lastProgressMs;
        if (elapsed > timeoutMs) {
            log.info("Task {} timeout detected: no progress for {}ms, timeoutMs={}ms",
                    taskId, elapsed, timeoutMs);
            return true;
        }
        return false;
    }

    // Read multiplier live so config changes affect already-running tasks.
    private long getTaskTimeoutMs() {
        return Math.max(
                Config.streaming_task_timeout_multiplier * jobProperties.getMaxIntervalSecond() * 1000L,
                Config.streaming_task_min_timeout_sec * 1000L);
    }

    StreamingTaskStatus fetchTaskStatus() {
        if (runningBackendId <= 0) {
            return null;
        }
        Backend backend = Env.getCurrentSystemInfo().getBackend(runningBackendId);
        if (backend == null) {
            return null;
        }
        try {
            InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                    .setApi("/api/getTaskStatus/" + getTaskId())
                    .build();
            TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
            Future<PRequestCdcClientResult> future = BackendServiceProxy.getInstance()
                    .requestCdcClient(address, request, Config.streaming_cdc_light_rpc_timeout_sec);
            PRequestCdcClientResult result = future.get(Config.streaming_cdc_light_rpc_timeout_sec, TimeUnit.SECONDS);
            if (TStatusCode.findByValue(result.getStatus().getStatusCode()) != TStatusCode.OK) {
                return null;
            }
            ResponseBody<StreamingTaskStatus> body = objectMapper.readValue(
                    result.getResponse(),
                    new TypeReference<ResponseBody<StreamingTaskStatus>>() {
                    });
            return body.getCode() == RestApiStatusCode.OK.code ? body.getData() : null;
        } catch (Exception e) {
            log.warn("fetch task status failed, job {} task {}", getJobId(), getTaskId(), e);
            return null;
        }
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = super.getTvfInfo(jobName);
        trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        Map<String, Object> statistic = new HashMap<>();
        statistic.put("scannedRows", scannedRows);
        statistic.put("loadBytes", loadBytes);
        trow.addToColumnValue(new TCell().setStringVal(new Gson().toJson(statistic)));

        if (this.getUserIdentity() == null) {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(this.getUserIdentity().getQualifiedUser()));
        }
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(runningOffset == null
                ? FeConstants.null_string : runningOffset.showRange()));
        return trow;
    }
}
