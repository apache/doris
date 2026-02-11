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
import org.apache.doris.job.cdc.request.CommitOffsetRequest;
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
    private long scannedRows = 0L;
    private long loadBytes = 0L;
    private long filteredRows = 0L;
    private long loadedRows = 0L;
    private long timeoutMs;
    private long runningBackendId;

    public StreamingMultiTblTask(Long jobId,
            long taskId,
            DataSourceType dataSourceType,

            SourceOffsetProvider offsetProvider,
            Map<String, String> sourceProperties,
            String targetDb,
            Map<String, String> targetProperties,
            StreamingJobProperties jobProperties,
            UserIdentity userIdentity) {
        super(jobId, taskId, userIdentity);
        this.dataSourceType = dataSourceType;
        this.offsetProvider = offsetProvider;
        this.sourceProperties = sourceProperties;
        this.targetProperties = targetProperties;
        this.jobProperties = jobProperties;
        this.targetDb = targetDb;
        this.timeoutMs = Config.streaming_task_timeout_multiplier * jobProperties.getMaxIntervalSecond() * 1000L;
    }

    @Override
    public void before() throws Exception {
        if (getIsCanceled().get()) {
            log.info("streaming multi task has been canceled, task id is {}", getTaskId());
            return;
        }
        this.status = TaskStatus.RUNNING;
        this.startTimeMs = System.currentTimeMillis();
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
        Backend backend = StreamingJobUtils.selectBackend();
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
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
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
        } catch (ExecutionException | InterruptedException ex) {
            log.error("Send write request failed: ", ex);
            throw new JobException(ex);
        }
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
        request.setJobId(getJobId());
        request.setConfig(sourceProperties);
        request.setDataSource(dataSourceType.name());

        request.setTaskId(getTaskId() + "");
        request.setToken(getToken());
        request.setTargetDb(targetDb);

        Map<String, String> props = generateStreamLoadProps();
        request.setStreamLoadProps(props);

        Map<String, Object> splitMeta = offset.generateMeta();
        Preconditions.checkArgument(!splitMeta.isEmpty(), "split meta is empty");
        request.setMeta(splitMeta);
        String feAddr = Env.getCurrentEnv().getMasterHost() + ":" + Env.getCurrentEnv().getMasterHttpPort();
        request.setFrontendAddress(feAddr);
        request.setMaxInterval(jobProperties.getMaxIntervalSecond());
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
    public void successCallback(CommitOffsetRequest offsetRequest) {
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
        super.onFail(errMsg);
    }

    @Override
    public void cancel(boolean needWaitCancelComplete) {
        // No manual cancellation is required; the task ID will be checked for consistency in the beforeCommit function.
        super.cancel(needWaitCancelComplete);
    }

    @Override
    public void closeOrReleaseResources() {
        // no need
    }

    public boolean isTimeout() {
        if (startTimeMs == null) {
            // It's still pending, waiting for scheduling.
            return false;
        }
        return (System.currentTimeMillis() - startTimeMs) > timeoutMs;
    }

    /**
     * When a task encounters a write error, it will time out.
     * The job needs to obtain the reason for the timeout,
     * such as a data quality error, and needs to expose it to the user.
     */
    public String getTimeoutReason() {
        try {
            if (runningBackendId <= 0) {
                log.info("No running backend for task {}", runningBackendId);
                return "";
            }
            Backend backend = Env.getCurrentSystemInfo().getBackend(runningBackendId);
            InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                    .setApi("/api/getFailReason/" + getTaskId())
                    .build();
            TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
            InternalService.PRequestCdcClientResult result = null;
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.warn("Failed to get task timeout reason, {}", result.getStatus().getErrorMsgs(0));
                return "";
            }
            String response = result.getResponse();
            try {
                ResponseBody<String> responseObj = objectMapper.readValue(
                        response,
                        new TypeReference<ResponseBody<String>>() {
                        }
                );
                if (responseObj.getCode() == RestApiStatusCode.OK.code) {
                    return responseObj.getData();
                }
            } catch (JsonProcessingException e) {
                log.warn("Failed to get task timeout reason, response: {}", response);
            }
        } catch (ExecutionException | InterruptedException ex) {
            log.error("Send get task fail reason request failed: ", ex);
        }
        return "";
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
