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

import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.jdbc.JdbcOffset;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PRequestCdcClientResult;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.util.HashMap;
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

    public StreamingMultiTblTask(Long jobId,
            long taskId,
            DataSourceType dataSourceType,
            SourceOffsetProvider offsetProvider,
            Map<String, String> sourceProperties,
            String targetDb,
            Map<String, String> targetProperties,
            StreamingJobProperties jobProperties) {
        super(jobId, taskId);
        this.dataSourceType = dataSourceType;
        this.offsetProvider = offsetProvider;
        this.sourceProperties = sourceProperties;
        this.targetProperties = targetProperties;
        this.jobProperties = jobProperties;
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
            log.info("task has been canceled, task id is {}", getTaskId());
            return;
        }
        log.info("start to run streaming multi task, offset is {}", runningOffset.toString());
        sendWriteRequest();
    }

    private void sendWriteRequest() throws JobException {
        Backend backend = StreamingJobUtils.selectBackend(jobId);
        Map<String, Object> params = buildRequestParams();
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/writeRecords")
                .setParams(GsonUtils.GSON.toJson(params)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.error("Failed to get split from backend, {}", result.getStatus().getErrorMsgs(0));
                throw new JobException(
                        "Failed to get split from backend," + result.getStatus().getErrorMsgs(0) + ", response: "
                                + result.getResponse());
            }
            String response = result.getResponse();
            ResponseBody<String> responseObj = objectMapper.readValue(
                    response,
                    new TypeReference<ResponseBody<String>>() {
                    }
            );
            if (responseObj.getCode() == RestApiStatusCode.OK.code) {
                log.info("Send write records request successfully, response: {}", responseObj.getData());
                return;
            }
            throw new JobException("Failed to send write records request , error message: " + responseObj);
        } catch (ExecutionException | InterruptedException | IOException ex) {
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

    private Map<String, Object> buildRequestParams() throws JobException {
        JdbcOffset offset = (JdbcOffset) runningOffset;
        Map<String, Object> params = new HashMap<>();
        params.put("jobId", getJobId());
        params.put("labelName", getLabelName());
        params.put("dataSource", dataSourceType);
        params.put("meta", offset.getSplit());
        params.put("config", sourceProperties);
        params.put("targetDb", targetDb);
        params.put("token", getToken());
        params.put("taskId", getTaskId());
        params.put("frontendAddress",
                Env.getCurrentEnv().getMasterHost() + ":" + Env.getCurrentEnv().getMasterHttpPort());
        params.put("maxInterval", jobProperties.getMaxIntervalSecond());
        return params;
    }

    @Override
    public boolean onSuccess() throws JobException {
        if (getIsCanceled().get()) {
            return false;
        }
        log.info("streaming multi task {} send write request run successfully.", getTaskId());
        return false;
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
        // close cdc client connection
    }

    public boolean isTimeout() {
        // todo: need to config
        return (System.currentTimeMillis() - createTimeMs) > 300 * 1000;
    }
}
