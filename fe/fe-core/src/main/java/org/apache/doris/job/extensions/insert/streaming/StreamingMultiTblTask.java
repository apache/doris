package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.job.base.Job;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.jdbc.JdbcOffset;
import org.apache.doris.job.offset.jdbc.split.SnapshotSplit;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStatusCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
@Getter
public class StreamingMultiTblTask extends AbstractStreamingTask {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private DataSourceType dataSourceType;
    private SourceOffsetProvider offsetProvider;
    private Map<String, String> sourceProperties;
    private Map<String, String> targetProperties;
    private String targetDb;

    public StreamingMultiTblTask(Long jobId,
                                long taskId,
                                DataSourceType dataSourceType,
                                SourceOffsetProvider offsetProvider,
                                Map<String, String> sourceProperties,
                                String targetDb,
                                Map<String, String> targetProperties) {
        super(jobId, taskId);
        this.dataSourceType = dataSourceType;
        this.offsetProvider = offsetProvider;
        this.sourceProperties = sourceProperties;
        this.targetProperties = targetProperties;
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
        Map<String, Object> params = buildRequestParams();

        String url = "http://127.0.0.1:9096/api/writeRecords";
        // Prepare request body
        String requestBody = null;
        try {
            requestBody = objectMapper.writeValueAsString(params);
        }catch (IOException e) {
            throw new JobException("Failed to serialize request body: " + e.getMessage(), e);
        }
        // Create HTTP POST request
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader("token", getToken());
        StringEntity stringEntity = new StringEntity(requestBody, "UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);
        // Set request headers
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

        // Execute request
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            String response = client.execute(httpPost, httpResponse -> {
                int statusCode = httpResponse.getStatusLine().getStatusCode();
                String responseBody = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                if (statusCode != 200) {
                    throw new RuntimeException("Failed to get split from CDC client, HTTP status code: " + statusCode);
                }
                return responseBody;
            });

            ResponseBody<String> responseObj = objectMapper.readValue(
                    response,
                    new TypeReference<ResponseBody<String>>() {}
            );
            if (responseObj.getCode() == RestApiStatusCode.OK.code) {
                log.info("Send write records request successfully, response: {}", responseObj.getData());
                return;
            }
            throw new JobException("Failed to send write records request , error message: " + responseObj);
        } catch (Exception ex) {
            log.error("Send write request: ", ex);
            throw new JobException("Failed to send write request: " + ex.getMessage(), ex);
        }

        /**
         Backend backend = selectBackend(jobId);
         Map<String, Object> params = buildSplitParams(table);
         InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
         .setApi("/api/fetchSplits")
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
         throw new JobException("Failed to get split from backend," + result.getStatus().getErrorMsgs(0) + ", response: " + result.getResponse());
         }
         } catch (ExecutionException | InterruptedException ex) {
         log.error("Get splits error: ", ex);
         throw new JobException(ex);
         }
         log.info("========fetch cdc split {}", result.getResponse());
         return "";
         **/
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

    private Map<String, Object> buildRequestParams() {
        JdbcOffset offset = (JdbcOffset) runningOffset;
        Map<String, Object> params = new HashMap<>();
        params.put("jobId", getJobId());
        params.put("labelName", getLabelName());
        params.put("dataSource", dataSourceType);
        params.put("meta", offset.getSplit());
        params.put("config", sourceProperties);
        params.put("targetDb", targetDb);
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
        //todo: need to config
        return (System.currentTimeMillis() - createTimeMs) > 300 * 1000;
    }
}
