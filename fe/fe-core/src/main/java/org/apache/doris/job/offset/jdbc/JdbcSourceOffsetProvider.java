package org.apache.doris.job.offset.jdbc;

import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.jdbc.split.BinlogSplit;
import org.apache.doris.job.offset.jdbc.split.SnapshotSplit;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import static org.apache.doris.job.common.LoadConstants.STARTUP_MODE;
import static org.apache.doris.job.util.StreamingJobUtils.createMetaTableIfNotExist;
import static org.apache.doris.job.util.StreamingJobUtils.insertSplitsToMeta;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@Setter
@Log4j2
public class JdbcSourceOffsetProvider implements SourceOffsetProvider {
    public static final String BINLOG_SPLIT_ID = "binlog-split";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private Long jobId;
    private DataSourceType sourceType;
    private Map<String, String> sourceProperties = new HashMap<>();

    List<SnapshotSplit> remainingSplits;
    List<SnapshotSplit> finishedSplits;

    JdbcOffset currentOffset;
    Map<String, String> endBinlogOffset;

    @Override
    public String getSourceType() {
        return "jdbc";
    }

    @Override
    public Offset getNextOffset(StreamingJobProperties jobProps, Map<String, String> properties) {
        JdbcOffset nextOffset = new JdbcOffset();
        if (!remainingSplits.isEmpty()) {
            // snapshot read
            SnapshotSplit snapshotSplit = remainingSplits.get(0);
            nextOffset.setSplit(snapshotSplit);
            return nextOffset;
        }else if (currentOffset.getSplit().snapshotSplit()){
            // snapshot to binlog
            BinlogSplit binlogSplit = new BinlogSplit();
            binlogSplit.setSplitId(BINLOG_SPLIT_ID);
            binlogSplit.setFinishedSplits(finishedSplits);
            nextOffset.setSplit(binlogSplit);
            return nextOffset;
        } else {
            // only binlog
            return currentOffset;
        }
    }

    @Override
    public String getShowCurrentOffset() {
        return null;
    }

    @Override
    public String getShowMaxOffset() {
        return null;
    }

    @Override
    public InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand, Offset nextOffset) {
        // not need
        return null;
    }

    @Override
    public void updateOffset(Offset offset) {

    }

    @Override
    public void fetchRemoteMeta(Map<String, String> properties) throws Exception {
        // todo: change to request be
        Map<String, Object> params = buildBaseParams();
        String url = "http://127.0.0.1:9096/api/fetchEndOffset";
        // Prepare request body
        String requestBody = GsonUtils.GSON.toJson(params);

        // Create HTTP POST request
        HttpPost httpPost = new HttpPost(url);
        StringEntity stringEntity = new StringEntity(requestBody, "UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);
        // Set request headers
        httpPost.setHeader("Content-Type", "application/json");

        // Execute request
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            String response = client.execute(httpPost, httpResponse -> {
                int statusCode = httpResponse.getStatusLine().getStatusCode();
                String responseBody = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                if (statusCode != 200) {
                    throw new RuntimeException("Failed to get remote offset from CDC client, HTTP status code: " + statusCode);
                }
                return responseBody;
            });

            ResponseBody<Map<String, String>> responseObj = objectMapper.readValue(
                    response,
                    new TypeReference<ResponseBody<Map<String, String>>>() {}
            );
            endBinlogOffset = responseObj.getData();
        } catch (Exception ex) {
            log.error("Get splits error: ", ex);
            throw new JobException("Failed to request CDC client: " + ex.getMessage(), ex);
        }
    }

    @Override
    public boolean hasMoreDataToConsume() {
        if(!remainingSplits.isEmpty()) {
            return true;
        }
        if (currentOffset != null && !currentOffset.getSplit().snapshotSplit()) {
            BinlogSplit binlogSplit = (BinlogSplit) currentOffset.getSplit();
            Map<String, String> startingOffset = binlogSplit.getStartingOffset();
            // todo: should compare offset
        }
        return true;
    }

    @Override
    public Offset deserializeOffset(String offset) {
        return null;
    }

    @Override
    public Offset deserializeOffsetProperty(String offset) {
        return null;
    }

    public void splitChunks(List<String> createTbls) throws JobException {
        // todo: When splitting takes a long time, it needs to be changed to asynchronous.
        if(checkNeedSplitChunks(sourceProperties)) {
            Map<String, List<SnapshotSplit>> tableSplits = new HashMap<>();
            for (String tbl : createTbls) {
                List<SnapshotSplit> snapshotSplits = requestTableSplits(tbl);
                tableSplits.put(tbl, snapshotSplits);
            }
            // save chunk list to system table
            saveChunkMeta(tableSplits);
            this.remainingSplits = tableSplits.values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        }
    }

    private void saveChunkMeta(Map<String, List<SnapshotSplit>> tableSplits) throws JobException {
        try {
            createMetaTableIfNotExist();
            insertSplitsToMeta(getJobId(), tableSplits);
        } catch (Exception e) {
            log.warn("save chunk meta error: ", e);
            throw new JobException(e.getMessage());
        }
    }

    private List<SnapshotSplit> requestTableSplits(String table) throws JobException {
        Map<String, Object> params = buildSplitParams(table);
        String url = "http://127.0.0.1:9096/api/fetchSplits";
        // Prepare request body
        String requestBody = GsonUtils.GSON.toJson(params);

        // Create HTTP POST request
        HttpPost httpPost = new HttpPost(url);
        StringEntity stringEntity = new StringEntity(requestBody, "UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);
        // Set request headers
        httpPost.setHeader("Content-Type", "application/json");

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

            ResponseBody<List<SnapshotSplit>> responseObj = objectMapper.readValue(
                    response,
                    new TypeReference<ResponseBody<List<SnapshotSplit>>>() {}
            );
            List<SnapshotSplit> splits = responseObj.getData();
            return splits;

        } catch (Exception ex) {
            log.error("Get splits error: ", ex);
            throw new JobException("Failed to request CDC client: " + ex.getMessage(), ex);
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

    private Map<String, Object> buildBaseParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("jobId", getJobId());
        params.put("dataSource", sourceType);
        params.put("config", sourceProperties);
        return params;
    }

    private Map<String, Object> buildSplitParams(String table) {
        Map<String, Object> params = buildBaseParams();
        params.put("snapshotTable", table);
        return params;
    }

    public static Backend selectBackend(Long jobId) throws JobException {
        Backend backend = null;
        BeSelectionPolicy policy = null;

        policy = new BeSelectionPolicy.Builder()
                .setEnableRoundRobin(true)
                .needLoadAvailable().build();
        List<Long> backendIds;
        backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
        if (backendIds.isEmpty()) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        // jobid % backendSize
        long index = backendIds.get(jobId.intValue() % backendIds.size());
        backend = Env.getCurrentSystemInfo().getBackend(index);
        if (backend == null) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        return backend;
    }

    private boolean checkNeedSplitChunks(Map<String, String> sourceProperties) {
        String startMode = sourceProperties.get(STARTUP_MODE);
        if (startMode == null) {
            return false;
        }
        return "snapshot".equalsIgnoreCase(startMode) || "initial".equalsIgnoreCase(startMode);
    }
}
