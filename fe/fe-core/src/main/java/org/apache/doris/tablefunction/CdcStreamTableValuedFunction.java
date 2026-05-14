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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.FetchRecordRequest;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CdcStreamTableValuedFunction extends ExternalFileTableValuedFunction {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String URI = "http://127.0.0.1:CDC_CLIENT_PORT/api/fetchRecordStream";
    private static final String ENABLE_CDC_CLIENT_KEY = "enable_cdc_client";
    private static final String HTTP_ENABLE_RANGE_REQUEST_KEY = "http.enable.range.request";
    private static final String HTTP_ENABLE_CHUNK_RESPONSE_KEY = "http.enable.chunk.response";
    private static final String HTTP_METHOD_KEY = "http.method";
    private static final String HTTP_PAYLOAD_KEY = "http.payload";
    public static final String JOB_ID_KEY = "job.id";
    public static final String TASK_ID_KEY = "task.id";
    public static final String META_KEY = "meta";

    public CdcStreamTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        validate(properties);
        processProps(properties);
    }

    private void processProps(Map<String, String> properties) throws AnalysisException {
        Map<String, String> copyProps = new HashMap<>(properties);
        copyProps.put("format", "json");

        // Standalone TVF: random jobId. TVF-in-job: job.id injected by rewriteTvfParams.
        String jobId = copyProps.computeIfAbsent(JOB_ID_KEY,
                k -> UUID.randomUUID().toString().replace("-", ""));

        // Default PG slot/pub so cdcclient auto-creates per-job resources
        StreamingJobUtils.populateDefaultSourceProperties(
                DataSourceType.valueOf(copyProps.get(DataSourceConfigKeys.TYPE).toUpperCase()),
                copyProps, jobId);

        super.parseCommonProperties(copyProps);
        this.processedParams.put(ENABLE_CDC_CLIENT_KEY, "true");
        this.processedParams.put(URI_KEY, URI);
        this.processedParams.put(HTTP_ENABLE_RANGE_REQUEST_KEY, "false");
        this.processedParams.put(HTTP_ENABLE_CHUNK_RESPONSE_KEY, "true");
        this.processedParams.put(HTTP_METHOD_KEY, "POST");

        String payload = generateParams(copyProps);
        this.processedParams.put(HTTP_PAYLOAD_KEY, payload);
        this.backendConnectProperties.putAll(processedParams);
        generateFileStatus();
    }

    private String generateParams(Map<String, String> properties) throws AnalysisException {
        FetchRecordRequest recordRequest = new FetchRecordRequest();
        recordRequest.setJobId(properties.get(JOB_ID_KEY));
        recordRequest.setDataSource(properties.get(DataSourceConfigKeys.TYPE));
        recordRequest.setConfig(properties);
        try {
            // for tvf with job
            if (properties.containsKey(TASK_ID_KEY)) {
                recordRequest.setTaskId(properties.remove(TASK_ID_KEY));
                String meta = properties.remove(META_KEY);
                Preconditions.checkArgument(StringUtils.isNotEmpty(meta), "meta is required when task.id is provided");
                Map<String, Object> metaMap = objectMapper.readValue(meta, new TypeReference<Map<String, Object>>() {});
                recordRequest.setMeta(metaMap);
            }
            return objectMapper.writeValueAsString(recordRequest);
        } catch (IOException e) {
            LOG.warn("Failed to serialize fetch record request", e);
            throw new AnalysisException("Failed to serialize fetch record request: " + e.getMessage(), e);
        }
    }

    private void validate(Map<String, String> properties) throws AnalysisException {
        if (!properties.containsKey(DataSourceConfigKeys.JDBC_URL)) {
            throw new AnalysisException("jdbc_url is required");
        }
        if (!properties.containsKey(DataSourceConfigKeys.TYPE)) {
            throw new AnalysisException("type is required");
        }
        if (!properties.containsKey(DataSourceConfigKeys.TABLE)) {
            throw new AnalysisException("table is required");
        }
        if (!properties.containsKey(DataSourceConfigKeys.OFFSET)) {
            throw new AnalysisException("offset is required");
        }
    }

    private void generateFileStatus() {
        this.fileStatuses.clear();
        this.fileStatuses.add(new TBrokerFileStatus(URI, false, Integer.MAX_VALUE, false));
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        DataSourceType dataSourceType =
                DataSourceType.valueOf(processedParams.get(DataSourceConfigKeys.TYPE).toUpperCase());
        JdbcClient jdbcClient = StreamingJobUtils.getJdbcClient(dataSourceType, processedParams);
        try {
            String database = StreamingJobUtils.getRemoteDbName(dataSourceType, processedParams);
            String table = processedParams.get(DataSourceConfigKeys.TABLE);
            if (!jdbcClient.isTableExist(database, table)) {
                throw new AnalysisException("Table does not exist: " + table);
            }
            return jdbcClient.getColumnsFromJdbc(database, table);
        } finally {
            jdbcClient.closeClient();
        }
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_HTTP;
    }

    @Override
    public String getFilePath() {
        return URI;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("CdcStreamTvfBroker", StorageType.HTTP, processedParams);
    }

    @Override
    public String getTableName() {
        return "CdcStreamTableValuedFunction";
    }

    @Override
    public List<String> getPathPartitionKeys() {
        return new ArrayList<>();
    }
}
