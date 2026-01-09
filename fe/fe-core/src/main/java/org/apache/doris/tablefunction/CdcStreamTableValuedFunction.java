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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CdcStreamTableValuedFunction extends ExternalFileTableValuedFunction {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static String URI = "http://127.0.0.1:{}/api/fetchRecordStream";
    private final Map<String, String> originProps;
    public CdcStreamTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        this.originProps = properties;
        processProps(properties);
        validate(properties);
    }

    private void processProps(Map<String, String> properties) throws AnalysisException {
        Map<String, String> copyProps = new HashMap<>(properties);
        copyProps.put("format", "json");
        super.parseCommonProperties(copyProps);
        this.processedParams.put("enable_cdc_client", "true");
        this.processedParams.put("uri", URI);
        this.processedParams.put("http.enable.range.request", "false");
        this.processedParams.put("http.chunk.response", "true");
        this.processedParams.put("http.method", "POST");

        String payload = generateParams(properties);
        this.processedParams.put("http.payload", payload);
        this.backendConnectProperties.putAll(processedParams);
        generateFileStatus();
    }

    private String generateParams(Map<String, String> properties) throws AnalysisException {
        FetchRecordRequest recordRequest = new FetchRecordRequest();
        recordRequest.setJobId(UUID.randomUUID().toString().replace("-", ""));
        recordRequest.setDataSource(properties.get(DataSourceConfigKeys.TYPE));
        recordRequest.setConfig(properties);
        try {
            return objectMapper.writeValueAsString(recordRequest);
        } catch (IOException e) {
            LOG.info("Failed to serialize fetch record request," + e.getMessage());
            throw new AnalysisException(e.getMessage());
        }
    }

    private void validate(Map<String, String> properties) {
        Preconditions.checkArgument(properties.containsKey(DataSourceConfigKeys.JDBC_URL), "jdbc_url is required");
        Preconditions.checkArgument(properties.containsKey(DataSourceConfigKeys.TYPE), "type is required");
        Preconditions.checkArgument(properties.containsKey(DataSourceConfigKeys.TABLE), "table is required");
    }

    private void generateFileStatus() {
        this.fileStatuses.clear();
        this.fileStatuses.add(new TBrokerFileStatus(URI, false, Integer.MAX_VALUE, false));
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        DataSourceType dataSourceType = DataSourceType.valueOf(processedParams.get(DataSourceConfigKeys.TYPE).toUpperCase());
        JdbcClient jdbcClient = StreamingJobUtils.getJdbcClient(dataSourceType, processedParams);
        String database = StreamingJobUtils.getRemoteDbName(dataSourceType, processedParams);
        String table = processedParams.get(DataSourceConfigKeys.TABLE);
        boolean tableExist = jdbcClient.isTableExist(database, table);
        Preconditions.checkArgument(tableExist, "Table does not exist: " + table);
        return jdbcClient.getColumnsFromJdbc(database, table);
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
        return new BrokerDesc("CdcStreamTvfBroker", StorageType.HTTP, originProps);
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
