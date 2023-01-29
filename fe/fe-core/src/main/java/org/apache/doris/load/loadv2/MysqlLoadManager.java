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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.io.ByteBufferNetworkInputStream;
import org.apache.doris.load.LoadJobRowResult;
import org.apache.doris.load.loadv2.LoadTask.MergeType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Joiner;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class MysqlLoadManager {
    private static final Logger LOG = LogManager.getLogger(MysqlLoadManager.class);

    private final ThreadPoolExecutor mysqlLoadPool;

    public MysqlLoadManager() {
        this.mysqlLoadPool = ThreadPoolManager.newDaemonCacheThreadPool(4, "Mysql Load", true);
    }

    public LoadJobRowResult executeMySqlLoadJobFromStmt(ConnectContext context, LoadStmt stmt)
            throws IOException, LoadException {
        LoadJobRowResult loadResult = new LoadJobRowResult();
        // Mysql data load only have one data desc
        DataDescription dataDesc = stmt.getDataDescriptions().get(0);
        String database = dataDesc.getDbName();
        String table = dataDesc.getTableName();
        List<String> filePaths = dataDesc.getFilePaths();
        try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {
            for (String file : filePaths) {
                InputStreamEntity entity = getInputStreamEntity(context, dataDesc.isClientLocal(), file);
                HttpPut request = generateRequestForMySqlLoad(entity, dataDesc, database, table);
                try (final CloseableHttpResponse response = httpclient.execute(request)) {
                    JSONObject result = JSON.parseObject(EntityUtils.toString(response.getEntity()));
                    if (!result.getString("Status").equalsIgnoreCase("Success")) {
                        LOG.warn("Execute stream load for mysql data load failed with message: " + request);
                        throw new LoadException(result.getString("Message"));
                    }
                    loadResult.incRecords(result.getLong("NumberLoadedRows"));
                    loadResult.incSkipped(result.getIntValue("NumberFilteredRows"));
                }
            }
        }
        return loadResult;
    }

    private InputStreamEntity getInputStreamEntity(ConnectContext context, boolean isClientLocal, String file)
            throws IOException {
        InputStream inputStream;
        if (isClientLocal) {
            // mysql client will check the file exist.
            replyClientForReadFile(context, file);
            inputStream = new ByteBufferNetworkInputStream();
            fillByteBufferAsync(context, (ByteBufferNetworkInputStream) inputStream);
        } else {
            // server side file had already check after analyze.
            inputStream = Files.newInputStream(Paths.get(file));
        }
        return new InputStreamEntity(inputStream, -1, ContentType.TEXT_PLAIN);
    }

    private void replyClientForReadFile(ConnectContext context, String path) throws IOException {
        context.getSerializer().reset();
        context.getSerializer().writeByte((byte) 0xfb);
        context.getSerializer().writeEofString(path);
        context.getMysqlChannel().sendAndFlush(context.getSerializer().toByteBuffer());
    }

    private void fillByteBufferAsync(ConnectContext context, ByteBufferNetworkInputStream inputStream) {
        mysqlLoadPool.submit(() -> {
            ByteBuffer buffer = null;
            try {
                buffer = context.getMysqlChannel().fetchOnePacket();
                // MySql client will send an empty packet when eof
                while (buffer != null && buffer.limit() != 0) {
                    inputStream.fillByteBuffer(buffer);
                    buffer = context.getMysqlChannel().fetchOnePacket();
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                inputStream.markFinished();
            }
        });
    }

    // public only for test
    public HttpPut generateRequestForMySqlLoad(
            InputStreamEntity entity,
            DataDescription desc,
            String database,
            String table) throws LoadException {
        final HttpPut httpPut = new HttpPut(selectBackendForMySqlLoad(database, table));

        httpPut.addHeader("Expect", "100-continue");
        httpPut.addHeader("Content-Type", "text/plain");

        Map<String, String> props = desc.getProperties();
        if (props != null) {
            // auth
            if (!props.containsKey("auth")) {
                throw new LoadException("Must have auth(user:password) in properties.");
            }
            // TODO: use token to send request to avoid double auth.
            String auth = props.get("auth");
            String base64Auth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            httpPut.addHeader("Authorization", "Basic " + base64Auth);

            // max_filter_ratio
            if (props.containsKey(LoadStmt.KEY_IN_PARAM_MAX_FILTER_RATIO)) {
                String maxFilterRatio = props.get(LoadStmt.KEY_IN_PARAM_MAX_FILTER_RATIO);
                httpPut.addHeader(LoadStmt.KEY_IN_PARAM_MAX_FILTER_RATIO, maxFilterRatio);
            }

            // exec_mem_limit
            if (props.containsKey(LoadStmt.EXEC_MEM_LIMIT)) {
                String memory = props.get(LoadStmt.EXEC_MEM_LIMIT);
                httpPut.addHeader(LoadStmt.EXEC_MEM_LIMIT, memory);
            }

            // strict_mode
            if (props.containsKey(LoadStmt.STRICT_MODE)) {
                String strictMode = props.get(LoadStmt.STRICT_MODE);
                httpPut.addHeader(LoadStmt.STRICT_MODE, strictMode);
            }
        }

        // column_separator
        if (desc.getColumnSeparator() != null) {
            httpPut.addHeader(LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR, desc.getColumnSeparator());
        }

        // line_delimiter
        if (desc.getLineDelimiter() != null) {
            httpPut.addHeader(LoadStmt.KEY_IN_PARAM_LINE_DELIMITER, desc.getLineDelimiter());
        }

        // merge_type
        if (!desc.getMergeType().equals(MergeType.APPEND)) {
            httpPut.addHeader(LoadStmt.KEY_IN_PARAM_MERGE_TYPE, desc.getMergeType().name());
        }

        // columns
        if (desc.getFileFieldNames() != null) {
            List<String> fields = desc.getFileFieldNames();
            StringBuilder fieldString = new StringBuilder();
            fieldString.append(Joiner.on(",").join(fields));

            if (desc.getColumnMappingList() != null) {
                fieldString.append(",");
                List<String> mappings = new ArrayList<>();
                for (Expr expr : desc.getColumnMappingList()) {
                    mappings.add(expr.toSql().replaceAll("`", ""));
                }
                fieldString.append(Joiner.on(",").join(mappings));
            }
            httpPut.addHeader(LoadStmt.KEY_IN_PARAM_COLUMNS, fieldString.toString());
        }

        // partitions
        if (desc.getPartitionNames() != null && !desc.getPartitionNames().getPartitionNames().isEmpty()) {
            List<String> ps = desc.getPartitionNames().getPartitionNames();
            String pNames = Joiner.on(",").join(ps);
            if (desc.getPartitionNames().isTemp()) {
                httpPut.addHeader(LoadStmt.KEY_IN_PARAM_TEMP_PARTITIONS, pNames);
            } else {
                httpPut.addHeader(LoadStmt.KEY_IN_PARAM_PARTITIONS, pNames);
            }
        }
        httpPut.setEntity(entity);
        return httpPut;
    }

    private String selectBackendForMySqlLoad(String database, String table) throws LoadException {
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needLoadAvailable().build();
        List<Long> backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
        if (backendIds.isEmpty()) {
            throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }

        Backend backend = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("http://");
        sb.append(backend.getHost());
        sb.append(":");
        sb.append(backend.getHttpPort());
        sb.append("/api/");
        sb.append(database);
        sb.append("/");
        sb.append(table);
        sb.append("/_stream_load");
        return  sb.toString();
    }
}
