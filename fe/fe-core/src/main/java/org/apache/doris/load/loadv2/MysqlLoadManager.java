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
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.ByteBufferNetworkInputStream;
import org.apache.doris.load.LoadJobRowResult;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.collect.EvictingQueue;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MysqlLoadManager {
    private static final Logger LOG = LogManager.getLogger(MysqlLoadManager.class);

    private  ThreadPoolExecutor mysqlLoadPool;
    private final TokenManager tokenManager;

    private static class MySqlLoadContext {
        private boolean finished;
        private HttpPut request;
        private boolean isCancelled;

        public MySqlLoadContext() {
            this.finished = false;
            this.isCancelled = false;
        }

        public boolean isFinished() {
            return finished;
        }

        public void setFinished(boolean finished) {
            this.finished = finished;
        }

        public HttpPut getRequest() {
            return request;
        }

        public void setRequest(HttpPut request) {
            this.request = request;
        }

        public boolean isCancelled() {
            return isCancelled;
        }

        public void setCancelled(boolean cancelled) {
            isCancelled = cancelled;
        }
    }

    private static class MySqlLoadFailRecord {
        private final String label;

        private final String errorUrl;
        private final long startTime;

        public MySqlLoadFailRecord(String label, String errorUrl) {
            this.label = label;
            this.errorUrl = errorUrl;
            this.startTime = System.currentTimeMillis();
        }

        public String getLabel() {
            return label;
        }

        public String getErrorUrl() {
            return errorUrl;
        }

        public boolean isExpired() {
            // hard code the expired value for one day.
            return System.currentTimeMillis() > startTime + 24 * 60 * 60 * 1000;
        }
    }

    private final Map<String, MySqlLoadContext> loadContextMap = new ConcurrentHashMap<>();
    private  EvictingQueue<MySqlLoadFailRecord> failedRecords;
    private ScheduledExecutorService periodScheduler;

    public MysqlLoadManager(TokenManager tokenManager) {
        this.tokenManager = tokenManager;
    }

    public void start() {
        this.periodScheduler = Executors.newScheduledThreadPool(1,
                new CustomThreadFactory("mysql-load-fail-record-cleaner"));
        int poolSize = Config.mysql_load_thread_pool;
        // MySqlLoad pool can accept 4 + 4 * 5 = 24  requests by default.
        this.mysqlLoadPool = ThreadPoolManager.newDaemonFixedThreadPool(poolSize, poolSize * 5,
                "Mysql Load", true);
        this.failedRecords = EvictingQueue.create(Config.mysql_load_in_memory_record);
        this.periodScheduler.scheduleAtFixedRate(this::cleanFailedRecords, 1, 24, TimeUnit.HOURS);
    }

    public LoadJobRowResult executeMySqlLoadJobFromStmt(ConnectContext context, LoadStmt stmt, String loadId)
            throws IOException, UserException {
        return executeMySqlLoadJobFromStmt(context, stmt.getDataDescriptions().get(0), loadId);
    }

    public LoadJobRowResult executeMySqlLoadJobFromStmt(ConnectContext context, DataDescription dataDesc, String loadId)
            throws IOException, UserException {
        LoadJobRowResult loadResult = new LoadJobRowResult();
        List<String> filePaths = dataDesc.getFilePaths();
        String database = ClusterNamespace.getNameFromFullName(dataDesc.getDbName());
        String table = dataDesc.getTableName();
        int oldTimeout = context.getExecTimeout();
        int newTimeOut = extractTimeOut(dataDesc);
        if (newTimeOut > oldTimeout) {
            // set query timeout avoid by killed TimeoutChecker
            SessionVariable sessionVariable = context.getSessionVariable();
            sessionVariable.setIsSingleSetVar(true);
            VariableMgr.setVar(sessionVariable,
                    new SetVar(SessionVariable.QUERY_TIMEOUT, new StringLiteral(String.valueOf(newTimeOut))));
        }
        String token = tokenManager.acquireToken();
        boolean clientLocal = dataDesc.isClientLocal();
        MySqlLoadContext loadContext = new MySqlLoadContext();
        loadContextMap.put(loadId, loadContext);
        LOG.info("Executing mysql load with id: {}.", loadId);
        try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {
            for (String file : filePaths) {
                InputStreamEntity entity = getInputStreamEntity(context, clientLocal, file, loadId);
                HttpPut request = generateRequestForMySqlLoad(entity, dataDesc, database, table, token);
                loadContext.setRequest(request);
                try (final CloseableHttpResponse response = httpclient.execute(request)) {
                    String body = EntityUtils.toString(response.getEntity());
                    JsonObject result = JsonParser.parseString(body).getAsJsonObject();
                    if (!result.get("Status").getAsString().equalsIgnoreCase("Success")) {
                        String errorUrl = Optional.ofNullable(result.get("ErrorURL"))
                                .map(JsonElement::getAsString).orElse("");
                        failedRecords.offer(new MySqlLoadFailRecord(loadId, errorUrl));
                        LOG.warn("Execute mysql load failed with request: {} and response: {}, job id: {}",
                                request, body, loadId);
                        throw new LoadException(result.get("Message").getAsString() + " with load id " + loadId);
                    }
                    loadResult.incRecords(result.get("NumberLoadedRows").getAsLong());
                    loadResult.incSkipped(result.get("NumberFilteredRows").getAsInt());
                }
            }
        } catch (Throwable t) {
            LOG.warn("Execute mysql load {} failed, msg: {}", loadId, t);
            // drain the data from client conn util empty packet received, otherwise the connection will be reset
            if (clientLocal && loadContextMap.containsKey(loadId) && !loadContextMap.get(loadId).isFinished()) {
                LOG.warn("Not drained yet, try reading left data from client connection for load {}.", loadId);
                ByteBuffer buffer = context.getMysqlChannel().fetchOnePacket();
                // MySql client will send an empty packet when eof
                while (buffer != null && buffer.limit() != 0) {
                    buffer = context.getMysqlChannel().fetchOnePacket();
                }
                LOG.debug("Finished reading the left bytes.");
            }
            // make cancel message to user
            if (loadContextMap.containsKey(loadId) && loadContextMap.get(loadId).isCancelled()) {
                throw new LoadException("Cancelled");
            } else {
                throw t;
            }
        } finally {
            LOG.info("Mysql load job {} finished, loaded records: {}", loadId, loadResult.getRecords());
            loadContextMap.remove(loadId);
        }
        return loadResult;
    }

    public LoadJobRowResult executeMySqlLoadJobFromStmt(ConnectContext context, InsertStmt insertStmt, String loadId)
            throws UserException, IOException {
        return executeMySqlLoadJobFromStmt(context, (DataDescription) insertStmt.getDataDescList().get(0), loadId);
    }

    public void cancelMySqlLoad(String loadId) {
        if (loadContextMap.containsKey(loadId)) {
            loadContextMap.get(loadId).setCancelled(true);
            loadContextMap.get(loadId).getRequest().abort();
            LOG.info("Cancel MySqlLoad with id {}", loadId);
        } else {
            LOG.info("Load id: {} may be already finished.", loadId);
        }
    }

    public String getErrorUrlByLoadId(String loadId) {
        for (MySqlLoadFailRecord record : failedRecords) {
            if (loadId.equals(record.getLabel())) {
                return record.getErrorUrl();
            }
        }
        return null;
    }

    private void cleanFailedRecords() {
        while (!failedRecords.isEmpty() && failedRecords.peek().isExpired()) {
            failedRecords.poll();
        }
    }

    private int extractTimeOut(DataDescription desc) {
        if (desc.getProperties() != null && desc.getProperties().containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
            return Integer.parseInt(desc.getProperties().get(LoadStmt.TIMEOUT_PROPERTY));
        }
        return -1;
    }

    private String getColumns(DataDescription desc) {
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
            return fieldString.toString();
        }
        return null;
    }

    private InputStreamEntity getInputStreamEntity(
            ConnectContext context,
            boolean isClientLocal,
            String file,
            String loadId)
            throws IOException {
        InputStream inputStream;
        if (isClientLocal) {
            // mysql client will check the file exist.
            replyClientForReadFile(context, file);
            inputStream = new ByteBufferNetworkInputStream();
            fillByteBufferAsync(context, (ByteBufferNetworkInputStream) inputStream, loadId);
        } else {
            // server side file had already check after analyze.
            inputStream = Files.newInputStream(Paths.get(file));
        }
        return new InputStreamEntity(inputStream, -1, ContentType.TEXT_PLAIN);
    }

    private void replyClientForReadFile(ConnectContext context, String path) throws IOException {
        MysqlSerializer serializer = context.getMysqlChannel().getSerializer();
        serializer.reset();
        serializer.writeByte((byte) 0xfb);
        serializer.writeEofString(path);
        context.getMysqlChannel().sendAndFlush(serializer.toByteBuffer());
    }

    private void fillByteBufferAsync(ConnectContext context, ByteBufferNetworkInputStream inputStream, String loadId) {
        mysqlLoadPool.submit(() -> {
            ByteBuffer buffer;
            try {
                buffer = context.getMysqlChannel().fetchOnePacket();
                // MySql client will send an empty packet when eof
                while (buffer != null && buffer.limit() != 0) {
                    inputStream.fillByteBuffer(buffer);
                    buffer = context.getMysqlChannel().fetchOnePacket();
                }
                if (loadContextMap.containsKey(loadId)) {
                    loadContextMap.get(loadId).setFinished(true);
                }
            } catch (IOException | InterruptedException e) {
                LOG.warn("Failed fetch packet from mysql client for load: " + loadId, e);
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
            String table,
            String token) throws LoadException {
        final HttpPut httpPut = new HttpPut(selectBackendForMySqlLoad(database, table));

        httpPut.addHeader("Expect", "100-continue");
        httpPut.addHeader("Content-Type", "text/plain");
        httpPut.addHeader("token", token);

        Map<String, String> props = desc.getProperties();
        if (props != null) {
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

            // timeout
            if (props.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
                String timeout = props.get(LoadStmt.TIMEOUT_PROPERTY);
                httpPut.addHeader(LoadStmt.TIMEOUT_PROPERTY, timeout);
            }

            // timezone
            if (props.containsKey(LoadStmt.TIMEZONE)) {
                String timezone = props.get(LoadStmt.TIMEZONE);
                httpPut.addHeader(LoadStmt.TIMEZONE, timezone);
            }

            // trim quotes
            if (props.containsKey(LoadStmt.KEY_TRIM_DOUBLE_QUOTES)) {
                String trimQuotes = props.get(LoadStmt.KEY_TRIM_DOUBLE_QUOTES);
                httpPut.addHeader(LoadStmt.KEY_TRIM_DOUBLE_QUOTES, trimQuotes);
            }

            // enclose
            if (props.containsKey(LoadStmt.KEY_ENCLOSE)) {
                String enclose = props.get(LoadStmt.KEY_ENCLOSE);
                httpPut.addHeader(LoadStmt.KEY_ENCLOSE, enclose);
            }

            //escape
            if (props.containsKey(LoadStmt.KEY_ESCAPE)) {
                String escape = props.get(LoadStmt.KEY_ESCAPE);
                httpPut.addHeader(LoadStmt.KEY_ESCAPE, escape);
            }
        }

        // skip_lines
        if (desc.getSkipLines() != 0) {
            httpPut.addHeader(LoadStmt.KEY_SKIP_LINES, Integer.toString(desc.getSkipLines()));
        }

        // column_separator
        if (desc.getColumnSeparator() != null) {
            httpPut.addHeader(LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR, desc.getColumnSeparator());
        }

        // line_delimiter
        if (desc.getLineDelimiter() != null) {
            httpPut.addHeader(LoadStmt.KEY_IN_PARAM_LINE_DELIMITER, desc.getLineDelimiter());
        }

        // columns
        String columns = getColumns(desc);
        if (columns != null) {
            httpPut.addHeader(LoadStmt.KEY_IN_PARAM_COLUMNS, columns);
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
