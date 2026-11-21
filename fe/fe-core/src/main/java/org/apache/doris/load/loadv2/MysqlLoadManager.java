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
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.load.LoadJobRowResult;
import org.apache.doris.load.StreamLoadHandler;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.nereids.load.NereidsDataDescription;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.MysqlDataDescription;
import org.apache.doris.nereids.trees.plans.commands.load.MysqlLoadCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.EvictingQueue;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.concurrent.Cancellable;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.EOFException;
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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MysqlLoadManager {
    private static final Logger LOG = LogManager.getLogger(MysqlLoadManager.class);

    private  ThreadPoolExecutor mysqlLoadPool;

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

    private  EvictingQueue<MySqlLoadFailRecord> failedRecords;
    private ScheduledExecutorService periodScheduler;

    public MysqlLoadManager() {
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

    public LoadJobRowResult executeMySqlLoadJob(StmtExecutor executor, MysqlDataDescription dataDesc, String loadId)
            throws IOException, UserException {
        ConnectContext context = executor.getContext();
        LoadJobRowResult loadResult = new LoadJobRowResult();
        List<String> filePaths = dataDesc.getFilePaths();
        String database = dataDesc.getDbName();
        String table = dataDesc.getTableName();
        int oldTimeout = context.getExecTimeoutS();
        int newTimeOut = extractTimeOut(dataDesc);
        if (newTimeOut > oldTimeout) {
            // set query timeout avoid by killed TimeoutChecker
            SessionVariable sessionVariable = context.getSessionVariable();
            sessionVariable.setIsSingleSetVar(true);
            VariableMgr.setVar(sessionVariable,
                new SetVar(SessionVariable.QUERY_TIMEOUT, new StringLiteral(String.valueOf(newTimeOut))));
        }
        String token = Env.getCurrentEnv().getTokenManager().acquireToken();
        boolean clientLocal = dataDesc.isClientLocal();
        LOG.info("Executing mysql load with id: {}.", loadId);
        try (final CloseableHttpClient httpclient = HttpUtils.getHttpClient()) {
            for (String file : filePaths) {
                HttpPut request = generateRequestForMySqlLoadV2(context, dataDesc, database, table, token);
                String body = uploadFile(executor, httpclient, request, clientLocal, file);
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
        } catch (Throwable t) {
            LOG.warn("Execute mysql load {} failed, msg: {}", loadId, t);
            if (executor.getPendingCancelReason() != null) {
                throw new LoadException("Cancelled");
            } else {
                throw t;
            }
        } finally {
            LOG.info("Mysql load job {} finished, loaded records: {}", loadId, loadResult.getRecords());
        }
        return loadResult;
    }

    public LoadJobRowResult executeMySqlLoadJobFromCommand(StmtExecutor executor, NereidsDataDescription dataDesc,
            String loadId) throws IOException, UserException {
        ConnectContext context = executor.getContext();
        LoadJobRowResult loadResult = new LoadJobRowResult();
        List<String> filePaths = dataDesc.getFilePaths();
        String database = dataDesc.getDbName();
        String table = dataDesc.getTableName();
        int oldTimeout = context.getExecTimeoutS();
        int newTimeOut = extractTimeOut(dataDesc);
        if (newTimeOut > oldTimeout) {
            // set query timeout avoid by killed TimeoutChecker
            SessionVariable sessionVariable = context.getSessionVariable();
            sessionVariable.setIsSingleSetVar(true);
            VariableMgr.setVar(sessionVariable,
                    new SetVar(SessionVariable.QUERY_TIMEOUT, new StringLiteral(String.valueOf(newTimeOut))));
        }
        String token = Env.getCurrentEnv().getTokenManager().acquireToken();
        boolean clientLocal = dataDesc.isClientLocal();
        LOG.info("Executing mysql load with id: {}.", loadId);
        try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {
            for (String file : filePaths) {
                HttpPut request = generateRequestForMySqlLoad(context, dataDesc, database, table, token);
                String body = uploadFile(executor, httpclient, request, clientLocal, file);
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
        } catch (Throwable t) {
            LOG.warn("Execute mysql load {} failed, msg: {}", loadId, t);
            if (executor.getPendingCancelReason() != null) {
                throw new LoadException("Cancelled");
            } else {
                throw t;
            }
        } finally {
            LOG.info("Mysql load job {} finished, loaded records: {}", loadId, loadResult.getRecords());
        }
        return loadResult;
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

    private int extractTimeOut(NereidsDataDescription desc) {
        if (desc.getProperties() != null && desc.getProperties().containsKey(LoadCommand.TIMEOUT_PROPERTY)) {
            return Integer.parseInt(desc.getProperties().get(LoadCommand.TIMEOUT_PROPERTY));
        }
        return -1;
    }

    private int extractTimeOut(MysqlDataDescription desc) {
        if (desc.getProperties() != null && desc.getProperties().containsKey(MysqlLoadCommand.TIMEOUT_PROPERTY)) {
            return Integer.parseInt(desc.getProperties().get(MysqlLoadCommand.TIMEOUT_PROPERTY));
        }
        return -1;
    }

    private int extractTimeOut(DataDescription desc) {
        if (desc.getProperties() != null && desc.getProperties().containsKey(LoadCommand.TIMEOUT_PROPERTY)) {
            return Integer.parseInt(desc.getProperties().get(LoadCommand.TIMEOUT_PROPERTY));
        }
        return -1;
    }

    private String getColumns(NereidsDataDescription desc) {
        if (desc.getFileFieldNames() != null) {
            List<String> fields = desc.getFileFieldNames();
            StringBuilder fieldString = new StringBuilder();
            fieldString.append(Joiner.on(",").join(fields));

            if (desc.getColumnMappingList() != null) {
                fieldString.append(",");
                List<String> mappings = new ArrayList<>();
                for (Expression expr : desc.getColumnMappingList()) {
                    mappings.add(expr.toSql().replaceAll("`", ""));
                }
                fieldString.append(Joiner.on(",").join(mappings));
            }
            return fieldString.toString();
        }
        return null;
    }

    private String getColumns(MysqlDataDescription desc) {
        List<String> fields = desc.getColumns();
        if (!fields.isEmpty()) {
            StringBuilder fieldString = new StringBuilder();
            fieldString.append(Joiner.on(",").join(fields));

            if (!desc.getColumnMappingList().isEmpty()) {
                fieldString.append(",");
                List<String> mappings = new ArrayList<>();
                for (Expression expression : desc.getColumnMappingList()) {
                    mappings.add(expression.toSql().replaceAll("`", ""));
                }
                fieldString.append(Joiner.on(",").join(mappings));
            }
            return fieldString.toString();
        }
        return null;
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
                    mappings.add(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE).replaceAll("`", ""));
                }
                fieldString.append(Joiner.on(",").join(mappings));
            }
            return fieldString.toString();
        }
        return null;
    }

    String uploadFile(StmtExecutor executor, CloseableHttpClient httpClient, HttpPut request,
            boolean clientLocal, String file) throws IOException {
        Runnable cancel = request::abort;
        executor.registerQueryCancellationCallback(cancel);
        try {
            if (request.isAborted()) {
                throw new IOException("Cancelled");
            }
            if (clientLocal) {
                return uploadLocalFile(mysqlLoadPool, executor.getContext().getMysqlChannel(), httpClient,
                        request, file);
            }
            try (InputStream input = Files.newInputStream(Paths.get(file))) {
                return upload(httpClient, request, input);
            }
        } finally {
            executor.unregisterQueryCancellationCallback(cancel);
        }
    }

    static String uploadLocalFile(ThreadPoolExecutor pool, MysqlChannel channel, CloseableHttpClient httpClient,
            HttpPut request, String file) throws IOException {
        // Admission precedes the file request. One worker owns the channel until it has consumed this file's
        // terminator. A command-thread interrupt must not hand the channel back to the next command early.
        FutureTask<String> task = new FutureTask<>(() -> {
            if (request.isAborted()) {
                throw new IOException("Cancelled");
            }
            requestFile(channel, file);
            try (LocalFileInputStream input = new LocalFileInputStream(channel)) {
                return upload(httpClient, request, input);
            }
        });
        // HttpClient replaces this cancellation target once it starts using the HTTP connection.
        // Only a task removed from the queue can be cancelled without waiting for its channel operations.
        Cancellable withdraw = () -> pool.remove(task) && task.cancel(false);
        request.setCancellable(withdraw);
        pool.execute(task);
        if (request.isAborted()) {
            // Cancellation may have arrived before execute finished enqueuing the task.
            withdraw.cancel();
        }
        try {
            return Uninterruptibles.getUninterruptibly(task);
        } catch (CancellationException e) {
            throw new IOException("Cancelled", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.throwIfInstanceOf(cause, IOException.class);
            Throwables.throwIfUnchecked(cause);
            throw new IOException(cause);
        }
    }

    private static String upload(CloseableHttpClient client, HttpPut request, InputStream input) throws IOException {
        request.setEntity(new InputStreamEntity(input, -1, ContentType.TEXT_PLAIN));
        try (CloseableHttpResponse response = client.execute(request)) {
            return EntityUtils.toString(response.getEntity());
        }
    }

    private static void requestFile(MysqlChannel channel, String path) throws IOException {
        MysqlSerializer serializer = channel.getSerializer();
        serializer.reset();
        serializer.writeByte((byte) 0xfb);
        serializer.writeEofString(path);
        try {
            channel.sendAndFlush(serializer.toByteBuffer());
        } catch (IOException failure) {
            // The peer may already have seen part of the request.
            channel.close();
            throw failure;
        }
    }

    /** A client file ends at exactly one empty MySQL packet. Closing drains any unread part of this file. */
    private static final class LocalFileInputStream extends InputStream {
        private final MysqlChannel channel;
        private ByteBuffer packet = ByteBuffer.allocate(0);
        private boolean finished;

        LocalFileInputStream(MysqlChannel channel) {
            this.channel = channel;
        }

        private boolean nextPacket() throws IOException {
            if (finished) {
                return false;
            }
            try {
                ByteBuffer next = channel.fetchOnePacket();
                if (next == null) {
                    throw new EOFException("Client disconnected before the LOCAL INFILE terminator");
                }
                packet = next;
                finished = !packet.hasRemaining();
                return !finished;
            } catch (IOException failure) {
                // A short read or a sequence error leaves no usable command boundary.
                finished = true;
                channel.close();
                throw failure;
            }
        }

        @Override
        public int read() throws IOException {
            return packet.hasRemaining() || nextPacket() ? Byte.toUnsignedInt(packet.get()) : -1;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            if (length == 0) {
                return 0;
            }
            if (!packet.hasRemaining() && !nextPacket()) {
                return -1;
            }
            int count = Math.min(length, packet.remaining());
            packet.get(bytes, offset, count);
            return count;
        }

        @Override
        public void close() throws IOException {
            while (nextPacket()) {
                // A rejected or aborted HTTP request may have consumed none of the client's upload.
            }
        }
    }

    private HttpPut generateRequestForMySqlLoad(ConnectContext context,
            NereidsDataDescription desc,
            String database,
            String table,
            String token) throws LoadException {
        final HttpPut httpPut = new HttpPut(selectBackendForMySqlLoad(context, database, table));

        httpPut.addHeader("Expect", "100-continue");
        httpPut.addHeader("Content-Type", "text/plain");
        httpPut.addHeader("token", token);

        UserIdentity uid = context.getCurrentUserIdentity();
        if (uid == null || StringUtils.isEmpty(uid.getQualifiedUser())) {
            throw new LoadException("user is null");
        }
        // NOTE: set pass word empty here because password is only used when login from mysql client.
        // All authentication actions after login in do not require a password
        String auth = String.format("%s:%s", uid.getQualifiedUser(), "");
        String authEncoding = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        httpPut.addHeader("Authorization", "Basic " + authEncoding);

        Map<String, String> props = desc.getProperties();
        FileFormatProperties fileFormatProperties = desc.getFileFormatProperties();
        if (!(fileFormatProperties instanceof CsvFileFormatProperties)) {
            throw new LoadException("Only support csv file format for mysql load");
        }
        CsvFileFormatProperties csvFileFormatProperties = (CsvFileFormatProperties) fileFormatProperties;
        if (props != null) {
            // max_filter_ratio
            if (props.containsKey(LoadCommand.KEY_IN_PARAM_MAX_FILTER_RATIO)) {
                String maxFilterRatio = props.get(LoadCommand.KEY_IN_PARAM_MAX_FILTER_RATIO);
                httpPut.addHeader(LoadCommand.KEY_IN_PARAM_MAX_FILTER_RATIO, maxFilterRatio);
            }

            // exec_mem_limit
            if (props.containsKey(LoadCommand.EXEC_MEM_LIMIT)) {
                String memory = props.get(LoadCommand.EXEC_MEM_LIMIT);
                httpPut.addHeader(LoadCommand.EXEC_MEM_LIMIT, memory);
            }

            // strict_mode
            if (props.containsKey(LoadCommand.STRICT_MODE)) {
                String strictMode = props.get(LoadCommand.STRICT_MODE);
                httpPut.addHeader(LoadCommand.STRICT_MODE, strictMode);
            }

            // timeout
            if (props.containsKey(LoadCommand.TIMEOUT_PROPERTY)) {
                String timeout = props.get(LoadCommand.TIMEOUT_PROPERTY);
                httpPut.addHeader(LoadCommand.TIMEOUT_PROPERTY, timeout);
            }

            // timezone
            if (props.containsKey(LoadCommand.TIMEZONE)) {
                String timezone = props.get(LoadCommand.TIMEZONE);
                httpPut.addHeader(LoadCommand.TIMEZONE, timezone);
            }
        }

        httpPut.addHeader(CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES,
                String.valueOf(csvFileFormatProperties.isTrimDoubleQuotes()));
        httpPut.addHeader(CsvFileFormatProperties.PROP_ENCLOSE,
                new String(new byte[] {csvFileFormatProperties.getEnclose()}));
        httpPut.addHeader(CsvFileFormatProperties.PROP_ESCAPE,
                new String(new byte[] {csvFileFormatProperties.getEscape()}));
        httpPut.addHeader(CsvFileFormatProperties.PROP_SKIP_LINES,
                Integer.toString(csvFileFormatProperties.getSkipLines()));
        httpPut.addHeader(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, csvFileFormatProperties.getColumnSeparator());
        httpPut.addHeader(CsvFileFormatProperties.PROP_LINE_DELIMITER, csvFileFormatProperties.getLineDelimiter());

        // columns
        String columns = getColumns(desc);
        if (columns != null) {
            httpPut.addHeader(LoadCommand.KEY_IN_PARAM_COLUMNS, columns);
        }

        // partitions
        if (desc.getPartitionNamesInfo() != null && !desc.getPartitionNamesInfo().getPartitionNames().isEmpty()) {
            List<String> ps = desc.getPartitionNamesInfo().getPartitionNames();
            String pNames = Joiner.on(",").join(ps);
            if (desc.getPartitionNamesInfo().isTemp()) {
                httpPut.addHeader(LoadCommand.KEY_IN_PARAM_TEMP_PARTITIONS, pNames);
            } else {
                httpPut.addHeader(LoadCommand.KEY_IN_PARAM_PARTITIONS, pNames);
            }
        }

        // cloud cluster
        if (Config.isCloudMode()) {
            String clusterName = "";
            try {
                clusterName = context.getCloudCluster();
            } catch (Exception e) {
                LOG.warn("failed to get compute group: " + e.getMessage());
                throw new LoadException("failed to get compute group: " + e.getMessage());
            }
            if (Strings.isNullOrEmpty(clusterName)) {
                throw new LoadException("cloud compute group is empty");
            }
            httpPut.addHeader(LoadCommand.KEY_CLOUD_CLUSTER, clusterName);
        }

        return httpPut;
    }

    private HttpPut generateRequestForMySqlLoadV2(ConnectContext context,
            MysqlDataDescription desc,
            String database,
            String table,
            String token) throws LoadException {
        final HttpPut httpPut = new HttpPut(selectBackendForMySqlLoad(context, database, table));

        httpPut.addHeader("Expect", "100-continue");
        httpPut.addHeader("Content-Type", "text/plain");
        httpPut.addHeader("token", token);

        UserIdentity uid = context.getCurrentUserIdentity();
        if (uid == null || StringUtils.isEmpty(uid.getQualifiedUser())) {
            throw new LoadException("user is null");
        }
        // NOTE: set pass word empty here because password is only used when login from mysql client.
        // All authentication actions after login in do not require a password
        String auth = String.format("%s:%s", uid.getQualifiedUser(), "");
        String authEncoding = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        httpPut.addHeader("Authorization", "Basic " + authEncoding);

        Map<String, String> props = desc.getProperties();
        FileFormatProperties fileFormatProperties = desc.getFileFormatProperties();
        if (!(fileFormatProperties instanceof CsvFileFormatProperties)) {
            throw new LoadException("Only support csv file format for mysql load");
        }
        CsvFileFormatProperties csvFileFormatProperties = (CsvFileFormatProperties) fileFormatProperties;
        if (props != null) {
            // max_filter_ratio
            if (props.containsKey(MysqlLoadCommand.MAX_FILTER_RATIO_PROPERTY)) {
                String maxFilterRatio = props.get(MysqlLoadCommand.MAX_FILTER_RATIO_PROPERTY);
                httpPut.addHeader(MysqlLoadCommand.MAX_FILTER_RATIO_PROPERTY, maxFilterRatio);
            }

            // exec_mem_limit
            if (props.containsKey(MysqlLoadCommand.EXEC_MEM_LIMIT_PROPERTY)) {
                String memory = props.get(MysqlLoadCommand.EXEC_MEM_LIMIT_PROPERTY);
                httpPut.addHeader(MysqlLoadCommand.EXEC_MEM_LIMIT_PROPERTY, memory);
            }

            // strict_mode
            if (props.containsKey(MysqlLoadCommand.STRICT_MODE_PROPERTY)) {
                String strictMode = props.get(MysqlLoadCommand.STRICT_MODE_PROPERTY);
                httpPut.addHeader(MysqlLoadCommand.STRICT_MODE_PROPERTY, strictMode);
            }

            // timeout
            if (props.containsKey(MysqlLoadCommand.TIMEOUT_PROPERTY)) {
                String timeout = props.get(MysqlLoadCommand.TIMEOUT_PROPERTY);
                httpPut.addHeader(MysqlLoadCommand.TIMEOUT_PROPERTY, timeout);
            }

            // timezone
            if (props.containsKey(MysqlLoadCommand.TIMEZONE_PROPERTY)) {
                String timezone = props.get(MysqlLoadCommand.TIMEZONE_PROPERTY);
                httpPut.addHeader(MysqlLoadCommand.TIMEZONE_PROPERTY, timezone);
            }
        }

        httpPut.addHeader(CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES,
                String.valueOf(csvFileFormatProperties.isTrimDoubleQuotes()));
        httpPut.addHeader(CsvFileFormatProperties.PROP_ENCLOSE,
                new String(new byte[] {csvFileFormatProperties.getEnclose()}));
        httpPut.addHeader(CsvFileFormatProperties.PROP_ESCAPE,
                new String(new byte[] {csvFileFormatProperties.getEscape()}));
        httpPut.addHeader(CsvFileFormatProperties.PROP_SKIP_LINES,
                Integer.toString(csvFileFormatProperties.getSkipLines()));
        httpPut.addHeader(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, csvFileFormatProperties.getColumnSeparator());
        httpPut.addHeader(CsvFileFormatProperties.PROP_LINE_DELIMITER, csvFileFormatProperties.getLineDelimiter());

        // columns
        String columns = getColumns(desc);
        if (columns != null) {
            httpPut.addHeader(MysqlLoadCommand.KEY_IN_PARAM_COLUMNS, columns);
        }

        // partitions
        if (!desc.getPartitionNamesInfo().getPartitionNames().isEmpty()) {
            List<String> ps = desc.getPartitionNamesInfo().getPartitionNames();
            String pNames = Joiner.on(",").join(ps);
            if (desc.getPartitionNamesInfo().isTemp()) {
                httpPut.addHeader(MysqlLoadCommand.KEY_IN_PARAM_TEMP_PARTITIONS, pNames);
            } else {
                httpPut.addHeader(MysqlLoadCommand.KEY_IN_PARAM_PARTITIONS, pNames);
            }
        }

        // cloud cluster
        if (Config.isCloudMode()) {
            String clusterName = "";
            try {
                clusterName = context.getCloudCluster();
            } catch (Exception e) {
                LOG.warn("failed to get compute group: " + e.getMessage());
                throw new LoadException("failed to get compute group: " + e.getMessage());
            }
            if (Strings.isNullOrEmpty(clusterName)) {
                throw new LoadException("cloud compute group is empty");
            }
            httpPut.addHeader(MysqlLoadCommand.KEY_CLOUD_CLUSTER, clusterName);
        }

        return httpPut;
    }

    private String selectBackendForMySqlLoad(ConnectContext context, String database, String table)
            throws LoadException {
        Backend backend = null;
        if (Config.isCloudMode()) {
            String clusterName = "";
            try {
                clusterName = context.getCloudCluster();
            } catch (Exception e) {
                LOG.warn("failed to get cloud cluster: " + e.getMessage());
                throw new LoadException("failed to get cloud cluster: " + e);
            }
            backend = StreamLoadHandler.selectBackend(clusterName);
            if (backend == null) {
                throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG
                        + ", cluster: " + clusterName);
            }
        } else {
            BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needLoadAvailable().build();
            // The backend selection policy may reorder all eligible candidates.
            List<Long> backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, -1);
            if (backendIds.isEmpty()) {
                throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
            }
            List<Backend> candidates = new ArrayList<>();
            for (Long backendId : backendIds) {
                Backend candidate = Env.getCurrentSystemInfo().getBackend(backendId);
                if (candidate != null) {
                    candidates.add(candidate);
                }
            }
            backend = BackendSelectionManager.chooseLoadBackend(context, candidates);
            if (backend == null) {
                throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
            }
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
