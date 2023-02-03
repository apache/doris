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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.io.ByteBufferNetworkInputStream;
import org.apache.doris.load.LoadJobRowResult;
import org.apache.doris.load.loadv2.LoadTask.MergeType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterTxnExecutor;
import org.apache.doris.qe.StreamLoadTxnExecutor;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
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
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
        List<String> filePaths = dataDesc.getFilePaths();
        if (Config.use_http_mysql_load_job) {
            String database = dataDesc.getDbName();
            String table = dataDesc.getTableName();
            try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {
                for (String file : filePaths) {
                    InputStreamEntity entity = getInputStreamEntity(context, dataDesc.isClientLocal(), file);
                    HttpPut request = generateRequestForMySqlLoad(entity, dataDesc, database, table);
                    try (final CloseableHttpResponse response = httpclient.execute(request)) {
                        JsonObject result = JsonParser.parseString(EntityUtils.toString(response.getEntity()))
                                .getAsJsonObject();
                        if (!result.get("Status").getAsString().equalsIgnoreCase("Success")) {
                            LOG.warn("Execute stream load for mysql data load failed with message: " + request);
                            throw new LoadException(result.get("Message").getAsString());
                        }
                        loadResult.incRecords(result.get("NumberLoadedRows").getAsLong());
                        loadResult.incSkipped(result.get("NumberFilteredRows").getAsInt());
                    }
                }
            }
        } else {
            StreamLoadTxnExecutor executor = null;
            try {
                String database = dataDesc.getFullDatabaseName();
                String table = dataDesc.getTableName();
                TransactionEntry entry = prepareTransactionEntry(database, table);
                openTxn(context, entry);
                executor = beginTxn(context, entry, dataDesc);
                // sendData
                for (String file : filePaths) {
                    sendData(context, executor, file);
                }
                executor.commitTransaction();
            } catch (Exception e) {
                LOG.error("Failed to load mysql data into doris", e);
                if (executor != null) {
                    try {
                        executor.abortTransaction();
                    } catch (Exception ex) {
                        throw new LoadException("Failed when abort the transaction", ex);
                    }
                }
                throw new LoadException("Load failed when execute the mysql data load", e);
            }
        }
        return loadResult;
    }

    private TransactionEntry prepareTransactionEntry(String database, String table)
            throws TException {
        TTxnParams txnConf = new TTxnParams();
        txnConf.setNeedTxn(true).setEnablePipelineTxnLoad(Config.enable_pipeline_load)
                .setThriftRpcTimeoutMs(5000).setTxnId(-1).setDb("").setTbl("");
        Database dbObj = Env.getCurrentInternalCatalog()
                .getDbOrException(database, s -> new TException("database is invalid for dbName: " + s));
        Table tblObj = dbObj.getTableOrException(table, s -> new TException("table is invalid: " + s));
        txnConf.setDbId(dbObj.getId()).setTbl(table).setDb(database);

        TransactionEntry txnEntry = new TransactionEntry();
        txnEntry.setTxnConf(txnConf);
        txnEntry.setTable(tblObj);
        txnEntry.setDb(dbObj);
        String label = UUID.randomUUID().toString();
        txnEntry.setLabel(label);
        return txnEntry;
    }

    private void openTxn(ConnectContext context, TransactionEntry txnEntry) throws Exception {
        TTxnParams txnParams = txnEntry.getTxnConf();
        long timeoutSecond = ConnectContext.get().getSessionVariable().getQueryTimeoutS();
        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;
        if (Env.getCurrentEnv().isMaster()) {
            long txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                    txnParams.getDbId(), Lists.newArrayList(txnEntry.getTable().getId()),
                    txnEntry.getLabel(), new TransactionState.TxnCoordinator(
                            TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                    sourceType, timeoutSecond);
            txnParams.setTxnId(txnId);
            String authCodeUuid = Env.getCurrentGlobalTransactionMgr().getTransactionState(
                    txnParams.getDbId(), txnParams.getTxnId()).getAuthCode();
            txnParams.setAuthCodeUuid(authCodeUuid);
        } else {
            String authCodeUuid = UUID.randomUUID().toString();
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(context);
            TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();
            request.setDb(txnParams.getDb()).setTbl(txnParams.getTbl()).setAuthCodeUuid(authCodeUuid)
                    .setCluster(txnEntry.getDb().getClusterName()).setLabel(txnEntry.getLabel())
                    .setUser("").setUserIp("").setPasswd("");
            TLoadTxnBeginResult result = masterTxnExecutor.beginTxn(request);
            txnParams.setTxnId(result.getTxnId());
            txnParams.setAuthCodeUuid(authCodeUuid);
        }
    }

    private StreamLoadTxnExecutor beginTxn(ConnectContext context, TransactionEntry txnEntry, DataDescription desc)
            throws Exception {
        TTxnParams txnParams = txnEntry.getTxnConf();
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(txnParams.getTxnId())
                .setDb(txnParams.getDb())
                .setTbl(txnParams.getTbl())
                .setFileType(TFileType.FILE_STREAM)
                .setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND)
                .setThriftRpcTimeoutMs(5000)
                .setLoadId(context.queryId());
        if (desc.getProperties() != null) {
            // max_filter_ratio
            if (desc.getProperties().containsKey(LoadStmt.KEY_IN_PARAM_MAX_FILTER_RATIO)) {
                String maxFilterRatio = desc.getProperties().get(LoadStmt.KEY_IN_PARAM_MAX_FILTER_RATIO);
                request.setMaxFilterRatio(Float.parseFloat(maxFilterRatio));
            }

            // exec_mem_limit
            if (desc.getProperties().containsKey(LoadStmt.EXEC_MEM_LIMIT)) {
                String memory = desc.getProperties().get(LoadStmt.EXEC_MEM_LIMIT);
                request.setExecMemLimit(Long.parseLong(memory));
            }

            // strict_mode
            if (desc.getProperties().containsKey(LoadStmt.STRICT_MODE)) {
                String strictMode = desc.getProperties().get(LoadStmt.STRICT_MODE);
                request.setStrictMode(Boolean.parseBoolean(strictMode));
            }
        }

        // column_separator
        if (desc.getColumnSeparator() != null) {
            request.setColumnSeparator(desc.getColumnSeparator());
        }

        // line_delimiter
        if (desc.getLineDelimiter() != null) {
            request.setLineDelimiter(desc.getLineDelimiter());
        }

        // columns
        String columns = getColumns(desc);
        if (columns != null) {
            request.setColumns(columns);
        }

        // partitions
        if (desc.getPartitionNames() != null && !desc.getPartitionNames().getPartitionNames().isEmpty()) {
            List<String> ps = desc.getPartitionNames().getPartitionNames();
            String pNames = Joiner.on(",").join(ps);
            request.setIsTempPartition(desc.getPartitionNames().isTemp());
            request.setPartitions(pNames);
        }
        if (desc.getSkipLines() != 0) {
            request.setSkipLines(desc.getSkipLines());
        }
        // execute begin txn
        StreamLoadTxnExecutor executor = new StreamLoadTxnExecutor(txnEntry, TFileFormatType.FORMAT_CSV_PLAIN);
        executor.beginTransaction(request);
        return executor;
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

    private void sendData(ConnectContext context, StreamLoadTxnExecutor executor, String file) throws Exception {
        replyClientForReadFile(context, file);
        ByteBuffer buffer;
        try {
            buffer = context.getMysqlChannel().fetchOnePacket();
            // MySql client will send an empty packet when eof
            while (buffer != null && buffer.limit() != 0) {
                executor.sendData(buffer);
                buffer = context.getMysqlChannel().fetchOnePacket();
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
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
            ByteBuffer buffer;
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
        httpPut.addHeader("cluster_token", Env.getCurrentEnv().getToken());

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

        // merge_type
        if (!desc.getMergeType().equals(MergeType.APPEND)) {
            httpPut.addHeader(LoadStmt.KEY_IN_PARAM_MERGE_TYPE, desc.getMergeType().name());
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
