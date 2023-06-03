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

package org.apache.doris.regression.suite

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.google.gson.Gson
import groovy.json.JsonSlurper
import com.google.common.collect.ImmutableList
import org.apache.doris.regression.action.BenchmarkAction
import org.apache.doris.regression.json.BinlogData
import org.apache.doris.regression.json.PartitionRecords
import org.apache.doris.regression.util.DataUtils
import org.apache.doris.regression.util.OutputUtils
import org.apache.doris.regression.action.CreateMVAction
import org.apache.doris.regression.action.ExplainAction
import org.apache.doris.regression.action.RestoreAction
import org.apache.doris.regression.action.StreamLoadAction
import org.apache.doris.regression.action.SuiteAction
import org.apache.doris.regression.action.TestAction
import org.apache.doris.regression.action.HttpCliAction
import org.apache.doris.regression.util.JdbcUtils
import org.apache.doris.regression.util.Hdfs
import org.apache.doris.regression.util.SuiteUtils
import org.apache.doris.regression.util.SyncerUtils
import org.apache.doris.thrift.TBeginTxnResult
import org.apache.doris.thrift.TBinlog
import org.apache.doris.thrift.TBinlogType
import org.apache.doris.thrift.TCommitTxnResult
import org.apache.doris.thrift.TGetBinlogResult
import org.apache.doris.thrift.TIngestBinlogRequest
import org.apache.doris.thrift.TIngestBinlogResult
import org.apache.doris.thrift.TNetworkAddress
import org.apache.doris.thrift.TStatus
import org.apache.doris.thrift.TStatusCode
import org.apache.doris.thrift.TTabletCommitInfo
import org.apache.doris.thrift.TUniqueId
import org.apache.thrift.transport.TTransportException
import org.apache.tools.ant.taskdefs.Sync
import org.junit.jupiter.api.Assertions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.util.logging.Slf4j

import java.util.Map.Entry
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors
import java.util.stream.LongStream
import static org.apache.doris.regression.util.DataUtils.sortByToString

import java.sql.PreparedStatement
import java.sql.ResultSetMetaData
import org.junit.Assert

@Slf4j
class Suite implements GroovyInterceptable {
    final SuiteContext context
    final String name
    final String group
    final Logger logger = LoggerFactory.getLogger(this.class)

    final List<Closure> successCallbacks = new Vector<>()
    final List<Closure> failCallbacks = new Vector<>()
    final List<Closure> finishCallbacks = new Vector<>()
    final List<Throwable> lazyCheckExceptions = new Vector<>()
    final List<Future> lazyCheckFutures = new Vector<>()

    enum ccrCluster {
        SOURCE,
        TARGET
    }

    Suite(String name, String group, SuiteContext context) {
        this.name = name
        this.group = group
        this.context = context
    }

    String getConf(String key, String defaultValue = null) {
        String value = context.config.otherConfigs.get(key)
        return value == null ? defaultValue : value
    }

    Properties getConfs(String prefix) {
        Properties p = new Properties()
        for (String name : context.config.otherConfigs.stringPropertyNames()) {
            if (name.startsWith(prefix + ".")) {
                p.put(name.substring(prefix.length() + 1), context.config.getProperty(name))
            }
        }
        return p
    }

    void onSuccess(Closure callback) {
        successCallbacks.add(callback)
    }

    void onFail(Closure callback) {
        failCallbacks.add(callback)
    }

    void onFinish(Closure callback) {
        finishCallbacks.add(callback)
    }

    LongStream range(long startInclusive, long endExclusive) {
        return LongStream.range(startInclusive, endExclusive)
    }

    LongStream rangeClosed(long startInclusive, long endInclusive) {
        return LongStream.rangeClosed(startInclusive, endInclusive)
    }

    String toCsv(List<Object> rows) {
        StringBuilder sb = new StringBuilder()
        for (int i = 0; i < rows.size(); ++i) {
            Object row = rows.get(i)
            if (!(row instanceof List)) {
                row = ImmutableList.of(row)
            }
            sb.append(OutputUtils.toCsvString(row as List)).append("\n")
        }
        sb.toString()
    }

    Object parseJson(String str) {
        def jsonSlurper = new JsonSlurper()
        return jsonSlurper.parseText(str)
    }

    public <T> T lazyCheck(Closure<T> closure) {
        try {
            T result = closure.call()
            if (result instanceof Future) {
                lazyCheckFutures.add(result)
            }
            return result
        } catch (Throwable t) {
            lazyCheckExceptions.add(t)
            return null
        }
    }

    void doLazyCheck() {
        if (!lazyCheckExceptions.isEmpty()) {
            throw lazyCheckExceptions.get(0)
        }
        lazyCheckFutures.forEach { it.get() }
    }

    public <T> Tuple2<T, Long> timer(Closure<T> actionSupplier) {
        return SuiteUtils.timer(actionSupplier)
    }

    public <T> ListenableFuture<T> thread(String threadName = null, Closure<T> actionSupplier) {
        return MoreExecutors.listeningDecorator(context.actionExecutors).submit((Callable<T>) {
            long startTime = System.currentTimeMillis()
            def originThreadName = Thread.currentThread().name
            try {
                Thread.currentThread().setName(threadName == null ? originThreadName : threadName)
                context.scriptContext.eventListeners.each { it.onThreadStarted(context) }

                return actionSupplier.call()
            } catch (Throwable t) {
                context.scriptContext.eventListeners.each { it.onThreadFailed(context, t) }
                throw t
            } finally {
                try {
                    context.closeThreadLocal()
                } catch (Throwable t) {
                    logger.warn("Close thread local context failed", t)
                }
                long finishTime = System.currentTimeMillis()
                context.scriptContext.eventListeners.each { it.onThreadFinished(context, finishTime - startTime) }
                Thread.currentThread().setName(originThreadName)
            }
        })
    }

    public <T> ListenableFuture<T> lazyCheckThread(String threadName = null, Closure<T> actionSupplier) {
        return lazyCheck {
            thread(threadName, actionSupplier)
        }
    }

    public <T> ListenableFuture<T> combineFutures(ListenableFuture<T> ... futures) {
        return Futures.allAsList(futures)
    }

    public <T> ListenableFuture<List<T>> combineFutures(Iterable<? extends ListenableFuture<? extends T>> futures) {
        return Futures.allAsList(futures)
    }

    public <T> T connect(String user = context.config.jdbcUser, String password = context.config.jdbcPassword,
                         String url = context.config.jdbcUrl, Closure<T> actionSupplier) {
        return context.connect(user, password, url, actionSupplier)
    }

    private Boolean checkBinlog(TBinlog binlog) {
        SyncerContext syncerContext = context.getSyncerContext()

        // step 1: check binlog availability
        if (binlog == null) {
            return false
        }

        // step 2: check and set seq to context
        if (binlog.isSetCommitSeq()) {
            syncerContext.seq = binlog.getCommitSeq()
            logger.info("Now last seq is ${syncerContext.seq}")
        } else {
            logger.error("Invalid binlog! binlog seq is unset.")
            return false
        }

        // step 3: print binlog type
        if (binlog.isSetType()) {
            String typeName
            switch (binlog.getType()) {
                case TBinlogType.UPSERT:
                    typeName = "UPSERT"
                    break
                case TBinlogType.CREATE_TABLE:
                    typeName = "CREATE_TABLE"
                    break
                case TBinlogType.ADD_PARTITION:
                    typeName = "ADD_PARTITION"
                    break
                default:
                    typeName = "UNKNOWN"
                    break
            }
            logger.info("binlog type name is ${typeName}")
        }

        // step 4: check binlog data is set and get metadata
        if (binlog.isSetData()) {
            String data = binlog.getData()
            logger.info("binlog data is ${data}")
            Gson gson = new Gson()
            getSourceMeta(syncerContext, gson.fromJson(data, BinlogData.class))
            logger.info("Source tableId: ${syncerContext.sourceTableId}, ${syncerContext.sourcePartitionMap}")
        } else {
            logger.error("binlog data is not contains data!")
            return false
        }

        return true
    }

    private Boolean checkGetBinlog(TGetBinlogResult result, ccrCluster cluster) {
        SyncerContext syncerContext = context.getSyncerContext()
        TBinlog binlog = null

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.BINLOG_TOO_OLD_COMMIT_SEQ:
                    case TStatusCode.OK:
                        if (result.isSetBinlogs()) {
                            binlog = result.getBinlogs().first()
                        }
                        break
                    case TStatusCode.BINLOG_DISABLE:
                        logger.error("Binlog is disabled!")
                        break
                    case TStatusCode.BINLOG_TOO_NEW_COMMIT_SEQ:
                        logger.error("Binlog is too new! Msg: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.BINLOG_NOT_FOUND_DB:
                        logger.error("Binlog not found DB! DB: ${syncerContext.db}")
                        break
                    case TStatusCode.BINLOG_NOT_FOUND_TABLE:
                        logger.error("Binlog not found table!")
                        break
                    default:
                        logger.error("Binlog result is an unexpected code: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TGetBinlogResult! result: ${result}")
        }

        // step 2: check binlog
        return checkBinlog(binlog)
    }

    private Boolean checkBeginTxn(TBeginTxnResult result) {
        SyncerContext syncerContext = context.getSyncerContext()
        Boolean isCheckedOK = false

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        isCheckedOK = true
                        break
                    case TStatusCode.LABEL_ALREADY_EXISTS:
                        logger.error("Begin transaction label is exist! job status: ${result.getJobStatus()}")
                        break
                    case TStatusCode.ANALYSIS_ERROR:
                        logger.error("Begin transaction analysis error! error massage: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.INTERNAL_ERROR:
                        logger.error("Begin transaction internal error! error massage: ${status.getErrorMsgs()}")
                        break
                    default:
                        logger.error("Begin transaction result is an unexpected code: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TBeginTxnResult! result: ${result}")
        }

        // step 2: check and set txnId to context
        if (isCheckedOK && result.isSetTxnId()) {
            logger.info("Begin transaction id is ${result.getTxnId()}")
            syncerContext.txnId = result.getTxnId()
        } else {
            logger.error("Begin transaction txnId is unset!")
            isCheckedOK = false
        }

        // step 3: print result information
        if (isCheckedOK) {
            if (result.isSetDbId()) {
                logger.info("Begin transaction db id is ${result.getDbId()}")
            }
        }
        return isCheckedOK
    }

    private Boolean checkIngestBinlog(TIngestBinlogResult result) {
        Boolean isCheckedOK = false
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        isCheckedOK = true
                        break
                    case TStatusCode.NOT_IMPLEMENTED_ERROR:
                        logger.error("Ingest binlog enable feature binlog is false! job status: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.ANALYSIS_ERROR:
                        logger.error("Ingest binlog analysis error! error massage: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.RUNTIME_ERROR:
                        logger.error("Ingest binlog runtime error! error massage: ${status.getErrorMsgs()}")
                        break
                    default:
                        logger.error("Ingest binlog result is an unexpected code: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TIngestBinlogResult! result: ${result}")
        }
        return isCheckedOK
    }

    private Boolean checkCommitTxn(TCommitTxnResult result) {
        SyncerContext syncerContext = context.getSyncerContext()
        Boolean isCheckedOK = false

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        isCheckedOK = true
                        break
                    case TStatusCode.PUBLISH_TIMEOUT:
                        logger.error("Commit transaction publish timeout! job status: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.ANALYSIS_ERROR:
                        logger.error("Commit transaction analysis error! error massage: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.INTERNAL_ERROR:
                        logger.error("Commit transaction internal error! error massage: ${status.getErrorMsgs()}")
                        break
                    default:
                        logger.error("Commit transaction result is an unexpected code: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TCommitTxnResult! result: ${result}")
        }

        if (isCheckedOK) {
            syncerContext.sourcePartitionMap.clear()
            syncerContext.commitInfos.clear()
            syncerContext.closeSourceBackendClients()
        }

        return isCheckedOK
    }

    HashMap<Long, BackendClientImpl> getBackendClientsImpl(ccrCluster cluster) throws TTransportException {
        HashMap<Long, BackendClientImpl> clientsMap = new HashMap<Long, BackendClientImpl>()
        String backendSQL = "SHOW PROC '/backends'"
        List<List<Object>> backendInformation
        if (cluster == ccrCluster.SOURCE) {
            backendInformation = sql(backendSQL)
        } else {
            backendInformation = target_sql(backendSQL)
        }
        for (List<Object> row : backendInformation) {
            TNetworkAddress address = new TNetworkAddress(row[1] as String, row[3] as int)
            BackendClientImpl client = new BackendClientImpl(address, row[4] as int)
            clientsMap.put(row[0] as Long, client)
        }
        return clientsMap
    }

    void getBackendClients() {
        logger.info("Begin to get backend's maps.")
        SyncerContext syncerContext = context.getSyncerContext()

        // get source backend clients
        try {
            syncerContext.sourceBackendClients = getBackendClientsImpl(ccrCluster.SOURCE)
        } catch (TTransportException e) {
            logger.error("Create source cluster backend client fail: ${e.toString()}")
        }

        // get target backend clients
        try {
            syncerContext.targetBackendClients = getBackendClientsImpl(ccrCluster.TARGET)
        } catch (TTransportException e) {
            logger.error("Create target cluster backend client fail: ${e.toString()}")
        }
    }

    void getSourceMeta(SyncerContext syncerContext, BinlogData binlogData) {
        Entry<Long, PartitionRecords> tableEntry = binlogData.tableRecords.entrySet()[0]
        String metaSQL = "SHOW PROC '/dbs/" + binlogData.dbId.toString() + "/" +
                                              tableEntry.key.toString() + "/partitions/"

        syncerContext.sourceTableId = tableEntry.key
        tableEntry.value.partitionRecords.forEach(data -> {
            PartitionMeta partitionMeta = new PartitionMeta(data.version)
            List<List<Object>> sqlInfo
            String partitionSQL = metaSQL + data.partitionId.toString()
            sqlInfo = sql(partitionSQL + "'")
            partitionSQL += "/" + (sqlInfo[0][0] as Long).toString()
            sqlInfo = sql(partitionSQL + "'")
            sqlInfo.forEach(row -> {
                partitionMeta.tabletMeta.put((row[0] as Long), (row[2] as Long))
            })
            syncerContext.sourcePartitionMap.put(data.partitionId, partitionMeta)
        })
    }

    Boolean get_target_meta(String table) {
        logger.info("Get target cluster meta data.")
        SyncerContext syncerContext = context.getSyncerContext()
        String baseSQL = "SHOW PROC '/dbs"
        List<List<Object>> sqlInfo

        // step 1: get target dbId
        Long dbId = -1
        sqlInfo = target_sql(baseSQL + "'")
        for (List<Object> row : sqlInfo) {
            if ((row[1] as String).contains(context.dbName)) {
                dbId = row[0] as Long
                break
            }
        }
        if (dbId == -1) {
            logger.error("Target cluster get ${context.dbName} db error.")
            return false
        }

        // step 2: get target dbId/tableId
        baseSQL += "/" + dbId.toString()
        sqlInfo = target_sql(baseSQL + "'")
        Long tableId = -1
        for (List<Object> row : sqlInfo) {
            if ((row[1] as String).contains(table)) {
                tableId = (row[0] as Long)
                break
            }
        }
        if (tableId == -1) {
            logger.error("Target cluster get ${table} tableId fault.")
            return false
        }
        syncerContext.targetTableId = tableId

        // step 3: get partitionIds
        baseSQL += "/" + tableId.toString() + "/partitions"
        ArrayList<Long> partitionIds = new ArrayList<Long>()
        sqlInfo = target_sql(baseSQL + "'")
        for (List<Object> row : sqlInfo) {
            partitionIds.add(row[0] as Long)
        }
        if (partitionIds.isEmpty()) {
            logger.error("Target cluster get partitions fault.")
            return false
        }

        // step 4: get partitionMetas
        for (Long id : partitionIds) {
            PartitionMeta meta = new PartitionMeta(-1)

            // step 4.1: get partition/indexId
            String partitionSQl = baseSQL + "/" + id.toString()
            Long indexId
            sqlInfo = target_sql(partitionSQl + "'")
            if (sqlInfo.isEmpty()) {
                logger.error("Target cluster partition-${id} indexId fault.")
                return false
            }
            indexId = sqlInfo[0][0] as Long

            // step 4.2: get partition/indexId/tabletId
            partitionSQl += "/" + indexId.toString()
            sqlInfo = target_sql(partitionSQl + "'")
            for (List<Object> row : sqlInfo) {
                meta.tabletMeta.put(row[0] as Long, row[2] as Long)
            }
            if (meta.tabletMeta.isEmpty()) {
                logger.error("Target cluster get (partitionId/indexId)-(${id}/${indexId}) tabletIds fault.")
                return false
            }

            syncerContext.targetPartitionMap.put(id, meta)
        }

        logger.info("Target cluster metadata: ${syncerContext.targetPartitionMap}")
        return true
    }

    Boolean get_binlog(String table) {
        SyncerContext syncerContext = context.getSyncerContext()
        FrontendClientImpl clientImpl = context.getSourceFrontClient()
        logger.info("Get binlog from source cluster ${context.config.feSourceThriftNetworkAddress}, binlog seq: ${syncerContext.seq}")
        TGetBinlogResult result = SyncerUtils.getBinLog(clientImpl, syncerContext, table)
        return checkGetBinlog(result, ccrCluster.SOURCE)
    }

    Boolean begin_txn(String table) {
        logger.info("Begin transaction to target cluster ${context.config.feTargetThriftNetworkAddress}")
        getBackendClients()
        SyncerContext syncerContext = context.getSyncerContext()
        FrontendClientImpl clientImpl = context.getTargetFrontClient()
        TBeginTxnResult result = SyncerUtils.beginTxn(clientImpl, syncerContext, table)
        return checkBeginTxn(result)
    }

    Boolean ingest_binlog(String table) {
        logger.info("Begin to ingest binlog.")
        SyncerContext syncerContext = context.getSyncerContext()

        // step 1: Check meta data is valid
        if (!syncerContext.metaIsValid()) {
            logger.error("Meta data miss match")ß
            return false
        }

        // step 2: Begin ingest task
        Iterator sourcePartitionIter = syncerContext.sourcePartitionMap.iterator()
        Iterator targetPartitionIter = syncerContext.targetPartitionMap.iterator()

        // step 3:
        while (sourcePartitionIter.hasNext()) {
            Entry srcPartition = sourcePartitionIter.next()
            Entry tarPartition = targetPartitionIter.next()
            Iterator srcTabletIter = srcPartition.value.tabletMeta.iterator()
            Iterator tarTabletIter = tarPartition.value.tabletMeta.iterator()

            while (srcTabletIter.hasNext()) {
                Entry srcTabletMap = srcTabletIter.next()
                Entry tarTabletMap = tarTabletIter.next()

                BackendClientImpl srcClient = syncerContext.sourceBackendClients.get(srcTabletMap.value)
                if (srcClient == null) {
                    logger.error("Can't find src tabletId-${srcTabletMap.key} -> beId-${srcTabletMap.value}")
                    return false
                }
                BackendClientImpl tarClient = syncerContext.targetBackendClients.get(tarTabletMap.value)
                if (tarClient == null) {
                    logger.error("Can't find target tabletId-${tarTabletMap.key} -> beId-${tarTabletMap.value}")
                    return false
                }

                TIngestBinlogRequest request = new TIngestBinlogRequest()
                TUniqueId uid = new TUniqueId(-1, -1)
                request.setTxnId(syncerContext.txnId)
                request.setRemoteTabletId(srcTabletMap.key)
                request.setBinlogVersion(srcPartition.value.version)
                request.setRemoteHost(srcClient.address.hostname)
                request.setRemotePort(srcClient.httpPort.toString())
                request.setPartitionId(srcPartition.key)
                request.setLocalTabletId(tarTabletMap.key)
                request.setLoadId(uid)
                logger.info("request -> ${request}")
                TIngestBinlogResult result = tarClient.client.ingestBinlog(request)
                if (!checkIngestBinlog(result)) {
                    logger.error("Ingest result error.")
                    return false
                }
                syncerContext.commitInfos.add(new TTabletCommitInfo(tarTabletMap.key, tarTabletMap.value))
            }
        }

        return true
    }

    Boolean commit_txn(String table) {
        logger.info("Commit transaction to target cluster ${context.config.feTargetThriftNetworkAddress}")
        SyncerContext syncerContext = context.getSyncerContext()
        FrontendClientImpl clientImpl = context.getTargetFrontClient()
        TCommitTxnResult result = SyncerUtils.commitTxn(clientImpl, syncerContext)
        return checkCommitTxn(result)
    }

    List<List<Object>> sql(String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}sql: ${sqlStr}".toString())
        def (result, meta) = JdbcUtils.executeToList(context.getConnection(), sqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    List<List<Object>> target_sql(String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}target_sql: ${sqlStr}".toString())
        def (result, meta) = JdbcUtils.executeToList(context.getTargetConnection(), sqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    List<List<String>> sql_meta(String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}sql: ${sqlStr}".toString())
        def (tmp, rsmd) = JdbcUtils.executeToList(context.getConnection(), sqlStr)
        int count = rsmd.getColumnCount();
        List<List<String>> result = new ArrayList<>()
        for (int i = 0; i < count; i++) {
            List<String> item = new ArrayList<>()
            String columnName = rsmd.getColumnName(i + 1);
            int columnType = rsmd.getColumnType(i+1);
            String columnTypeName = rsmd.getColumnTypeName(i+1);
            item.add(columnName);
            item.add(columnTypeName);
            result.add(item);
        }
        return result;
    }

    List<List<Object>> order_sql(String sqlStr) {
        return sql(sqlStr,  true)
    }

    List<List<Object>> sortRows(List<List<Object>> result) {
        if (result == null) {
            return null
        }
        return DataUtils.sortByToString(result)
    }

    String selectUnionAll(List list) {
        def toSelectString = { Object value ->
            if (value == null) {
                return "null"
            } else if (value instanceof Number) {
                return value.toString()
            } else {
                return "'${value.toString()}'".toString()
            }
        }
        AtomicBoolean isFirst = new AtomicBoolean(true)
        String sql = list.stream()
            .map({ row ->
                StringBuilder sb = new StringBuilder("SELECT ")
                if (row instanceof List) {
                    if (isFirst.get()) {
                        String columns = row.withIndex().collect({ column, index ->
                            "${toSelectString(column)} AS c${index + 1}"
                        }).join(", ")
                        sb.append(columns)
                        isFirst.set(false)
                    } else {
                        String columns = row.collect({ column ->
                            "${toSelectString(column)}"
                        }).join(", ")
                        sb.append(columns)
                    }
                } else {
                    if (isFirst.get()) {
                        sb.append(toSelectString(row)).append(" AS c1")
                        isFirst.set(false)
                    } else {
                        sb.append(toSelectString(row))
                    }
                }
                return sb.toString()
            }).collect(Collectors.joining("\nUNION ALL\n"))
        return sql
    }

    void explain(Closure actionSupplier) {
        runAction(new ExplainAction(context), actionSupplier)
    }

    void createMV(String sql) {
        (new CreateMVAction(context, sql)).run()
    }

    void createMV(String sql, String expection) {
        (new CreateMVAction(context, sql, expection)).run()
    }

    void test(Closure actionSupplier) {
        runAction(new TestAction(context), actionSupplier)
    }

    void benchmark(Closure actionSupplier) {
        runAction(new BenchmarkAction(context), actionSupplier)
    }

    String getBrokerName() {
        String brokerName = context.config.otherConfigs.get("brokerName")
        return brokerName
    }

    String getHdfsFs() {
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        return hdfsFs
    }

    String getHdfsUser() {
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        return hdfsUser
    }

    String getHdfsPasswd() {
        String hdfsPasswd = context.config.otherConfigs.get("hdfsPasswd")
        return hdfsPasswd
    }

    String getHdfsDataDir() {
        String dataDir = context.config.dataPath + "/" + group + "/"
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        Hdfs hdfs = new Hdfs(hdfsFs, hdfsUser, dataDir)
        return hdfs.genRemoteDataDir()
    }

    boolean enableHdfs() {
        String enableHdfs = context.config.otherConfigs.get("enableHdfs");
        return enableHdfs.equals("true");
    }

    String uploadToHdfs(String localFile) {
        String dataDir = context.config.dataPath + "/" + group + "/"
        localFile = dataDir + localFile
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        Hdfs hdfs = new Hdfs(hdfsFs, hdfsUser, dataDir)
        String remotePath = hdfs.upload(localFile)
        return remotePath;
    }

    String getLoalFilePath(String fileName) {
        if (!new File(fileName).isAbsolute()) {
            fileName = new File(context.dataPath, fileName).getAbsolutePath()
        }
        def file = new File(fileName)
        if (!file.exists()) {
            log.warn("Stream load input file not exists: ${file}".toString())
            throw new IllegalStateException("Stream load input file not exists: ${file}");
        }
        def localFile = file.canonicalPath
        log.info("Set stream load input: ${file.canonicalPath}".toString())
        return localFile;
    }

    boolean enableBrokerLoad() {
        String enableBrokerLoad = context.config.otherConfigs.get("enableBrokerLoad");
        return (enableBrokerLoad != null && enableBrokerLoad.equals("true"));
    }

    String getS3Region() {
        String s3Region = context.config.otherConfigs.get("s3Region");
        return s3Region
    }

    String getS3BucketName() {
        String s3BucketName = context.config.otherConfigs.get("s3BucketName");
        return s3BucketName
    }

    String getS3Endpoint() {
        String s3Endpoint = context.config.otherConfigs.get("s3Endpoint");
        return s3Endpoint
    }

    String getS3AK() {
        String ak = context.config.otherConfigs.get("ak");
        return ak
    }

    String getS3SK() {
        String sk = context.config.otherConfigs.get("sk");
        return sk
    }

    String getS3Url() {
        String s3BucketName = context.config.otherConfigs.get("s3BucketName");
        String s3Endpoint = context.config.otherConfigs.get("s3Endpoint");
        String s3Url = "http://${s3BucketName}.${s3Endpoint}"
        return s3Url
    }
    
    void scpFiles(String username, String host, String files, String filePath, boolean fromDst=true) {
        String cmd = "scp -r ${username}@${host}:${files} ${filePath}"
        if (!fromDst) {
            cmd = "scp -r ${files} ${username}@${host}:${filePath}"
        }
        logger.info("Execute: ${cmd}".toString())
        Process process = cmd.execute()
        def code = process.waitFor()
        Assert.assertEquals(0, code)
    }
    
    void sshExec(String username, String host, String cmd) {
        String command = "ssh ${username}@${host} '${cmd}'"
        def cmds = ["/bin/bash", "-c", command]
        logger.info("Execute: ${cmds}".toString())
        Process p = cmds.execute()
        def errMsg = new StringBuilder()
        def msg = new StringBuilder()
        p.waitForProcessOutput(msg, errMsg)
        assert errMsg.length() == 0: "error occurred!" + errMsg
        assert p.exitValue() == 0
    }
    

    void getBackendIpHttpPort(Map<String, String> backendId_to_backendIP, Map<String, String> backendId_to_backendHttpPort) {
        List<List<Object>> backends = sql("show backends");
        String backend_id;
        for (List<Object> backend : backends) {
            backendId_to_backendIP.put(String.valueOf(backend[0]), String.valueOf(backend[1]));
            backendId_to_backendHttpPort.put(String.valueOf(backend[0]), String.valueOf(backend[4]));
        }
        return;
    } 

    int getTotalLine(String filePath) {
        def file = new File(filePath)
        int lines = 0;
        file.eachLine {
            lines++;
        }
        return lines;
    }

    boolean deleteFile(String filePath) {
        def file = new File(filePath)
        file.delete()
    }

    void waitingMTMVTaskFinished(String mvName) {
        String showTasks = "SHOW MTMV TASK ON " + mvName
        List<List<String>> showTaskMetaResult = sql_meta(showTasks)
        int index = showTaskMetaResult.indexOf(['State', 'CHAR'])
        String status = "PENDING"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 30 * 60 * 1000 // 30 min
        do {
            result = sql(showTasks)
            if (!result.isEmpty()) {
                status = result.last().get(index)
            }
            println "The state of ${showTasks} is ${status}"
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status == 'PENDING' || status == 'RUNNING'))
        if (status != "SUCCESS") {
            println "status is not success"
            println result.toString()
        }
        Assert.assertEquals("SUCCESS", status)
    }

    List<String> downloadExportFromHdfs(String label) {
        String dataDir = context.config.dataPath + "/" + group + "/"
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        Hdfs hdfs = new Hdfs(hdfsFs, hdfsUser, dataDir)
        return hdfs.downLoad(label)
    }

    void streamLoad(Closure actionSupplier) {
        runAction(new StreamLoadAction(context), actionSupplier)
    }

    void restore(Closure actionSupplier) {
        runAction(new RestoreAction(context), actionSupplier)
    }

    void httpTest(Closure actionSupplier) {
        runAction(new HttpCliAction(context), actionSupplier)
    }

    void runAction(SuiteAction action, Closure actionSupplier) {
        actionSupplier.setDelegate(action)
        actionSupplier.setResolveStrategy(Closure.DELEGATE_FIRST)
        actionSupplier.call(action)
        action.run()
    }

    PreparedStatement prepareStatement(String sql) {
        return JdbcUtils.prepareStatement(context.getConnection(), sql)
    } 

    void quickRunTest(String tag, Object arg, boolean isOrder = false) {
        if (context.config.generateOutputFile || context.config.forceGenerateOutputFile) {
            Tuple2<List<List<Object>>, ResultSetMetaData> tupleResult = null
            if (arg instanceof PreparedStatement) {
                tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (PreparedStatement) arg)
            } else {
                tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (String) arg)
            }
            def (result, meta) = tupleResult
            if (isOrder) {
                result = sortByToString(result)
            }
            Iterator<List<Object>> realResults = result.iterator()
            // generate and save to .out file
            def writer = context.getOutputWriter(context.config.forceGenerateOutputFile)
            writer.write(realResults, tag)
        } else {
            if (!context.outputFile.exists()) {
                throw new IllegalStateException("Missing outputFile: ${context.outputFile.getAbsolutePath()}")
            }

            if (!context.getOutputIterator().hasNextTagBlock(tag)) {
                throw new IllegalStateException("Missing output block for tag '${tag}': ${context.outputFile.getAbsolutePath()}")
            }

            OutputUtils.TagBlockIterator expectCsvResults = context.getOutputIterator().next()
            Tuple2<List<List<Object>>, ResultSetMetaData> tupleResult = null
            if (arg instanceof PreparedStatement) {
                tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (PreparedStatement) arg)
            } else {
                tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (String) arg)
            }
            def (realResults, meta) = tupleResult
            if (isOrder) {
                realResults = sortByToString(realResults)
            }

            Iterator<List<Object>> realResultsIter = realResults.iterator()
            def realWriter = context.getRealOutputWriter(true)
            realWriter.write(realResultsIter, tag)

            String errorMsg = null
            try {
                errorMsg = OutputUtils.checkOutput(expectCsvResults, realResults.iterator(),
                    { row -> OutputUtils.toCsvString(row as List<Object>) },
                    { row ->  OutputUtils.toCsvString(row) },
                    "Check tag '${tag}' failed", meta)
            } catch (Throwable t) {
                throw new IllegalStateException("Check tag '${tag}' failed, sql:\n${arg}", t)
            }
            if (errorMsg != null) {
                logger.warn("expect results: " + expectCsvResults + "\nrealResults: " + realResults)
                throw new IllegalStateException("Check tag '${tag}' failed:\n${errorMsg}\n\nsql:\n${arg}")
            }
        }
    }

    void quickTest(String tag, String sql, boolean isOrder = false) {
        logger.info("Execute tag: ${tag}, ${isOrder ? "order_" : ""}sql: ${sql}".toString())
        quickRunTest(tag, sql, isOrder) 
    }

    void quickExecute(String tag, PreparedStatement stmt) {
        logger.info("Execute tag: ${tag}, sql: ${stmt}".toString())
        quickRunTest(tag, stmt) 
    }
    
    @Override
    Object invokeMethod(String name, Object args) {
        // qt: quick test
        if (name.startsWith("qt_")) {
            return quickTest(name.substring("qt_".length()), (args as Object[])[0] as String)
        } else if (name.startsWith("order_qt_")) {
            return quickTest(name.substring("order_qt_".length()), (args as Object[])[0] as String, true)
        } else if (name.startsWith("qe_")) {
            return quickExecute(name.substring("qe_".length()), (args as Object[])[0] as PreparedStatement)
        } else if (name.startsWith("assert") && name.length() > "assert".length()) {
            // delegate to junit Assertions dynamically
            return Assertions."$name"(*args) // *args: spread-dot
        } else if (name.startsWith("try_")) {
            String realMethod = name.substring("try_".length())
            try {
                return this."$realMethod"(*args)
            } catch (Throwable t) {
                // do nothing
                return null
            }
        } else {
            // invoke origin method
            return metaClass.invokeMethod(this, name, args)
        }
    }

}

