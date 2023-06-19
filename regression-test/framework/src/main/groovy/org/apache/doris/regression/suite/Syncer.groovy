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

import com.google.gson.Gson
import org.apache.doris.regression.Config
import org.apache.doris.regression.suite.client.BackendClientImpl
import org.apache.doris.regression.suite.client.FrontendClientImpl
import org.apache.doris.regression.util.SyncerUtils
import org.apache.doris.thrift.TBeginTxnResult
import org.apache.doris.thrift.TBinlog
import org.apache.doris.regression.json.BinlogData
import org.apache.doris.thrift.TBinlogType
import org.apache.doris.thrift.TCommitTxnResult
import org.apache.doris.thrift.TGetBinlogResult
import org.apache.doris.thrift.TGetSnapshotResult
import org.apache.doris.thrift.TIngestBinlogRequest
import org.apache.doris.thrift.TIngestBinlogResult
import org.apache.doris.thrift.TNetworkAddress
import org.apache.doris.thrift.TRestoreSnapshotResult
import org.apache.doris.thrift.TStatus
import org.apache.doris.thrift.TStatusCode
import org.apache.doris.thrift.TTabletCommitInfo
import org.apache.doris.thrift.TUniqueId
import org.apache.thrift.transport.TTransportException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.util.logging.Slf4j

import java.util.Map.Entry

@Slf4j
class Syncer {
    final SyncerContext context
    final Suite suite
    final Logger logger = LoggerFactory.getLogger(Syncer.class)

    Syncer(Suite suite, Config config) {
        this.suite = suite
        context = new SyncerContext(suite.context.dbName, config)
    }

    enum ccrCluster {
        SOURCE,
        TARGET
    }

    private Boolean checkBinlog(TBinlog binlog, Boolean update) {

        // step 1: check binlog availability
        if (binlog == null) {
            return false
        }

        // step 2: check and set seq to context
        if (binlog.isSetCommitSeq()) {
            context.seq = binlog.getCommitSeq()
            logger.info("Now last seq is ${context.seq}")
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
            if (update) {
                Gson gson = new Gson()
                getSourceMeta(gson.fromJson(data, BinlogData.class))
                logger.info("Source tableId: ${context.sourceTableMap}, ${context.sourceTableMap}")
            }
        } else {
            logger.error("binlog data is not contains data!")
            return false
        }

        return true
    }

    private Boolean checkGetBinlog(String table, TGetBinlogResult result, Boolean update) {
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
                        logger.error("Binlog not found DB! DB: ${context.db}")
                        break
                    case TStatusCode.BINLOG_NOT_FOUND_TABLE:
                        logger.error("Binlog not found table ${table}!")
                        break
                    case TStatusCode.ANALYSIS_ERROR:
                        logger.error("Binlog result analysis error: ${status.getErrorMsgs()}")
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
        return checkBinlog(binlog, update)
    }

    private Boolean checkBeginTxn(TBeginTxnResult result) {
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
            context.txnId = result.getTxnId()
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
            logger.info("CommitInfos: ${context.commitInfos}")
            context.commitInfos.clear()
        }

        return isCheckedOK
    }

    Boolean checkRestoreSnapshot(TRestoreSnapshotResult result) {
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
                    default:
                        logger.error("Restore SnapShot result code is: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TRestoreSnapshotResult! result: ${result}")
        }

        return isCheckedOK
    }

    Boolean checkSnapshotFinish() {
        String checkSQL = "SHOW BACKUP FROM " + context.db
        List<Object> row = suite.sql(checkSQL)[0]
        logger.info("Now row is ${row}")

        return (row[3] as String) == "FINISHED"
    }

    Boolean checkRestoreFinish() {
        String checkSQL = "SHOW RESTORE FROM " + context.db
        List<Object> row = suite.sql(checkSQL)[0]
        logger.info("Now row is ${row}")

        return (row[4] as String) == "FINISHED"
    }

    Boolean checkGetSnapshot() {
        TGetSnapshotResult result = context.getSnapshotResult
        Boolean isCheckedOK = false

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        if (!result.isSetMeta()) {
                            logger.error("TGetSnapshotResult meta is unset.")
                        } else if (!result.isSetJobInfo()) {
                            logger.error("TGetSnapshotResult job info is unset.")
                        } else {
                            isCheckedOK = true
                        }
                        break
                    default:
                        logger.error("Get SnapShot result code is: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TGetSnapshotResult! result: ${result}")
        }

        return isCheckedOK
    }

    HashMap<Long, BackendClientImpl> getBackendClientsImpl(ccrCluster cluster) throws TTransportException {
        HashMap<Long, BackendClientImpl> clientsMap = new HashMap<Long, BackendClientImpl>()
        String backendSQL = "SHOW PROC '/backends'"
        List<List<Object>> backendInformation
        if (cluster == ccrCluster.SOURCE) {
            backendInformation = suite.sql(backendSQL)
        } else {
            backendInformation = suite.target_sql(backendSQL)
        }
        for (List<Object> row : backendInformation) {
            TNetworkAddress address = new TNetworkAddress(row[1] as String, row[3] as int)
            BackendClientImpl client = new BackendClientImpl(address, row[4] as int)
            clientsMap.put(row[0] as Long, client)
        }
        return clientsMap
    }

    void getSourceMeta(BinlogData binlogData) {
        logger.info("data struct: ${binlogData}")
        context.sourceTableIdToName.clear()
        context.sourceTableMap.clear()

        context.sourceDbId = binlogData.dbId

        String metaSQL = "SHOW PROC '/dbs/" + binlogData.dbId.toString()
        List<List<Object>> sqlInfo = suite.sql(metaSQL + "'")

        // Get table information
        for (List<Object> row : sqlInfo) {
            context.sourceTableIdToName.put(row[0] as Long, row[1] as String)
        }

        // Get table meta in binlog
        binlogData.tableRecords.forEach((tableId, partitionRecord) -> {
            String tableName = context.sourceTableIdToName.get(tableId)
            TableMeta tableMeta = new TableMeta(tableId)

            partitionRecord.partitionRecords.forEach {
                String partitionSQL = metaSQL + "/" + tableId.toString() + "/partitions/" + it.partitionId.toString()
                sqlInfo = suite.sql(partitionSQL + "'")
                PartitionMeta partitionMeta = new PartitionMeta(sqlInfo[0][0] as Long, it.version)
                partitionSQL += "/" + partitionMeta.indexId.toString()
                sqlInfo = suite.sql(partitionSQL + "'")
                sqlInfo.forEach(row -> {
                    partitionMeta.tabletMeta.put((row[0] as Long), (row[2] as Long))
                })
                tableMeta.partitionMap.put(it.partitionId, partitionMeta)
            }
            context.sourceTableMap.put(tableName, tableMeta)
        })
    }

    ArrayList<TTabletCommitInfo> copyCommitInfos() {
        return new ArrayList<TTabletCommitInfo>(context.commitInfos)
    }

    ArrayList<TTabletCommitInfo> resetCommitInfos() {
        def info = copyCommitInfos()
        context.commitInfos = new ArrayList<TTabletCommitInfo>()
        return info
    }

    void addCommitInfo(long tabletId, long backendId) {
        context.commitInfos.add(new TTabletCommitInfo(tabletId, backendId))
    }

    Boolean getBackendClients() {
        logger.info("Begin to get backend's maps.")

        // get source backend clients
        try {
            context.sourceBackendClients = getBackendClientsImpl(ccrCluster.SOURCE)
        } catch (TTransportException e) {
            logger.error("Create source cluster backend client fail: ${e.toString()}")
            return false
        }

        // get target backend clients
        try {
            context.targetBackendClients = getBackendClientsImpl(ccrCluster.TARGET)
        } catch (TTransportException e) {
            logger.error("Create target cluster backend client fail: ${e.toString()}")
            return false
        }
        return true
    }

    void closeBackendClients() {
        context.closeBackendClients()
    }

    Boolean restoreSnapshot() {
        logger.info("Restore snapshot ${context.labelName}")
        FrontendClientImpl clientImpl = context.getSourceFrontClient()

        // step 1: recode job info
        Gson gson = new Gson()
        Map jsonMap = gson.fromJson(new String(context.getSnapshotResult.getJobInfo()), Map.class)
        getBackendClients()
        jsonMap.put("extra_info", context.genExtraInfo())
        logger.info("json map ${jsonMap}.")
        context.getSnapshotResult.setJobInfo(gson.toJson(jsonMap).getBytes())

        // step 2: restore
        TRestoreSnapshotResult result = SyncerUtils.restoreSnapshot(clientImpl, context)
        return checkRestoreSnapshot(result)
    }

    Boolean getSnapshot(String labelName, String table) {
        logger.info("Get snapshot ${labelName}")
        FrontendClientImpl clientImpl = context.getSourceFrontClient()
        context.getSnapshotResult = SyncerUtils.getSnapshot(clientImpl, labelName, table, context)
        context.labelName = labelName
        context.tableName = table
        return checkGetSnapshot()
    }

    Boolean getTargetMeta(String table = "") {
        logger.info("Get target cluster meta data.")
        String baseSQL = "SHOW PROC '/dbs"
        List<List<Object>> sqlInfo

        // step 1: get target dbId
        Long dbId = -1
        sqlInfo = suite.target_sql(baseSQL + "'")
        for (List<Object> row : sqlInfo) {
            String[] dbName = (row[1] as String).split(":")
            if (dbName[1] == ("TEST_" + context.db)) {
                dbId = row[0] as Long
                break
            }
        }
        if (dbId == -1) {
            logger.error("Target cluster get ${context.db} db error.")
            return false
        }
        context.targetDbId = dbId

        // step 2: get target dbId/tableId
        baseSQL += "/" + dbId.toString()
        sqlInfo = suite.target_sql(baseSQL + "'")
        if (table == "") {
            for (List<Object> row : sqlInfo) {
                context.targetTableMap.put(row[1] as String, new TableMeta(row[0] as long))
            }
        } else {
            for (List<Object> row : sqlInfo) {
                if ((row[1] as String) == table) {
                    context.targetTableMap.put(row[1] as String, new TableMeta(row[0] as long))
                    break
                }
            }
        }


        // step 3: get partitionIds
        context.targetTableMap.values().forEach {
            baseSQL += "/" + it.id.toString() + "/partitions"
            ArrayList<Long> partitionIds = new ArrayList<Long>()
            sqlInfo = suite.target_sql(baseSQL + "'")
            for (List<Object> row : sqlInfo) {
                partitionIds.add(row[0] as Long)
            }
            if (partitionIds.isEmpty()) {
                logger.error("Target cluster get partitions fault.")
                return false
            }

            // step 4: get partitionMetas
            for (Long id : partitionIds) {

                // step 4.1: get partition/indexId
                String partitionSQl = baseSQL + "/" + id.toString()
                sqlInfo = suite.target_sql(partitionSQl + "'")
                if (sqlInfo.isEmpty()) {
                    logger.error("Target cluster partition-${id} indexId fault.")
                    return false
                }
                PartitionMeta meta = new PartitionMeta(sqlInfo[0][0] as Long, -1)

                // step 4.2: get partition/indexId/tabletId
                partitionSQl += "/" + meta.indexId.toString()
                sqlInfo = suite.target_sql(partitionSQl + "'")
                for (List<Object> row : sqlInfo) {
                    meta.tabletMeta.put(row[0] as Long, row[2] as Long)
                }
                if (meta.tabletMeta.isEmpty()) {
                    logger.error("Target cluster get (partitionId/indexId)-(${id}/${meta.indexId}) tabletIds fault.")
                    return false
                }

                it.partitionMap.put(id, meta)
            }
        }


        logger.info("Target cluster metadata: ${context.targetTableMap}")
        return true
    }

    Boolean checkTargetVersion() {
        logger.info("Check target tablets version")
        context.targetTableMap.values().forEach {
            String baseSQL = "SHOW PROC '/dbs/" + context.targetDbId.toString() + "/" +
                    it.id.toString() + "/partitions/"
            it.partitionMap.forEach((id, meta) -> {
                String versionSQL = baseSQL + id.toString() + "/" + meta.indexId.toString()
                List<List<Object>> sqlInfo = suite.target_sql(versionSQL + "'")
                for (List<Object> row : sqlInfo) {
                    Long tabletVersion = row[4] as Long
                    if (tabletVersion != meta.version) {
                        logger.error(
                                "Version miss match! partitionId: ${id}, tabletId: ${row[0] as Long}," +
                                " Now version is ${meta.version}, but tablet version is ${tabletVersion}")
                        return false
                    }
                }
            })
        }

        return true
    }

    Boolean getBinlog(String table = "", Boolean update = true) {
        logger.info("Get binlog from source cluster ${context.config.feSourceThriftNetworkAddress}, binlog seq: ${context.seq}")
        FrontendClientImpl clientImpl = context.getSourceFrontClient()
        TGetBinlogResult result = SyncerUtils.getBinLog(clientImpl, context, table)
        return checkGetBinlog(table, result, update)
    }

    Boolean beginTxn(String table) {
        logger.info("Begin transaction to target cluster ${context.config.feTargetThriftNetworkAddress}")
        FrontendClientImpl clientImpl = context.getTargetFrontClient()
        TBeginTxnResult result = SyncerUtils.beginTxn(clientImpl, context, table)
        return checkBeginTxn(result)
    }

    Boolean ingestBinlog(long fakePartitionId = -1, long fakeVersion = -1) {
        logger.info("Begin to ingest binlog.")

        // step 1: Check meta data is valid
        if (!context.metaIsValid()) {
            logger.error("Meta data miss match, src: ${context.sourceTableMap}, target: ${context.targetTableMap}")
            return false
        }

        // step 2: Begin ingest binlog
        // step 2.1: ingest each table in meta
        for (Entry<String, TableMeta> tableInfo : context.sourceTableMap) {
            String tableName = tableInfo.key
            TableMeta srcTableMeta = tableInfo.value
            TableMeta tarTableMeta = context.targetTableMap.get(tableName)

            Iterator sourcePartitionIter = srcTableMeta.partitionMap.iterator()
            Iterator targetPartitionIter = tarTableMeta.partitionMap.iterator()
            // step 2.2: ingest each partition in the table
            while (sourcePartitionIter.hasNext()) {
                Entry srcPartition = sourcePartitionIter.next()
                Entry tarPartition = targetPartitionIter.next()
                Iterator srcTabletIter = srcPartition.value.tabletMeta.iterator()
                Iterator tarTabletIter = tarPartition.value.tabletMeta.iterator()

                // step 2.3: ingest each tablet in the partition
                while (srcTabletIter.hasNext()) {
                    Entry srcTabletMap = srcTabletIter.next()
                    Entry tarTabletMap = tarTabletIter.next()

                    BackendClientImpl srcClient = context.sourceBackendClients.get(srcTabletMap.value)
                    if (srcClient == null) {
                        logger.error("Can't find src tabletId-${srcTabletMap.key} -> beId-${srcTabletMap.value}")
                        return false
                    }
                    BackendClientImpl tarClient = context.targetBackendClients.get(tarTabletMap.value)
                    if (tarClient == null) {
                        logger.error("Can't find target tabletId-${tarTabletMap.key} -> beId-${tarTabletMap.value}")
                        return false
                    }

                    tarPartition.value.version = srcPartition.value.version
                    long partitionId = fakePartitionId == -1 ? srcPartition.key : fakePartitionId
                    long version = fakeVersion == -1 ? srcPartition.value.version : fakeVersion

                    TIngestBinlogRequest request = new TIngestBinlogRequest()
                    TUniqueId uid = new TUniqueId(-1, -1)
                    request.setTxnId(context.txnId)
                    request.setRemoteTabletId(srcTabletMap.key)
                    request.setBinlogVersion(version)
                    request.setRemoteHost(srcClient.address.hostname)
                    request.setRemotePort(srcClient.httpPort.toString())
                    request.setPartitionId(partitionId)
                    request.setLocalTabletId(tarTabletMap.key)
                    request.setLoadId(uid)
                    logger.info("request -> ${request}")
                    TIngestBinlogResult result = tarClient.client.ingestBinlog(request)
                    if (!checkIngestBinlog(result)) {
                        return false
                    }

                    addCommitInfo(tarTabletMap.key, tarTabletMap.value)
                }
            }
        }

        return true
    }

    Boolean commitTxn() {
        logger.info("Commit transaction to target cluster ${context.config.feTargetThriftNetworkAddress}, txnId: ${context.txnId}")
        FrontendClientImpl clientImpl = context.getTargetFrontClient()
        TCommitTxnResult result = SyncerUtils.commitTxn(clientImpl, context)
        return checkCommitTxn(result)
    }
}
