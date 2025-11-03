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

package org.apache.doris.regression.util

import org.apache.doris.regression.suite.SyncerContext
import org.apache.doris.regression.suite.client.BackendClientImpl
import org.apache.doris.regression.suite.client.FrontendClientImpl
import org.apache.doris.thrift.TBeginTxnRequest
import org.apache.doris.thrift.TBeginTxnResult
import org.apache.doris.thrift.TCommitTxnRequest
import org.apache.doris.thrift.TCommitTxnResult
import org.apache.doris.thrift.TGetMasterTokenRequest
import org.apache.doris.thrift.TGetMasterTokenResult
import org.apache.doris.thrift.TGetSnapshotRequest
import org.apache.doris.thrift.TGetSnapshotResult
import org.apache.doris.thrift.TIngestBinlogRequest
import org.apache.doris.thrift.TIngestBinlogResult
import org.apache.doris.thrift.TRestoreSnapshotRequest
import org.apache.doris.thrift.TRestoreSnapshotResult
import org.apache.doris.thrift.TSnapshotType
import org.apache.doris.thrift.TSubTxnInfo
import org.apache.thrift.TException
import org.apache.doris.thrift.TGetBinlogRequest
import org.apache.doris.thrift.TGetBinlogResult
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SyncerUtils {
    private static final Logger logger = LoggerFactory.getLogger(SyncerUtils.class)

    private static <T> void setAuthorInformation(T request, SyncerContext context) {
        request.setUser(context.user)
        request.setPasswd(context.passwd)
    }

    private static String newLabel(SyncerContext context, Long tableId) {
        return String.format("ccr_sync_job:%s:%d:%d", context.db, tableId, context.seq)
    }

    static TGetBinlogResult getBinLog(FrontendClientImpl clientImpl, SyncerContext context, String table, Long tableId) throws TException {
        TGetBinlogRequest request = new TGetBinlogRequest()
        setAuthorInformation(request, context)
        request.setDb(context.db)
        if (!table.isEmpty()) {
            request.setTable(table)
        }
        if (tableId != -1) {
            request.setTableId(tableId)
        }
        request.setPrevCommitSeq(context.seq)
        return clientImpl.client.getBinlog(request)
    }

    static TBeginTxnResult beginTxn(FrontendClientImpl clientImpl, SyncerContext context, Long tableId, Long subTxnNum) throws TException {
        TBeginTxnRequest request = new TBeginTxnRequest()
        setAuthorInformation(request, context)
        request.setDb("TEST_" + context.db)
        if (tableId != -1) {
            request.addToTableIds(tableId)
        }
        request.setLabel(newLabel(context, tableId))
        if (subTxnNum > 0) {
            request.setSubTxnNum(subTxnNum)
        }
        logger.info("begin txn request: ${request}")
        return clientImpl.client.beginTxn(request)
    }

    static TIngestBinlogResult ingestBinlog(BackendClientImpl clientImpl, TIngestBinlogRequest request) throws TException {
        return clientImpl.client.ingestBinlog(request)
    }

    static TCommitTxnResult commitTxn(FrontendClientImpl clientImpl, SyncerContext context) throws TException {
        TCommitTxnRequest request = new TCommitTxnRequest()
        setAuthorInformation(request, context)
        request.setDb("TEST_" + context.db)
        request.setTxnId(context.txnId)
        if (context.txnInsert) {
            request.setTxnInsert(true)
            request.setSubTxnInfos(context.subTxnInfos)
        } else {
            request.setCommitInfos(context.commitInfos)
        }
        logger.info("commit txn request: ${request}")
        return clientImpl.client.commitTxn(request)
    }

    static TGetSnapshotResult getSnapshot(FrontendClientImpl clientImpl, String labelName, String table, SyncerContext context) throws TException {
        TGetSnapshotRequest request = new TGetSnapshotRequest()
        setAuthorInformation(request, context)
        request.setDb(context.db)
        request.setLabelName(labelName)
        if (table != null) {
            request.setTable(table)
        }
        request.setSnapshotType(TSnapshotType.LOCAL)
        request.setSnapshotName("")
        return clientImpl.client.getSnapshot(request)
    }

    static TRestoreSnapshotResult restoreSnapshot(FrontendClientImpl clientImpl, SyncerContext context, boolean forCCR) throws TException {
        TRestoreSnapshotRequest request = new TRestoreSnapshotRequest()
        setAuthorInformation(request, context)
        if (forCCR) {
            request.setDb("TEST_" + context.db)
        } else {
            request.setDb(context.db)
        }
        if (context.tableName != null) {
            request.setTable(context.tableName)
        }
        request.setRepoName("__keep_on_local__")
        request.setLabelName(context.labelName)
        HashMap<String, String> properties = new HashMap<>()
        properties.put("reserve_replica", "true")
        request.setProperties(properties)
        request.setMeta(context.getSnapshotResult.getMeta())
        request.setJobInfo(context.getSnapshotResult.getJobInfo())
        return clientImpl.client.restoreSnapshot(request)
    }

    static TGetMasterTokenResult getMasterToken(FrontendClientImpl clientImpl, SyncerContext context) throws TException {
        TGetMasterTokenRequest request = new TGetMasterTokenRequest()
        request.setUser(context.user)
        request.setPassword(context.passwd)
        return clientImpl.client.getMasterToken(request)
    }
}
