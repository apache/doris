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
import org.apache.doris.thrift.TGetSnapshotRequest
import org.apache.doris.thrift.TGetSnapshotResult
import org.apache.doris.thrift.TIngestBinlogRequest
import org.apache.doris.thrift.TIngestBinlogResult
import org.apache.doris.thrift.TRestoreSnapshotRequest
import org.apache.doris.thrift.TRestoreSnapshotResult
import org.apache.doris.thrift.TSnapshotType
import org.apache.thrift.TException
import org.apache.doris.thrift.TGetBinlogRequest
import org.apache.doris.thrift.TGetBinlogResult

class SyncerUtils {
    private static <T> void setAuthorInformation(T request, SyncerContext context) {
        request.setUser(context.user)
        request.setPasswd(context.passwd)
    }

    private static String newLabel(SyncerContext context, String table) {
        return String.format("ccr_sync_job:%s:%s:%d", context.db, table, context.seq)
    }

    static TGetBinlogResult getBinLog(FrontendClientImpl clientImpl, SyncerContext context, String table) throws TException {
        TGetBinlogRequest request = new TGetBinlogRequest()
        setAuthorInformation(request, context)
        request.setDb(context.db)
        if (!table.isEmpty()) {
            request.setTable(table)
        }
        request.setPrevCommitSeq(context.seq)
        return clientImpl.client.getBinlog(request)
    }

    static TBeginTxnResult beginTxn(FrontendClientImpl clientImpl, SyncerContext context, String table) throws TException {
        TBeginTxnRequest request = new TBeginTxnRequest()
        setAuthorInformation(request, context)
        request.setDb("TEST_" + context.db)
        if (table != null) {
            request.addToTables(table)
        }
        request.setLabel(newLabel(context, table))
        return clientImpl.client.beginTxn(request)

    }

    static TIngestBinlogResult ingestBinlog(BackendClientImpl clientImpl, TIngestBinlogRequest request) throws TException {
        return clientImpl.client.ingestBinlog(request)
    }

    static TCommitTxnResult commitTxn(FrontendClientImpl clientImpl, SyncerContext context) throws TException {
        TCommitTxnRequest request = new TCommitTxnRequest()
        setAuthorInformation(request, context)
        request.setDb("TEST_" + context.db)
        request.setCommitInfos(context.commitInfos)
        request.setTxnId(context.txnId)
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

    static TRestoreSnapshotResult restoreSnapshot(FrontendClientImpl clientImpl, SyncerContext context) throws TException {
        TRestoreSnapshotRequest request = new TRestoreSnapshotRequest()
        setAuthorInformation(request, context)
        request.setDb(context.db)
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
}
