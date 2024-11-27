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

package org.apache.doris.qe;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;

import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InsertStreamTxnExecutor {
    private long txnId;
    private TUniqueId loadId;
    private TransactionEntry txnEntry;

    public InsertStreamTxnExecutor(TransactionEntry txnEntry) {
        this.txnEntry = txnEntry;
    }

    public void beginTransaction(TStreamLoadPutRequest request) throws UserException, TException, TimeoutException,
            InterruptedException, ExecutionException {
        TTxnParams txnConf = txnEntry.getTxnConf();
        OlapTable table = (OlapTable) txnEntry.getTable();
        // StreamLoadTask's id == request's load_id
        StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
        StreamLoadPlanner planner = new StreamLoadPlanner((Database) txnEntry.getDb(), table, streamLoadTask);
        boolean isMowTable = ((OlapTable) txnEntry.getTable()).getEnableUniqueKeyMergeOnWrite();
        TPipelineFragmentParamsList pipelineParamsList = new TPipelineFragmentParamsList();
        if (!table.tryReadLock(1, TimeUnit.MINUTES)) {
            throw new UserException("get table read lock timeout, database=" + table.getDatabase().getId() + ",table="
                    + table.getName());
        }
        try {
            // Will using load id as query id in fragment
            TPipelineFragmentParams tRequest = planner.plan(streamLoadTask.getId());
            tRequest.setTxnConf(txnConf).setImportLabel(txnEntry.getLabel());
            tRequest.setIsMowTable(isMowTable);
            for (Map.Entry<Integer, List<TScanRangeParams>> entry : tRequest.local_params.get(0).per_node_scan_ranges
                    .entrySet()) {
                for (TScanRangeParams scanRangeParams : entry.getValue()) {
                    scanRangeParams.scan_range.ext_scan_range.file_scan_range.params.setFormatType(
                            TFileFormatType.FORMAT_PROTO);
                    scanRangeParams.scan_range.ext_scan_range.file_scan_range.params.setCompressType(
                            TFileCompressType.PLAIN);
                }
            }
            txnConf.setFragmentInstanceId(tRequest.local_params.get(0).fragment_instance_id);
            this.loadId = request.getLoadId();
            this.txnEntry.setpLoadId(Types.PUniqueId.newBuilder()
                    .setHi(loadId.getHi())
                    .setLo(loadId.getLo()).build());

            pipelineParamsList.addToParamsList(tRequest);

            TransactionState transactionState = Env.getCurrentGlobalTransactionMgr()
                    .getTransactionState(table.getDatabase().getId(), streamLoadTask.getTxnId());
            if (transactionState != null) {
                transactionState.addTableIndexes(table);
            }
        } finally {
            table.readUnlock();
        }

        BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needLoadAvailable().needQueryAvailable().build();
        List<Long> beIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
        if (beIds.isEmpty()) {
            throw new UserException("No available backend to match the policy: " + policy);
        }

        Backend backend = Env.getCurrentSystemInfo().getBackendsByCurrentCluster().get(beIds.get(0));
        txnConf.setUserIp(backend.getHost());
        txnEntry.setBackend(backend);
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            Future<InternalService.PExecPlanFragmentResult> future;
            future = BackendServiceProxy.getInstance().execPlanFragmentsAsync(address, pipelineParamsList, false);
            InternalService.PExecPlanFragmentResult result = future.get(5, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new TException("failed to execute plan fragment: " + result.getStatus().getErrorMsgsList());
            }
        } catch (RpcException e) {
            throw new TException(e);
        }
    }

    public void commitTransaction() throws TException, TimeoutException,
            InterruptedException, ExecutionException {
        TTxnParams txnConf = txnEntry.getTxnConf();
        Types.PUniqueId fragmentInstanceId = Types.PUniqueId.newBuilder()
                .setHi(txnConf.getFragmentInstanceId().getHi())
                .setLo(txnConf.getFragmentInstanceId().getLo()).build();


        Backend backend = txnEntry.getBackend();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            Future<InternalService.PCommitResult> future = BackendServiceProxy
                    .getInstance().commit(address, fragmentInstanceId, this.txnEntry.getpLoadId());
            InternalService.PCommitResult result = future.get(5, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new TException("failed to commit txn: " + result.getStatus().getErrorMsgsList());
            }
        } catch (RpcException e) {
            throw new TException(e);
        }
    }

    public void abortTransaction() throws TException, TimeoutException,
            InterruptedException, ExecutionException {
        TTxnParams txnConf = txnEntry.getTxnConf();
        Types.PUniqueId fragmentInstanceId = Types.PUniqueId.newBuilder()
                .setHi(txnConf.getFragmentInstanceId().getHi())
                .setLo(txnConf.getFragmentInstanceId().getLo()).build();

        Backend be = txnEntry.getBackend();
        TNetworkAddress address = new TNetworkAddress(be.getHost(), be.getBrpcPort());
        try {
            Future<InternalService.PRollbackResult> future = BackendServiceProxy.getInstance().rollback(address,
                    fragmentInstanceId, this.txnEntry.getpLoadId());
            InternalService.PRollbackResult result = future.get(5, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new TException("failed to rollback txn: " + result.getStatus().getErrorMsgsList());
            }
        } catch (RpcException e) {
            throw new TException(e);
        }
    }

    public void sendData() throws TException, TimeoutException,
            InterruptedException, ExecutionException {
        if (txnEntry.getDataToSend() == null || txnEntry.getDataToSend().isEmpty()) {
            return;
        }

        TTxnParams txnConf = txnEntry.getTxnConf();
        Types.PUniqueId fragmentInstanceId = Types.PUniqueId.newBuilder()
                .setHi(txnConf.getFragmentInstanceId().getHi())
                .setLo(txnConf.getFragmentInstanceId().getLo()).build();

        Backend backend = txnEntry.getBackend();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            Future<InternalService.PSendDataResult> future = BackendServiceProxy.getInstance().sendData(
                    address, fragmentInstanceId, this.txnEntry.getpLoadId(), txnEntry.getDataToSend());
            InternalService.PSendDataResult result = future.get(5, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new TException("failed to insert data: " + result.getStatus().getErrorMsgsList());
            }
        } catch (RpcException e) {
            throw new TException(e);
        } finally {
            txnEntry.clearDataToSend();
        }
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    public void setLoadId(TUniqueId loadId) {
        this.loadId = loadId;
        this.txnEntry.setpLoadId(Types.PUniqueId.newBuilder()
                .setHi(loadId.getHi())
                .setLo(loadId.getLo()).build());
    }

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public TransactionEntry getTxnEntry() {
        return txnEntry;
    }

}
