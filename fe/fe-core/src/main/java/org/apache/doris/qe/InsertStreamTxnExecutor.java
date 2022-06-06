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

import org.apache.doris.catalog.Catalog;
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
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TExecPlanFragmentParamsList;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionEntry;

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
        StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
        StreamLoadPlanner planner = new StreamLoadPlanner(txnEntry.getDb(), (OlapTable) txnEntry.getTable(), streamLoadTask);
        TExecPlanFragmentParams tRequest = planner.plan(streamLoadTask.getId());
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder().setCluster(txnEntry.getDb().getClusterName())
                .needLoadAvailable().needQueryAvailable().build();
        List<Long> beIds = Catalog.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
        if (beIds.isEmpty()) {
            throw new UserException("No available backend to match the policy: " + policy);
        }

        tRequest.setTxnConf(txnConf).setImportLabel(txnEntry.getLabel());
        for (Map.Entry<Integer, List<TScanRangeParams>> entry : tRequest.params.per_node_scan_ranges.entrySet()) {
            for (TScanRangeParams scanRangeParams : entry.getValue()) {
                for (TBrokerRangeDesc desc : scanRangeParams.scan_range.broker_scan_range.ranges) {
                    desc.setFormatType(TFileFormatType.FORMAT_PROTO);
                }
            }
        }
        txnConf.setFragmentInstanceId(tRequest.params.fragment_instance_id);

        Backend backend = Catalog.getCurrentSystemInfo().getIdToBackend().get(beIds.get(0));
        txnConf.setUserIp(backend.getHost());
        txnEntry.setBackend(backend);
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            TExecPlanFragmentParamsList paramsList = new TExecPlanFragmentParamsList();
            paramsList.addToParamsList(tRequest);
            Future<InternalService.PExecPlanFragmentResult> future =
                    BackendServiceProxy.getInstance().execPlanFragmentsAsync(address, paramsList, false);
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
            Future<InternalService.PCommitResult> future = BackendServiceProxy.getInstance().commit(address, fragmentInstanceId);
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
                    fragmentInstanceId);
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
                    address, fragmentInstanceId, txnEntry.getDataToSend());
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
