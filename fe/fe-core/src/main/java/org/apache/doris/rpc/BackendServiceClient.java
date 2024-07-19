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

package org.apache.doris.rpc;

import org.apache.doris.common.Config;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.PBackendServiceGrpc;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BackendServiceClient {
    public static final Logger LOG = LogManager.getLogger(BackendServiceClient.class);

    private static final int MAX_RETRY_NUM = 10;
    private final TNetworkAddress address;
    private final PBackendServiceGrpc.PBackendServiceFutureStub stub;
    private final PBackendServiceGrpc.PBackendServiceBlockingStub blockingStub;
    private final ManagedChannel channel;
    private final long execPlanTimeout;

    public BackendServiceClient(TNetworkAddress address, Executor executor) {
        this.address = address;
        channel = NettyChannelBuilder.forAddress(address.getHostname(), address.getPort())
                .executor(executor).keepAliveTime(Config.grpc_keep_alive_second, TimeUnit.SECONDS)
                .flowControlWindow(Config.grpc_max_message_size_bytes)
                .keepAliveWithoutCalls(true)
                .maxInboundMessageSize(Config.grpc_max_message_size_bytes).enableRetry().maxRetryAttempts(MAX_RETRY_NUM)
                .usePlaintext().build();
        stub = PBackendServiceGrpc.newFutureStub(channel);
        blockingStub = PBackendServiceGrpc.newBlockingStub(channel);
        // execPlanTimeout should be greater than future.get timeout, otherwise future will throw ExecutionException
        execPlanTimeout = Config.remote_fragment_exec_timeout_ms + 5000;
    }

    // Is the underlying channel in a normal state? (That means the RPC call will not fail immediately)
    public boolean isNormalState() {
        ConnectivityState state = channel.getState(false);
        return state == ConnectivityState.CONNECTING
                || state == ConnectivityState.IDLE
                || state == ConnectivityState.READY;
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentAsync(
            InternalService.PExecPlanFragmentRequest request) {
        return stub.withDeadlineAfter(execPlanTimeout, TimeUnit.MILLISECONDS)
                .execPlanFragment(request);
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentPrepareAsync(
            InternalService.PExecPlanFragmentRequest request) {
        return stub.withDeadlineAfter(execPlanTimeout, TimeUnit.MILLISECONDS)
                .execPlanFragmentPrepare(request);
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentStartAsync(
            InternalService.PExecPlanFragmentStartRequest request) {
        return stub.withDeadlineAfter(execPlanTimeout, TimeUnit.MILLISECONDS)
                .execPlanFragmentStart(request);
    }

    public ListenableFuture<InternalService.PCancelPlanFragmentResult> cancelPlanFragmentAsync(
            InternalService.PCancelPlanFragmentRequest request) {
        return stub.withDeadlineAfter(execPlanTimeout, TimeUnit.MILLISECONDS)
                .cancelPlanFragment(request);
    }

    public Future<InternalService.PFetchDataResult> fetchDataAsync(InternalService.PFetchDataRequest request) {
        return stub.fetchData(request);
    }

    public Future<InternalService.PTabletKeyLookupResponse> fetchTabletDataAsync(
            InternalService.PTabletKeyLookupRequest request) {
        return stub.tabletFetchData(request);
    }

    public InternalService.PFetchDataResult fetchDataSync(InternalService.PFetchDataRequest request) {
        return blockingStub.fetchData(request);
    }

    public Future<InternalService.PFetchArrowFlightSchemaResult> fetchArrowFlightSchema(
            InternalService.PFetchArrowFlightSchemaRequest request) {
        return stub.fetchArrowFlightSchema(request);
    }

    public Future<InternalService.POutfileWriteSuccessResult> outfileWriteSuccessAsync(
            InternalService.POutfileWriteSuccessRequest request) {
        return stub.outfileWriteSuccess(request);
    }

    public Future<InternalService.PFetchTableSchemaResult> fetchTableStructureAsync(
            InternalService.PFetchTableSchemaRequest request) {
        return stub.fetchTableSchema(request);
    }

    public Future<InternalService.PJdbcTestConnectionResult> testJdbcConnection(
            InternalService.PJdbcTestConnectionRequest request) {
        return stub.testJdbcConnection(request);
    }

    public Future<InternalService.PCacheResponse> updateCache(InternalService.PUpdateCacheRequest request) {
        return stub.updateCache(request);
    }

    public Future<InternalService.PFetchCacheResult> fetchCache(InternalService.PFetchCacheRequest request) {
        return stub.fetchCache(request);
    }

    public Future<InternalService.PCacheResponse> clearCache(InternalService.PClearCacheRequest request) {
        return stub.clearCache(request);
    }

    public Future<InternalService.PProxyResult> getInfo(InternalService.PProxyRequest request) {
        return stub.getInfo(request);
    }

    public Future<InternalService.PSendDataResult> sendData(InternalService.PSendDataRequest request) {
        return stub.sendData(request);
    }

    public Future<InternalService.PRollbackResult> rollback(InternalService.PRollbackRequest request) {
        return stub.rollback(request);
    }

    public Future<InternalService.PCommitResult> commit(InternalService.PCommitRequest request) {
        return stub.commit(request);
    }

    public Future<InternalService.PConstantExprResult> foldConstantExpr(InternalService.PConstantExprRequest request) {
        return stub.foldConstantExpr(request);
    }

    public Future<InternalService.PFetchColIdsResponse> getColIdsByTabletIds(
            InternalService.PFetchColIdsRequest request) {
        return stub.getColumnIdsByTabletIds(request);
    }

    public Future<InternalService.PReportStreamLoadStatusResponse> reportStreamLoadStatus(
                            InternalService.PReportStreamLoadStatusRequest request) {
        return stub.reportStreamLoadStatus(request);
    }

    public Future<InternalService.PFetchRemoteSchemaResponse> fetchRemoteTabletSchemaAsync(
            InternalService.PFetchRemoteSchemaRequest request) {
        return stub.fetchRemoteTabletSchema(request);
    }

    public Future<InternalService.PGlobResponse> glob(InternalService.PGlobRequest request) {
        return stub.glob(request);
    }

    public Future<InternalService.PGroupCommitInsertResponse> groupCommitInsert(
            InternalService.PGroupCommitInsertRequest request) {
        return stub.groupCommitInsert(request);
    }

    public Future<InternalService.PGetWalQueueSizeResponse> getWalQueueSize(
            InternalService.PGetWalQueueSizeRequest request) {
        return stub.getWalQueueSize(request);
    }

    public Future<InternalService.PAlterVaultSyncResponse> alterVaultSync(
            InternalService.PAlterVaultSyncRequest request) {
        return stub.alterVaultSync(request);
    }


    public void shutdown() {
        ConnectivityState state = channel.getState(false);
        LOG.warn("shut down backend service client: {}, channel state: {}", address, state);
        if (!channel.isShutdown()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("Timed out gracefully shutting down connection: {}. ", channel);
                }
            } catch (InterruptedException e) {
                return;
            }
        }

        if (!channel.isTerminated()) {
            channel.shutdownNow();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("Timed out forcefully shutting down connection: {}. ", channel);
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }
}
