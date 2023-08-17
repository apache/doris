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
import org.apache.doris.common.telemetry.Telemetry;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.PBackendServiceGrpc;
import org.apache.doris.thrift.TNetworkAddress;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.opentelemetry.context.Context;
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
                .executor(executor)
                .flowControlWindow(Config.grpc_max_message_size_bytes)
                .keepAliveWithoutCalls(true)
                .maxInboundMessageSize(Config.grpc_max_message_size_bytes).enableRetry().maxRetryAttempts(MAX_RETRY_NUM)
                .intercept(new OpenTelemetryClientInterceptor()).usePlaintext().build();
        stub = PBackendServiceGrpc.newFutureStub(channel);
        blockingStub = PBackendServiceGrpc.newBlockingStub(channel);
        // execPlanTimeout should be greater than future.get timeout, otherwise future will throw ExecutionException
        execPlanTimeout = Config.remote_fragment_exec_timeout_ms + 5000;
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

    public Future<InternalService.PCancelPlanFragmentResult> cancelPlanFragmentAsync(
            InternalService.PCancelPlanFragmentRequest request) {
        return stub.cancelPlanFragment(request);
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

    public Future<InternalService.PFetchTableSchemaResult> fetchTableStructureAsync(
            InternalService.PFetchTableSchemaRequest request) {
        return stub.fetchTableSchema(request);
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

    public Future<InternalService.PFetchRemoteSchemaResponse> fetchRemoteTabletSchemaAsync(
            InternalService.PFetchRemoteSchemaRequest request) {
        return stub.fetchRemoteTabletSchema(request);
    }

    public Future<InternalService.PGlobResponse> glob(InternalService.PGlobRequest request) {
        return stub.glob(request);
    }

    public void shutdown() {
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

        LOG.warn("shut down backend service client: {}", address);
    }

    /**
     * OpenTelemetry span interceptor.
     */
    public static class OpenTelemetryClientInterceptor implements ClientInterceptor {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor,
                CallOptions callOptions, Channel channel) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    channel.newCall(methodDescriptor, callOptions)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    // Inject the request with the current context
                    Telemetry.getOpenTelemetry().getPropagators().getTextMapPropagator()
                            .inject(Context.current(), headers, (carrier, key, value) -> carrier.put(
                                    Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value));
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
