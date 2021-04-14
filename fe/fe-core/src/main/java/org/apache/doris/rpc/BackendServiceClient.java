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

import java.util.concurrent.Future;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

public class BackendServiceClient {
    private static final int MAX_RETRY_NUM = 3;
    private final PBackendServiceGrpc.PBackendServiceFutureStub stub;
    private final PBackendServiceGrpc.PBackendServiceBlockingStub blockingStub;
    private final ManagedChannel channel;

    public BackendServiceClient(TNetworkAddress address) {
        channel = NettyChannelBuilder.forAddress(address.getHostname(), address.getPort())
                .flowControlWindow(Config.grpc_max_message_size_bytes)
                .maxInboundMessageSize(Config.grpc_max_message_size_bytes)
                .enableRetry().maxRetryAttempts(MAX_RETRY_NUM)
                .usePlaintext().build();
        stub = PBackendServiceGrpc.newFutureStub(channel);
        blockingStub = PBackendServiceGrpc.newBlockingStub(channel);
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentAsync(
            InternalService.PExecPlanFragmentRequest request) {
        return stub.execPlanFragment(request);
    }

    public Future<InternalService.PCancelPlanFragmentResult> cancelPlanFragmentAsync(
            InternalService.PCancelPlanFragmentRequest request) {
        return stub.cancelPlanFragment(request);
    }

    public Future<InternalService.PFetchDataResult> fetchDataAsync(InternalService.PFetchDataRequest request) {
        return stub.fetchData(request);
    }

    public InternalService.PFetchDataResult fetchDataSync(InternalService.PFetchDataRequest request) {
        return blockingStub.fetchData(request);
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

    public Future<InternalService.PTriggerProfileReportResult> triggerProfileReport(
            InternalService.PTriggerProfileReportRequest request) {
        return stub.triggerProfileReport(request);
    }

    public Future<InternalService.PProxyResult> getInfo(InternalService.PProxyRequest request) {
        return stub.getInfo(request);
    }
}
