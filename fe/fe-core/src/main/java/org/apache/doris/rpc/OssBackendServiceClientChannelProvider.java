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
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.tls.PrivateRpcTls;
import org.apache.doris.tls.server.TlsProtocolSet.Protocol;

import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class OssBackendServiceClientChannelProvider implements BackendServiceClientChannelProvider {
    @Override
    public ManagedChannel createChannel(TNetworkAddress address, String resolvedIp, Executor executor) {
        return createChannelForPurpose(address, resolvedIp, executor, true);
    }

    @Override
    public ManagedChannel createChannel(TNetworkAddress address, String resolvedIp, Executor executor,
            boolean internalRpc) {
        // Legacy subclasses own their policy through the original provider entry point.
        if (getClass() != OssBackendServiceClientChannelProvider.class) {
            return createChannel(address, resolvedIp, executor);
        }
        return createChannelForPurpose(address, resolvedIp, executor, internalRpc);
    }

    private ManagedChannel createChannelForPurpose(TNetworkAddress address, String resolvedIp, Executor executor,
            boolean internalRpc) {
        String targetHost = (resolvedIp != null && !resolvedIp.isEmpty()) ? resolvedIp : address.getHostname();
        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(targetHost, address.getPort())
                .keepAliveTime(Config.grpc_keep_alive_second, TimeUnit.SECONDS)
                .flowControlWindow(Config.grpc_max_message_size_bytes)
                .keepAliveWithoutCalls(true)
                .maxInboundMessageSize(Config.grpc_max_message_size_bytes)
                .enableRetry()
                .maxRetryAttempts(10);
        // Remote Doris workers expose their own endpoint contract, independently of local private TLS.
        if (internalRpc && PrivateRpcTls.enabled(Protocol.BRPC)) {
            // DNS resolution may change the connect address, but must not change the certified peer identity.
            channelBuilder.sslContext(PrivateRpcTls.grpcClientContext()).overrideAuthority(
                    HostAndPort.fromParts(address.getHostname(), address.getPort()).toString());
        } else {
            channelBuilder.usePlaintext();
        }
        if (Config.grpc_backend_client_use_direct_executor) {
            channelBuilder.directExecutor();
        } else {
            channelBuilder.executor(executor);
        }
        return channelBuilder.build();
    }
}
