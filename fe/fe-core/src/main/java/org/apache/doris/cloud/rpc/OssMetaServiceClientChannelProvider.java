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

package org.apache.doris.cloud.rpc;

import org.apache.doris.common.Config;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;

public class OssMetaServiceClientChannelProvider implements MetaServiceClientChannelProvider {
    @Override
    public ManagedChannel createChannel(String target) {
        return NettyChannelBuilder.forTarget(target)
                .flowControlWindow(Config.grpc_max_message_size_bytes)
                .maxInboundMessageSize(Config.grpc_max_message_size_bytes)
                .defaultServiceConfig(MetaServiceClient.serviceConfig())
                .defaultLoadBalancingPolicy("round_robin")
                .enableRetry()
                .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, Config.meta_service_brpc_connect_timeout_ms)
                .usePlaintext()
                .build();
    }

    @Override
    public long currentConfigVersion() {
        return 0L;
    }
}
