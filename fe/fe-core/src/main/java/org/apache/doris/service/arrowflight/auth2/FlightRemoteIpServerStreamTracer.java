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

package org.apache.doris.service.arrowflight.auth2;

import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Captures the gRPC peer address before Arrow Flight header authentication runs.
 * Arrow registers header authentication ahead of user interceptors, so use ServerStreamTracer to
 * seed the remote IP into the gRPC Context for Basic credential validation.
 */
public class FlightRemoteIpServerStreamTracer extends ServerStreamTracer {
    static final String UNKNOWN_REMOTE_IP = "0.0.0.0";
    private static final Context.Key<RemoteIpHolder> REMOTE_IP_CONTEXT_KEY =
            Context.key("doris.arrow.flight.remote_ip");

    @Override
    public Context filterContext(Context context) {
        return context.withValue(REMOTE_IP_CONTEXT_KEY, new RemoteIpHolder());
    }

    @Override
    public void serverCallStarted(ServerCallInfo<?, ?> callInfo) {
        RemoteIpHolder holder = REMOTE_IP_CONTEXT_KEY.get();
        if (holder == null) {
            return;
        }

        Attributes attributes = callInfo.getAttributes();
        SocketAddress remoteAddress = attributes == null ? null : attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        holder.setRemoteIp(extractRemoteIp(remoteAddress));
    }

    public static String getRemoteIp() {
        RemoteIpHolder holder = REMOTE_IP_CONTEXT_KEY.get();
        if (holder == null) {
            return UNKNOWN_REMOTE_IP;
        }
        return holder.getRemoteIp();
    }

    static String extractRemoteIp(SocketAddress remoteAddress) {
        if (!(remoteAddress instanceof InetSocketAddress)) {
            return UNKNOWN_REMOTE_IP;
        }

        InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddress;
        InetAddress address = inetSocketAddress.getAddress();
        if (address != null && isNotEmpty(address.getHostAddress())) {
            return address.getHostAddress();
        }
        if (isNotEmpty(inetSocketAddress.getHostString())) {
            return inetSocketAddress.getHostString();
        }
        return UNKNOWN_REMOTE_IP;
    }

    private static boolean isNotEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    public static class Factory extends ServerStreamTracer.Factory {
        @Override
        public ServerStreamTracer newServerStreamTracer(String fullMethodName, Metadata headers) {
            return new FlightRemoteIpServerStreamTracer();
        }
    }

    private static class RemoteIpHolder {
        private volatile String remoteIp = UNKNOWN_REMOTE_IP;

        String getRemoteIp() {
            return remoteIp;
        }

        void setRemoteIp(String remoteIp) {
            this.remoteIp = isNotEmpty(remoteIp) ? remoteIp : UNKNOWN_REMOTE_IP;
        }
    }
}
