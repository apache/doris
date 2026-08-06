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
import io.grpc.MethodDescriptor;
import io.grpc.ServerStreamTracer;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class FlightRemoteIpServerStreamTracerTest {

    @Test
    public void testGetRemoteIpFromServerCallAttributes() {
        FlightRemoteIpServerStreamTracer tracer = new FlightRemoteIpServerStreamTracer();
        Context context = tracer.filterContext(Context.current());
        Context previous = context.attach();
        try {
            tracer.serverCallStarted(new TestServerCallInfo(new InetSocketAddress("10.26.20.3", 12345)));

            Assert.assertEquals("10.26.20.3", FlightRemoteIpServerStreamTracer.getRemoteIp());
        } finally {
            context.detach(previous);
        }
    }

    @Test
    public void testFallbackRemoteIpWithoutServerCallAttributes() {
        FlightRemoteIpServerStreamTracer tracer = new FlightRemoteIpServerStreamTracer();
        Context context = tracer.filterContext(Context.current());
        Context previous = context.attach();
        try {
            tracer.serverCallStarted(new TestServerCallInfo(null));

            Assert.assertEquals("0.0.0.0", FlightRemoteIpServerStreamTracer.getRemoteIp());
        } finally {
            context.detach(previous);
        }
    }

    @Test
    public void testFallbackRemoteIpWithoutFlightContext() {
        Assert.assertEquals("0.0.0.0", FlightRemoteIpServerStreamTracer.getRemoteIp());
    }

    private static class TestServerCallInfo extends ServerStreamTracer.ServerCallInfo<Object, Object> {
        private final Attributes attributes;

        TestServerCallInfo(SocketAddress remoteAddress) {
            Attributes.Builder builder = Attributes.newBuilder();
            if (remoteAddress != null) {
                builder.set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, remoteAddress);
            }
            this.attributes = builder.build();
        }

        @Override
        public MethodDescriptor<Object, Object> getMethodDescriptor() {
            return null;
        }

        @Override
        public Attributes getAttributes() {
            return attributes;
        }

        @Override
        public String getAuthority() {
            return null;
        }
    }
}
