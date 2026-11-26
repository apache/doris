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

package org.apache.doris.common;

import org.apache.doris.thrift.HeartbeatService;
import org.apache.doris.thrift.TBackendInfo;
import org.apache.doris.thrift.THeartbeatResult;
import org.apache.doris.thrift.TMasterInfo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;

class ThriftServerTest {
    @Test
    void servesQueuedRpcOnBoundSocket() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket listener = new ServerSocket(0, 8, loopback);
                ServerSocket competitor = new ServerSocket();
                TSocket client = new TSocket(loopback.getHostAddress(), listener.getLocalPort(), 5000)) {
            Assertions.assertThrows(BindException.class, () -> competitor.bind(listener.getLocalSocketAddress()));
            // Queue the connection before start(). Replacing the listener would discard this connection.
            client.open();
            HeartbeatService.Iface heartbeat = master -> new THeartbeatResult(new TStatus(TStatusCode.OK),
                    new TBackendInfo(master.getNetworkAddress().getPort(), 80));
            ThriftServer server = new ThriftServer(listener.getLocalPort(),
                    new HeartbeatService.Processor<>(heartbeat));
            server.start(listener);
            try {
                HeartbeatService.Client service = new HeartbeatService.Client(new TBinaryProtocol(client));
                TMasterInfo master = new TMasterInfo(new TNetworkAddress("localhost", listener.getLocalPort()), 1, 1);
                THeartbeatResult reply = service.heartbeat(master);
                Assertions.assertEquals(listener.getLocalPort(), reply.getBackendInfo().getBePort());
            } finally {
                client.close();
                server.stop();
                server.join();
            }
            Assertions.assertTrue(listener.isClosed());
        }
    }
}
