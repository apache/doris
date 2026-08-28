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

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ThriftServerTest {
    @Test
    public void testFramedThreadPoolRoundTrip() throws Exception {
        assertThreadPoolRoundTrip(true);
    }

    @Test
    public void testUnframedThreadPoolRoundTrip() throws Exception {
        assertThreadPoolRoundTrip(false);
    }

    private void assertThreadPoolRoundTrip(boolean useFramedTransport) throws Exception {
        TServerSocket serverTransport = new TServerSocket(0);
        int port = serverTransport.getServerSocket().getLocalPort();
        TProcessor processor = (input, output) -> {
            int request = input.readI32();
            output.writeI32(request + 1);
            output.getTransport().flush();
        };
        TThreadPoolServer server = ThriftServer.createThreadPoolServer(
                serverTransport, processor, useFramedTransport);
        CountDownLatch started = new CountDownLatch(1);
        server.setServerEventHandler(new TServerEventHandler() {
            @Override
            public void preServe() {
                started.countDown();
            }

            @Override
            public ServerContext createContext(TProtocol input, TProtocol output) {
                return null;
            }

            @Override
            public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
            }

            @Override
            public void processContext(
                    ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
            }
        });

        Thread serverThread = new Thread(server::serve, "framed-thrift-server-test");
        serverThread.setDaemon(true);
        serverThread.start();
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));

        TSocket clientSocket = new TSocket("127.0.0.1", port, 5000);
        TTransport clientTransport = useFramedTransport ? new TFramedTransport(clientSocket) : clientSocket;
        try {
            clientTransport.open();
            TBinaryProtocol protocol = new TBinaryProtocol(clientTransport);
            protocol.writeI32(41);
            clientTransport.flush();
            Assert.assertEquals(42, protocol.readI32());
        } finally {
            clientTransport.close();
            server.stop();
            serverThread.join(5000);
        }
        Assert.assertFalse(serverThread.isAlive());
    }
}
