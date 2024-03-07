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

import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class ThriftServerEventProcessor implements TServerEventHandler {
    private static final Logger LOG = LogManager.getLogger(ThriftServerEventProcessor.class);

    private ThriftServer thriftServer;

    private static ThreadLocal<ThriftServerContext> connectionContext;

    public ThriftServerEventProcessor(ThriftServer thriftServer) {
        this.thriftServer = thriftServer;
        connectionContext = new ThreadLocal<>();
    }

    public static ThriftServerContext getConnectionContext() {
        return connectionContext.get();
    }

    @Override
    public void preServe() {
    }

    @Override
    public ServerContext createContext(TProtocol input, TProtocol output) {
        // param input is class org.apache.thrift.protocol.TBinaryProtocol
        TSocket tSocket = null;
        TTransport transport = input.getTransport();
        switch (thriftServer.getType()) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case THREADED_SELECTOR:
                // class org.apache.thrift.transport.TFramedTransport
                Preconditions.checkState(transport instanceof TFramedTransport);
                // NOTE: we need patch code in TNonblockingServer, we don't use for now.
                //  see https://issues.apache.org/jira/browse/THRI FT-1053
                if (LOG.isDebugEnabled()) {
                    LOG.debug("TFramedTransport cannot create thrift context. server type: {}", thriftServer.getType());
                }
                return null;
            case SIMPLE:
            case THREAD_POOL:
                // org.apache.thrift.transport.TSocket
                Preconditions.checkState(transport instanceof TSocket);
                tSocket = (TSocket) transport;
                break;
                // CHECKSTYLE IGNORE THIS LINE
        }
        if (tSocket == null) {
            LOG.warn("fail to get client socket. server type: {}", thriftServer.getType());
            return null;
        }
        SocketAddress socketAddress = tSocket.getSocket().getRemoteSocketAddress();
        InetSocketAddress inetSocketAddress = null;
        if (socketAddress instanceof InetSocketAddress) {
            inetSocketAddress = (InetSocketAddress) socketAddress;
        } else {
            LOG.warn("fail to get client socket address. server type: {}",
                    thriftServer.getType());
            return null;
        }
        TNetworkAddress clientAddress = new TNetworkAddress(
                inetSocketAddress.getHostString(),
                inetSocketAddress.getPort());

        thriftServer.addConnect(clientAddress);

        if (LOG.isDebugEnabled()) {
            LOG.debug("create thrift context. client: {}, server type: {}", clientAddress, thriftServer.getType());
        }
        return new ThriftServerContext(clientAddress);
    }

    @Override
    public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
        if (serverContext == null) {
            return;
        }

        Preconditions.checkState(serverContext instanceof ThriftServerContext);
        ThriftServerContext thriftServerContext = (ThriftServerContext) serverContext;
        TNetworkAddress clientAddress = thriftServerContext.getClient();
        connectionContext.remove();
        thriftServer.removeConnect(clientAddress);
        if (LOG.isDebugEnabled()) {
            LOG.debug("delete thrift context. client: {}, server type: {}", clientAddress, thriftServer.getType());
        }
    }

    @Override
    public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
        if (serverContext == null) {
            return;
        }

        ThriftServerContext thriftServerContext = (ThriftServerContext) serverContext;
        TNetworkAddress clientAddress = thriftServerContext.getClient();
        connectionContext.set(new ThriftServerContext(clientAddress));
    }
}
