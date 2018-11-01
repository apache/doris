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

import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

/**
 * Created by zhaochun on 14-9-2.
 */
public class ThriftServer {
    private static final Logger           LOG  = Logger.getLogger(ThriftServer.class);
    private              ThriftServerType type = ThriftServerType.THREAD_POOL;
    private int        port;
    private TProcessor processor;
    private TServer    server;
    private Thread     serverThread;
    
    public ThriftServer(int port, TProcessor processor) {
        this.port = port;
        this.processor = processor;
    }

    private void createSimpleServer() throws TTransportException {
        TServer.Args args = new TServer.Args(new TServerSocket(port)).protocolFactory(
            new TBinaryProtocol.Factory()).processor(processor);
        server = new TSimpleServer(args);
    }

    private void createThreadedServer() throws TTransportException {
        TThreadedSelectorServer.Args args =
            new TThreadedSelectorServer.Args(new TNonblockingServerSocket(port)).protocolFactory(
                new TBinaryProtocol.Factory()).processor(processor);
        server = new TThreadedSelectorServer(args);
    }

    private void createThreadPoolServer() throws TTransportException {
        TThreadPoolServer.Args args =
            new TThreadPoolServer.Args(new TServerSocket(port)).protocolFactory(
                new TBinaryProtocol.Factory()).processor(processor);
        server = new TThreadPoolServer(args);
    }

    public void start() throws IOException {
        try {
            switch (type) {
                case SIMPLE:
                    createSimpleServer();
                    break;
                case THREADED:
                    createThreadedServer();
                    break;
                case THREAD_POOL:
                    createThreadPoolServer();
                    break;
                default:
                    break;
            }
        } catch (TTransportException ex) {
            LOG.warn("create thrift server failed.", ex);
            throw new IOException("create thrift server failed.", ex);
        }
        serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                server.serve();
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
    }

    public void stop() {
        if (server != null) {
            server.stop();
        }
    }

    public void join() throws InterruptedException {
        if (server != null && server.isServing()) {
            server.stop();
        }
        serverThread.join();
    }

    public static enum ThriftServerType {
        SIMPLE,
        THREADED,
        THREAD_POOL
    }
}
