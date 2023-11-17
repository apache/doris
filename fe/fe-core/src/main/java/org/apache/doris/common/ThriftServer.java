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

import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

public class ThriftServer {
    private static final Logger LOG = LogManager.getLogger(ThriftServer.class);
    private ThriftServerType type;
    private int port;
    private TProcessor processor;
    private TServer server;
    private Thread serverThread;
    private Set<TNetworkAddress> connects;

    public static final String SIMPLE = "SIMPLE";
    public static final String THREADED_SELECTOR = "THREADED_SELECTOR";
    public static final String THREAD_POOL = "THREAD_POOL";

    public enum ThriftServerType {
        // TSimplerServer
        SIMPLE(ThriftServer.SIMPLE),
        // TThreadedSelectorServer
        THREADED_SELECTOR(ThriftServer.THREADED_SELECTOR),
        // TThreadPoolServer
        THREAD_POOL(ThriftServer.THREAD_POOL);

        private final String value;

        ThriftServerType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static ThriftServerType getThriftServerType(String value) {
            for (ThriftServerType val : ThriftServerType.values()) {
                if (val.getValue().equalsIgnoreCase(value)) {
                    return val;
                }
            }
            return ThriftServerType.THREAD_POOL;
        }
    }

    public ThriftServer(int port, TProcessor processor) {
        this.port = port;
        this.processor = processor;
        this.connects = Sets.newConcurrentHashSet();
        this.type = ThriftServerType.getThriftServerType(Config.thrift_server_type);
    }

    public ThriftServerType getType() {
        return type;
    }

    private void createSimpleServer() throws TTransportException {
        TServer.Args args = new TServer.Args(new TServerSocket(port)).protocolFactory(
                new TBinaryProtocol.Factory(Config.fe_thrift_max_pkg_bytes, -1)).processor(processor);
        server = new TSimpleServer(args);
    }

    private void createThreadedServer() throws TTransportException {
        TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(
                new TNonblockingServerSocket(port, Config.thrift_client_timeout_ms)).protocolFactory(
                        new TBinaryProtocol.Factory(Config.fe_thrift_max_pkg_bytes, -1)).processor(processor);
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                Config.thrift_server_max_worker_threads, "thrift-server-pool", true);
        args.executorService(threadPoolExecutor);
        server = new TThreadedSelectorServer(args);
    }

    private void createThreadPoolServer() throws TTransportException {
        TServerSocket.ServerSocketTransportArgs socketTransportArgs;

        if (FrontendOptions.isBindIPV6()) {
            socketTransportArgs = new TServerSocket.ServerSocketTransportArgs()
                .bindAddr(new InetSocketAddress("::0", port))
                .clientTimeout(Config.thrift_client_timeout_ms)
                .backlog(Config.thrift_backlog_num);
        } else {
            socketTransportArgs = new TServerSocket.ServerSocketTransportArgs()
                .bindAddr(new InetSocketAddress("0.0.0.0", port))
                .clientTimeout(Config.thrift_client_timeout_ms)
                .backlog(Config.thrift_backlog_num);
        }

        TThreadPoolServer.Args serverArgs =
                new TThreadPoolServer.Args(new TServerSocket(socketTransportArgs)).protocolFactory(
                        new TBinaryProtocol.Factory(Config.fe_thrift_max_pkg_bytes, -1)).processor(processor);
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                Config.thrift_server_max_worker_threads, "thrift-server-pool", true);
        serverArgs.executorService(threadPoolExecutor);
        server = new TThreadPoolServer(serverArgs);
    }

    public void start() throws IOException {
        try {
            switch (type) {
                case SIMPLE:
                    createSimpleServer();
                    break;
                case THREADED_SELECTOR:
                    createThreadedServer();
                    break;
                default:
                    createThreadPoolServer();
            }
        } catch (TTransportException ex) {
            LOG.warn("create thrift server failed.", ex);
            throw new IOException("create thrift server failed.", ex);
        }

        ThriftServerEventProcessor eventProcessor = new ThriftServerEventProcessor(this);
        server.setServerEventHandler(eventProcessor);

        serverThread = new Thread(() -> server.serve());
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

    public void addConnect(TNetworkAddress clientAddress) {
        connects.add(clientAddress);
    }

    public void removeConnect(TNetworkAddress clientAddress) {
        connects.remove(clientAddress);
    }
}
