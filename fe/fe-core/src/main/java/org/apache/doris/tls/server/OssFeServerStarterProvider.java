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

package org.apache.doris.tls.server;

import org.apache.doris.common.Config;
import org.apache.doris.common.ThriftServer;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlServer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.service.arrowflight.DorisFlightSqlService;
import org.apache.doris.thrift.FrontendService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TProcessor;

import java.lang.reflect.Proxy;

public class OssFeServerStarterProvider implements FeServerStarterProvider {
    @Override
    public ServerStarter createThriftServerStarter(int port) {
        return new OssThriftServerStarter(port);
    }

    @Override
    public ServerStarter createMysqlServerStarter(int port, ConnectScheduler scheduler) {
        return new OssMysqlServerStarter(port, scheduler);
    }

    @Override
    public ServerStarter createFlightServerStarter(int port) {
        return new OssFlightServerStarter(port);
    }

    private static final class OssThriftServerStarter implements ServerStarter {
        private static final Logger LOG = LogManager.getLogger(OssThriftServerStarter.class);
        private final int port;
        private ThriftServer server;

        private OssThriftServerStarter(int port) {
            this.port = port;
        }

        @Override
        public void start() throws Exception {
            if (Config.enable_tls && TlsProtocolSet.isProtocolIncluded(TlsProtocolSet.Protocol.THRIFT)) {
                throw new UnsupportedOperationException("FE thrift TLS requires TLS module");
            }
            FrontendServiceImpl service = new FrontendServiceImpl(ExecuteEnv.getInstance());
            Logger feServiceLogger = LogManager.getLogger(FrontendServiceImpl.class);
            FrontendService.Iface instance = (FrontendService.Iface) Proxy.newProxyInstance(
                    FrontendServiceImpl.class.getClassLoader(),
                    FrontendServiceImpl.class.getInterfaces(),
                    (proxy, method, args) -> {
                        long begin = System.currentTimeMillis();
                        String name = method.getName();
                        if (MetricRepo.isInit) {
                            MetricRepo.THRIFT_COUNTER_RPC_ALL.getOrAdd(name).increase(1L);
                        }
                        feServiceLogger.debug("receive request for {}", name);
                        try {
                            return method.invoke(service, args);
                        } finally {
                            ConnectContext.remove();
                            feServiceLogger.debug("finish process request for {}", name);
                            if (MetricRepo.isInit) {
                                MetricRepo.THRIFT_COUNTER_RPC_LATENCY.getOrAdd(name)
                                        .increase(System.currentTimeMillis() - begin);
                            }
                        }
                    });
            TProcessor processor = new FrontendService.Processor<>(instance);
            server = new ThriftServer(port, processor);
            server.start();
            LOG.info("FE thrift starter started on {}", port);
        }

        @Override
        public void stop() throws Exception {
            if (server != null) {
                server.stop();
                server.join();
            }
        }
    }

    private static final class OssMysqlServerStarter implements ServerStarter {
        private final MysqlServer mysqlServer;

        private OssMysqlServerStarter(int port, ConnectScheduler scheduler) {
            this.mysqlServer = new MysqlServer(port, scheduler);
        }

        @Override
        public void start() {
            if (Config.enable_tls && TlsProtocolSet.isProtocolIncluded(TlsProtocolSet.Protocol.MYSQL)) {
                throw new UnsupportedOperationException("FE MySQL TLS requires TLS module");
            }
            if (!mysqlServer.start()) {
                throw new IllegalStateException("mysql server start failed");
            }
        }

        @Override
        public void stop() {
            mysqlServer.stop();
        }
    }

    private static final class OssFlightServerStarter implements ServerStarter {
        private final int port;
        private DorisFlightSqlService flightSqlService;

        private OssFlightServerStarter(int port) {
            this.port = port;
        }

        @Override
        public void start() {
            if (Config.enable_tls && TlsProtocolSet.isProtocolIncluded(TlsProtocolSet.Protocol.ARROWFLIGHT)) {
                throw new UnsupportedOperationException("FE Arrow Flight TLS requires TLS module");
            }
            if (port == -1) {
                return;
            }
            flightSqlService = new DorisFlightSqlService(port);
            if (!flightSqlService.start()) {
                throw new IllegalStateException("arrow flight sql server start failed");
            }
        }

        @Override
        public void stop() {
            if (flightSqlService != null) {
                flightSqlService.stop();
            }
        }
    }
}
