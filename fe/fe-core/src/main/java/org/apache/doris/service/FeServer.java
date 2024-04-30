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

package org.apache.doris.service;

import org.apache.doris.common.ThriftServer;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.FrontendService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.lang.reflect.Proxy;

/**
 * Doris frontend thrift server
 */
public class FeServer {
    private static final Logger LOG = LogManager.getLogger(FeServer.class);

    private int port;
    private ThriftServer server;

    public FeServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
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
                    Object r = null;
                    try {
                        r = method.invoke(service, args);
                    } catch (Throwable t) {
                        feServiceLogger.warn("errors while process request for {}", name, t);
                        // If exception occurs, do not deal it, just keep as the previous
                        throw t;
                    } finally {
                        ConnectContext.remove();
                        feServiceLogger.debug("finish process request for {}", name);
                        if (MetricRepo.isInit) {
                            long end = System.currentTimeMillis();
                            MetricRepo.THRIFT_COUNTER_RPC_LATENCY.getOrAdd(name)
                                    .increase(end - begin);
                        }
                    }
                    return r;
                });
        // setup frontend server
        TProcessor tprocessor = new FrontendService.Processor<>(instance);
        server = new ThriftServer(port, tprocessor);
        server.start();
        LOG.info("thrift server started.");
    }
}
