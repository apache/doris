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
import org.apache.doris.thrift.FrontendService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TProcessor;

import java.io.IOException;

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
        // setup frontend server
        TProcessor tprocessor = new FrontendService.Processor<FrontendService.Iface>(
                new FrontendServiceImpl(ExecuteEnv.getInstance()));
        server = new ThriftServer(port, tprocessor);
        server.start();
        LOG.info("thrift server started.");
    }
}
