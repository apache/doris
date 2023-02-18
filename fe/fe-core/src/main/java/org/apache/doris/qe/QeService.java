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

package org.apache.doris.qe;

import org.apache.doris.mysql.nio.NMysqlServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is the encapsulation of the entire front-end service,
 * including the creation of services that support the MySQL protocol
 */
public class QeService {
    private static final Logger LOG = LogManager.getLogger(QeService.class);

    private int port;
    // MySQL protocol service
    private NMysqlServer mysqlServer;

    @Deprecated
    public QeService(int port) {
        this.port = port;
    }

    public QeService(int port, ConnectScheduler scheduler) {
        this.port = port;
        this.mysqlServer = new NMysqlServer(port, scheduler);
    }

    public void start() throws Exception {
        // Set up help module
        try {
            HelpModule.getInstance().setUpModule();
        } catch (Exception e) {
            LOG.warn("Help module failed, because:", e);
            throw e;
        }

        if (!mysqlServer.start()) {
            LOG.error("mysql server start failed");
            System.exit(-1);
        }
        LOG.info("QE service start.");
    }
}
