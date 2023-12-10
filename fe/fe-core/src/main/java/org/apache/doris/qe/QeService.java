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

import org.apache.doris.mysql.MysqlServer;
import org.apache.doris.service.arrowflight.DorisFlightSqlService;

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
    private MysqlServer mysqlServer;

    private int arrowFlightSQLPort;
    private DorisFlightSqlService dorisFlightSqlService;

    @Deprecated
    public QeService(int port, int arrowFlightSQLPort) {
        this.port = port;
        this.arrowFlightSQLPort = arrowFlightSQLPort;
    }

    public QeService(int port, int arrowFlightSQLPort, ConnectScheduler scheduler) {
        this.port = port;
        this.arrowFlightSQLPort = arrowFlightSQLPort;
        this.mysqlServer = new MysqlServer(port, scheduler);
    }

    public void start() throws Exception {
        // Set up help module
        try {
            HelpModule.getInstance().setUpModule(HelpModule.HELP_ZIP_FILE_NAME);
        } catch (Exception e) {
            LOG.warn("Help module failed, because:", e);
            throw e;
        }

        if (!mysqlServer.start()) {
            LOG.error("mysql server start failed");
            System.exit(-1);
        }
        if (arrowFlightSQLPort != -1) {
            this.dorisFlightSqlService = new DorisFlightSqlService(arrowFlightSQLPort);
            if (!dorisFlightSqlService.start()) {
                System.exit(-1);
            }
        } else {
            LOG.info("No Arrow Flight SQL service that needs to be started.");
        }
        LOG.info("QE service start.");
    }
}
