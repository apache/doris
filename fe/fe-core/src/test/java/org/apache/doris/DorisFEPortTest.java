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

package org.apache.doris;

import org.apache.doris.common.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

class DorisFEPortTest {
    @Test
    void occupiedPortsAreCheckedOnlyWhenTheirServicesStart() throws IOException {
        String originalJournal = Config.edit_log_type;
        int originalEditLogPort = Config.edit_log_port;
        int originalQueryPort = Config.query_port;
        try (ServerSocket editLog = new ServerSocket(0); ServerSocket query = new ServerSocket(0)) {
            Config.edit_log_port = editLog.getLocalPort();
            Config.query_port = query.getLocalPort();
            Config.edit_log_type = "local";
            DorisFE.StartupOptions options = new DorisFE.StartupOptions();
            options.enableHttpServer = false;
            options.enableQeService = false;
            options.useEphemeralRpcPort = true;
            DorisFE.checkAllPorts(options);

            Config.edit_log_type = "bdb";
            IOException editLogFailure = Assertions.assertThrows(IOException.class,
                    () -> DorisFE.checkAllPorts(options));
            Assertions.assertEquals("port " + editLog.getLocalPort() + " already in use", editLogFailure.getMessage());

            Config.edit_log_type = "local";
            options.enableQeService = true;
            IOException queryFailure = Assertions.assertThrows(IOException.class,
                    () -> DorisFE.checkAllPorts(options));
            Assertions.assertEquals("port " + query.getLocalPort() + " already in use", queryFailure.getMessage());
        } finally {
            Config.edit_log_type = originalJournal;
            Config.edit_log_port = originalEditLogPort;
            Config.query_port = originalQueryPort;
        }
    }
}
