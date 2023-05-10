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

package org.apache.doris.analysis;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ModifyNodeHostNameClause extends AlterClause {
    protected String hostPort;
    protected String host;
    protected String newHost;
    protected int port;

    protected ModifyNodeHostNameClause(String hostPort, String newHost) {
        super(AlterOpType.ALTER_OTHER);
        this.hostPort = hostPort;
        this.newHost = newHost;
    }

    public String getHost() {
        return host;
    }

    public String getNewHost() {
        return newHost;
    }

    public int getPort() {
        return port;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        HostInfo hostInfo = SystemInfoService.getHostAndPort(hostPort);
        this.host = hostInfo.getHost();
        this.port = hostInfo.getPort();

        try {
            // validate hostname
            if (!InetAddressValidator.getInstance().isValid(newHost)) {
                // if no IP address for the host could be found, 'getByName'
                // will throw UnknownHostException
                InetAddress.getByName(newHost);
            } else {
                throw new AnalysisException("Invalid hostname: " + newHost);
            }
        } catch (UnknownHostException e) {
            throw new AnalysisException("Unknown hostname:  " + e.getMessage());
        }
    }

    @Override
    public String toSql() {
        throw new NotImplementedException("toSql() method not implemented");
    }
}
