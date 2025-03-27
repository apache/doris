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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * ModifyFrontendOrBackendHostNameOp
 */
public class ModifyFrontendOrBackendHostNameOp extends AlterSystemOp {
    private String hostPort;
    private String host;
    private String newHost;
    private int port;
    private ModifyOpType modifyOpType;

    /**
     * ModifyOpType
     */
    public enum ModifyOpType {
        Frontend,
        Backend
    }

    public ModifyFrontendOrBackendHostNameOp(String hostPort, String newHost,
            ModifyOpType modifyOpType) {
        super(AlterOpType.ALTER_OTHER);
        this.hostPort = hostPort;
        this.newHost = newHost;
        this.modifyOpType = modifyOpType;
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

    public ModifyOpType getModifyOpType() {
        return modifyOpType;
    }

    @Override
    public void validate(ConnectContext ctx) throws AnalysisException {
        HostInfo hostInfo = SystemInfoService.getHostAndPort(hostPort);
        this.host = hostInfo.getHost();
        this.port = hostInfo.getPort();

        try {
            // validate hostname
            if (!InetAddressValidator.getInstance().isValid(newHost)) {
                // if no IP address for the host could be found, 'getByName'
                // will throw UnknownHostException
                InetAddress.getByName(newHost);
            }
        } catch (UnknownHostException e) {
            throw new AnalysisException("Unknown hostname:  " + e.getMessage());
        }
    }

    @Override
    public String toSql() {
        if (modifyOpType.equals(ModifyOpType.Frontend)
                || modifyOpType.equals(ModifyOpType.Backend)) {
            StringBuilder sb = new StringBuilder();
            sb.append("ALTER SYSTEM MODIFY ");
            sb.append(modifyOpType.name().toUpperCase());
            sb.append(" \"").append(hostPort).append("\"");
            sb.append(" HOSTNAME ").append("\"");
            sb.append(newHost).append("\"");
            return sb.toString();
        }
        throw new NotImplementedException("Unknown modify op type:  " + modifyOpType);
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }
}
