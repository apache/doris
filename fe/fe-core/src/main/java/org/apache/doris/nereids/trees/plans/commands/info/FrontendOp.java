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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import org.apache.commons.lang3.NotImplementedException;

import java.util.Map;

/**
 * FrontendOp
 */
public abstract class FrontendOp extends AlterSystemOp {
    protected String hostPort;
    protected String host;
    protected int port;
    protected FrontendNodeType role;

    protected FrontendOp(String hostPort, FrontendNodeType role) {
        super(AlterOpType.ALTER_OTHER);
        this.hostPort = hostPort;
        this.role = role;
    }

    @Override
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    ctx.getQualifiedUser());
        }

        HostInfo hostInfo = SystemInfoService.getHostAndPort(hostPort);
        this.host = hostInfo.getHost();
        this.port = hostInfo.getPort();
    }

    @Override
    public Map<String, String> getProperties() {
        throw new NotImplementedException("Not support getProperties for BackendOp");
    }

    public String getIp() {
        return host;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
