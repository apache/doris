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

import org.apache.doris.analysis.AddFollowerClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

/**
 * AddFrontendOp
 */
public class AddFollowerOp extends FrontendOp {
    public AddFollowerOp(String hostPort) {
        super(hostPort, FrontendNodeType.FOLLOWER);
    }

    @Override
    public void validate(ConnectContext ctx) throws AnalysisException {
        super.validate(ctx);
        SystemInfoService.HostInfo hostInfo = SystemInfoService.getHostAndPort(hostPort);
        this.host = hostInfo.getHost();
        this.port = hostInfo.getPort();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER CLUSTER ADD FOLLOWER \"");
        sb.append(hostPort).append("\"");
        return sb.toString();
    }

    @Override
    public AlterClause translateToLegacyAlterClause() {
        return new AddFollowerClause(hostPort, host, port, role);
    }
}
