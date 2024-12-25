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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.List;

public class CancelAlterSystemStmt extends CancelStmt implements NotFallbackInParser {

    protected List<String> params;
    @Getter
    private final List<HostInfo> hostInfos;

    @Getter
    private final List<String> ids;

    public CancelAlterSystemStmt(List<String> params) {
        this.params = params;
        this.hostInfos = Lists.newArrayList();
        this.ids = Lists.newArrayList();
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.OPERATOR.getPrivs().toString());
        }
        for (String param : params) {
            if (!param.contains(":")) {
                ids.add(param);
            } else {
                HostInfo hostInfo = SystemInfoService.getHostAndPort(param);
                this.hostInfos.add(hostInfo);
            }

        }
        Preconditions.checkState(!this.hostInfos.isEmpty() || !this.ids.isEmpty(),
                "hostInfos or ids can not be empty");

    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CANCEL DECOMMISSION BACKEND ");
        if (!ids.isEmpty()) {
            for (int i = 0; i < hostInfos.size(); i++) {
                sb.append("\"").append(hostInfos.get(i)).append("\"");
                if (i != hostInfos.size() - 1) {
                    sb.append(", ");
                }
            }
        } else {
            for (int i = 0; i < params.size(); i++) {
                sb.append("\"").append(params.get(i)).append("\"");
                if (i != params.size() - 1) {
                    sb.append(", ");
                }
            }
        }


        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CANCEL;
    }
}
