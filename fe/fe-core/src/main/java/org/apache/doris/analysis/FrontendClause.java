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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Map;

public class FrontendClause extends AlterClause {
    protected List<String> params;
    @Getter
    protected List<String> names;
    protected FrontendNodeType role;
    @Getter
    protected List<HostInfo> hostInfos;

    protected FrontendClause(List<String> params, FrontendNodeType role) {
        super(AlterOpType.ALTER_OTHER);
        this.params = params;
        this.role = role;
        this.hostInfos = Lists.newArrayList();
        this.names = Lists.newArrayList();
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                                analyzer.getQualifiedUser());
        }
        for (String param : params) {
            if (!param.contains(":")) {
                names.add(param);
            } else {
                HostInfo hostInfo = SystemInfoService.getHostAndPort(param);
                this.hostInfos.add(hostInfo);
            }
        }
    }

    @Override
    public String toSql() {
        throw new NotImplementedException("FrontendClause.toSql() not implemented");
    }

    @Override
    public Map<String, String> getProperties() {
        throw new NotImplementedException("FrontendClause.getProperties() not implemented");
    }

}
