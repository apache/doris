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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;

import java.util.List;

/**
 * ShowWorkloadGroupsCommand
 */
public class ShowWorkloadGroupsCommand extends ShowCommand {

    private String pattern;

    public ShowWorkloadGroupsCommand(String pattern) {
        super(PlanType.SHOW_WORKLOAD_GROUP_COMMAND);
        this.pattern = pattern;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowWorkloadGroups();
    }

    private ShowResultSet handleShowWorkloadGroups() throws AnalysisException {
        PatternMatcher matcher = null;
        if (this.pattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(this.pattern,
                CaseSensibility.WORKLOAD_GROUP.getCaseSensibility());
        }
        List<List<String>> workloadGroupsInfos = Env.getCurrentEnv().getWorkloadGroupMgr().getResourcesInfo(matcher);
        return new ShowResultSet(getMetaData(), workloadGroupsInfos);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowWorkloadGroupsCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    public String toSql() {
        String sql = "SHOW WORKLOAD GROUPS";
        if (this.pattern != null) {
            sql += " LIKE '" + pattern + "'";
        }
        return sql;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
