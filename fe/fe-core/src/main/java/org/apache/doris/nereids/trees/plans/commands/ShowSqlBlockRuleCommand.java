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

import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * show sql block rule command
 */
public class ShowSqlBlockRuleCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(50)))
                    .addColumn(new Column("Sql", ScalarType.createVarchar(65535)))
                    .addColumn(new Column("SqlHash", ScalarType.createVarchar(65535)))
                    .addColumn(new Column("PartitionNum", ScalarType.createVarchar(10)))
                    .addColumn(new Column("TabletNum", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Cardinality", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Global", ScalarType.createVarchar(4)))
                    .addColumn(new Column("Enable", ScalarType.createVarchar(4)))
                    .build();
    private final String ruleName; // optional

    /**
     * constructor
     */
    public ShowSqlBlockRuleCommand(String ruleName) {
        super(PlanType.SHOW_BLOCK_RULE_COMMAND);
        this.ruleName = ruleName;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        List<List<String>> rows = Lists.newArrayList();
        List<SqlBlockRule> sqlBlockRules = Env.getCurrentEnv().getSqlBlockRuleMgr().getSqlBlockRule(ruleName);
        sqlBlockRules.forEach(rule -> rows.add(rule.getShowInfo()));
        return new ShowResultSet(META_DATA, rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowSqlBlockRuleCommand(this, context);
    }
}
