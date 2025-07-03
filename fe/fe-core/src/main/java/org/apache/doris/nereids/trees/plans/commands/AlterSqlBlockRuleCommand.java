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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.SqlBlockUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * alter Sql block rule Commands.
 */
public class AlterSqlBlockRuleCommand extends SqlBlockRuleCommand {

    /**
    * constructor
    */
    public AlterSqlBlockRuleCommand(String ruleName, Map<String, String> properties) {
        super(ruleName, properties, PlanType.ALTER_SQL_BLOCK_RULE_COMMAND);
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        Env.getCurrentEnv().getSqlBlockRuleMgr().alterSqlBlockRule(new SqlBlockRule(ruleName,
                    sql, sqlHash, partitionNum,
                    tabletNum, cardinality, global, enable));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterSqlBlockRuleCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }

    @Override
    public void setProperties(Map<String, String> properties) throws AnalysisException {
        this.sql = properties.getOrDefault(SQL_PROPERTY, SqlBlockRuleCommand.STRING_NOT_SET);
        this.sqlHash = properties.getOrDefault(SQL_HASH_PROPERTY, SqlBlockRuleCommand.STRING_NOT_SET);
        String partitionNumString = properties.get(SCANNED_PARTITION_NUM);
        String tabletNumString = properties.get(SCANNED_TABLET_NUM);
        String cardinalityString = properties.get(SCANNED_CARDINALITY);

        SqlBlockUtil.checkSqlAndSqlHashSetBoth(sql, sqlHash);
        SqlBlockUtil.checkSqlAndLimitationsSetBoth(sql, sqlHash,
                partitionNumString, tabletNumString, cardinalityString);
        this.partitionNum = Util.getLongPropertyOrDefault(partitionNumString, SqlBlockRuleCommand.LONG_NOT_SET, null,
                SCANNED_PARTITION_NUM + " should be a long");
        this.tabletNum = Util.getLongPropertyOrDefault(tabletNumString, SqlBlockRuleCommand.LONG_NOT_SET, null,
                SCANNED_TABLET_NUM + " should be a long");
        this.cardinality = Util.getLongPropertyOrDefault(cardinalityString, SqlBlockRuleCommand.LONG_NOT_SET, null,
                SCANNED_CARDINALITY + " should be a long");
        // allow null, represents no modification
        String globalStr = properties.get(GLOBAL_PROPERTY);
        this.global = StringUtils.isNotEmpty(globalStr) ? Boolean.parseBoolean(globalStr) : null;
        String enableStr = properties.get(ENABLE_PROPERTY);
        this.enable = StringUtils.isNotEmpty(enableStr) ? Boolean.parseBoolean(enableStr) : null;
    }
}
