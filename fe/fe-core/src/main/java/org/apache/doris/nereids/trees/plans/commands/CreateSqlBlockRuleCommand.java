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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlBlockUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * create Sql block rule Commands.
 */
public class CreateSqlBlockRuleCommand extends SqlBlockRuleCommand {
    private static final String NAME_TYPE = "SQL BLOCK RULE NAME";
    private final boolean ifNotExists;

    /**
    * constructor
    */
    public CreateSqlBlockRuleCommand(String ruleName, boolean ifNotExists, Map<String, String> properties) {
        super(ruleName, properties, PlanType.CREATE_SQL_BLOCK_RULE_COMMAND);
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check name
        FeNameFormat.checkCommonName(NAME_TYPE, ruleName);
        // avoid a rule block any ddl for itself
        if (StringUtils.isNotEmpty(sql) && Pattern.compile(sql).matcher(this.ruleName).find()) {
            throw new AnalysisException("sql of SQL_BLOCK_RULE should not match its name");
        }
        Env.getCurrentEnv().getSqlBlockRuleMgr().createSqlBlockRule(new SqlBlockRule(ruleName, sql,
                    sqlHash, partitionNum,
                    tabletNum, cardinality, global, enable), ifNotExists);
    }

    @Override
    public void setProperties(Map<String, String> properties) throws UserException {
        this.sql = properties.getOrDefault(SQL_PROPERTY, SqlBlockRuleCommand.STRING_NOT_SET);
        this.sqlHash = properties.getOrDefault(SQL_HASH_PROPERTY, SqlBlockRuleCommand.STRING_NOT_SET);
        String partitionNumString = properties.get(SCANNED_PARTITION_NUM);
        String tabletNumString = properties.get(SCANNED_TABLET_NUM);
        String cardinalityString = properties.get(SCANNED_CARDINALITY);

        SqlBlockUtil.checkSqlAndSqlHashSetBoth(sql, sqlHash);
        SqlBlockUtil.checkPropertiesValidate(sql, sqlHash, partitionNumString, tabletNumString, cardinalityString);

        this.partitionNum = Util.getLongPropertyOrDefault(partitionNumString, 0L, null,
                SCANNED_PARTITION_NUM + " should be a long");
        this.tabletNum = Util.getLongPropertyOrDefault(tabletNumString, 0L, null,
                SCANNED_TABLET_NUM + " should be a long");
        this.cardinality = Util.getLongPropertyOrDefault(cardinalityString, 0L, null,
                SCANNED_CARDINALITY + " should be a long");

        this.global = Util.getBooleanPropertyOrDefault(properties.get(GLOBAL_PROPERTY), false,
                GLOBAL_PROPERTY + " should be a boolean");
        this.enable = Util.getBooleanPropertyOrDefault(properties.get(ENABLE_PROPERTY), true,
                ENABLE_PROPERTY + " should be a boolean");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateSqlBlockRuleCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
