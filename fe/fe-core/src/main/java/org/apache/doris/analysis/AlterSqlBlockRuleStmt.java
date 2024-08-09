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
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.SqlBlockUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class AlterSqlBlockRuleStmt extends DdlStmt {

    public static final Long LONG_NOT_SET = SqlBlockUtil.LONG_MINUS_ONE;

    private final String ruleName;

    private String sql;

    private String sqlHash;

    private Long partitionNum;

    private Long tabletNum;

    private Long cardinality;

    private Boolean global;

    private Boolean enable;

    private final Map<String, String> properties;

    public AlterSqlBlockRuleStmt(String ruleName, Map<String, String> properties) {
        this.ruleName = ruleName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check properties
        CreateSqlBlockRuleStmt.checkCommonProperties(properties);
        setProperties(properties);
    }

    private void setProperties(Map<String, String> properties) throws AnalysisException {
        this.sql = properties.getOrDefault(CreateSqlBlockRuleStmt.SQL_PROPERTY, CreateSqlBlockRuleStmt.STRING_NOT_SET);
        this.sqlHash = properties.getOrDefault(CreateSqlBlockRuleStmt.SQL_HASH_PROPERTY,
                CreateSqlBlockRuleStmt.STRING_NOT_SET);
        String partitionNumString = properties.get(CreateSqlBlockRuleStmt.SCANNED_PARTITION_NUM);
        String tabletNumString = properties.get(CreateSqlBlockRuleStmt.SCANNED_TABLET_NUM);
        String cardinalityString = properties.get(CreateSqlBlockRuleStmt.SCANNED_CARDINALITY);

        SqlBlockUtil.checkSqlAndSqlHashSetBoth(sql, sqlHash);
        SqlBlockUtil.checkSqlAndLimitationsSetBoth(sql, sqlHash,
                partitionNumString, tabletNumString, cardinalityString);
        this.partitionNum = Util.getLongPropertyOrDefault(partitionNumString, LONG_NOT_SET, null,
                CreateSqlBlockRuleStmt.SCANNED_PARTITION_NUM + " should be a long");
        this.tabletNum = Util.getLongPropertyOrDefault(tabletNumString, LONG_NOT_SET, null,
                CreateSqlBlockRuleStmt.SCANNED_TABLET_NUM + " should be a long");
        this.cardinality = Util.getLongPropertyOrDefault(cardinalityString, LONG_NOT_SET, null,
                CreateSqlBlockRuleStmt.SCANNED_CARDINALITY + " should be a long");
        // allow null, represents no modification
        String globalStr = properties.get(CreateSqlBlockRuleStmt.GLOBAL_PROPERTY);
        this.global = StringUtils.isNotEmpty(globalStr) ? Boolean.parseBoolean(globalStr) : null;
        String enableStr = properties.get(CreateSqlBlockRuleStmt.ENABLE_PROPERTY);
        this.enable = StringUtils.isNotEmpty(enableStr) ? Boolean.parseBoolean(enableStr) : null;
    }

    public String getRuleName() {
        return ruleName;
    }

    public String getSql() {
        return sql;
    }

    public Long getPartitionNum() {
        return partitionNum;
    }

    public Long getTabletNum() {
        return tabletNum;
    }

    public Long getCardinality() {
        return cardinality;
    }

    public Boolean getGlobal() {
        return global;
    }

    public Boolean getEnable() {
        return enable;
    }

    public String getSqlHash() {
        return sqlHash;
    }

    @Override
    public String toSql() {
        // ALTER SQL_BLOCK_RULE test_rule PROPERTIES("sql"="select \\* from test_table","enable"="true")
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER SQL_BLOCK_RULE ")
                .append(ruleName)
                .append(" \nPROPERTIES(\n")
                .append(new PrintableMap<>(properties, " = ", true, true, true))
                .append(")");
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}
