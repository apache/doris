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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import org.apache.commons.lang3.StringUtils;

/*
 Create sqlBlockRule statement

 syntax:
      show sql_block_rule
      show sql_block_rule for rule_name
*/
public class ShowSqlBlockRuleStmt extends ShowStmt implements NotFallbackInParser {

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

    private String ruleName; // optional

    public ShowSqlBlockRuleStmt(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getRuleName() {
        return ruleName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW SQL_BLOCK_RULE");
        if (StringUtils.isNotEmpty(ruleName)) {
            sb.append(" FOR ").append(ruleName);
        }
        return sb.toString();
    }
}
