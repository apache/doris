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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;

import lombok.Getter;

/**
 * Create policy statement.
 * syntax:
 * CREATE ROW POLICY [IF NOT EXISTS] test_row_policy ON test_table AS {PERMISSIVE|RESTRICTIVE} TO user USING (a = ’xxx‘)
 */
public class CreatePolicyStmt extends DdlStmt {

    @Getter
    private final PolicyTypeEnum type;

    @Getter
    private final boolean ifNotExists;

    @Getter
    private final String policyName;

    @Getter
    private final TableName tableName;

    @Getter
    private final FilterType filterType;

    @Getter
    private final UserIdentity user;

    @Getter
    private Expr wherePredicate;

    /**
     * Use for cup.
     **/
    public CreatePolicyStmt(PolicyTypeEnum type, boolean ifNotExists, String policyName, TableName tableName,
                            String filterType, UserIdentity user, Expr wherePredicate) {
        this.type = type;
        this.ifNotExists = ifNotExists;
        this.policyName = policyName;
        this.tableName = tableName;
        this.filterType = FilterType.of(filterType);
        this.user = user;
        this.wherePredicate = wherePredicate;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        tableName.analyze(analyzer);
        user.analyze(analyzer.getClusterName());
        if (user.isRootUser() || user.isAdminUser()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "CreatePolicyStmt",
                    user.getQualifiedUser(), user.getHost(), tableName.getTbl());
        }
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ").append(type).append(" POLICY ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS");
        }
        sb.append(policyName).append(" ON ").append(tableName.toSql()).append(" AS ").append(filterType)
                .append(" TO ").append(user.getQualifiedUser()).append(" USING ").append(wherePredicate.toSql());
        return sb.toString();
    }
}
