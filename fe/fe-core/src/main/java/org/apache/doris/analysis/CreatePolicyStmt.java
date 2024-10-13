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
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;

import lombok.Getter;

import java.util.Map;

/**
 * Create policy statement.
 * syntax:
 * CREATE ROW POLICY [IF NOT EXISTS] test_row_policy ON test_table AS {PERMISSIVE|RESTRICTIVE} TO user USING (a = ’xxx‘)
 */
public class CreatePolicyStmt extends DdlStmt implements NotFallbackInParser {

    @Getter
    private final PolicyTypeEnum type;

    @Getter
    private final boolean ifNotExists;

    @Getter
    private final String policyName;

    @Getter
    private TableName tableName = null;

    @Getter
    private ColumnName colName = null;

    @Getter
    private FilterType filterType = null;

    @Getter
    private UserIdentity user = null;

    @Getter
    private String roleName = null;

    @Getter
    private Expr wherePredicate;

    @Getter
    private Map<String, String> properties;

    @Getter
    private String dataMaskType;

    /**
     * Use for cup.
     **/
    public CreatePolicyStmt(PolicyTypeEnum type, boolean ifNotExists, String policyName, TableName tableName,
            String filterType, UserIdentity user, String roleName, Expr wherePredicate) {
        this.type = type;
        this.ifNotExists = ifNotExists;
        this.policyName = policyName;
        this.tableName = tableName;
        this.filterType = FilterType.of(filterType);
        this.user = user;
        this.roleName = roleName;
        this.wherePredicate = wherePredicate;
    }

    /**
     * Use for cup.
     */
    public CreatePolicyStmt(PolicyTypeEnum type, boolean ifNotExists, String policyName,
            Map<String, String> properties) {
        this.type = type;
        this.ifNotExists = ifNotExists;
        this.policyName = policyName;
        this.properties = properties;
    }

    /**
     * Use for cup.
     **/
    public CreatePolicyStmt(PolicyTypeEnum type, boolean ifNotExists, String policyName, ColumnName colName,
                            UserIdentity user, String roleName, String dataMaskType) {
        this.type = type;
        this.ifNotExists = ifNotExists;
        this.policyName = policyName;
        this.colName = colName;
        this.user = user;
        this.roleName = roleName;
        this.dataMaskType = dataMaskType;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        switch (type) {
            case STORAGE:
                if (!Config.enable_storage_policy) {
                    throw new UserException("storage policy feature is disabled by default. "
                            + "Enable it by setting 'enable_storage_policy=true' in fe.conf");
                }
                // check auth
                // check if can create policy and use storage_resource
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            PrivPredicate.ADMIN.getPrivs().toString());
                }
                break;
            case ROW:
            case DATA_MASK:
            default:
                String tbl = "";
                if (tableName != null) {
                    tableName.analyze(analyzer);
                    tbl = tableName.getTbl();
                }
                if (colName != null) {
                    colName.analyze(analyzer);
                    tbl = colName.getTbl();
                }
                if (user != null) {
                    user.analyze();
                    if (user.isRootUser() || user.isAdminUser()) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "CreatePolicyStmt",
                                user.getQualifiedUser(), user.getHost(), tbl);
                    }
                }
                // check auth
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            PrivPredicate.GRANT.getPrivs().toString());
                }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        String typeName = type.name();
        if (type == PolicyTypeEnum.DATA_MASK) {
            typeName = "DATA MASK";
        }
        sb.append("CREATE ").append(typeName).append(" POLICY ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS");
        }
        sb.append(policyName);
        switch (type) {
            case STORAGE:
                sb.append(" PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false)).append(")");
                break;
            case ROW:
                sb.append(" ON ").append(tableName.toSql()).append(" AS ").append(filterType)
                        .append(" TO ");
                if (user == null) {
                    sb.append("ROLE ").append(roleName);
                } else {
                    sb.append(user.getQualifiedUser());
                }
                sb.append(" USING ").append(wherePredicate.toSql());
                break;
            case DATA_MASK:
                sb.append(" ON ").append(colName.toSql()).append(" TO ");
                if (user == null) {
                    sb.append("ROLE ").append(roleName);
                } else {
                    sb.append(user.getQualifiedUser());
                }
                sb.append(" USING ").append(dataMaskType);
                break;
            default:
                throw new RuntimeException("no support type : " + type);
        }
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
