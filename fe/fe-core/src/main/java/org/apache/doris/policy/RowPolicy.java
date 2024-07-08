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

package org.apache.doris.policy;

import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Save policy for filtering data.
 **/
@Data
public class RowPolicy extends Policy implements RowFilterPolicy {

    public static final ShowResultSetMetaData ROW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("PolicyName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("CatalogName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("DbName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("TableName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("FilterType", ScalarType.createVarchar(20)))
                    .addColumn(new Column("WherePredicate", ScalarType.createVarchar(65535)))
                    .addColumn(new Column("User", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Role", ScalarType.createVarchar(20)))
                    .addColumn(new Column("OriginStmt", ScalarType.createVarchar(65535)))
                    .build();

    private static final Logger LOG = LogManager.getLogger(RowPolicy.class);

    /**
     * Policy bind user.
     **/
    @SerializedName(value = "user")
    private UserIdentity user = null;

    @SerializedName(value = "roleName")
    private String roleName = null;

    @SerializedName(value = "dbId")
    @Deprecated
    private long dbId = -1;

    @SerializedName(value = "tableId")
    @Deprecated
    private long tableId = -1;

    @SerializedName(value = "ctlName")
    private String ctlName;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableName")
    private String tableName;

    /**
     * PERMISSIVE | RESTRICTIVE, If multiple types exist, the last type prevails.
     **/
    @SerializedName(value = "filterType")
    private FilterType filterType = null;

    /**
     * Use for Serialization/deserialization.
     **/
    @SerializedName(value = "originStmt")
    private String originStmt;
    @SerializedName(value = "stmtIdx")
    private int stmtIdx;

    private Expr wherePredicate = null;

    public RowPolicy() {
        super(PolicyTypeEnum.ROW);
    }

    /**
     * Policy for Table. Policy of ROW or others.
     *
     * @param policyId policy id
     * @param policyName policy name
     * @param dbId database i
     * @param user username
     * @param roleName roleName
     * @param originStmt origin stmt
     * @param tableId table id
     * @param filterType filter type
     * @param wherePredicate where predicate
     */
    public RowPolicy(long policyId, final String policyName, long dbId, UserIdentity user, String roleName,
            String originStmt, int stmtIdx,
            final long tableId, final FilterType filterType, final Expr wherePredicate) {
        super(policyId, PolicyTypeEnum.ROW, policyName);
        this.user = user;
        this.roleName = roleName;
        this.dbId = dbId;
        this.tableId = tableId;
        this.filterType = filterType;
        this.originStmt = originStmt;
        this.stmtIdx = stmtIdx;
        this.wherePredicate = wherePredicate;
    }

    public RowPolicy(long policyId, final String policyName, String ctlName, String dbName, String tableName,
            UserIdentity user, String roleName,
            String originStmt, int stmtIdx, final FilterType filterType, final Expr wherePredicate) {
        super(policyId, PolicyTypeEnum.ROW, policyName);
        this.user = user;
        this.roleName = roleName;
        this.ctlName = ctlName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.filterType = filterType;
        this.originStmt = originStmt;
        this.stmtIdx = stmtIdx;
        this.wherePredicate = wherePredicate;
    }

    /**
     * Use for SHOW POLICY.
     **/
    public List<String> getShowInfo() throws AnalysisException {
        return Lists.newArrayList(this.policyName, ctlName, dbName, tableName, this.type.name(),
                this.filterType.name(), this.wherePredicate.toSql(),
                this.user == null ? null : this.user.getQualifiedUser(), this.roleName, this.originStmt);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (wherePredicate != null) {
            return;
        }
        try {
            SqlScanner input = new SqlScanner(new StringReader(originStmt), 0L);
            SqlParser parser = new SqlParser(input);
            CreatePolicyStmt stmt = (CreatePolicyStmt) SqlParserUtils.getStmt(parser, stmtIdx);
            wherePredicate = stmt.getWherePredicate();
        } catch (Exception e) {
            String errorMsg = String.format("table policy parse originStmt error, originStmt: %s, stmtIdx: %s.",
                    originStmt, stmtIdx);
            // Only print logs to avoid cluster failure to start
            LOG.warn(errorMsg, e);
        }
    }

    @Override
    public RowPolicy clone() {
        return new RowPolicy(this.id, this.policyName, this.dbId, this.user, this.roleName, this.originStmt,
                this.stmtIdx,
                this.tableId,
                this.filterType, this.wherePredicate);
    }

    private boolean checkMatched(String ctlName, String dbName, String tableName, PolicyTypeEnum type,
            String policyName, UserIdentity user, String roleName) {
        return super.checkMatched(type, policyName)
                && (StringUtils.isEmpty(ctlName) || StringUtils.equals(ctlName, this.ctlName))
                && (StringUtils.isEmpty(dbName) || StringUtils.equals(dbName, this.dbName))
                && (StringUtils.isEmpty(tableName) || StringUtils.equals(tableName, this.tableName))
                && (StringUtils.isEmpty(roleName) || StringUtils.equals(roleName, this.roleName))
                && (user == null || Objects.equals(user, this.user));
    }

    @Override
    public boolean matchPolicy(Policy checkedPolicyCondition) {
        if (!(checkedPolicyCondition instanceof RowPolicy)) {
            return false;
        }
        RowPolicy rowPolicy = (RowPolicy) checkedPolicyCondition;
        return checkMatched(rowPolicy.getCtlName(), rowPolicy.getDbName(), rowPolicy.getTableName(),
                rowPolicy.getType(),
                rowPolicy.getPolicyName(), rowPolicy.getUser(), rowPolicy.getRoleName());
    }

    @Override
    public boolean matchPolicy(DropPolicyLog checkedDropPolicyLogCondition) {
        return checkMatched(checkedDropPolicyLogCondition.getCtlName(), checkedDropPolicyLogCondition.getDbName(),
                checkedDropPolicyLogCondition.getTableName(),
                checkedDropPolicyLogCondition.getType(), checkedDropPolicyLogCondition.getPolicyName(),
                checkedDropPolicyLogCondition.getUser(), checkedDropPolicyLogCondition.getRoleName());
    }

    @Override
    public boolean isInvalid() {
        return (wherePredicate == null);
    }

    @Override
    public Expression getFilterExpression() throws AnalysisException {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = getOriginStmt();
        if (getStmtIdx() != 0) {
            // Under normal circumstances, the index will only be equal to 0
            throw new AnalysisException("Invalid row policy [" + getPolicyIdent() + "], " + sql);
        }
        CreatePolicyCommand command = (CreatePolicyCommand) nereidsParser.parseSingle(sql);
        Optional<Expression> wherePredicate = command.getWherePredicate();
        if (!wherePredicate.isPresent()) {
            throw new AnalysisException("Invalid row policy [" + getPolicyIdent() + "], " + sql);
        }
        return wherePredicate.get();
    }

    @Override
    public String getPolicyIdent() {
        return getPolicyName();
    }

}
