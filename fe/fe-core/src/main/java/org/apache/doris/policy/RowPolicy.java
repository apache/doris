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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Save policy for filtering data.
 **/
@Data
public class RowPolicy extends Policy {

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

    private Expression wherePredicate = null;

    // The predicate as the administrator wrote it. Known without any parsing on the path that creates a
    // policy - CREATE ROW POLICY already extracted it - and recovered from originStmt otherwise; never
    // persisted, see getFilterSql(). Excluded from equality and toString because it is a lazily filled cache
    // on the recovery path - two policies created from the same statement must not compare differently
    // depending on whether a query has already asked for the predicate text.
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private volatile String wherePredicateSql = null;

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
            final long tableId, final FilterType filterType, final Expression wherePredicate) {
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
            String originStmt, int stmtIdx, final FilterType filterType, final Expression wherePredicate) {
        this(policyId, policyName, ctlName, dbName, tableName, user, roleName, originStmt, stmtIdx, filterType,
                wherePredicate, null);
    }

    /**
     * As above, with the predicate text the statement was parsed from already known.
     *
     * <p>Passing it is how {@code CREATE ROW POLICY} avoids ever recovering it: the parser had it and threw
     * it away, and recovering it means re-parsing the whole statement text - which is not always the same
     * statement, since a policy created inside a multi-statement request is one of several in
     * {@code originStmt}.
     */
    public RowPolicy(long policyId, final String policyName, String ctlName, String dbName, String tableName,
            UserIdentity user, String roleName,
            String originStmt, int stmtIdx, final FilterType filterType, final Expression wherePredicate,
            String wherePredicateSql) {
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
        this.wherePredicateSql = StringUtils.isEmpty(wherePredicateSql) ? null : wherePredicateSql;
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
        if (this.wherePredicate != null) {
            return;
        }
        try {
            CreatePolicyCommand command = parseCreateStatement();
            Optional<Expression> wherePredicate = command.getWherePredicate();
            if (!wherePredicate.isPresent()) {
                LOG.warn("Invalid row policy [" + getPolicyIdent() + "], " + getOriginStmt());
                return;
            }
            this.wherePredicate = wherePredicate.get();
            if (!StringUtils.isEmpty(command.getWherePredicateSql())) {
                this.wherePredicateSql = command.getWherePredicateSql();
            }
        } catch (Exception e) {
            String errorMsg = String.format("table policy parse originStmt error, originStmt: %s, stmtIdx: %s.",
                    originStmt, stmtIdx);
            // Only print logs to avoid cluster failure to start
            LOG.warn(errorMsg, e);
        }
    }

    /**
     * The {@code CREATE ROW POLICY} this policy was made by, out of the statement text stored with it.
     *
     * <p>{@code originStmt} holds the whole request, which is not necessarily one statement: a request may
     * carry several separated by semicolons, and {@code stmtIdx} says which one this policy is. Parsing the
     * text as a single statement therefore recovers the wrong policy, or nothing at all.
     */
    private CreatePolicyCommand parseCreateStatement() throws AnalysisException {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = getOriginStmt();
        // Under the mode a security policy's text is read under rather than the caller's: getFilterSql() can
        // reach here on the thread of the very user this policy restricts, and sql_mode is theirs to set with
        // no privilege at all. See SqlModeHelper#MODE_FOR_POLICY_TEXT.
        if (stmtIdx <= 0) {
            return SqlModeHelper.withSqlMode(SqlModeHelper.MODE_FOR_POLICY_TEXT,
                    () -> (CreatePolicyCommand) nereidsParser.parseSingle(sql));
        }
        List<Pair<LogicalPlan, StatementContext>> statements = SqlModeHelper.withSqlMode(
                SqlModeHelper.MODE_FOR_POLICY_TEXT, () -> nereidsParser.parseMultiple(sql));
        if (stmtIdx >= statements.size()) {
            throw new AnalysisException("Invalid row policy [" + getPolicyIdent() + "]: statement " + stmtIdx
                    + " of " + statements.size() + " in " + sql);
        }
        return (CreatePolicyCommand) statements.get(stmtIdx).first;
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

    /**
     * The predicate as SQL text, which is the form the authorization layer hands to the planner.
     *
     * <p>It is the text the administrator wrote, recovered from the stored statement - not a rendering of
     * the parsed predicate. Rendering would not survive the round trip: {@code toSql()} on a compound
     * predicate produces the diagnostic form {@code AND[a,b]}, which does not parse back, so any policy
     * combining two conditions would break.</p>
     */
    public String getFilterSql() throws AnalysisException {
        if (wherePredicate == null) {
            throw new AnalysisException("Invalid row policy [" + getPolicyIdent() + "], " + getOriginStmt());
        }
        if (wherePredicateSql == null) {
            wherePredicateSql = parseWherePredicateSql();
        }
        return wherePredicateSql;
    }

    private String parseWherePredicateSql() throws AnalysisException {
        try {
            CreatePolicyCommand command = parseCreateStatement();
            if (!StringUtils.isEmpty(command.getWherePredicateSql())) {
                return command.getWherePredicateSql();
            }
        } catch (Exception e) {
            LOG.warn("failed to recover the predicate text of row policy [{}]", getPolicyIdent(), e);
        }
        // The statement parsed once already (that is where wherePredicate came from), so reaching here means
        // the stored statement no longer matches what the parser produces. Refuse the query rather than let
        // the table be read unfiltered.
        throw new AnalysisException("Invalid row policy [" + getPolicyIdent() + "], " + getOriginStmt());
    }

    public String getPolicyIdent() {
        return getPolicyName();
    }

}
