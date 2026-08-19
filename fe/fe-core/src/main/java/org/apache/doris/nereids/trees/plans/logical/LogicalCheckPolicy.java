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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Logical Check Policy
 */
public class LogicalCheckPolicy<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements PropagateFuncDeps {

    public LogicalCheckPolicy(CHILD_TYPE child) {
        super(PlanType.LOGICAL_CHECK_POLICY, child);
    }

    public LogicalCheckPolicy(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_CHECK_POLICY, groupExpression, logicalProperties, child);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCheckPolicy(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalCheckPolicy");
    }

    @Override
    public String toDigest() {
        return child().toDigest();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalCheckPolicy that = (LogicalCheckPolicy) o;
        return child().equals(that.child());
    }

    @Override
    public int hashCode() {
        return child().hashCode();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalCheckPolicy<>(groupExpression, Optional.of(getLogicalProperties()), child()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalCheckPolicy<>(groupExpression, logicalProperties, children.get(0)));
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalCheckPolicy<>(children.get(0)));
    }

    /**
     * find related policy for logicalPlan.
     *
     * @param logicalPlan include tableName and dbName
     * @param cascadesContext include information about user and policy
     */
    public RelatedPolicy findPolicy(LogicalPlan logicalPlan, CascadesContext cascadesContext) {
        if (!(logicalPlan instanceof CatalogRelation || logicalPlan instanceof LogicalView)) {
            return RelatedPolicy.NO_POLICY;
        }
        Optional<Map<TableIf, Set<Expression>>> mvRefreshPredicates = cascadesContext.getStatementContext()
                .getMvRefreshPredicates();
        if (mvRefreshPredicates.isPresent()) {
            return findPolicyByMvRefresh(mvRefreshPredicates.get(), logicalPlan);
        }
        ConnectContext connectContext = cascadesContext.getConnectContext();
        AccessControllerManager accessManager = connectContext.getEnv().getAccessManager();
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        // An exemption the engine keeps for itself rather than offering to the authorization source: the two
        // literal accounts root@'%' and admin@'%' - not everyone holding ADMIN_PRIV - are subject to no row
        // filter and no column mask, whichever source governs the table. It predates the plugin contract and
        // is documented as an engine-reserved exemption in fe-authorization/README.md, alongside the two the
        // manager applies; a source is never asked, so it cannot grant these accounts a policy of its own.
        if (currentUserIdentity.isRootUser() || currentUserIdentity.isAdminUser()) {
            return RelatedPolicy.NO_POLICY;
        }

        TableIf table = logicalPlan instanceof CatalogRelation ? ((CatalogRelation) logicalPlan).getTable()
                : ((LogicalView<?>) logicalPlan).getView();
        DatabaseIf database = table.getDatabase();
        if (database == null) {
            return RelatedPolicy.NO_POLICY;
        }
        CatalogIf catalog = database.getCatalog();
        if (catalog == null) {
            return RelatedPolicy.NO_POLICY;
        }
        String ctlName = catalog.getName();
        String dbName = database.getFullName();
        String tableName = table.getName();

        NereidsParser nereidsParser = new NereidsParser();
        ImmutableList.Builder<NamedExpression> dataMasks
                = ImmutableList.builderWithExpectedSize(logicalPlan.getOutput().size());

        StatementContext statementContext = cascadesContext.getStatementContext();
        Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
        boolean hasDataMask = false;
        // One question for the whole relation rather than one per column: that is what the contract offers
        // and why - a source answering over the network would otherwise be reached once per column.
        Set<String> outputColumns = new LinkedHashSet<>();
        for (Slot slot : logicalPlan.getOutput()) {
            outputColumns.add(slot.getName());
        }
        Map<String, DataMaskSpec> masksByColumn = accessManager.evalDataMaskPolicies(
                currentUserIdentity, ctlName, dbName, tableName, outputColumns);
        for (Slot slot : logicalPlan.getOutput()) {
            Optional<DataMaskSpec> dataMaskPolicy = Optional.ofNullable(
                    masksByColumn.get(slot.getName().toLowerCase(Locale.ROOT)));
            if (dataMaskPolicy.isPresent()) {
                Expression unboundExpr = parsePolicyExpression(nereidsParser, dataMaskPolicy.get().getMaskSql());
                Expression childOfAlias
                        = unboundExpr instanceof UnboundAlias ? unboundExpr.child(0) : unboundExpr;
                Alias alias = new Alias(
                        StatementScopeIdGenerator.newExprId(),
                        ImmutableList.of(childOfAlias),
                        slot.getName(), slot.getQualifier(), false
                );
                dataMasks.add(alias);
                hasDataMask = true;
            } else {
                dataMasks.add(slot);
            }
            if (sqlCacheContext.isPresent()) {
                sqlCacheContext.get().addDataMaskPolicy(ctlName, dbName, tableName, slot.getName(), dataMaskPolicy);
            }
        }

        List<RowFilterSpec> rowPolicies = accessManager.evalRowFilterPolicies(
                currentUserIdentity, ctlName, dbName, tableName);
        if (sqlCacheContext.isPresent()) {
            sqlCacheContext.get().setRowFilterPolicy(ctlName, dbName, tableName, rowPolicies);
        }

        return new RelatedPolicy(
                Optional.ofNullable(CollectionUtils.isEmpty(rowPolicies)
                        ? null : mergeRowPolicy(rowPolicies, nereidsParser)),
                hasDataMask ? Optional.of(dataMasks.build()) : Optional.empty()
        );
    }

    /**
     * Parses text a security policy is made of, under the mode such text is written in rather than the
     * caller's.
     *
     * <p>The caller here is the very user the policy restricts, and {@code sql_mode} is theirs to set with no
     * privilege at all. See {@link SqlModeHelper#MODE_FOR_POLICY_TEXT}.
     *
     * <p>The result is not cached across statements, and cannot be here: what the planner holds is the SQL
     * text a source handed over, not the source's own object, so a cache would have to be keyed by that text
     * and would outlive the policy it came from. A predicate containing a subquery could not be shared
     * anyway - the {@code RelationId} and {@code ExprId} inside come from the statement's own generator. A
     * built-in row policy therefore pays one parse per governed relation per query, where before this
     * contract it paid none; a source reached over the network was always going to hand over text.
     */
    private static Expression parsePolicyExpression(NereidsParser parser, String sql) {
        return SqlModeHelper.withSqlMode(SqlModeHelper.MODE_FOR_POLICY_TEXT, () -> parser.parseExpression(sql));
    }

    private RelatedPolicy findPolicyByMvRefresh(Map<TableIf, Set<Expression>> mvRefreshPredicates,
            LogicalPlan logicalPlan) {
        TableIf table = logicalPlan instanceof CatalogRelation ? ((CatalogRelation) logicalPlan).getTable()
                : ((LogicalView<?>) logicalPlan).getView();
        if (mvRefreshPredicates.containsKey(table)) {
            return new RelatedPolicy(Optional.of(ExpressionUtils.or(mvRefreshPredicates.get(table))), Optional.empty());
        }
        return RelatedPolicy.NO_POLICY;
    }

    private Expression mergeRowPolicy(List<RowFilterSpec> policies, NereidsParser nereidsParser) {
        List<Expression> orList = new ArrayList<>();
        List<Expression> andList = new ArrayList<>();
        for (RowFilterSpec policy : policies) {
            // The authorization source hands us the predicate as SQL text - the form both a Ranger policy and
            // a CREATE ROW POLICY statement natively have - and parsing it is the engine's job.
            Expression wherePredicate = parsePolicyExpression(nereidsParser, policy.getFilterSql());
            refuseUnlessRestricting(policy, wherePredicate);
            switch (policy.getMergeType()) {
                case PERMISSIVE:
                    orList.add(wherePredicate);
                    break;
                case RESTRICTIVE:
                    andList.add(wherePredicate);
                    break;
                default:
                    throw new IllegalStateException("Invalid operator");
            }
        }
        if (!andList.isEmpty() && !orList.isEmpty()) {
            return new And(ExpressionUtils.and(andList), ExpressionUtils.or(orList));
        } else if (andList.isEmpty()) {
            return ExpressionUtils.or(orList);
        } else if (orList.isEmpty()) {
            return ExpressionUtils.and(andList);
        } else {
            return null;
        }
    }

    /**
     * Refuses a row filter payload that is not a predicate, naming the policy it came from.
     *
     * <p>Without this the payload reaches the ordinary filter path, where
     * {@code TypeCoercionUtils.castIfNotSameType(conjunct, BooleanType)} coerces whatever it is: a payload of
     * {@code 1} becomes {@code cast(1 as boolean)}, a filter that admits every row. A row filter that restricts
     * nothing is the one way a data policy must not fail, and a source outside this repository - reached over
     * the network, writing this text in a UI of its own - is where such a payload comes from.
     *
     * <p>Only checked where the type is known before binding, which is the case that matters: a comparison, a
     * conjunction or a literal reports its type with its children still unbound, while a payload naming a
     * column or a function does not report one until it is bound, and the coercion above is what it meets.
     */
    private static void refuseUnlessRestricting(RowFilterSpec policy, Expression parsed) {
        DataType notAPredicate = nonPredicateTypeOf(parsed);
        if (notAPredicate != null) {
            throw new AnalysisException("row filter policy " + policy.getPolicyIdent() + " is not a predicate:"
                    + " [" + policy.getFilterSql() + "] is of type " + notAPredicate + ", and a row filter has"
                    + " to be a boolean expression in Doris dialect over the table's columns");
        }
    }

    /**
     * The type that proves {@code parsed} is not a predicate, or null when nothing about it proves that yet.
     *
     * <p>Public because {@code CREATE ROW POLICY} asks the same question of the predicate it is about to
     * store. Answering it there as well is what turns "every query this policy governs fails from now on"
     * into "this statement is refused", and the account that can fix it is the one typing the statement.
     *
     * <p>A {@code CASE} is asked branch by branch rather than as a whole: {@link CaseWhen#getDataType()}
     * reports the type of the first branch alone, so asking it directly refuses
     * {@code CASE WHEN c THEN NULL ELSE k1 = 1 END} and accepts the same filter with its branches swapped. A
     * branch of no type of its own - a bare {@code NULL} - decides nothing there and is passed over.
     */
    public static DataType nonPredicateTypeOf(Expression parsed) {
        try {
            if (parsed instanceof CaseWhen) {
                CaseWhen caseWhen = (CaseWhen) parsed;
                for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                    DataType branch = whenClause.getDataType();
                    if (!branch.isNullType() && !branch.isBooleanType()) {
                        return branch;
                    }
                }
                if (caseWhen.getDefaultValue().isPresent()) {
                    DataType otherwise = caseWhen.getDefaultValue().get().getDataType();
                    if (!otherwise.isNullType() && !otherwise.isBooleanType()) {
                        return otherwise;
                    }
                }
                return null;
            }
            DataType type = parsed.getDataType();
            return type.isBooleanType() ? null : type;
        } catch (Exception e) {
            // Not knowable yet - a payload naming columns or functions, resolved when the filter is bound.
            return null;
        }
    }

    /**
     * RelatedPolicy
     */
    public static class RelatedPolicy {
        public static final RelatedPolicy NO_POLICY = new RelatedPolicy(Optional.empty(), Optional.empty());

        public final Optional<Expression> rowPolicyFilter;
        public final Optional<List<NamedExpression>> dataMaskProjects;

        public RelatedPolicy(Optional<Expression> rowPolicyFilter, Optional<List<NamedExpression>> dataMaskProjects) {
            this.rowPolicyFilter = rowPolicyFilter;
            this.dataMaskProjects = dataMaskProjects;
        }
    }
}
