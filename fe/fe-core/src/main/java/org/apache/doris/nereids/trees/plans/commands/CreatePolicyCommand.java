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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.RowPolicy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Create policy command use for row policy and storage policy.
 */
public class CreatePolicyCommand extends Command implements ForwardWithSync {

    private final PolicyTypeEnum policyType;
    private final String policyName;
    private final boolean ifNotExists;
    private final TableNameInfo tableNameInfo;
    private final Optional<FilterType> filterType;
    private final UserIdentity user;
    private final String roleName;
    private final Optional<Expression> wherePredicate;
    private final Map<String, String> properties;

    /**
     * ctor of this command.
     */
    public CreatePolicyCommand(PolicyTypeEnum policyType, String policyName, boolean ifNotExists,
            TableNameInfo tableNameInfo, Optional<FilterType> filterType, UserIdentity user, String roleName,
            Optional<Expression> wherePredicate, Map<String, String> properties) {
        super(PlanType.CREATE_POLICY_COMMAND);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifNotExists = ifNotExists;
        this.tableNameInfo = tableNameInfo;
        this.filterType = filterType;
        this.user = user;
        this.roleName = roleName;
        this.wherePredicate = wherePredicate;
        this.properties = properties;
    }

    public Optional<Expression> getWherePredicate() {
        return wherePredicate;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreatePolicyCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Policy policy = createPolicy(ctx, executor);
        Env.getCurrentEnv().getPolicyMgr().createPolicy(policy, ifNotExists);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    public void validate(ConnectContext ctx) throws AnalysisException {
        switch (policyType) {
            case STORAGE:
                if (!Config.enable_storage_policy) {
                    throw new AnalysisException("storage policy feature is disabled by default. "
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
            default:
                // check auth
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            PrivPredicate.GRANT.getPrivs().toString());
                }
                tableNameInfo.analyze(ctx.getNameSpaceContext());
                if (user != null) {
                    user.analyze();
                    if (user.isRootUser() || user.isAdminUser()) {
                        throw new AnalysisException("not allow add row policy for system user");
                    }
                    if (!Env.getCurrentEnv().getAuth().doesUserExist(user)) {
                        throw new AnalysisException("user not exist: " + user);
                    }
                }

                if (!StringUtils.isEmpty(roleName)) {
                    if (!Env.getCurrentEnv().getAuth().doesRoleExist(roleName)) {
                        throw new AnalysisException("role not exist: " + roleName);
                    }
                }
                if (!wherePredicate.isPresent()) {
                    throw new AnalysisException("wherePredicate can not be null");
                }
                TableIf tableIf = Env.getCurrentEnv().getCatalogMgr()
                        .getCatalogOrAnalysisException(tableNameInfo.getCtl())
                        .getDbOrAnalysisException(tableNameInfo.getDb())
                        .getTableOrAnalysisException(tableNameInfo.getTbl());
                try {
                    wherePredicate.get().foreach(expr -> {
                        if (expr instanceof UnboundSlot) {
                            UnboundSlot slot = (UnboundSlot) expr;
                            if (tableIf.getColumn(slot.getName()) == null) {
                                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                        "column not exist: " + slot.getName());
                            }
                        } else if (expr instanceof SubqueryExpr) {
                            // Exists/InSubquery/ScalarSubquery are leaf expressions: their subquery
                            // plan isn't a child in the expression tree, so the foreach above never
                            // looks inside it. A subquery that reaches back into tableIf (the row
                            // policy's own table) is a correlated subquery, and this command has no
                            // way to keep that reference resolvable once the policy is stored and
                            // re-parsed at query time, so reject it here instead of silently
                            // dropping the policy later.
                            rejectCorrelatedSubquery((SubqueryExpr) expr, tableIf);
                        }
                    });
                } catch (org.apache.doris.nereids.exceptions.AnalysisException e) {
                    // foreach's Consumer can't throw the checked common.AnalysisException this
                    // method declares, so the walk above throws the unchecked Nereids one and we
                    // convert it here. Otherwise callers (StmtExecutor included) see this routine
                    // rejection as an unexpected RuntimeException instead of a normal user error.
                    throw new AnalysisException(e.getMessage());
                }

        }
    }

    // ponytail: name-based, not a real bind. Matching is scope-blind: it can't tell a qualifier
    // that's genuinely the outer table from one that merely reuses its name for something else,
    // and an unqualified column is treated as outer whenever the outer table happens to have a
    // same-named column, even if the subquery would actually resolve it locally first (e.g. a
    // self-join on the policy's own table). That's the deliberate trade-off of Option A from
    // apache/doris#62729: false rejections are safe here, silently dropping a policy is not.
    // Upgrade to real correlation detection (Option B) if this starts blocking legitimate policies.
    private static void rejectCorrelatedSubquery(SubqueryExpr subquery, TableIf outerTable) {
        walkPlanExpressions(subquery.getQueryPlan(), expr -> checkNoOuterReference(expr, outerTable));
    }

    // Plan.foreach() only recurses children(); a CTE's WITH-list lives in extraPlans() instead,
    // so a correlated reference inside `WITH x AS (...)` would otherwise never be visited.
    private static void walkPlanExpressions(Plan plan, java.util.function.Consumer<Expression> visitor) {
        for (Expression expr : plan.getExpressions()) {
            visitor.accept(expr);
        }
        for (Plan child : plan.children()) {
            walkPlanExpressions(child, visitor);
        }
        for (Plan extra : plan.extraPlans()) {
            walkPlanExpressions(extra, visitor);
        }
    }

    private static void checkNoOuterReference(Expression expr, TableIf outerTable) {
        if (expr instanceof UnboundSlot) {
            List<String> nameParts = ((UnboundSlot) expr).getNameParts();
            if (nameParts.size() > 1) {
                // Qualified reference, e.g. "main_table.ref_id" or a nested field
                // "main_table.payload.ref_id": correlated iff one of the qualifier parts
                // (everything but the last) names the outer table. Checking the qualifier
                // instead of the bare column name is what keeps a same-named but
                // differently-qualified local column (e.g. "l.ref_id") from being flagged.
                boolean referencesOuterTable = nameParts.subList(0, nameParts.size() - 1).stream()
                        .anyMatch(part -> part.equalsIgnoreCase(outerTable.getName()));
                if (referencesOuterTable) {
                    throw new org.apache.doris.nereids.exceptions.AnalysisException(
                            "Correlated subquery expressions in the USING clause are not supported for row "
                                    + "policies: found reference to outer table '" + outerTable.getName() + "'");
                }
            } else {
                String columnName = nameParts.get(0);
                if (outerTable.getColumn(columnName) != null) {
                    throw new org.apache.doris.nereids.exceptions.AnalysisException(
                            "Correlated subquery expressions in the USING clause are not supported for row "
                                    + "policies: found reference to outer column '" + columnName + "'");
                }
            }
        }
        if (expr instanceof UnboundStar) {
            // A leaf like UnboundSlot: "* REPLACE(main_table.ref_id AS ref_id)" and
            // "* EXCEPT(main_table.ref_id)" carry expressions that aren't tree children either.
            UnboundStar star = (UnboundStar) expr;
            star.getExceptedSlots().forEach(e -> checkNoOuterReference(e, outerTable));
            star.getReplacedAlias().forEach(e -> checkNoOuterReference(e, outerTable));
        }
        if (expr instanceof SubqueryExpr) {
            rejectCorrelatedSubquery((SubqueryExpr) expr, outerTable);
        }
        for (Expression child : expr.children()) {
            checkNoOuterReference(child, outerTable);
        }
    }

    private Policy createPolicy(ConnectContext ctx, StmtExecutor executor) throws AnalysisException {
        long policyId = Env.getCurrentEnv().getNextId();
        switch (policyType) {
            case STORAGE:
                StoragePolicy storagePolicy = new StoragePolicy(policyId, policyName);
                storagePolicy.init(properties, ifNotExists);
                return storagePolicy;
            case ROW:
                return new RowPolicy(policyId, policyName, tableNameInfo.getCtl(),
                        tableNameInfo.getDb(), tableNameInfo.getTbl(), user, roleName,
                        executor.getOriginStmt().originStmt, executor.getOriginStmt().idx, filterType.get(),
                        wherePredicate.get());
            default:
                throw new AnalysisException("Unknown policy type: " + policyType);
        }
    }

    private static class ExpressionToExpr extends ExpressionTranslator {
        @Override
        public Expr visitUnboundSlot(UnboundSlot unboundSlot, PlanTranslatorContext context) {
            String inputCol = unboundSlot.getName();
            return new SlotRef(null, inputCol);
        }
    }
}
