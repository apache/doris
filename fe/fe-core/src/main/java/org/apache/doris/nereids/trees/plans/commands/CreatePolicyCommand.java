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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.RowPolicy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
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

    private void validate(ConnectContext ctx) throws AnalysisException {
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
                tableNameInfo.analyze(ctx);
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
                TableIf tableIf = Env.getCurrentEnv().getCatalogMgr()
                        .getCatalogOrAnalysisException(tableNameInfo.getCtl())
                        .getDbOrAnalysisException(tableNameInfo.getDb())
                        .getTableOrAnalysisException(tableNameInfo.getTbl());
                Expression expression = wherePredicate.get();
                System.out.println(expression);
                System.out.println(tableIf);

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

    /**
     * translate to legacy expr, which do not need complex expression and table columns
     */
    private Expr translateToLegacyExpr(Expression expression, ConnectContext ctx) {
        LogicalEmptyRelation plan = new LogicalEmptyRelation(
                ConnectContext.get().getStatementContext().getNextRelationId(),
                new ArrayList<>());
        CascadesContext cascadesContext = CascadesContext.initContext(ctx.getStatementContext(), plan,
                PhysicalProperties.ANY);
        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext(cascadesContext);
        ExpressionToExpr translator = new ExpressionToExpr();
        return expression.accept(translator, planTranslatorContext);
    }

    private static class ExpressionToExpr extends ExpressionTranslator {
        @Override
        public Expr visitUnboundSlot(UnboundSlot unboundSlot, PlanTranslatorContext context) {
            String inputCol = unboundSlot.getName();
            return new SlotRef(null, inputCol);
        }
    }
}
