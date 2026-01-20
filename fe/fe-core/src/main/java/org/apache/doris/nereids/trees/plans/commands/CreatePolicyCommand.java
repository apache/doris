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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.DorisDataMaskPolicy;
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

    private PolicyTypeEnum policyType;
    private String policyName;
    private boolean ifNotExists;
    private TableNameInfo tableNameInfo;
    private Optional<FilterType> filterType;
    private UserIdentity user;
    private String roleName;
    private Optional<Expression> wherePredicate;
    private Map<String, String> properties;
    private String dataMaskType;
    private int priority = 0;

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

    /**
     * ctor for create data mask policy command.
     */
    public CreatePolicyCommand(PolicyTypeEnum policyType, String policyName, boolean ifNotExists,
                               TableNameInfo tableNameInfo, UserIdentity user, String roleName, String dataMaskType,
                               int priority) {
        super(PlanType.CREATE_POLICY_COMMAND);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifNotExists = ifNotExists;
        this.tableNameInfo = tableNameInfo;
        this.user = user;
        this.roleName = roleName;
        this.dataMaskType = dataMaskType;
        this.priority = priority;
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
            case DATA_MASK:
                checkAuthAndAnalyze(ctx);
                ColumnNameInfo col = (ColumnNameInfo) tableNameInfo;
                TableIf table = Env.getCurrentEnv().getCatalogMgr()
                        .getCatalogOrAnalysisException(tableNameInfo.getCtl())
                        .getDbOrAnalysisException(tableNameInfo.getDb())
                        .getTableOrAnalysisException(tableNameInfo.getTbl());
                Column column = table.getColumn(col.getCol());
                if (column == null) {
                    throw new AnalysisException("column not exist: " + col.getCol());
                }
                break;
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
                checkAuthAndAnalyze(ctx);
                if (!wherePredicate.isPresent()) {
                    throw new AnalysisException("wherePredicate can not be null");
                }
                TableIf tableIf = Env.getCurrentEnv().getCatalogMgr()
                        .getCatalogOrAnalysisException(tableNameInfo.getCtl())
                        .getDbOrAnalysisException(tableNameInfo.getDb())
                        .getTableOrAnalysisException(tableNameInfo.getTbl());
                wherePredicate.get().foreach(expr -> {
                    if (expr instanceof UnboundSlot) {
                        UnboundSlot slot = (UnboundSlot) expr;
                        if (tableIf.getColumn(slot.getName()) == null) {
                            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                "column not exist: " + slot.getName());
                        }
                    }
                });
                break;
            default:
                throw new AnalysisException("Unknown policy type: " + policyType);
        }
    }

    private void checkAuthAndAnalyze(ConnectContext ctx) throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
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
            case DATA_MASK:
                ColumnNameInfo col = (ColumnNameInfo) tableNameInfo;
                return new DorisDataMaskPolicy(policyId, policyName, user, roleName,
                    col.getCtl(), col.getDb(), col.getTbl(), col.getCol(), dataMaskType, priority);
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
