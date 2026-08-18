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
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.RowPolicy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * Create policy command use for row policy and storage policy.
 */
public class CreatePolicyCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(CreatePolicyCommand.class);

    private final PolicyTypeEnum policyType;
    private final String policyName;
    private final boolean ifNotExists;
    private final TableNameInfo tableNameInfo;
    private final Optional<FilterType> filterType;
    private final UserIdentity user;
    private final String roleName;
    private final Optional<Expression> wherePredicate;
    // The row predicate exactly as written, null for a storage policy.
    private final String wherePredicateSql;
    private final Map<String, String> properties;
    /**
     * The predicate as it will be understood, which is what this statement accepts, stores and shows.
     *
     * <p>Not the one the parser handed over: that one was read under the creator's {@code sql_mode}, and the
     * text is re-read under {@link SqlModeHelper#MODE_FOR_POLICY_TEXT} every time the policy is applied to a
     * query. Two bits of {@code sql_mode} change what the same text means, so accepting it under one mode and
     * enforcing it under another lets a policy mean one thing in {@code SHOW ROW POLICY} and another in the
     * queries it restricts - or, with {@code NO_BACKSLASH_ESCAPES}, be accepted here and fail to parse on
     * every query it governs from then on. Settled once, in {@link #validate}.
     */
    private Optional<Expression> policyPredicate = Optional.empty();

    /**
     * ctor of this command.
     */
    public CreatePolicyCommand(PolicyTypeEnum policyType, String policyName, boolean ifNotExists,
            TableNameInfo tableNameInfo, Optional<FilterType> filterType, UserIdentity user, String roleName,
            Optional<Expression> wherePredicate, String wherePredicateSql, Map<String, String> properties) {
        super(PlanType.CREATE_POLICY_COMMAND);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifNotExists = ifNotExists;
        this.tableNameInfo = tableNameInfo;
        this.filterType = filterType;
        this.user = user;
        this.roleName = roleName;
        this.wherePredicate = wherePredicate;
        this.wherePredicateSql = wherePredicateSql;
        this.properties = properties;
    }

    public Optional<Expression> getWherePredicate() {
        return wherePredicate;
    }

    public String getWherePredicateSql() {
        return wherePredicateSql;
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
                policyPredicate = Optional.of(predicateUnderPolicyMode());
                TableIf tableIf = Env.getCurrentEnv().getCatalogMgr()
                        .getCatalogOrAnalysisException(tableNameInfo.getCtl())
                        .getDbOrAnalysisException(tableNameInfo.getDb())
                        .getTableOrAnalysisException(tableNameInfo.getTbl());
                policyPredicate.get().foreach(expr -> {
                    if (expr instanceof UnboundSlot) {
                        UnboundSlot slot = (UnboundSlot) expr;
                        if (tableIf.getColumn(slot.getName()) == null) {
                            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                    "column not exist: " + slot.getName());
                        }
                    }
                });

        }
    }

    /**
     * The stored predicate text read under the mode a security policy's text is read under.
     *
     * <p>Re-parsed rather than reused because {@code sql_mode} is a session variable any account may set with
     * no privilege at all, and the account creating a row policy is not the one it restricts: taking the
     * creator's reading of {@code ||} or of a backslash and enforcing a different one is a policy that does not
     * say what it was accepted as saying. Text the fixed mode cannot read is refused here, which is the whole
     * point of doing this at creation time - the alternative is a policy that stores fine and then fails every
     * single query it governs.
     */
    private Expression predicateUnderPolicyMode() throws AnalysisException {
        if (StringUtils.isEmpty(wherePredicateSql)) {
            // Nothing to re-read: no statement text was recorded, so the parse that produced the predicate is
            // all there is. Not reachable from CREATE ROW POLICY, which always records it.
            return wherePredicate.get();
        }
        try {
            Expression underPolicyMode = SqlModeHelper.withSqlMode(SqlModeHelper.MODE_FOR_POLICY_TEXT,
                    () -> new NereidsParser().parseExpression(wherePredicateSql));
            if (!underPolicyMode.equals(wherePredicate.get())) {
                // Both modes read the text, and they read it differently. This statement is the only place
                // that holds both readings, so it is the only place that can say so; the creator's is the one
                // being dropped, and the operator would otherwise learn that only from a query behaving in a
                // way SHOW ROW POLICY does not explain.
                LOG.warn("row policy {} on {} was written under a sql_mode that reads it as [{}], and is"
                                + " stored as [{}], which is how it is read on every query it restricts:"
                                + " predicate text [{}]", policyName, tableNameInfo,
                        wherePredicate.get().toSql(), underPolicyMode.toSql(), wherePredicateSql);
            }
            return underPolicyMode;
        } catch (Exception e) {
            throw new AnalysisException("the predicate of a row policy is read under the default sql_mode"
                    + " rather than this session's, because it is read again on every query the policy"
                    + " restricts, on the thread of the very user it restricts. Under that mode this predicate"
                    + " cannot be parsed: " + wherePredicateSql, e);
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
                // The predicate text goes in with the policy: this parse already has it, and recovering it
                // later means parsing the request text again - a request that may hold several statements.
                // The predicate stored alongside it is the one read under the policy-text mode, so that what
                // SHOW ROW POLICY renders is what the queries this policy restricts will actually be filtered
                // by. See predicateUnderPolicyMode().
                return new RowPolicy(policyId, policyName, tableNameInfo.getCtl(),
                        tableNameInfo.getDb(), tableNameInfo.getTbl(), user, roleName,
                        executor.getOriginStmt().originStmt, executor.getOriginStmt().idx, filterType.get(),
                        policyPredicate.get(), wherePredicateSql);
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
