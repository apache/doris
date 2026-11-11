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

package org.apache.doris.qe;

import org.apache.doris.alter.AlterUserOpType;
import org.apache.doris.analysis.SetType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SessionUser;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.commands.AlterUserCommand;
import org.apache.doris.nereids.trees.plans.commands.ExecuteCommand;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowVariablesCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AlterUserInfo;
import org.apache.doris.nereids.trees.plans.commands.info.SetCharsetAndCollateVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetNamesVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetPassVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetSessionVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetVarOp;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.Locale;
import java.util.Set;

/** Restricts a client bootstrap session without changing internal users or their privileges. */
public final class BootstrapPasswordGuard {
    private static final Set<String> CONNECTION_FUNCTIONS = Set.of(
            "connection_id", "current_user", "database", "schema", "session_user", "user", "version");

    private BootstrapPasswordGuard() {
    }

    public static boolean isAllowedCommand(MysqlCommand command) {
        switch (command) {
            case COM_QUERY:
            case COM_QUIT:
            case COM_PING:
            case COM_CHANGE_USER:
            case COM_RESET_CONNECTION:
            case COM_SET_OPTION:
            case COM_STMT_PREPARE:
            case COM_STMT_EXECUTE:
            case COM_STMT_CLOSE:
            case COM_STMT_RESET:
            case COM_STMT_FETCH:
            case COM_STMT_SEND_LONG_DATA:
                return true;
            default:
                return false;
        }
    }

    public static void check(ConnectContext ctx, LogicalPlan plan) throws UserException {
        if (!ctx.isPasswordResetRequired()) {
            return;
        }
        if (isAllowed(ctx, plan)) {
            return;
        }
        throw new AnalysisException(ErrorCode.ERR_MUST_CHANGE_PASSWORD.formatErrorMsg(),
                ErrorCode.ERR_MUST_CHANGE_PASSWORD);
    }

    private static boolean isAllowed(ConnectContext ctx, LogicalPlan plan) throws UserException {
        if (plan instanceof ExecuteCommand) {
            PreparedStatementContext prepared = ctx.getPreparedStementContext(((ExecuteCommand) plan).getStmtName());
            return prepared != null && isAllowed(ctx, prepared.command.getLogicalPlan());
        }
        if (plan instanceof PrepareCommand) {
            return isAllowed(ctx, ((PrepareCommand) plan).getLogicalPlan());
        }
        if (plan instanceof ShowVariablesCommand) {
            return true;
        }
        if (plan instanceof AlterUserCommand) {
            AlterUserCommand command = (AlterUserCommand) plan;
            command.validate();
            AlterUserInfo info = command.getAlterUserInfo();
            return info.getOpType() == AlterUserOpType.SET_PASSWORD && info.getAuthIntegration() == null
                    && info.getUserIdent().equals(ctx.getCurrentUserIdentity()) && info.getPassword().length != 0;
        }
        if (plan instanceof SetOptionsCommand) {
            return isAllowedSet(ctx, (SetOptionsCommand) plan);
        }
        if (!(plan instanceof UnboundResultSink)) {
            return false;
        }
        LogicalPlan query = (LogicalPlan) plan.child(0);
        if (query instanceof LogicalLimit) {
            query = (LogicalPlan) query.child(0);
        }
        if (!(query instanceof UnboundOneRowRelation)) {
            return false;
        }
        // SubqueryExpr owns a separate plan: allowing only these expression shapes also excludes
        // nested table reads, UDF calls and file sinks without relying on Plan.children() traversal.
        return query.getExpressions().stream().allMatch(BootstrapPasswordGuard::isConnectionExpression);
    }

    private static boolean isAllowedSet(ConnectContext ctx, SetOptionsCommand command) throws UserException {
        for (SetVarOp op : command.getSetVarOps()) {
            if (op instanceof SetPassVarOp) {
                SetPassVarOp password = (SetPassVarOp) op;
                password.validate(ctx);
                if (!password.getUserIdent().equals(ctx.getCurrentUserIdentity())
                        || password.getPassword().length == 0) {
                    return false;
                }
                continue;
            }
            if (op instanceof SetNamesVarOp || op instanceof SetCharsetAndCollateVarOp) {
                continue;
            }
            if (!(op instanceof SetSessionVarOp) || op.getType() == SetType.GLOBAL) {
                return false;
            }
            Expression value = ((SetSessionVarOp) op).getExpression();
            if (value != null && !(value instanceof Literal)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isConnectionExpression(Expression expression) {
        if (expression instanceof Alias || expression instanceof UnboundAlias) {
            return isConnectionExpression(expression.child(0));
        }
        if (expression instanceof Literal || expression instanceof UnboundVariable
                || expression instanceof CurrentUser || expression instanceof SessionUser) {
            return true;
        }
        if (!(expression instanceof UnboundFunction)) {
            return false;
        }
        UnboundFunction function = (UnboundFunction) expression;
        return function.getDbName() == null && function.arity() == 0
                && CONNECTION_FUNCTIONS.contains(function.getName().toLowerCase(Locale.ROOT));
    }
}
