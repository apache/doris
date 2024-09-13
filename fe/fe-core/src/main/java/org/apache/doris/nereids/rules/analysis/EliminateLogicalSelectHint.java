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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.MustFallbackException;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.hint.OrderedHint;
import org.apache.doris.nereids.hint.UseCboRuleHint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.properties.SelectHintUseCboRule;
import org.apache.doris.nereids.properties.SelectHintUseMv;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;
import java.util.Optional;

/**
 * eliminate logical select hint and set them to cascade context
 */
public class EliminateLogicalSelectHint extends OneRewriteRuleFactory {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Rule build() {
        return logicalSelectHint().thenApply(ctx -> {
            LogicalSelectHint<Plan> selectHintPlan = ctx.root;
            for (SelectHint hint : selectHintPlan.getHints()) {
                String hintName = hint.getHintName();
                if (hintName.equalsIgnoreCase("SET_VAR")) {
                    setVar((SelectHintSetVar) hint, ctx.statementContext);
                } else if (hintName.equalsIgnoreCase("ORDERED")) {
                    try {
                        ctx.cascadesContext.getConnectContext().getSessionVariable()
                                .disableNereidsJoinReorderOnce();
                    } catch (DdlException e) {
                        throw new RuntimeException(e);
                    }
                    OrderedHint ordered = new OrderedHint("Ordered");
                    ordered.setStatus(Hint.HintStatus.SUCCESS);
                    ctx.cascadesContext.getHintMap().put("Ordered", ordered);
                    ctx.statementContext.addHint(ordered);
                } else if (hintName.equalsIgnoreCase("LEADING")) {
                    extractLeading((SelectHintLeading) hint, ctx.cascadesContext,
                            ctx.statementContext, selectHintPlan);
                } else if (hintName.equalsIgnoreCase("USE_CBO_RULE")) {
                    extractRule((SelectHintUseCboRule) hint, ctx.statementContext);
                } else if (hintName.equalsIgnoreCase("USE_MV")) {
                    extractMv((SelectHintUseMv) hint, ConnectContext.get().getStatementContext());
                } else if (hintName.equalsIgnoreCase("NO_USE_MV")) {
                    extractMv((SelectHintUseMv) hint, ConnectContext.get().getStatementContext());
                } else {
                    logger.warn("Can not process select hint '{}' and skip it", hint.getHintName());
                }
            }
            return selectHintPlan.child();
        }).toRule(RuleType.ELIMINATE_LOGICAL_SELECT_HINT);
    }

    private void setVar(SelectHintSetVar selectHint, StatementContext context) {
        SessionVariable sessionVariable = context.getConnectContext().getSessionVariable();
        // set temporary session value, and then revert value in the 'finally block' of StmtExecutor#execute
        sessionVariable.setIsSingleSetVar(true);
        for (Entry<String, Optional<String>> kv : selectHint.getParameters().entrySet()) {
            String key = kv.getKey();
            Optional<String> value = kv.getValue();
            if (value.isPresent()) {
                try {
                    VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(value.get())));
                    context.invalidCache(key);
                } catch (Throwable t) {
                    throw new AnalysisException("Can not set session variable '"
                        + key + "' = '" + value.get() + "'", t);
                }
            }
        }
        // if sv set enable_nereids_planner=true and hint set enable_nereids_planner=false, we should set
        // enable_fallback_to_original_planner=true and revert it after executing.
        // throw exception to fall back to original planner
        if (!sessionVariable.isEnableNereidsPlanner()) {
            throw new MustFallbackException("The nereids is disabled in this sql, fallback to original planner");
        }
    }

    private void extractLeading(SelectHintLeading selectHint, CascadesContext context,
                                    StatementContext statementContext, LogicalSelectHint<Plan> selectHintPlan) {
        LeadingHint hint = new LeadingHint("Leading", selectHint.getParameters(), selectHint.toString());
        if (context.getHintMap().get("Leading") != null) {
            hint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
            context.getHintMap().get("Leading").setStatus(Hint.HintStatus.UNUSED);
            hint.setErrorMessage("one query block can only have one leading clause");
            statementContext.addHint(hint);
            context.setLeadingJoin(false);
            return;
        }
        statementContext.addHint(hint);
        context.getHintMap().put("Leading", hint);
        if (hint.getTablelist().size() < 2) {
            hint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
            context.getHintMap().get("Leading").setStatus(Hint.HintStatus.UNUSED);
            hint.setErrorMessage("less than two tables is not allowed in leading clause");
            statementContext.addHint(hint);
            context.setLeadingJoin(false);
            return;
        }
        if (!hint.isSyntaxError()) {
            hint.setStatus(Hint.HintStatus.SUCCESS);
        }
        if (selectHintPlan.isIncludeHint("Ordered")
                || ConnectContext.get().getSessionVariable().isDisableJoinReorder()
                || context.isLeadingDisableJoinReorder()) {
            context.setLeadingJoin(false);
            hint.setStatus(Hint.HintStatus.UNUSED);
        } else {
            context.setLeadingJoin(true);
        }
        assert (selectHint != null);
        assert (context != null);
    }

    private void extractRule(SelectHintUseCboRule selectHint, StatementContext statementContext) {
        // rule hint need added to statementContext only cause it's set in all scopes
        for (String parameter : selectHint.getParameters()) {
            UseCboRuleHint hint = new UseCboRuleHint(parameter, selectHint.isNotUseCboRule());
            statementContext.addHint(hint);
        }
    }

    private void extractMv(SelectHintUseMv selectHint, StatementContext statementContext) {
        boolean isAllMv = selectHint.getParameters().isEmpty();
        UseMvHint useMvHint = new UseMvHint(selectHint.getHintName(), selectHint.getParameters(),
                selectHint.isUseMv(), isAllMv);
        for (Hint hint : statementContext.getHints()) {
            if (hint.getHintName().equals(selectHint.getHintName())) {
                hint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                hint.setErrorMessage("only one " + selectHint.getHintName() + " hint is allowed");
                useMvHint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                useMvHint.setErrorMessage("only one " + selectHint.getHintName() + " hint is allowed");
            }
        }
        if (!useMvHint.isSyntaxError()) {
            ConnectContext.get().getSessionVariable().setEnableSyncMvCostBasedRewrite(false);
        }
        statementContext.addHint(useMvHint);
    }

}
