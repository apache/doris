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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.hint.OrderedHint;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * eliminate group by constant, like:
 * select 1, 'str', count(*) from t group by t.id, 1, 'str', 3, 2;
 * transform to:
 * select 1, 'str', count(*) from t group by t.id.
 * we are ensured before:
 * 1. aggregation node output contains all the expressions of group by expressions.
 * 2. others are aggregation functions.
 * so we can do the rule by:
 * 1. eliminate all the literal expression that are not integerLiteral of group by expressions.
 * 2. check whether the left literal expressions are in range and replace them to slots.
 */
public class EliminateLogicalSelectHint extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalSelectHint().thenApply(ctx -> {
            LogicalSelectHint<Plan> selectHintPlan = ctx.root;
            for (Entry<String, SelectHint> hint : selectHintPlan.getHints().entrySet()) {
                String hintName = hint.getKey();
                if (hintName.equalsIgnoreCase("SET_VAR")) {
                    setVar((SelectHintSetVar) hint.getValue(), ctx.statementContext);
                } else if (hintName.equalsIgnoreCase("ORDERED")) {
                    ConnectContext.get().getSessionVariable().setDisableJoinReorder(true);
                    OrderedHint ordered = new OrderedHint("Ordered");
                    ordered.setStatus(Hint.HintStatus.SUCCESS);
                    ctx.cascadesContext.getHintMap().put("Ordered", ordered);
                } else if (hintName.equalsIgnoreCase("LEADING")) {
                    extractLeading((SelectHintLeading) hint.getValue(), ctx.cascadesContext,
                            ctx.statementContext, selectHintPlan.getHints());
                } else {
                    // logger.warn("Can not process select hint '{}' and skip it", hint.getKey());
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
            try {
                sessionVariable.enableFallbackToOriginalPlannerOnce();
            } catch (Throwable t) {
                throw new AnalysisException("failed to set fallback to original planner to true", t);
            }
            throw new AnalysisException("The nereids is disabled in this sql, fallback to original planner");
        }
    }

    private void extractLeading(SelectHintLeading selectHint, CascadesContext context,
                                    StatementContext statementContext, Map<String, SelectHint> hints) {
        LeadingHint hint = new LeadingHint("Leading", selectHint.getParameters(), selectHint.toString());
        if (context.getHintMap().get("Leading") != null) {
            hint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
            hint.setErrorMessage("one query block can only have one leading clause");
            statementContext.addHint(hint);
            context.setLeadingJoin(false);
            return;
        }
        hint.setStatus(Hint.HintStatus.SUCCESS);
        statementContext.addHint(hint);
        context.getHintMap().put("Leading", hint);
        if (hints.get("ordered") != null) {
            context.setLeadingJoin(false);
        } else {
            context.setLeadingJoin(true);
        }
        assert (selectHint != null);
        assert (context != null);
    }

}
