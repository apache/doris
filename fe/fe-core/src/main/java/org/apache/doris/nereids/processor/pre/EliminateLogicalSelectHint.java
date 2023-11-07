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

package org.apache.doris.nereids.processor.pre;

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.hint.OrderedHint;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * eliminate set var hint, and set var to session variables.
 */
public class EliminateLogicalSelectHint extends PlanPreprocessor implements CustomRewriter {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, jobContext.getCascadesContext().getStatementContext());
    }

    @Override
    public LogicalPlan visitLogicalSelectHint(
            LogicalSelectHint<? extends Plan> selectHintPlan,
            StatementContext context) {
        for (Entry<String, SelectHint> hint : selectHintPlan.getHints().entrySet()) {
            String hintName = hint.getKey();
            if (hintName.equalsIgnoreCase("SET_VAR")) {
                setVar((SelectHintSetVar) hint.getValue(), context);
            } else if (hintName.equalsIgnoreCase("ORDERED")) {
                ConnectContext.get().getSessionVariable().setDisableJoinReorder(true);
                OrderedHint ordered = new OrderedHint("Ordered");
                ordered.setStatus(Hint.HintStatus.SUCCESS);
                context.getHintMap().put("Ordered", ordered);
            } else if (hintName.equalsIgnoreCase("LEADING")) {
                extractLeading((SelectHintLeading) hint.getValue(), context, selectHintPlan.getHints());
            } else {
                logger.warn("Can not process select hint '{}' and skip it", hint.getKey());
            }
        }

        return (LogicalPlan) selectHintPlan.child().accept(this, context);
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

    private void extractLeading(SelectHintLeading selectHint, StatementContext context, Map<String, SelectHint> hints) {
        LeadingHint hint = new LeadingHint("Leading", selectHint.getParameters(), selectHint.toString());
        if (context.getHintMap().get("Leading") != null) {
            hint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
            hint.setErrorMessage("can only have one leading clause");
        }
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
