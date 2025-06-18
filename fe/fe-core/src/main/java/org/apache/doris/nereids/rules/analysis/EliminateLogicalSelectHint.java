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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.HintContext;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.hint.OrderedHint;
import org.apache.doris.nereids.hint.QbNameTreeNode;
import org.apache.doris.nereids.hint.UseCboRuleHint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.properties.SelectHintOrdered;
import org.apache.doris.nereids.properties.SelectHintQbName;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.properties.SelectHintUseCboRule;
import org.apache.doris.nereids.properties.SelectHintUseMv;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
            Plan newPlan = null;
            for (SelectHint hint : selectHintPlan.getHints()) {
                if (hint instanceof SelectHintSetVar) {
                    ((SelectHintSetVar) hint).setVarOnceInSql(ctx.statementContext);
                } else if (hint instanceof SelectHintOrdered) {
                    if (!ctx.cascadesContext.getConnectContext().getSessionVariable()
                            .setVarOnce(SessionVariable.DISABLE_JOIN_REORDER, "true")) {
                        throw new RuntimeException("set DISABLE_JOIN_REORDER=true once failed");
                    }
                    OrderedHint ordered = new OrderedHint("Ordered");
                    ordered.setStatus(Hint.HintStatus.SUCCESS);
                    ctx.cascadesContext.getHintMap().put("Ordered", ordered);
                    ctx.statementContext.addHint(ordered);
                } else if (hint instanceof SelectHintLeading) {
                    extractLeading((SelectHintLeading) hint, ctx.cascadesContext,
                            ctx.statementContext, selectHintPlan);
                } else if (hint instanceof SelectHintUseCboRule) {
                    extractRule((SelectHintUseCboRule) hint, ctx.statementContext);
                } else if (hint instanceof SelectHintUseMv) {
                    extractMv((SelectHintUseMv) hint, ConnectContext.get().getStatementContext());
                } else if (hint instanceof SelectHintQbName) {
                    if (newPlan != null) {
                        continue;
                    }
                    Optional<String> hintContext = Optional.of(((SelectHintQbName) hint).getQbName());
                    QbNameTreeNode currentQbName = ctx.statementContext.findQbNameNode(hintContext);
                    if (currentQbName == null) {
                        QbNameTreeNode parentQbName = ctx.statementContext
                                .findOrGetRootQbNameNode(ctx.cascadesContext.getOuterQbName());
                        currentQbName = parentQbName.addChildQbName(hintContext);
                    }
                    newPlan = setQbNameForAllPlanNodes(selectHintPlan.child(), hintContext, currentQbName,
                            ctx.statementContext);
                } else {
                    logger.warn("Can not process select hint '{}' and skip it", hint.getHintName());
                }
            }
            return newPlan != null ? newPlan : selectHintPlan.child();
        }).toRule(RuleType.ELIMINATE_LOGICAL_SELECT_HINT);
    }

    private void extractLeading(SelectHintLeading selectHint, CascadesContext context,
            StatementContext statementContext, LogicalSelectHint<Plan> selectHintPlan) {
        if (selectHint.getErrorMessage() != null) {
            LeadingHint hint = new LeadingHint("Leading", selectHint.toString());
            hint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
            hint.setErrorMessage(selectHint.getErrorMessage());
            statementContext.addHint(hint);
            context.setLeadingJoin(false);
            return;
        }
        LeadingHint hint = new LeadingHint("Leading", selectHint.getParameters(), selectHint.getStrToHint(),
                selectHint.toString());
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
    }

    private void extractRule(SelectHintUseCboRule selectHint, StatementContext statementContext) {
        // rule hint need added to statementContext only cause it's set in all scopes
        for (String parameter : selectHint.getParameters()) {
            UseCboRuleHint hint = new UseCboRuleHint(parameter, selectHint.isNotUseCboRule());
            statementContext.addHint(hint);
        }
    }

    private void extractMv(SelectHintUseMv selectHint, StatementContext statementContext) {
        boolean isAllMv = selectHint.getTables().isEmpty();
        UseMvHint useMvHint = new UseMvHint(selectHint.getHintName(), selectHint.getTables(),
                selectHint.isUseMv(), isAllMv, statementContext.getHints());
        for (Hint hint : statementContext.getHints()) {
            if (hint.getHintName().equals(selectHint.getHintName())) {
                hint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                hint.setErrorMessage("only one " + selectHint.getHintName() + " hint is allowed");
                useMvHint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                useMvHint.setErrorMessage("only one " + selectHint.getHintName() + " hint is allowed");
            }
        }
        statementContext.addHint(useMvHint);
    }

    private Plan setQbNameForAllPlanNodes(Plan root, Optional<String> qbName, QbNameTreeNode parentQbName,
            StatementContext statementContext) {
        if (root instanceof LogicalSelectHint) {
            parentQbName.addChildQbName(getHintName((LogicalSelectHint<?>) root));
            return root;
        }
        int childCount = root.children().size();
        if (childCount > 0) {
            List<Plan> children = new ArrayList<>(childCount);
            for (Plan child : root.children()) {
                int oldChildCount = parentQbName.getChildCount();
                Plan newChild = setQbNameForAllPlanNodes(child, qbName, parentQbName, statementContext);
                children.add(newChild);
                if (newChild instanceof LogicalSubQueryAlias) {
                    LogicalSubQueryAlias logicalSubQueryAlias = (LogicalSubQueryAlias) newChild;
                    int newChildCount = parentQbName.getChildCount();
                    if (newChildCount != oldChildCount && newChildCount > 0) {
                        statementContext.addSubqueryAliasToQbName(Pair.of(qbName, logicalSubQueryAlias.getAlias()),
                                parentQbName.getChildSubList(oldChildCount, newChildCount));
                    }

                }
            }
            return root.withChildrenAndHintContext(children, Optional.of(new HintContext(qbName)));
        } else {
            return root.withHintContext(Optional.of(new HintContext(qbName)));
        }
    }

    private static Optional<String> getHintName(LogicalSelectHint<?> logicalSelectHint) {
        for (SelectHint hint : logicalSelectHint.getHints()) {
            if (hint instanceof SelectHintQbName) {
                return Optional.of(((SelectHintQbName) hint).getQbName());
            }
        }
        return Optional.empty();
    }

}
