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
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveCte;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.ProjectProcessor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Register CTE, includes checking columnAliases, checking CTE name, analyzing each CTE and store the
 * analyzed logicalPlan of CTE's query in CTEContext;
 * A LogicalProject node will be added to the root of the initial logicalPlan if there exist columnAliases.
 * Node LogicalCTE will be eliminated after registering.
 */
public class AnalyzeCTE extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return logicalCTE().thenApply(ctx -> {
            LogicalCTE<Plan> logicalCTE = ctx.root;

            // step 0. check duplicate cte name
            Set<String> uniqueAlias = Sets.newHashSet();
            List<String> aliases = logicalCTE.getAliasQueries().stream()
                    .map(LogicalSubQueryAlias::getAlias)
                    .collect(Collectors.toList());
            for (String alias : aliases) {
                if (uniqueAlias.contains(alias)) {
                    throw new AnalysisException("CTE name [" + alias + "] cannot be used more than once.");
                }
                uniqueAlias.add(alias);
            }

            // step 1. analyzed all cte plan
            Pair<CTEContext, List<LogicalCTEProducer<Plan>>> result = analyzeCte(logicalCTE, ctx.cascadesContext);
            CascadesContext outerCascadesCtx = CascadesContext.newContextWithCteContext(
                    ctx.cascadesContext, logicalCTE.child(), result.first, Optional.empty(), ImmutableList.of());
            outerCascadesCtx.withPlanProcess(ctx.cascadesContext.showPlanProcess(), () -> {
                outerCascadesCtx.newAnalyzer().analyze();
            });
            ctx.cascadesContext.setLeadingDisableJoinReorder(outerCascadesCtx.isLeadingDisableJoinReorder());
            Plan root = outerCascadesCtx.getRewritePlan();
            ctx.cascadesContext.addPlanProcesses(outerCascadesCtx.getPlanProcesses());
            // should construct anchor from back to front, because the cte behind depends on the front
            for (int i = result.second.size() - 1; i >= 0; i--) {
                root = new LogicalCTEAnchor<>(result.second.get(i).getCteId(), result.second.get(i), root);
            }
            return root;
        }).toRule(RuleType.ANALYZE_CTE);
    }

    /**
     * register and store CTEs in CTEContext
     */
    private Pair<CTEContext, List<LogicalCTEProducer<Plan>>> analyzeCte(
            LogicalCTE<Plan> logicalCTE, CascadesContext cascadesContext) {
        CTEContext outerCteCtx = cascadesContext.getCteContext();
        List<LogicalSubQueryAlias<Plan>> aliasQueries = logicalCTE.getAliasQueries();
        List<LogicalCTEProducer<Plan>> cteProducerPlans = new ArrayList<>();
        for (LogicalSubQueryAlias<Plan> aliasQuery : aliasQueries) {
            // we should use a chain to ensure visible of cte
            if (aliasQuery.isRecursiveCte()) {
                Pair<CTEContext, LogicalCTEProducer<Plan>> result = analyzeRecursiveCte(aliasQuery, outerCteCtx,
                        cascadesContext);
                outerCteCtx = result.first;
                cteProducerPlans.add(result.second);
            } else {
                LogicalPlan parsedCtePlan = (LogicalPlan) aliasQuery.child();
                CascadesContext innerCascadesCtx = CascadesContext.newContextWithCteContext(
                        cascadesContext, parsedCtePlan, outerCteCtx, Optional.empty(), ImmutableList.of());
                innerCascadesCtx.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
                    innerCascadesCtx.newAnalyzer().analyze();
                });
                cascadesContext.addPlanProcesses(innerCascadesCtx.getPlanProcesses());
                LogicalPlan analyzedCtePlan = (LogicalPlan) innerCascadesCtx.getRewritePlan();
                checkColumnAlias(aliasQuery, analyzedCtePlan.getOutput());
                CTEId cteId = StatementScopeIdGenerator.newCTEId();
                LogicalSubQueryAlias<Plan> logicalSubQueryAlias = aliasQuery
                        .withChildren(ImmutableList.of(analyzedCtePlan));
                outerCteCtx = new CTEContext(cteId, logicalSubQueryAlias, outerCteCtx);
                outerCteCtx.setAnalyzedPlan(logicalSubQueryAlias);
                cteProducerPlans.add(new LogicalCTEProducer<>(cteId, logicalSubQueryAlias));
            }
        }
        return Pair.of(outerCteCtx, cteProducerPlans);
    }

    private Pair<CTEContext, LogicalCTEProducer<Plan>> analyzeRecursiveCte(LogicalSubQueryAlias<Plan> aliasQuery,
            CTEContext outerCteCtx, CascadesContext cascadesContext) {
        Preconditions.checkArgument(aliasQuery.isRecursiveCte(), "alias query must be recursive cte");
        LogicalPlan parsedCtePlan = (LogicalPlan) aliasQuery.child();
        if (!(parsedCtePlan instanceof LogicalUnion) || parsedCtePlan.children().size() != 2) {
            throw new AnalysisException("recursive cte must be union");
        }
        // analyze anchor child, its output list will be recursive cte temp table's schema
        LogicalPlan anchorChild = (LogicalPlan) parsedCtePlan.child(0);
        CascadesContext innerAnchorCascadesCtx = CascadesContext.newContextWithCteContext(
                cascadesContext, anchorChild, outerCteCtx, Optional.of(aliasQuery.getAlias()), ImmutableList.of());
        innerAnchorCascadesCtx.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
            innerAnchorCascadesCtx.newAnalyzer().analyze();
        });
        cascadesContext.addPlanProcesses(innerAnchorCascadesCtx.getPlanProcesses());
        LogicalPlan analyzedAnchorChild = (LogicalPlan) innerAnchorCascadesCtx.getRewritePlan();
        checkColumnAlias(aliasQuery, analyzedAnchorChild.getOutput());

        // analyze recursive child
        LogicalPlan recursiveChild = (LogicalPlan) parsedCtePlan.child(1);
        CascadesContext innerRecursiveCascadesCtx = CascadesContext.newContextWithCteContext(
                cascadesContext, recursiveChild, outerCteCtx, Optional.of(aliasQuery.getAlias()),
                analyzedAnchorChild.getOutput());
        innerRecursiveCascadesCtx.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
            innerRecursiveCascadesCtx.newAnalyzer().analyze();
        });
        cascadesContext.addPlanProcesses(innerRecursiveCascadesCtx.getPlanProcesses());
        LogicalPlan analyzedRecursiveChild = (LogicalPlan) innerRecursiveCascadesCtx.getRewritePlan();
        LogicalUnion logicalUnion = (LogicalUnion) parsedCtePlan;

        // create LogicalRecursiveCte
        LogicalRecursiveCte analyzedCtePlan = new LogicalRecursiveCte(
                logicalUnion.getQualifier() == SetOperation.Qualifier.ALL,
                ImmutableList.of(analyzedAnchorChild, analyzedRecursiveChild));
        List<List<NamedExpression>> childrenProjections = analyzedCtePlan.collectChildrenProjections();
        int childrenProjectionSize = childrenProjections.size();
        ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList
                .builderWithExpectedSize(childrenProjectionSize);
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(childrenProjectionSize);
        for (int i = 0; i < childrenProjectionSize; i++) {
            Plan newChild;
            Plan child = analyzedCtePlan.child(i);
            if (childrenProjections.get(i).stream().allMatch(SlotReference.class::isInstance)) {
                newChild = child;
            } else {
                List<NamedExpression> parentProject = childrenProjections.get(i);
                newChild = ProjectProcessor.tryProcessProject(parentProject, child)
                        .orElseGet(() -> new LogicalProject<>(parentProject, child));
            }
            newChildren.add(newChild);
            childrenOutputs.add((List<SlotReference>) (List) newChild.getOutput());
        }
        analyzedCtePlan = analyzedCtePlan.withChildrenAndTheirOutputs(newChildren.build(), childrenOutputs.build());
        List<NamedExpression> newOutputs = analyzedCtePlan.buildNewOutputs();
        analyzedCtePlan = analyzedCtePlan.withNewOutputs(newOutputs);

        CTEId cteId = StatementScopeIdGenerator.newCTEId();
        LogicalSubQueryAlias<Plan> logicalSubQueryAlias = aliasQuery.withChildren(ImmutableList.of(analyzedCtePlan));
        outerCteCtx = new CTEContext(cteId, logicalSubQueryAlias, outerCteCtx);
        outerCteCtx.setAnalyzedPlan(logicalSubQueryAlias);
        LogicalCTEProducer<Plan> cteProducer = new LogicalCTEProducer<>(cteId, logicalSubQueryAlias);
        return Pair.of(outerCteCtx, cteProducer);
    }

    /**
     * check columnAliases' size and name
     */
    private void checkColumnAlias(LogicalSubQueryAlias<Plan> aliasQuery, List<Slot> outputSlots) {
        if (aliasQuery.getColumnAliases().isPresent()) {
            List<String> columnAlias = aliasQuery.getColumnAliases().get();
            // if the size of columnAlias is smaller than outputSlots' size, we will replace the corresponding number
            // of front slots with columnAlias.
            if (columnAlias.size() > outputSlots.size()) {
                throw new AnalysisException("CTE [" + aliasQuery.getAlias() + "] returns "
                        + columnAlias.size() + " columns, but " + outputSlots.size() + " labels were specified."
                        + " The number of column labels must be smaller or equal to the number of returned columns.");
            }

            Set<String> names = new HashSet<>();
            // column alias cannot be used more than once
            columnAlias.forEach(alias -> {
                if (names.contains(alias.toLowerCase())) {
                    throw new AnalysisException("Duplicated CTE column alias:"
                            + " [" + alias.toLowerCase() + "] in CTE [" + aliasQuery.getAlias() + "]");
                }
                names.add(alias);
            });
        }
    }
}
