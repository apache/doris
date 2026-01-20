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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWorkTableReference;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
            if (logicalCTE.isRecursive()
                    && !ctx.connectContext.getSessionVariable().isEnableNereidsDistributePlanner()) {
                throw new AnalysisException("please set enable_nereids_distribute_planner=true to use RECURSIVE CTE");
            }

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
                    ctx.cascadesContext, logicalCTE.child(), result.first, null);
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
            if (logicalCTE.isRecursive() && aliasQuery.isRecursiveCte()) {
                // if we have WITH RECURSIVE keyword, logicalCTE.isRecursive() will be true,
                // but we still need to check if aliasQuery is a real recursive cte or just normal cte
                Pair<CTEContext, LogicalCTEProducer<Plan>> result = analyzeRecursiveCte(aliasQuery, outerCteCtx,
                        cascadesContext);
                outerCteCtx = result.first;
                cteProducerPlans.add(result.second);
            } else {
                LogicalPlan parsedCtePlan = (LogicalPlan) aliasQuery.child();
                CascadesContext innerCascadesCtx = CascadesContext.newContextWithCteContext(
                        cascadesContext, parsedCtePlan, outerCteCtx, null);
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
        if (!(parsedCtePlan instanceof LogicalUnion)) {
            throw new AnalysisException(String.format("recursive cte must be union, don't support %s",
                    parsedCtePlan.getClass().getSimpleName()));
        }

        if (parsedCtePlan.arity() != 2) {
            throw new AnalysisException(String.format("recursive cte must have 2 children, but it has %d",
                    parsedCtePlan.arity()));
        }
        // analyze anchor child, its output list will be LogicalWorkTableReference's schema
        // also pass cte name to anchor side to let the relation binding happy. Then we check if the anchor reference
        // the recursive cte itself and report a user-friendly error message like bellow
        LogicalPlan anchorChild = (LogicalPlan) parsedCtePlan.child(0);
        CTEContext recursiveCteCtx = new CTEContext(StatementScopeIdGenerator.newCTEId(), aliasQuery.getAlias(), null);
        CascadesContext innerAnchorCascadesCtx = CascadesContext.newContextWithCteContext(
                cascadesContext, anchorChild, outerCteCtx, recursiveCteCtx);
        innerAnchorCascadesCtx.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
            innerAnchorCascadesCtx.newAnalyzer().analyze();
        });
        cascadesContext.addPlanProcesses(innerAnchorCascadesCtx.getPlanProcesses());
        LogicalPlan analyzedAnchorChild = (LogicalPlan) innerAnchorCascadesCtx.getRewritePlan();
        Set<LogicalWorkTableReference> recursiveCteScans = analyzedAnchorChild
                .collect(LogicalWorkTableReference.class::isInstance);
        for (LogicalWorkTableReference cteScan : recursiveCteScans) {
            if (cteScan.getTableName().equalsIgnoreCase(aliasQuery.getAlias())) {
                throw new AnalysisException(
                        String.format("recursive reference to query %s must not appear within its non-recursive term",
                                aliasQuery.getAlias()));
            }
        }
        checkColumnAlias(aliasQuery, analyzedAnchorChild.getOutput());
        // make all output nullable, the behavior is same as pg. It's much simpler than complex derivation of nullable
        // also we change the output same as cte's column aliases if it has. Because recursive part will try to use
        // column aliases first then the original output slot.
        analyzedAnchorChild = forceOutputNullable(analyzedAnchorChild,
                aliasQuery.getColumnAliases().orElse(ImmutableList.of()));
        analyzedAnchorChild = new LogicalRecursiveUnionAnchor<>(recursiveCteCtx.getCteId(), analyzedAnchorChild);
        // analyze recursive child, analyzedAnchorChild.getOutput() will be LogicalWorkTableReference's schema
        LogicalPlan recursiveChild = (LogicalPlan) parsedCtePlan.child(1);
        recursiveCteCtx.setRecursiveCteOutputs(analyzedAnchorChild.getOutput());
        CascadesContext innerRecursiveCascadesCtx = CascadesContext.newContextWithCteContext(
                cascadesContext, recursiveChild, outerCteCtx, recursiveCteCtx);
        innerRecursiveCascadesCtx.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
            innerRecursiveCascadesCtx.newAnalyzer().analyze();
        });
        cascadesContext.addPlanProcesses(innerRecursiveCascadesCtx.getPlanProcesses());
        LogicalPlan analyzedRecursiveChild = (LogicalPlan) innerRecursiveCascadesCtx.getRewritePlan();
        List<LogicalWorkTableReference> recursiveCteScanList = analyzedRecursiveChild
                .collectToList(item -> item instanceof LogicalWorkTableReference
                        && ((LogicalWorkTableReference) item).getCteId().equals(recursiveCteCtx.getCteId()));
        if (recursiveCteScanList.size() > 1) {
            throw new AnalysisException(String.format("recursive reference to query %s must not appear more than once",
                    aliasQuery.getAlias()));
        }
        List<Slot> anchorChildOutputs = analyzedAnchorChild.getOutput();
        List<DataType> anchorChildOutputTypes = new ArrayList<>(anchorChildOutputs.size());
        for (Slot slot : anchorChildOutputs) {
            anchorChildOutputTypes.add(slot.getDataType());
        }
        List<Slot> recursiveChildOutputs = analyzedRecursiveChild.getOutput();
        // anchor and recursive are union's children, so their output size must be same
        Preconditions.checkState(anchorChildOutputs.size() == recursiveChildOutputs.size(),
                "anchor and recursive child's output size must be same");
        for (int i = 0; i < recursiveChildOutputs.size(); ++i) {
            if (!recursiveChildOutputs.get(i).getDataType().equalsForRecursiveCte(anchorChildOutputTypes.get(i))) {
                throw new AnalysisException(String.format("%s recursive child's %d column's datatype in select list %s "
                        + "is different from anchor child's output datatype %s, please add cast manually "
                        + "to get expect datatype", aliasQuery.getAlias(), i + 1,
                        recursiveChildOutputs.get(i).getDataType(), anchorChildOutputTypes.get(i)));
            }
        }
        // make analyzedRecursiveChild's outputs all nullable and keep slot name unchanged
        analyzedRecursiveChild = new LogicalRecursiveUnionProducer<>(aliasQuery.getAlias(),
                forceOutputNullable(analyzedRecursiveChild, ImmutableList.of()));

        // create LogicalRecursiveUnion
        LogicalUnion logicalUnion = (LogicalUnion) parsedCtePlan;
        ImmutableList.Builder<NamedExpression> newOutputs = ImmutableList
                .builderWithExpectedSize(anchorChildOutputs.size());
        for (Slot slot : anchorChildOutputs) {
            newOutputs.add(new SlotReference(slot.toSql(), slot.getDataType(), slot.nullable(), ImmutableList.of()));
        }
        LogicalRecursiveUnion<? extends Plan, ? extends Plan> analyzedCtePlan = new LogicalRecursiveUnion(
                aliasQuery.getAlias(),
                logicalUnion.getQualifier(),
                newOutputs.build(),
                ImmutableList.of(analyzedAnchorChild.getOutput(), analyzedRecursiveChild.getOutput()),
                ImmutableList.of(analyzedAnchorChild, analyzedRecursiveChild));

        CTEId cteId = StatementScopeIdGenerator.newCTEId();
        LogicalSubQueryAlias<Plan> logicalSubQueryAlias = aliasQuery.withChildren(ImmutableList.of(analyzedCtePlan));
        outerCteCtx = new CTEContext(cteId, logicalSubQueryAlias, outerCteCtx);
        outerCteCtx.setAnalyzedPlan(logicalSubQueryAlias);
        LogicalCTEProducer<Plan> cteProducer = new LogicalCTEProducer<>(cteId, logicalSubQueryAlias);
        return Pair.of(outerCteCtx, cteProducer);
    }

    private LogicalPlan forceOutputNullable(LogicalPlan logicalPlan, List<String> aliasNames) {
        List<Slot> oldOutputs = logicalPlan.getOutput();
        int size = oldOutputs.size();
        List<NamedExpression> newOutputs = new ArrayList<>(oldOutputs.size());
        if (!aliasNames.isEmpty()) {
            for (int i = 0; i < size; ++i) {
                newOutputs.add(new Alias(new Nullable(oldOutputs.get(i)), aliasNames.get(i)));
            }
        } else {
            for (Slot slot : oldOutputs) {
                newOutputs.add(new Alias(new Nullable(slot), slot.getName()));
            }
        }
        return new LogicalProject<>(newOutputs, logicalPlan);
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
