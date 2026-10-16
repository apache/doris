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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * rewrite CteAnchor consumer side and producer side recursively， all CteAnchor must at top of the plan
 */
@DependsRules({PullUpCteAnchor.class, CTEInline.class})
public class RewriteCteChildren extends DefaultPlanRewriter<CascadesContext> implements CustomRewriter {

    private final List<RewriteJob> jobs;
    private final boolean runCboRules;

    public RewriteCteChildren(List<RewriteJob> jobs, boolean runCboRules) {
        this.jobs = jobs;
        this.runCboRules = runCboRules;
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, jobContext.getCascadesContext());
    }

    @Override
    public Plan visit(Plan plan, CascadesContext context) {
        Rewriter.getCteChildrenRewriter(context, jobs, runCboRules).execute();
        return context.getRewritePlan();
    }

    @Override
    public Plan visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            CascadesContext cascadesContext) {
        LogicalPlan outer;
        if (cascadesContext.getStatementContext().getRewrittenCteConsumer().containsKey(cteAnchor.getCteId())) {
            outer = cascadesContext.getStatementContext().getRewrittenCteConsumer().get(cteAnchor.getCteId());
        } else {
            CascadesContext outerCascadesCtx = CascadesContext.newSubtreeContext(
                    Optional.empty(), cascadesContext, cteAnchor.child(1),
                    cascadesContext.getCurrentJobContext().getRequiredProperties());
            AtomicReference<LogicalPlan> outerResult = new AtomicReference<>();
            StatsDerive statsDerive = new StatsDerive(false);
            cteAnchor.child(0).accept(statsDerive, new StatsDerive.DeriveContext());
            outerCascadesCtx.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
                outerResult.set((LogicalPlan) cteAnchor.child(1).accept(this, outerCascadesCtx));
            });
            outer = outerResult.get();
            cascadesContext.addPlanProcesses(outerCascadesCtx.getPlanProcesses());
            cascadesContext.getStatementContext().getRewrittenCteConsumer().put(cteAnchor.getCteId(), outer);
        }
        Set<LogicalCTEConsumer> cteConsumers = collectCteConsumers(outer, cteAnchor.getCteId());
        cascadesContext.getCteIdToConsumers().put(cteAnchor.getCteId(), cteConsumers);
        if (cteConsumers.isEmpty()) {
            return outer;
        }
        List<Slot> oldProducerOutputs = cteAnchor.child(0).getOutput();
        Plan producer = cteAnchor.child(0).accept(this, cascadesContext);
        Map<ExprId, ExprId> producerExprIdRemap = computeProducerExprIdRemap(oldProducerOutputs, producer.getOutput());
        if (!producerExprIdRemap.isEmpty()) {
            // The producer subtree is rewritten in its own context, so a rule of it may rename the exprIds of the
            // producer outputs, while the consumers, which are rewritten before the producer, still map to the
            // previous exprIds. Refresh both maps of every consumer of this CTE with the new producer slots.
            Map<ExprId, Slot> producerOutputSlots = producer.getOutput().stream()
                    .collect(Collectors.toMap(Slot::getExprId, Function.identity()));
            outer = (LogicalPlan) outer.<Plan, Void>accept(new CteConsumerOutputsRefresher(cteAnchor.getCteId(),
                    producerExprIdRemap, producerOutputSlots), null);
            cascadesContext.getStatementContext().getRewrittenCteConsumer().put(cteAnchor.getCteId(), outer);
            cascadesContext.getCteIdToConsumers()
                    .put(cteAnchor.getCteId(), collectCteConsumers(outer, cteAnchor.getCteId()));
        }
        return cteAnchor.withChildren(producer, outer);
    }

    private static Set<LogicalCTEConsumer> collectCteConsumers(Plan outer, CTEId cteId) {
        Set<LogicalCTEConsumer> cteConsumers = Sets.newHashSet();
        outer.foreach(p -> {
            if (p instanceof LogicalCTEConsumer) {
                LogicalCTEConsumer logicalCTEConsumer = (LogicalCTEConsumer) p;
                if (logicalCTEConsumer.getCteId().equals(cteId)) {
                    cteConsumers.add(logicalCTEConsumer);
                }
            }
            return false;
        });
        return cteConsumers;
    }

    /**
     * The outputs of a CTE producer body are the contract of its consumers, the consumers map to them by exprId. A
     * rule which rewrites the producer body may rename them, so compare the outputs before and after the rewrite to
     * know which exprIds changed. A column which is not renamed keeps its exprId, so it needs no mapping, and the
     * columns which are renamed are the ones whose exprId disappeared: match them by name, a rule which renames an
     * output keeps its name. A column which was pruned by the rewrite of the body, e.g. by ColumnPruning, has no
     * counterpart in the new outputs any more, so there is nothing to map for it, see the two CTE tests of
     * CteLimitPushdownPlanTest and DataTraitTest which prune the columns their consumers do not read.
     */
    @VisibleForTesting
    static Map<ExprId, ExprId> computeProducerExprIdRemap(List<Slot> oldOutputs, List<Slot> newOutputs) {
        Set<ExprId> newExprIds = newOutputs.stream().map(Slot::getExprId).collect(Collectors.toSet());
        Set<ExprId> matchedNewExprIds = Sets.newHashSetWithExpectedSize(newExprIds.size());
        List<Slot> renamedOldOutputs = new ArrayList<>();
        for (Slot oldOutput : oldOutputs) {
            if (newExprIds.contains(oldOutput.getExprId())) {
                matchedNewExprIds.add(oldOutput.getExprId());
            } else {
                renamedOldOutputs.add(oldOutput);
            }
        }
        Map<ExprId, ExprId> remap = new HashMap<>();
        for (Slot renamedOldOutput : renamedOldOutputs) {
            List<Slot> candidates = newOutputs.stream()
                    .filter(newOutput -> !matchedNewExprIds.contains(newOutput.getExprId()))
                    .filter(newOutput -> newOutput.getName().equals(renamedOldOutput.getName()))
                    .collect(Collectors.toList());
            if (candidates.size() > 1) {
                // the name is not a key: several outputs of the producer may share it. The lineage of a slot
                // survives a rename (Alias#toSlot copies it), so use it to find the right candidate.
                candidates = candidates.stream()
                        .filter(candidate -> sameLineage(renamedOldOutput, candidate))
                        .collect(Collectors.toList());
            }
            // a unique candidate is the renamed column; no candidate means the column was pruned, and several
            // same named candidates which cannot be told apart are not mapped at all instead of guessing
            if (candidates.size() == 1) {
                remap.put(renamedOldOutput.getExprId(), candidates.get(0).getExprId());
                matchedNewExprIds.add(candidates.get(0).getExprId());
            }
        }
        return remap;
    }

    private static boolean sameLineage(Slot left, Slot right) {
        if (!(left instanceof SlotReference) || !(right instanceof SlotReference)) {
            return false;
        }
        SlotReference leftReference = (SlotReference) left;
        SlotReference rightReference = (SlotReference) right;
        return leftReference.getOriginalTable().isPresent() && rightReference.getOriginalTable().isPresent()
                && leftReference.getOriginalTable().equals(rightReference.getOriginalTable())
                && leftReference.getOriginalColumn().equals(rightReference.getOriginalColumn());
    }

    /** Refresh the producer slots of the two output maps of the consumers of a CTE, see computeProducerExprIdRemap. */
    private static class CteConsumerOutputsRefresher extends DefaultPlanRewriter<Void> {
        private final CTEId cteId;
        private final Map<ExprId, ExprId> producerExprIdRemap;
        private final Map<ExprId, Slot> producerOutputSlots;

        CteConsumerOutputsRefresher(CTEId cteId, Map<ExprId, ExprId> producerExprIdRemap,
                Map<ExprId, Slot> producerOutputSlots) {
            this.cteId = cteId;
            this.producerExprIdRemap = producerExprIdRemap;
            this.producerOutputSlots = producerOutputSlots;
        }

        @Override
        public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, Void context) {
            if (!cteConsumer.getCteId().equals(cteId)) {
                return cteConsumer;
            }
            Map<Slot, Slot> consumerToProducerOutputMap = new LinkedHashMap<>();
            Multimap<Slot, Slot> producerToConsumerOutputMap = LinkedHashMultimap.create();
            for (Map.Entry<Slot, Slot> entry : cteConsumer.getConsumerToProducerOutputMap().entrySet()) {
                Slot newProducerSlot = refreshProducerSlot(entry.getValue());
                consumerToProducerOutputMap.put(entry.getKey(), newProducerSlot);
                for (Slot consumerSlot : cteConsumer.getProducerToConsumerOutputMap().get(entry.getValue())) {
                    producerToConsumerOutputMap.put(newProducerSlot, consumerSlot);
                }
            }
            return cteConsumer.withTwoMaps(consumerToProducerOutputMap, producerToConsumerOutputMap);
        }

        private Slot refreshProducerSlot(Slot producerSlot) {
            ExprId newExprId = producerExprIdRemap.get(producerSlot.getExprId());
            if (newExprId == null) {
                return producerSlot;
            }
            Slot newProducerSlot = producerOutputSlots.get(newExprId);
            Preconditions.checkArgument(newProducerSlot != null,
                    "the producer does not output the exprId %s", newExprId);
            return newProducerSlot;
        }
    }

    @Override
    public Plan visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer,
            CascadesContext cascadesContext) {
        LogicalPlan child;
        if (cascadesContext.getStatementContext().getRewrittenCteProducer().containsKey(cteProducer.getCteId())) {
            child = cascadesContext.getStatementContext().getRewrittenCteProducer().get(cteProducer.getCteId());
        } else {
            child = (LogicalPlan) cteProducer.child();
            child = tryToConstructFilter(cascadesContext, cteProducer.getCteId(), child);
            child = tryToConstructLimit(cascadesContext, cteProducer.getCteId(), child);
            Set<Slot> producerOutputs = cascadesContext.getStatementContext()
                    .getCteIdToOutputIds().get(cteProducer.getCteId());
            if (producerOutputs != null && producerOutputs.size() < child.getOutput().size()) {
                ImmutableList.Builder<NamedExpression> projectsBuilder
                        = ImmutableList.builderWithExpectedSize(producerOutputs.size());
                for (Slot slot : child.getOutput()) {
                    if (producerOutputs.contains(slot)) {
                        projectsBuilder.add(slot);
                    }
                }
                child = new LogicalProject<>(projectsBuilder.build(), child);
                child = pushPlanUnderAnchor(child);
            }
            CascadesContext rewrittenCtx = CascadesContext.newSubtreeContext(
                    Optional.of(cteProducer.getCteId()), cascadesContext, child, PhysicalProperties.ANY);
            AtomicReference<LogicalPlan> result = new AtomicReference<>(child);
            rewrittenCtx.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
                result.set((LogicalPlan) result.get().accept(this, rewrittenCtx));
            });
            child = result.get();
            cascadesContext.addPlanProcesses(rewrittenCtx.getPlanProcesses());
            cascadesContext.getStatementContext().getRewrittenCteProducer().put(cteProducer.getCteId(), child);
        }
        LogicalCTEProducer<? extends Plan> rewrittenProducer = (LogicalCTEProducer<? extends Plan>) cteProducer
                .withChildren(child);
        cascadesContext.getStatementContext().setCteProducer(rewrittenProducer.getCteId(), rewrittenProducer);
        return rewrittenProducer;
    }

    private LogicalPlan pushPlanUnderAnchor(LogicalPlan plan) {
        if (plan.child(0) instanceof LogicalCTEAnchor) {
            LogicalPlan child = (LogicalPlan) plan.withChildren(plan.child(0).child(1));
            return (LogicalPlan) plan.child(0).withChildren(
                    plan.child(0).child(0), pushPlanUnderAnchor(child));
        }
        return plan;
    }

    private LogicalPlan tryToConstructLimit(CascadesContext cascadesContext, CTEId cteId, LogicalPlan child) {
        Set<LogicalCTEConsumer> consumers = cascadesContext.getCteIdToConsumers().get(cteId);
        long limit = 0;
        for (LogicalCTEConsumer consumer : consumers) {
            Long rowsNeeded = cascadesContext.getConsumerIdToLimitRows().get(consumer.getRelationId());
            if (rowsNeeded == null) {
                return child;
            }
            limit = Math.max(limit, rowsNeeded);
        }
        return pushPlanUnderAnchor(new LogicalLimit<>(limit, 0, LimitPhase.ORIGIN, child));
    }

    /*
     * An expression can only be pushed down if it has filter expressions on all consumers that reference the slot.
     * For example, let's assume a producer has two consumers, consumer1 and consumer2:
     *
     * filter(a > 5 and b < 1) -> consumer1
     * filter(a < 8) -> consumer2
     *
     * In this case, the only expression that can be pushed down to the producer is filter(a > 5 or a < 8).
     */
    private LogicalPlan tryToConstructFilter(CascadesContext cascadesContext, CTEId cteId, LogicalPlan child) {
        Set<RelationId> consumerIds = cascadesContext.getCteIdToConsumers().get(cteId).stream()
                .map(LogicalCTEConsumer::getRelationId)
                .collect(Collectors.toSet());
        List<Set<Expression>> filtersAboveEachConsumer = cascadesContext.getConsumerIdToFilters().entrySet().stream()
                .filter(kv -> consumerIds.contains(kv.getKey()))
                .map(Entry::getValue)
                .collect(Collectors.toList());
        Set<Expression> someone = filtersAboveEachConsumer.stream().findFirst().orElse(null);
        if (someone == null) {
            return child;
        }
        int filterSize = cascadesContext.getCteIdToConsumers().get(cteId).size();
        Set<Expression> conjuncts = new HashSet<>();
        for (Expression f : someone) {
            int matchCount = 0;
            Set<SlotReference> slots = f.collect(e -> e instanceof SlotReference);
            Set<Expression> mightBeJoined = new HashSet<>();
            for (Set<Expression> another : filtersAboveEachConsumer) {
                if (another.equals(someone)) {
                    matchCount++;
                    continue;
                }
                Set<Expression> matched = new HashSet<>();
                for (Expression e : another) {
                    Set<SlotReference> otherSlots = e.collect(ae -> ae instanceof SlotReference);
                    if (otherSlots.equals(slots)) {
                        matched.add(e);
                    }
                }
                if (!matched.isEmpty()) {
                    matchCount++;
                }
                mightBeJoined.addAll(matched);
            }
            if (matchCount >= filterSize) {
                mightBeJoined.add(f);
                conjuncts.add(ExpressionUtils.or(mightBeJoined));
            }
        }
        if (!conjuncts.isEmpty()) {
            // distinct conjuncts
            ImmutableSet.Builder<Expression> newConjuncts = ImmutableSet.builderWithExpectedSize(conjuncts.size());
            for (Expression conjunct : conjuncts) {
                newConjuncts.addAll(ExpressionUtils.extractConjunction(conjunct));
            }
            LogicalPlan filter = new LogicalFilter<>(newConjuncts.build(), child);
            return pushPlanUnderAnchor(filter);
        }
        return child;
    }
}
