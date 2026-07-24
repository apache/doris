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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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
        Set<LogicalCTEConsumer> cteConsumers = Sets.newHashSet();
        outer.foreach(p -> {
            if (p instanceof LogicalCTEConsumer) {
                LogicalCTEConsumer logicalCTEConsumer = (LogicalCTEConsumer) p;
                if (logicalCTEConsumer.getCteId().equals(cteAnchor.getCteId())) {
                    cteConsumers.add(logicalCTEConsumer);
                }
            }
            return false;
        });
        cascadesContext.getCteIdToConsumers().put(cteAnchor.getCteId(), cteConsumers);
        if (cteConsumers.isEmpty()) {
            return outer;
        }
        // Save original producer output before rewrite, to detect ExprId changes
        // caused by rules like EliminateGroupByKey that wrap slots with any_value().
        List<Slot> oldProducerOutput = cteAnchor.child(0).getOutput();
        Plan producer = cteAnchor.child(0).accept(this, cascadesContext);
        // visitLogicalCTEProducer may insert a pruning Project that drops producer outputs
        // not needed by any consumer, changing output arity. Align the old output with the
        // same prune set so that ExprId changes of surviving slots are still propagated.
        Set<Slot> neededProducerOutputs = cascadesContext.getStatementContext()
                .getCteIdToOutputIds().get(cteAnchor.getCteId());
        if (neededProducerOutputs != null && neededProducerOutputs.size() < oldProducerOutput.size()) {
            oldProducerOutput = oldProducerOutput.stream()
                    .filter(neededProducerOutputs::contains)
                    .collect(Collectors.toList());
        }
        outer = syncCteConsumerSlotMaps(oldProducerOutput, producer.getOutput(),
                cteAnchor.getCteId(), outer, cascadesContext);
        return cteAnchor.withChildren(producer, outer);
    }

    /**
     * If the producer rewrite changed output ExprIds (e.g. any_value wrapping in
     * EliminateGroupByKey), update CTEConsumer slot maps in the consumer tree to match.
     *
     * @return the consumer tree, updated if any producer ExprIds changed
     */
    private LogicalPlan syncCteConsumerSlotMaps(List<Slot> oldProducerOutput, List<Slot> newProducerOutput,
            CTEId cteId, LogicalPlan outer, CascadesContext cascadesContext) {
        if (oldProducerOutput.size() != newProducerOutput.size()) {
            return outer;
        }
        Map<ExprId, ExprId> exprIdReplaceMap = new HashMap<>();
        for (int i = 0; i < oldProducerOutput.size(); i++) {
            ExprId oldId = oldProducerOutput.get(i).getExprId();
            ExprId newId = newProducerOutput.get(i).getExprId();
            if (!oldId.equals(newId)) {
                exprIdReplaceMap.put(oldId, newId);
            }
        }
        if (exprIdReplaceMap.isEmpty()) {
            return outer;
        }
        // Collect old→new CTEConsumer mappings by walking the consumer tree.
        Map<LogicalCTEConsumer, LogicalCTEConsumer> oldToNew = new LinkedHashMap<>();
        outer.foreach(p -> {
            if (p instanceof LogicalCTEConsumer) {
                LogicalCTEConsumer consumer = (LogicalCTEConsumer) p;
                if (consumer.getCteId().equals(cteId)) {
                    oldToNew.put(consumer, updateCteConsumerSlotMaps(consumer, exprIdReplaceMap));
                }
            }
            return false;
        });
        if (oldToNew.isEmpty()) {
            return outer;
        }
        outer = (LogicalPlan) outer.rewriteUp(p -> {
            Plan replacement = oldToNew.get(p);
            return replacement != null ? replacement : p;
        });
        // Re-collect updated consumers so cteIdToConsumers stays in sync.
        Set<LogicalCTEConsumer> updatedConsumers = Sets.newHashSet();
        outer.foreach(p -> {
            if (p instanceof LogicalCTEConsumer) {
                LogicalCTEConsumer c = (LogicalCTEConsumer) p;
                if (c.getCteId().equals(cteId)) {
                    updatedConsumers.add(c);
                }
            }
            return false;
        });
        cascadesContext.getCteIdToConsumers().put(cteId, updatedConsumers);
        cascadesContext.getStatementContext().getRewrittenCteConsumer().put(cteId, outer);
        return outer;
    }

    /**
     * Rebuild CTEConsumer slot maps so that producer-side slots reference the new ExprIds
     * produced by aggregate rewriting (e.g. any_value wrapping in EliminateGroupByKey).
     */
    private LogicalCTEConsumer updateCteConsumerSlotMaps(
            LogicalCTEConsumer cteConsumer, Map<ExprId, ExprId> exprIdReplaceMap) {
        Map<Slot, Slot> newConsumerToProducer = new LinkedHashMap<>();
        Multimap<Slot, Slot> newProducerToConsumer = LinkedHashMultimap.create();
        for (Slot producerSlot : cteConsumer.getConsumerToProducerOutputMap().values()) {
            ExprId newExprId = exprIdReplaceMap.get(producerSlot.getExprId());
            Slot effectiveProducerSlot = newExprId != null
                    ? (Slot) producerSlot.withExprId(newExprId)
                    : producerSlot;
            for (Slot consumerSlot : cteConsumer.getProducerToConsumerOutputMap().get(producerSlot)) {
                newProducerToConsumer.put(effectiveProducerSlot, consumerSlot);
                newConsumerToProducer.put(consumerSlot, effectiveProducerSlot);
            }
        }
        return (LogicalCTEConsumer) cteConsumer.withTwoMaps(newConsumerToProducer, newProducerToConsumer);
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
