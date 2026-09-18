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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Resolve the ctes which {@link CTEInline} kept materialized because they contain a volatile expression.
 *
 * <p>Every cte referenced by the recursive child of a recursive cte has to be inlined, because the
 * recursive child is reset and re-executed on every iteration and can not read a materialized cte.
 * A cte containing a volatile expression (rand(), uuid(), a volatile udf, ...) can not be inlined as a
 * whole either: the volatile expression would be evaluated once per iteration and once per reference
 * instead of once for the whole statement.
 *
 * <p>This rule runs after the recursive side has been simplified and the ctes have been pruned
 * ({@code RewriteCteChildren}), then decides per consumer whether the required outputs can be computed
 * without any volatile expression:
 * <ul>
 * <li>the producer does not contain a volatile expression anymore: the consumer is inlined as it is.</li>
 * <li>only outputs which the recursive side does not use hold a volatile expression: those outputs are
 * pruned from the inlined copy, while the materialized copy is kept for the other consumers of the
 * cte.</li>
 * <li>the recursive side still needs a volatile expression: the query is rejected, because neither
 * inlining nor keeping the cte materialized preserves the "evaluated once" semantics of the cte.</li>
 * </ul>
 *
 * <p>A recursive cte whose anchor is provably empty never starts the recursive execution, so nothing
 * has to be inlined there.
 */
public class CheckMustInlineVolatileCTE extends DefaultPlanRewriter<Void> implements CustomRewriter {
    private static final String INLINE_BLOCKED_MESSAGE = "recursive cte must inline all used ctes,"
            + " but inline is blocked by volatile function";

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        StatementContext statementContext = jobContext.getCascadesContext().getStatementContext();
        Set<CTEId> deferredCTEs = statementContext.getDeferredInlineVolatileCTEs();
        if (deferredCTEs.isEmpty()) {
            return plan;
        }
        while (true) {
            Map<LogicalCTEConsumer, CTEId> recursiveConsumers = collectRecursiveConsumers(plan, deferredCTEs);
            if (recursiveConsumers.isEmpty()) {
                return plan;
            }
            Set<ExprId> referencedExprIds = collectReferencedExprIds(plan);
            Map<LogicalCTEConsumer, Plan> replacements = new IdentityHashMap<>();
            for (Map.Entry<LogicalCTEConsumer, CTEId> entry : recursiveConsumers.entrySet()) {
                LogicalCTEProducer<?> producer = statementContext.getCteProducerByCteId(entry.getValue());
                if (producer == null) {
                    throw new AnalysisException(INLINE_BLOCKED_MESSAGE);
                }
                replacements.put(entry.getKey(), buildInline(entry.getKey(), producer.child(), referencedExprIds));
            }
            statementContext.getCteIdToConsumers().values()
                    .forEach(consumers -> consumers.removeIf(replacements::containsKey));
            plan = plan.rewriteDownShortCircuit(node -> replacements.getOrDefault(node, node));
        }
    }

    /**
     * Build the replacement of a consumer which sits below a recursive side. Only the outputs which are
     * referenced by the rest of the plan are kept, so a volatile output which the recursive side does
     * not use is pruned away instead of rejecting the query.
     */
    private Plan buildInline(LogicalCTEConsumer consumer, Plan producerBody, Set<ExprId> referencedExprIds) {
        List<Slot> requiredConsumerSlots = new ArrayList<>();
        for (Slot consumerSlot : consumer.getOutput()) {
            if (referencedExprIds.contains(consumerSlot.getExprId())) {
                requiredConsumerSlots.add(consumerSlot);
            }
        }
        if (requiredConsumerSlots.isEmpty()) {
            // nothing of this consumer is referenced, keep a single output to keep the plan valid
            requiredConsumerSlots.add(consumer.getOutput().get(0));
        }
        if (CTEInline.containsVolatileExpression(producerBody)) {
            Set<ExprId> requiredProducerExprIds = new HashSet<>(requiredConsumerSlots.size());
            for (Slot consumerSlot : requiredConsumerSlots) {
                requiredProducerExprIds.add(consumer.getProducerSlot(consumerSlot).getExprId());
            }
            producerBody = pruneUnneededOutputs(producerBody, requiredProducerExprIds)
                    .orElseThrow(() -> new AnalysisException(INLINE_BLOCKED_MESSAGE));
        }
        return CTEInline.inlineConsumer(consumer, producerBody, requiredConsumerSlots);
    }

    /**
     * Collect the consumers of the deferred ctes which are located below the recursive side of a
     * recursive cte. A recursive cte whose anchor is provably empty never starts the recursive
     * execution, so its recursive side never reads the cte again and is not collected.
     */
    private Map<LogicalCTEConsumer, CTEId> collectRecursiveConsumers(Plan plan, Set<CTEId> deferredCTEs) {
        Map<LogicalCTEConsumer, CTEId> recursiveConsumers = new IdentityHashMap<>();
        plan.foreach(node -> {
            if (!(node instanceof LogicalRecursiveUnion)) {
                return;
            }
            LogicalRecursiveUnion<?, ?> recursiveUnion = (LogicalRecursiveUnion<?, ?>) node;
            if (isProvablyEmpty(recursiveUnion.child(0))) {
                return;
            }
            recursiveUnion.child(1).foreach(recursiveSide -> {
                if (recursiveSide instanceof LogicalCTEConsumer) {
                    LogicalCTEConsumer consumer = (LogicalCTEConsumer) recursiveSide;
                    if (deferredCTEs.contains(consumer.getCteId())) {
                        recursiveConsumers.put(consumer, consumer.getCteId());
                    }
                }
            });
        });
        return recursiveConsumers;
    }

    private boolean isProvablyEmpty(Plan plan) {
        if (plan instanceof LogicalEmptyRelation) {
            return true;
        }
        if (plan instanceof LogicalRecursiveUnionAnchor || plan instanceof LogicalProject
                || plan instanceof LogicalFilter || plan instanceof LogicalSort || plan instanceof LogicalLimit) {
            return isProvablyEmpty(plan.child(0));
        }
        return false;
    }

    private Set<ExprId> collectReferencedExprIds(Plan plan) {
        Set<ExprId> referencedExprIds = new HashSet<>();
        plan.foreach(node -> {
            if (node instanceof Plan) {
                for (Expression expression : ((Plan) node).getExpressions()) {
                    collectSlotExprIds(expression, referencedExprIds);
                }
            }
        });
        return referencedExprIds;
    }

    private void collectSlotExprIds(Expression expression, Set<ExprId> referencedExprIds) {
        if (expression instanceof Slot) {
            referencedExprIds.add(((Slot) expression).getExprId());
        }
        for (Expression child : expression.children()) {
            collectSlotExprIds(child, referencedExprIds);
        }
    }

    /**
     * Prune the outputs which are not required from a copy of the producer body, so that the copy which
     * is inlined into the recursive side does not have to evaluate volatile expressions which are only
     * needed by the other consumers of the cte.
     *
     * <p>Returns empty when the required outputs can not be computed without a volatile expression.
     */
    private Optional<Plan> pruneUnneededOutputs(Plan plan, Set<ExprId> requiredExprIds) {
        if (requiredExprIds.isEmpty()) {
            return CTEInline.containsVolatileExpression(plan) ? Optional.empty() : Optional.of(plan);
        }
        if (plan instanceof LogicalProject) {
            return pruneProject((LogicalProject<?>) plan, requiredExprIds);
        }
        if (plan instanceof LogicalFilter || plan instanceof LogicalSort || plan instanceof LogicalLimit) {
            Set<ExprId> childRequiredExprIds = new HashSet<>(requiredExprIds);
            for (Expression expression : plan.getExpressions()) {
                if (expression.containsVolatileExpression()) {
                    return Optional.empty();
                }
                childRequiredExprIds.addAll(expression.getInputSlotExprIds());
            }
            return pruneUnneededOutputs(plan.child(0), childRequiredExprIds)
                    .map(child -> plan.withChildren(ImmutableList.of(child)));
        }
        // other nodes compute their outputs from their inputs, keep them unchanged. Such a copy is only
        // inlined when it does not contain any volatile expression at all.
        return CTEInline.containsVolatileExpression(plan) ? Optional.empty() : Optional.of(plan);
    }

    private Optional<Plan> pruneProject(LogicalProject<?> project, Set<ExprId> requiredExprIds) {
        if (project.isDistinct()) {
            // pruning the outputs of a distinct project could merge rows
            return CTEInline.containsVolatileExpression(project) ? Optional.empty() : Optional.of(project);
        }
        List<NamedExpression> keptProjects = new ArrayList<>(requiredExprIds.size());
        Set<ExprId> childRequiredExprIds = new HashSet<>();
        for (NamedExpression projectOutput : project.getProjects()) {
            if (!requiredExprIds.contains(projectOutput.getExprId())) {
                continue;
            }
            if (projectOutput.containsVolatileExpression()) {
                return Optional.empty();
            }
            keptProjects.add(projectOutput);
            childRequiredExprIds.addAll(projectOutput.getInputSlotExprIds());
        }
        if (keptProjects.size() != requiredExprIds.size()) {
            // the project does not produce all required outputs, do not prune it
            return CTEInline.containsVolatileExpression(project) ? Optional.empty() : Optional.of(project);
        }
        return pruneUnneededOutputs(project.child(), childRequiredExprIds).map(
                child -> project.withProjectsAndChild(keptProjects, (LogicalPlan) child));
    }
}
