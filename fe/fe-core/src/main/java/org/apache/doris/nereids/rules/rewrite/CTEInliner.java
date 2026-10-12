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
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.NondeterministicFunctionCollector;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generate an inlined alternative plan for CTE optimization.
 *
 * This class supports two modes controlled by the {@code unionAllOnly} flag:
 * <ul>
 *   <li><b>Full inline mode</b> ({@code unionAllOnly=false}): inlines all eligible CTEs
 *       and adds the result as an alternative in the Memo root group so the CBO can
 *       compare materialized vs inlined costs.</li>
 *   <li><b>Selective inline mode</b> ({@code unionAllOnly=true}): only inlines CTEs whose
 *       body contains UNION ALL; after filter push-down some union branches may be
 *       eliminated, directly replacing the rewrite plan.</li>
 * </ul>
 *
 * In both modes, CTEs containing non-deterministic functions or marked as
 * force-materialized are never inlined.
 */
public class CTEInliner extends DefaultPlanRewriter<Void> {

    private final StatementContext statementContext;
    // Map from CTEId to the CTE producer node (extracted from CTEAnchor.left())
    private final Map<CTEId, LogicalCTEProducer<?>> cteProducers = new HashMap<>();
    private final boolean unionAllOnly;

    public CTEInliner(StatementContext statementContext) {
        this(statementContext, false);
    }

    public CTEInliner(StatementContext statementContext, boolean unionAllOnly) {
        this.statementContext = statementContext;
        this.unionAllOnly = unionAllOnly;
    }

    /**
     * Generate a fully inlined alternative plan.
     * Returns null if no CTEs can be inlined.
     */
    public Plan generateInlinedPlan(Plan plan) {
        // First pass: collect all CTE producers that can be inlined
        collectCTEProducers(plan);

        if (cteProducers.isEmpty()) {
            return null;
        }

        // Second pass: inline all collected CTEs
        return plan.accept(this, null);
    }

    private void collectCTEProducers(Plan plan) {
        plan.foreach(p -> {
            if (p instanceof LogicalCTEAnchor) {
                LogicalCTEAnchor<?, ?> anchor = (LogicalCTEAnchor<?, ?>) p;
                CTEId cteId = anchor.getCteId();
                if (!statementContext.isForceMaterializeCTE(cteId)) {
                    LogicalCTEProducer<?> producer = (LogicalCTEProducer<?>) anchor.left();
                    if (containsNondeterministicFunction(producer)) {
                        // Never inline CTEs that contain non-deterministic functions,
                        // as inlining would cause each consumer to evaluate the function
                        // independently, changing query semantics.
                        return;
                    }
                    if (!unionAllOnly || containsUnionAll(producer)) {
                        cteProducers.put(cteId, producer);
                    }
                }
            }
        });
    }

    private boolean containsNondeterministicFunction(LogicalCTEProducer<?> producer) {
        List<Expression> nondeterministicFunctions = new ArrayList<>();
        producer.accept(NondeterministicFunctionCollector.INSTANCE, nondeterministicFunctions);
        return !nondeterministicFunctions.isEmpty();
    }

    private boolean containsUnionAll(LogicalCTEProducer<?> producer) {
        return producer.child().anyMatch(
                p -> p instanceof LogicalUnion && ((LogicalUnion) p).getQualifier() == Qualifier.ALL);
    }

    @Override
    public Plan visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor, Void context) {
        CTEId cteId = cteAnchor.getCteId();
        if (cteProducers.containsKey(cteId)) {
            // Inline: skip anchor and producer, process the right (consumer) subtree
            return cteAnchor.right().accept(this, null);
        } else {
            // Force materialize: keep the structure, only process the right subtree
            Plan right = cteAnchor.right().accept(this, null);
            return cteAnchor.withChildren(cteAnchor.left(), right);
        }
    }

    @Override
    public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, Void context) {
        LogicalCTEProducer<?> producer = cteProducers.get(cteConsumer.getCteId());
        if (producer != null) {
            // Inline this consumer: deep copy producer body + slot remap + LogicalProject
            // wrapper
            // Same logic as CTEInline.visitLogicalCTEConsumer
            DeepCopierContext deepCopierContext = new DeepCopierContext();
            Plan inlinedPlan = LogicalPlanDeepCopier.INSTANCE
                    .deepCopy((LogicalPlan) producer.child(), deepCopierContext);
            List<NamedExpression> projects = Lists.newArrayList();
            for (Slot consumerSlot : cteConsumer.getOutput()) {
                Slot producerSlot = cteConsumer.getProducerSlot(consumerSlot);
                ExprId inlineExprId = deepCopierContext.exprIdReplaceMap.get(producerSlot.getExprId());
                List<Expression> childrenExprs = new ArrayList<>();
                childrenExprs.add(producerSlot.withExprId(inlineExprId));
                Alias alias = new Alias(consumerSlot.getExprId(), childrenExprs, consumerSlot.getName(),
                        producerSlot.getQualifier(), false);
                projects.add(alias);
            }
            Plan result = new LogicalProject<>(projects, inlinedPlan);
            // Recursively process in case the inlined body contains more CTE consumers
            return result.accept(this, null);
        }
        return cteConsumer;
    }
}
