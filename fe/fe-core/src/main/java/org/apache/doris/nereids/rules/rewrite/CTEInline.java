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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * pull up LogicalCteAnchor to the top of plan to avoid CteAnchor break other rewrite rules pattern
 * The front producer may depend on the back producer in {@code List<LogicalCTEProducer<Plan>>}
 * After this rule, we normalize all CteAnchor in plan, all CteAnchor under CteProducer should pull out
 * and put all of them to the top of plan depends on dependency tree of them.
 */
public class CTEInline extends DefaultPlanRewriter<LogicalCTEProducer<?>> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        Plan root = plan.accept(this, null);
        // collect cte id to consumer
        root.foreach(p -> {
            if (p instanceof LogicalCTEConsumer) {
                jobContext.getCascadesContext().putCTEIdToConsumer(((LogicalCTEConsumer) p));
            }
        });
        return root;
    }

    @Override
    public Plan visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            LogicalCTEProducer<?> producer) {
        if (producer != null) {
            // process upper anchor
            List<Plan> children = cteAnchor.children().stream()
                    .map(c -> c.accept(this, producer))
                    .collect(ImmutableList.toImmutableList());
            return cteAnchor.withChildren(children);
        } else {
            // process this anchor
            List<LogicalCTEConsumer> consumers = cteAnchor.child(1).collectToList(p -> {
                if (p instanceof LogicalCTEConsumer) {
                    return ((LogicalCTEConsumer) p).getCteId().equals(cteAnchor.getCteId());
                }
                return false;
            });
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext.getSessionVariable().enableCTEMaterialize
                    && consumers.size() > connectContext.getSessionVariable().inlineCTEReferencedThreshold) {
                // not inline
                Plan right = cteAnchor.right().accept(this, null);
                return cteAnchor.withChildren(cteAnchor.left(), right);
            } else {
                // should inline
                Plan root = cteAnchor.right().accept(this, (LogicalCTEProducer<?>) cteAnchor.left());
                // process child
                return root.accept(this, null);
            }
        }
    }

    @Override
    public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, LogicalCTEProducer<?> producer) {
        if (producer != null && cteConsumer.getCteId().equals(producer.getCteId())) {
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
            return new LogicalProject<>(projects, inlinedPlan);
        }
        return cteConsumer;
    }
}
