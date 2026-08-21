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

package org.apache.doris.nereids.rules.rewrite.joinorder;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import org.apache.commons.compress.utils.Lists;

import java.util.List;
import java.util.stream.Stream;

/**JoinReorderRule*/
public class JoinReorderRule extends DefaultPlanRewriter<Void> {
    public Plan rewrite(Plan plan, Void context) {
        return plan.accept(this, context);
    }

    @Override
    public Plan visitLogicalJoin(
            LogicalJoin<? extends Plan, ? extends Plan> join,
            Void context) {
        if (!isReorderable(join)) {
            // The current join is a boundary, but its children may contain independent join clusters.
            return DefaultPlanRewriter.visitChildren(this, join, context);
        }

        // The current join is the root of a cluster. collectAtomsAndPredicates flattens the cluster
        // and recursively processes independent clusters below its boundaries.
        return reorderCluster(join, context);
    }

    private Plan reorderCluster(
            LogicalJoin<? extends Plan, ? extends Plan> root,
            Void context) {
        JoinCluster cluster = new JoinCluster(root.getOutput());

        // TODO: Remove the unused context parameter.
        collectAtomsAndPredicates(root, cluster, context);
        Plan res = reorder(cluster);
        return res == null ? root : res;
    }

    private void collectAtomsAndPredicates(
            Plan plan,
            JoinCluster cluster,
            Void context) {
        if (plan instanceof LogicalJoin
                && isReorderable((LogicalJoin<?, ?>) plan)) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;

            cluster.addPredicates(join.getHashJoinConjuncts());
            cluster.addPredicates(join.getOtherJoinConjuncts());

            collectAtomsAndPredicates(join.left(), cluster, context);
            collectAtomsAndPredicates(join.right(), cluster, context);
            return;
        }
        if (plan instanceof LogicalProject
                && isTransparentProject((LogicalProject<?>) plan)) {
            LogicalProject<?> project = (LogicalProject<?>) plan;

            /*
             * The project contains only existing slots and does not replace any ExprId, so predicates
             * from upper joins do not need to be rewritten and flattening can continue through it.
             * The project at the cluster root restores column pruning and the original output order.
             */
            collectAtomsAndPredicates(project.child(), cluster, context);
            return;
        }

        /*
         * The plan is a boundary of the current join cluster.
         * A boundary may contain independent join clusters, so rewrite it before adding it as an atom.
         */
        Plan rewrittenAtom = plan.accept(this, context);
        cluster.addInput(rewrittenAtom);
    }

    private boolean isTransparentProject(LogicalProject<?> project) {
        return !project.isDistinct() && project.isAllSlots();
    }

    private Plan reorder(JoinCluster joinCluster) {
        JoinReorderGreedy reorderGreedy = new JoinReorderGreedy();
        reorderGreedy.reorder(joinCluster.inputs, joinCluster.predicates);
        List<Plan> plans = reorderGreedy.getResult();
        if (plans.isEmpty()) {
            return null;
        } else {
            Plan afterReorder = plans.get(0);
            if (joinCluster.originalOutput.equals(afterReorder.getOutput())) {
                return afterReorder;
            } else {
                return new LogicalProject<>((List) joinCluster.originalOutput, afterReorder);
            }
        }
    }

    private static class JoinCluster {
        private final List<Plan> inputs = Lists.newArrayList();
        private final List<Expression> predicates = Lists.newArrayList();
        private final List<Slot> originalOutput;

        JoinCluster(List<Slot> originalOutput) {
            this.originalOutput = originalOutput;
        }

        private void addInput(Plan input) {
            inputs.add(input);
        }

        private void addPredicates(List<Expression> predicates) {
            this.predicates.addAll(predicates);
        }
    }

    private boolean isReorderable(LogicalJoin<?, ?> join) {
        return join.getJoinType().isInnerOrCrossJoin()
                && !join.isMarkJoin()
                && !join.getJoinType().isAsofJoin()
                && !join.isLeadingJoin()
                && !join.hasDistributeHint()
                && Stream.concat(
                        join.getHashJoinConjuncts().stream(),
                        join.getOtherJoinConjuncts().stream())
                .noneMatch(Expression::containsVolatileExpression);
    }
}
