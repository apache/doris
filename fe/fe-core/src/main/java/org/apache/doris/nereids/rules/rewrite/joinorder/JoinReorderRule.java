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
    // Join Reorder
    // 输入一个plan,输出一个reorder之后的plan
    // public Plan rewrite(Plan plan) {
    //
    //     List<Plan> joinClusters = Lists.newArrayList();
    //     // 1.提取join cluster List
    //     collectJoinCluster(plan, joinClusters);
    //     // 2.遍历list, to multijoin
    //     for (Plan joinCluster : joinClusters) {
    //         // 将joinCluster转化为multijoin
    //         MultiJoin multiJoin = toMultiJoin(joinCluster);
    //         // reorder Join 返回一个 plan
    //         Plan afterReorder = joinReorder(multiJoin);
    //         // 需要怎么组装回去
    //     }
    //     return plan;
    // }
    //
    // public Plan rewriteImplement(Plan plan) {
    //     // 输入：一棵plan树
    //     // 输出:对每个join cluster都reorder之后的plan树
    //
    //     // 什么输入不需要继续拆了？
    //     // 没有inner join / cross join的 不需要继续拆了
    //     // !plan.contains(join)
    //     // 这个可能会太慢了吧
    //     // plan 不是join ， 并且plan 也不是project + join，那么就不需要要继续拆分了，直接返回就行了。
    //     // project + join 或者是 join 是需要继续遍历孩子节点的。其他的节点都不需要。
    //     // if (!(plan instanceof LogicalJoin) && !(plan instanceof LogicalProject<?>
    //     //         && (plan.child(0) instanceof LogicalJoin))) {
    //     //     return plan;
    //     // }
    //     // 也不对啊，一个window孩子是join，那么也需要继续往下处理呢。
    //     // 那就是有join 就需要继续处理，那每次走到一个节点，都要遍历下面节点找到一个join吗？感觉时间复杂度有些高
    //
    //
    //
    // }
    //
    // private void collectJoinCluster(Plan plan, List<Plan> joinClusters) {
    //
    // }
    //
    // private MultiJoin toMultiJoin(Plan plan) {
    //
    // }
    //
    // private Plan joinReorder(MultiJoin multiJoin) {
    //
    // }

    public Plan rewrite(Plan plan, Void context) {
        return plan.accept(this, context);
    }

    @Override
    public Plan visitLogicalJoin(
            LogicalJoin<? extends Plan, ? extends Plan> join,
            Void context) {
        if (!isReorderable(join)) {
            // 当前 Join 是边界，但继续搜索它的孩子
            return DefaultPlanRewriter.visitChildren(this, join, context);
        }

        // 当前 Join 是一个 cluster root。
        // collectCluster 负责继续拍平同一个 cluster，
        // 并负责递归处理 cluster 边界下面的独立 cluster。
        return reorderCluster(join, context);
    }

    private Plan reorderCluster(
            LogicalJoin<? extends Plan, ? extends Plan> root,
            Void context) {
        JoinCluster cluster = new JoinCluster(root.getOutput());

        // context没用，需要删除一下哈
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
             * Project 只包含原始 Slot，ExprId 没有发生替换。
             * 因此上层 Join predicate 不需要改写，可以直接继续拍平。
             *
             * Project 的列裁剪和输出顺序由 cluster root 上的
             * restoreOutputProject 统一恢复。
             */
            collectAtomsAndPredicates(project.child(), cluster, context);
            return;
        }

        /*
         * plan 是当前 Join Cluster 的边界。
         *
         * 但边界节点下面可能还有独立的 Join Cluster，
         * 所以不能直接 cluster.addInput(plan)。
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
            // 这个地方应该根据joinCluster.originalOutput 加上一个project吧
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
                && Stream.concat(
                        join.getHashJoinConjuncts().stream(),
                        join.getOtherJoinConjuncts().stream())
                .noneMatch(Expression::containsVolatileExpression);
    }
}
