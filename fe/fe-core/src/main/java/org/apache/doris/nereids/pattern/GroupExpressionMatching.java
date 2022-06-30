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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * Get all pattern matching subtree in query plan from a group expression.
 */
public class GroupExpressionMatching implements Iterable<Plan> {
    private final Pattern<Plan, Plan> pattern;
    private final GroupExpression groupExpression;

    public GroupExpressionMatching(Pattern<? extends Plan, Plan> pattern, GroupExpression groupExpression) {
        this.pattern = (Pattern<Plan, Plan>) Objects.requireNonNull(pattern, "pattern can not be null");
        this.groupExpression = Objects.requireNonNull(groupExpression, "groupExpression can not be null");
    }

    @Override
    public GroupExpressionIterator iterator() {
        return new GroupExpressionIterator(pattern, groupExpression);
    }

    /**
     * Iterator to get all subtrees.
     */
    public static class GroupExpressionIterator implements Iterator<Plan> {
        private final List<Plan> results = Lists.newArrayList();
        private int resultIndex = 0;

        /**
         * Constructor.
         *
         * @param pattern pattern to match
         * @param groupExpression group expression to be matched
         */
        public GroupExpressionIterator(Pattern<Plan, Plan> pattern, GroupExpression groupExpression) {
            if (!pattern.matchOperator(groupExpression.getOperator())) {
                return;
            }

            // (logicalFilter(), multi()) match (logicalFilter()),
            // but (logicalFilter(), logicalFilter(), multi()) not match (logicalFilter())
            boolean extraMulti = pattern.arity() == groupExpression.arity() + 1
                    && (pattern.hasMultiChild() || pattern.hasMultiGroupChild());
            if (pattern.arity() > groupExpression.arity() && !extraMulti) {
                return;
            }

            // (multi()) match (logicalFilter(), logicalFilter()),
            // but (logicalFilter()) not match (logicalFilter(), logicalFilter())
            if (!pattern.isAny() && pattern.arity() < groupExpression.arity()
                    && !pattern.hasMultiChild() && !pattern.hasMultiGroupChild()) {
                return;
            }

            // Pattern.GROUP / Pattern.MULTI / Pattern.MULTI_GROUP can not match GroupExpression
            if (pattern.isGroup() || pattern.isMulti() || pattern.isMultiGroup()) {
                return;
            }

            // toTreeNode will wrap operator to plan, and set GroupPlan as children placeholder
            Plan root = groupExpression.getOperator().toTreeNode(groupExpression);
            // pattern.arity() == 0 equals to root.arity() == 0
            if (pattern.arity() == 0) {
                if (pattern.matchPredicates(root)) {
                    // if no children pattern, we treat all children as GROUP. e.g. Pattern.ANY.
                    // leaf plan will enter this branch too, e.g. logicalRelation().
                    results.add(root);
                }
            } else {
                // matching children group, one List<Plan> per child
                // first dimension is every child group's plan
                // second dimension is all matched plan in one group
                List<List<Plan>> childrenPlans = Lists.newArrayListWithCapacity(groupExpression.arity());
                for (int i = 0; i < groupExpression.arity(); ++i) {
                    Group childGroup = groupExpression.child(i);
                    List<Plan> childrenPlan = matchingChildGroup(pattern, childGroup, i);
                    childrenPlans.add(childrenPlan);
                    if (childrenPlan.isEmpty()) {
                        // current pattern is match but children patterns not match
                        return;
                    }
                }

                assembleAllCombinationPlanTree(root, pattern, groupExpression, childrenPlans);
            }
        }

        private List<Plan> matchingChildGroup(Pattern<? extends Plan, Plan> parentPattern,
                                              Group childGroup, int childIndex) {
            boolean isLastPattern = childIndex + 1 >= parentPattern.arity();
            int patternChildIndex = isLastPattern ? parentPattern.arity() - 1 : childIndex;
            Pattern<? extends Plan, Plan> childPattern = parentPattern.child(patternChildIndex);

            // translate MULTI and MULTI_GROUP to ANY and GROUP
            if (isLastPattern) {
                if (childPattern.isMulti()) {
                    childPattern = Pattern.ANY;
                } else if (childPattern.isMultiGroup()) {
                    childPattern = Pattern.GROUP;
                }
            }

            ImmutableList.Builder<Plan> matchingChildren = ImmutableList.builder();
            new GroupMatching(childPattern, childGroup).forEach(matchingChildren::add);
            return matchingChildren.build();
        }

        private void assembleAllCombinationPlanTree(Plan root, Pattern<Plan, Plan> rootPattern,
                                                    GroupExpression groupExpression,
                                                    List<List<Plan>> childrenPlans) {
            int[] childrenPlanIndex = new int[childrenPlans.size()];
            int offset = 0;

            // assemble all combination of plan tree by current root plan and children plan
            while (offset < childrenPlans.size()) {
                List<Plan> children = Lists.newArrayList();
                for (int i = 0; i < childrenPlans.size(); i++) {
                    children.add(childrenPlans.get(i).get(childrenPlanIndex[i]));
                }

                LogicalProperties logicalProperties = groupExpression.getParent().getLogicalProperties();
                // assemble children: replace GroupPlan to real plan,
                // withChildren will erase groupExpression, so we must
                // withGroupExpression too.
                Plan rootWithChildren = root.withChildren(children)
                        .withGroupExpression(root.getGroupExpression())
                        .withLogicalProperties(Optional.of(logicalProperties));
                if (rootPattern.matchPredicates(rootWithChildren)) {
                    results.add(rootWithChildren);
                }
                offset = 0;
                while (true) {
                    childrenPlanIndex[offset]++;
                    if (childrenPlanIndex[offset] == childrenPlans.get(offset).size()) {
                        childrenPlanIndex[offset] = 0;
                        offset++;
                        if (offset == childrenPlans.size()) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return resultIndex < results.size();
        }

        @Override
        public Plan next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return results.get(resultIndex++);
        }
    }
}
