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
import org.apache.doris.nereids.trees.plans.GroupPlan;
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
    private final Pattern<Plan> pattern;
    private final GroupExpression groupExpression;

    public GroupExpressionMatching(Pattern<? extends Plan> pattern, GroupExpression groupExpression) {
        this.pattern = (Pattern<Plan>) Objects.requireNonNull(pattern, "pattern can not be null");
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
        public GroupExpressionIterator(Pattern<Plan> pattern, GroupExpression groupExpression) {
            if (!pattern.matchRoot(groupExpression.getPlan())) {
                return;
            }

            int childrenGroupArity = groupExpression.arity();
            if (!(pattern instanceof SubTreePattern)) {
                // (logicalFilter(), multi()) match (logicalFilter()),
                // but (logicalFilter(), logicalFilter(), multi()) not match (logicalFilter())
                boolean extraMulti = pattern.arity() == childrenGroupArity + 1
                        && (pattern.hasMultiChild() || pattern.hasMultiGroupChild());
                if (pattern.arity() > childrenGroupArity && !extraMulti) {
                    return;
                }

                // (multi()) match (logicalFilter(), logicalFilter()),
                // but (logicalFilter()) not match (logicalFilter(), logicalFilter())
                if (!pattern.isAny() && pattern.arity() < childrenGroupArity
                        && !pattern.hasMultiChild() && !pattern.hasMultiGroupChild()) {
                    return;
                }
            }

            // Pattern.GROUP / Pattern.MULTI / Pattern.MULTI_GROUP can not match GroupExpression
            if (pattern.isGroup() || pattern.isMulti() || pattern.isMultiGroup()) {
                return;
            }

            // getPlan return the plan with GroupPlan as children
            Plan root = groupExpression.getPlan();
            if (pattern.arity() == 0 && !(pattern instanceof SubTreePattern)) {
                if (pattern.matchPredicates(root)) {
                    // if no children pattern, we treat all children as GROUP. e.g. Pattern.ANY.
                    // leaf plan will enter this branch too, e.g. logicalRelation().
                    results.add(root.withGroupExpression(Optional.of(groupExpression)));
                }
            } else if (childrenGroupArity > 0) {
                // matching children group, one List<Plan> per child
                // first dimension is every child group's plan
                // second dimension is all matched plan in one group
                List<List<Plan>> childrenPlans = Lists.newArrayListWithCapacity(childrenGroupArity);
                for (int i = 0; i < childrenGroupArity; ++i) {
                    Group childGroup = groupExpression.child(i);
                    List<Plan> childrenPlan = matchingChildGroup(pattern, childGroup, i);

                    if (childrenPlan.isEmpty()) {
                        if (pattern instanceof SubTreePattern) {
                            childrenPlan = ImmutableList.of(new GroupPlan(childGroup));
                        } else {
                            // current pattern is match but children patterns not match
                            return;
                        }
                    }
                    childrenPlans.add(childrenPlan);
                }
                assembleAllCombinationPlanTree(root, pattern, groupExpression, childrenPlans);
            } else if (pattern.arity() == 1 && (pattern.hasMultiChild() || pattern.hasMultiGroupChild())) {
                // leaf group with multi child pattern
                // e.g. logicalPlan(multi()) match LogicalOlapScan, because LogicalOlapScan is LogicalPlan
                //      and multi() pattern indicate zero or more children()
                if (pattern.matchPredicates(root)) {
                    results.add(root);
                }
            }
        }

        private List<Plan> matchingChildGroup(Pattern<? extends Plan> parentPattern,
                Group childGroup, int childIndex) {
            Pattern<? extends Plan> childPattern;
            if (parentPattern instanceof SubTreePattern) {
                childPattern = parentPattern;
            } else {
                boolean isLastPattern = childIndex + 1 >= parentPattern.arity();
                int patternChildIndex = isLastPattern ? parentPattern.arity() - 1 : childIndex;

                childPattern = parentPattern.child(patternChildIndex);
                // translate MULTI and MULTI_GROUP to ANY and GROUP
                if (isLastPattern) {
                    if (childPattern.isMulti()) {
                        childPattern = Pattern.ANY;
                    } else if (childPattern.isMultiGroup()) {
                        childPattern = Pattern.GROUP;
                    }
                }
            }

            ImmutableList.Builder<Plan> matchingChildren = ImmutableList.builder();
            new GroupMatching(childPattern, childGroup).forEach(matchingChildren::add);
            return matchingChildren.build();
        }

        private void assembleAllCombinationPlanTree(Plan root, Pattern<Plan> rootPattern,
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

                LogicalProperties logicalProperties = groupExpression.getOwnerGroup().getLogicalProperties();
                // assemble children: replace GroupPlan to real plan,
                // withChildren will erase groupExpression, so we must
                // withGroupExpression too.
                Plan rootWithChildren = root.withChildren(children)
                        .withLogicalProperties(Optional.of(logicalProperties))
                        .withGroupExpression(Optional.of(groupExpression));
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
