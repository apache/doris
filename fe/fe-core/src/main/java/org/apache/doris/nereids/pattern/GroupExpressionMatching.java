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
        private int resultsSize;

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
            int patternArity = pattern.arity();
            // (logicalFilter(), multi()) match (logicalFilter()),
            // but (logicalFilter(), logicalFilter(), multi()) not match (logicalFilter())
            boolean extraMulti = patternArity == childrenGroupArity + 1
                    && (pattern.hasMultiChild() || pattern.hasMultiGroupChild());
            if (patternArity > childrenGroupArity && !extraMulti) {
                return;
            }

            // (multi()) match (logicalFilter(), logicalFilter()),
            // but (logicalFilter()) not match (logicalFilter(), logicalFilter())
            if (!pattern.isAny() && patternArity < childrenGroupArity
                    && !pattern.hasMultiChild() && !pattern.hasMultiGroupChild()) {
                return;
            }

            // Pattern.GROUP / Pattern.MULTI / Pattern.MULTI_GROUP can not match GroupExpression
            if (pattern.isGroup() || pattern.isMulti() || pattern.isMultiGroup()) {
                return;
            }

            // getPlan return the plan with GroupPlan as children
            Plan root = groupExpression.getPlan();
            if (patternArity == 0) {
                if (pattern.matchPredicates(root)) {
                    // if no children pattern, we treat all children as GROUP. e.g. Pattern.ANY.
                    // leaf plan will enter this branch too, e.g. logicalRelation().
                    results.add(root);
                }
            } else if (childrenGroupArity > 0) {
                // matching children group, one List<Plan> per child
                // first dimension is every child group's plan
                // second dimension is all matched plan in one group
                List<Plan>[] childrenPlans = new List[childrenGroupArity];
                for (int i = 0; i < childrenGroupArity; ++i) {
                    Group childGroup = groupExpression.child(i);
                    List<Plan> childrenPlan = matchingChildGroup(pattern, childGroup, i);

                    if (childrenPlan.isEmpty()) {
                        // current pattern is match but children patterns not match
                        return;
                    }
                    childrenPlans[i] = childrenPlan;
                }
                assembleAllCombinationPlanTree(root, pattern, groupExpression, childrenPlans);
            } else if (patternArity == 1 && (pattern.hasMultiChild() || pattern.hasMultiGroupChild())) {
                // leaf group with multi child pattern
                // e.g. logicalPlan(multi()) match LogicalOlapScan, because LogicalOlapScan is LogicalPlan
                //      and multi() pattern indicate zero or more children()
                if (pattern.matchPredicates(root)) {
                    results.add(root);
                }
            }
            this.resultsSize = results.size();
        }

        private List<Plan> matchingChildGroup(Pattern<? extends Plan> parentPattern,
                Group childGroup, int childIndex) {
            Pattern<? extends Plan> childPattern;
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

            List<Plan> matchingChildren = GroupMatching.getAllMatchingPlans(childPattern, childGroup);
            return matchingChildren;
        }

        private void assembleAllCombinationPlanTree(Plan root, Pattern<Plan> rootPattern,
                GroupExpression groupExpression, List<Plan>[] childrenPlans) {
            int childrenPlansSize = childrenPlans.length;
            int[] childrenPlanIndex = new int[childrenPlansSize];
            int offset = 0;
            LogicalProperties logicalProperties = groupExpression.getOwnerGroup().getLogicalProperties();

            // assemble all combination of plan tree by current root plan and children plan
            Optional<GroupExpression> groupExprOption = Optional.of(groupExpression);
            Optional<LogicalProperties> logicalPropOption = Optional.of(logicalProperties);
            while (offset < childrenPlansSize) {
                ImmutableList.Builder<Plan> childrenBuilder = ImmutableList.builderWithExpectedSize(childrenPlansSize);
                for (int i = 0; i < childrenPlansSize; i++) {
                    childrenBuilder.add(childrenPlans[i].get(childrenPlanIndex[i]));
                }
                List<Plan> children = childrenBuilder.build();

                // assemble children: replace GroupPlan to real plan,
                // withChildren will erase groupExpression, so we must
                // withGroupExpression too.
                Plan rootWithChildren = root.withGroupExprLogicalPropChildren(groupExprOption,
                        logicalPropOption, children);
                if (rootPattern.matchPredicates(rootWithChildren)) {
                    results.add(rootWithChildren);
                }
                for (offset = 0; offset < childrenPlansSize; offset++) {
                    childrenPlanIndex[offset]++;
                    if (childrenPlanIndex[offset] == childrenPlans[offset].size()) {
                        // Reset the index when it reaches the size of the current child plan list
                        childrenPlanIndex[offset] = 0;
                    } else {
                        break;  // Break the loop when the index is within the size of the current child plan list
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return resultIndex < resultsSize;
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
