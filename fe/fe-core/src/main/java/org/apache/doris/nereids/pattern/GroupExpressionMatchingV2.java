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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanBuilder;

import com.google.common.collect.Lists;
import org.apache.commons.collections.ArrayStack;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * optimized group expression matching.
 */
public class GroupExpressionMatchingV2 implements Iterable<Plan> {
    private final Pattern<? extends Plan> pattern;
    private final GroupExpression groupExpression;

    public GroupExpressionMatchingV2(Pattern<? extends Plan> pattern, GroupExpression groupExpression) {
        this.pattern = Objects.requireNonNull(pattern, "pattern can not be null");
        this.groupExpression = Objects.requireNonNull(groupExpression, "groupExpression can not be null");
    }

    @Override
    public GroupExpressionMatchingV2.GroupExpressionIterator iterator() {
        return new GroupExpressionMatchingV2.GroupExpressionIterator(this);
    }

    /**
     * Iterator to get all subtrees.
     * The algorithm use a stack to maintain bfs.
     */
    public class GroupExpressionIterator implements Iterator<Plan> {
        private final GroupExpressionMatchingV2 container;
        private final ArrayStack stack = new ArrayStack();
        private Plan result;
        private int childIndex = 0;
        private int patternChildIndex = 0;
        private int curChildIndex = 0;

        public GroupExpressionIterator(GroupExpressionMatchingV2 container) {
            this.container = container;
            stack.push(0);
        }

        @Override
        public Plan next() {
            return result;
        }

        @Override
        public boolean hasNext() {
            if (pattern.arity() < ((Integer) stack.get(0))) {
                result = null;
                return false;
            }
            do {
                stack.push(((Integer) stack.pop()) + 1);
                result = match(container.pattern, container.groupExpression);
            } while (result != null && stack.size() > 1);
            return Objects.isNull(result);
        }

        private Plan match(Pattern<? extends Plan> pattern, GroupExpression groupExpression) {
            if (!pattern.matchRoot(groupExpression.getPlan())) {
                return null;
            }
            childIndex = 0;
            patternChildIndex = 0;
            List<Plan> childs = Lists.newArrayList();
            for (; childIndex < groupExpression.arity() && patternChildIndex < pattern.arity(); ++childIndex) {
                addChildIndexToStack();
                Group child = groupExpression.child(childIndex);
                Pattern<? extends Plan> childPattern = pattern.child(patternChildIndex);
                Plan childResult = match(childPattern, extractGroupExpression(childPattern, child));
                if (childResult == null) {
                    return null;
                }
                childs.add(childResult);
                if (!(checkType(childPattern) && checkRange(groupExpression.arity(), pattern.arity()))) {
                    ++patternChildIndex;
                }
            }
            return new PlanBuilder()
                    .setChildren(childs)
                    .setGroupExpression(groupExpression)
                    .setLogicalProperties(groupExpression.getOwnerGroup().getLogicalProperties())
                    .build(groupExpression.getPlan());
        }

        private boolean checkType(Pattern<? extends Plan> pattern) {
            return pattern.patternType.equals(PatternType.MULTI) || pattern.patternType.equals(PatternType.MULTI_GROUP);
        }

        private boolean checkRange(int groupExpressionArity, int patternArity) {
            return (groupExpressionArity - childIndex) <= (patternArity - patternChildIndex);
        }

        private GroupExpression extractGroupExpression(Pattern<? extends Plan> pattern, Group group) {
            return group.getLogicalExpression();
        }

        private void addChildIndexToStack() {
            curChildIndex++;
            for (int i = stack.size(); i <= curChildIndex; ++i) {
                stack.push(0);
            }
        }
    }
}
