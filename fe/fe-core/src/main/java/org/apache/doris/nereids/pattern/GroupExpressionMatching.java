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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Get all pattern matching subtree in query plan from a group expression.
 * TODO: adapt ANY and MULTI
 * TODO: add ut
 */
public class GroupExpressionMatching implements Iterable<Plan> {
    private final Pattern<? extends Plan, Plan> pattern;
    private final GroupExpression groupExpression;

    public GroupExpressionMatching(Pattern<? extends Plan, Plan> pattern, GroupExpression groupExpression) {
        this.pattern = Objects.requireNonNull(pattern);
        this.groupExpression = Objects.requireNonNull(groupExpression);
    }

    @Override
    public GroupExpressionIterator<Plan> iterator() {
        return new GroupExpressionIterator<>(pattern, groupExpression);
    }

    /**
     * Iterator to get all subtrees.
     */
    public static class GroupExpressionIterator<NODE_TYPE extends TreeNode<NODE_TYPE>> implements Iterator<NODE_TYPE> {
        private final List<NODE_TYPE> results;
        private int resultIndex = 0;

        /**
         * Constructor.
         *
         * @param pattern pattern to match
         * @param groupExpression group expression to be matched
         */
        public GroupExpressionIterator(Pattern<? extends NODE_TYPE, NODE_TYPE> pattern,
                GroupExpression groupExpression) {
            results = Lists.newArrayList();

            if (!pattern.matchOperator(groupExpression.getOperator())) {
                return;
            }
            if (pattern.arity() > groupExpression.arity()) {
                return;
            }
            if (pattern.arity() < groupExpression.arity()
                    && (!pattern.children().contains(Pattern.MULTI)
                    || !pattern.children().contains(Pattern.MULTI_FIXED))) {
                return;
            }

            NODE_TYPE root = groupExpression.getOperator().toTreeNode(groupExpression);

            List<List<NODE_TYPE>> childrenResults = Lists.newArrayListWithCapacity(groupExpression.arity());
            for (int i = 0; i < groupExpression.arity(); ++i) {
                childrenResults.add(Lists.newArrayList());
                int patternChildIndex = i >= pattern.arity() ? pattern.arity() - 1 : i;
                for (NODE_TYPE child : new GroupMatching<NODE_TYPE>(
                        pattern.child(patternChildIndex), groupExpression.child(i))) {
                    childrenResults.get(i).add(child);
                }
            }

            if (pattern.arity() == 0) {
                results.add(root);
            } else {
                int[] childrenResultsIndex = new int[groupExpression.arity()];
                int offset = 0;
                while (offset < childrenResults.size()) {
                    List<NODE_TYPE> children = Lists.newArrayList();
                    for (int i = 0; i < childrenResults.size(); i++) {
                        children.add(childrenResults.get(i).get(childrenResultsIndex[i]));
                    }
                    NODE_TYPE result = root.withChildren(children);
                    results.add(result);
                    offset = 0;
                    while (true) {
                        childrenResultsIndex[offset]++;
                        if (childrenResultsIndex[offset] == childrenResults.get(offset).size()) {
                            childrenResultsIndex[offset] = 0;
                            offset++;
                            if (offset == childrenResults.size()) {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return resultIndex < results.size();
        }

        @Override
        public NODE_TYPE next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return results.get(resultIndex++);
        }
    }
}
