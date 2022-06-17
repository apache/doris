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
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Get all pattern matching subtree in query plan from a group.
 */
public class GroupMatching<NODE_TYPE extends TreeNode> implements Iterable<NODE_TYPE> {
    private final Pattern pattern;
    private final Group group;

    public GroupMatching(Pattern pattern, Group group) {
        this.pattern = Objects.requireNonNull(pattern);
        this.group = Objects.requireNonNull(group);
    }

    public Iterator<NODE_TYPE> iterator() {
        return new GroupIterator<>(pattern, group);
    }

    /**
     * Iterator to get all subtrees from a group.
     */
    public static class GroupIterator<NODE_TYPE extends TreeNode<NODE_TYPE>> implements Iterator<NODE_TYPE> {
        private final Pattern pattern;
        private final List<Iterator<NODE_TYPE>> iterator;
        private int iteratorIndex = 0;

        /**
         * Constructor.
         *
         * @param pattern pattern to match
         * @param group group to be matched
         */
        public GroupIterator(Pattern<? extends Plan, Plan> pattern, Group group) {
            this.pattern = pattern;
            this.iterator = Lists.newArrayList();
            for (GroupExpression groupExpression : group.getLogicalExpressions()) {
                GroupExpressionMatching.GroupExpressionIterator groupExpressionIterator =
                        new GroupExpressionMatching(pattern, groupExpression).iterator();
                if (groupExpressionIterator.hasNext()) {
                    this.iterator.add(groupExpressionIterator);
                }
            }
            for (GroupExpression groupExpression : group.getPhysicalExpressions()) {
                GroupExpressionMatching.GroupExpressionIterator groupExpressionIterator =
                        new GroupExpressionMatching(pattern, groupExpression).iterator();
                if (groupExpressionIterator.hasNext()) {
                    this.iterator.add(groupExpressionIterator);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return iteratorIndex < iterator.size();
        }

        @Override
        public NODE_TYPE next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            if (OperatorType.FIXED == pattern.getOperatorType()
                    || OperatorType.MULTI_FIXED == pattern.getOperatorType()) {
                iteratorIndex = iterator.size();
                return iterator.get(0).next();
            } else {
                NODE_TYPE result = iterator.get(iteratorIndex).next();
                if (!iterator.get(iteratorIndex).hasNext()) {
                    iteratorIndex++;
                }
                return result;
            }
        }
    }
}
