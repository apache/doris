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
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.TreeNode;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Pattern node used in pattern matching.
 */
public class Pattern<TYPE extends NODE_TYPE, NODE_TYPE extends TreeNode<NODE_TYPE>>
        extends AbstractTreeNode<Pattern<? extends NODE_TYPE, NODE_TYPE>> {
    public static final Pattern ANY = new Pattern(OperatorType.ANY);
    public static final Pattern MULTI = new Pattern(OperatorType.MULTI);
    public static final Pattern FIXED = new Pattern(OperatorType.FIXED);
    public static final Pattern MULTI_FIXED = new Pattern(OperatorType.MULTI_FIXED);

    protected final List<Predicate<TYPE>> predicates;
    protected final OperatorType operatorType;

    /**
     * Constructor for Pattern.
     *
     * @param operatorType operator type to matching
     * @param children sub pattern
     */
    public Pattern(OperatorType operatorType, Pattern... children) {
        super(NodeType.PATTERN, children);
        this.operatorType = operatorType;
        this.predicates = ImmutableList.of();
    }

    /**
     * Constructor for Pattern.
     *
     * @param operatorType operator type to matching
     * @param predicates custom matching predicate
     * @param children sub pattern
     */
    public Pattern(OperatorType operatorType, List<Predicate<TYPE>> predicates, Pattern... children) {
        super(NodeType.PATTERN, children);
        this.operatorType = operatorType;
        this.predicates = ImmutableList.copyOf(predicates);
    }

    /**
     * get current type in Pattern.
     *
     * @return node type in pattern
     */
    public OperatorType getOperatorType() {
        return operatorType;
    }

    public boolean isFixed() {
        return operatorType == OperatorType.FIXED;
    }

    public boolean isAny() {
        return operatorType == OperatorType.ANY;
    }

    public boolean isMulti() {
        return operatorType == OperatorType.MULTI;
    }

    /**
     * Return ture if current Pattern match Operator in params.
     *
     * @param operator wait to match
     * @return ture if current Pattern match Operator in params
     */
    public boolean matchOperator(Operator operator) {
        if (operator == null) {
            return false;
        }
        if (operatorType == OperatorType.MULTI || operatorType == OperatorType.ANY
                || operatorType == OperatorType.MULTI_FIXED || operatorType == OperatorType.FIXED) {
            return true;
        }
        return getOperatorType().equals(operator.getType());
    }

    /**
     * Return ture if current Pattern match Plan in params.
     *
     * @param root wait to match
     * @return ture if current Pattern match Plan in params
     */
    public boolean matchRoot(TYPE root) {
        if (root == null) {
            return false;
        }

        if (root.arity() > this.arity() && !children.contains(MULTI)) {
            return false;
        }

        if (operatorType == OperatorType.MULTI || operatorType == OperatorType.ANY) {
            return true;
        }

        return doMatchRoot(root);
    }

    protected boolean doMatchRoot(TYPE root) {
        return getOperatorType().equals(root.getOperator().getType())
                && predicates.stream().allMatch(predicate -> predicate.test(root));
    }

    /**
     * Return ture if children patterns match Plan in params.
     *
     * @param root wait to match
     * @return ture if children Patterns match root's children in params
     */
    public boolean matchChildren(TYPE root) {
        for (int i = 0; i < arity(); i++) {
            Pattern child = child(i);
            if (!child.match(root.child(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return ture if children patterns match Plan in params.
     *
     * @param root wait to match
     * @return ture if current pattern and children patterns match root in params
     */
    public boolean match(TYPE root) {
        return matchRoot(root) && matchChildren(root);
    }

    @Override
    public Pattern<? extends NODE_TYPE, NODE_TYPE> withChildren(
            List<Pattern<? extends NODE_TYPE, NODE_TYPE>> children) {
        throw new RuntimeException();
    }

    public Pattern<TYPE, NODE_TYPE> withPredicates(List<Predicate<TYPE>> predicates) {
        return new Pattern(operatorType, predicates, children.toArray(new Pattern[0]));
    }

    @Override
    public Optional<GroupExpression> getGroupExpression() {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pattern pattern = (Pattern) o;
        return operatorType == pattern.operatorType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorType);
    }
}
