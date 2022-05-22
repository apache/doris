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
import java.util.function.Predicate;


/**
 * Pattern node used in pattern matching.
 */
public class Pattern<T extends TreeNode> extends AbstractTreeNode<Pattern<T>> {
    public static final Pattern ANY = new Pattern(OperatorType.ANY);
    public static final Pattern MULTI = new Pattern(OperatorType.MULTI);
    public static final Pattern FIXED = new Pattern(OperatorType.FIXED);
    public static final Pattern MULTI_FIXED = new Pattern(OperatorType.MULTI_FIXED);

    protected final List<Predicate<T>> predicates;
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
    public Pattern(OperatorType operatorType, List<Predicate<T>> predicates, Pattern... children) {
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
    public boolean matchRoot(T root) {
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

    protected boolean doMatchRoot(T root) {
        return getOperatorType().equals(root.getOperator().getType())
                && predicates.stream().allMatch(predicate -> predicate.test(root));
    }

    /**
     * Return ture if children patterns match Plan in params.
     *
     * @param root wait to match
     * @return ture if children Patterns match root's children in params
     */
    public boolean matchChildren(T root) {
        for (int i = 0; i < arity(); i++) {
            if (!child(i).match(root.child(i))) {
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
    public boolean match(T root) {
        return matchRoot(root) && matchChildren(root);
    }

    @Override
    public Pattern<T> newChildren(List<TreeNode> children) {
        throw new RuntimeException();
    }

    @Override
    public GroupExpression getGroupExpression() {
        throw new RuntimeException();
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

    @Override
    public List<Pattern> children() {
        return (List) children;
    }

    @Override
    public Pattern child(int index) {
        return (Pattern) children.get(index);
    }
}
