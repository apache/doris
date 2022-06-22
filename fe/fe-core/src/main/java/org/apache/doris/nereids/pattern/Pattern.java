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

    public static final Pattern ANY = new Pattern(PatternType.ANY);
    public static final Pattern MULTI = new Pattern(PatternType.MULTI);
    public static final Pattern GROUP = new Pattern(PatternType.GROUP);
    public static final Pattern MULTI_GROUP = new Pattern(PatternType.MULTI_GROUP);

    protected final List<Predicate<TYPE>> predicates;
    protected final PatternType patternType;
    protected final OperatorType operatorType;

    public Pattern(OperatorType operatorType, Pattern... children) {
        this(PatternType.NORMAL, operatorType, children);
    }

    public Pattern(OperatorType operatorType, List<Predicate<TYPE>> predicates, Pattern... children) {
        this(PatternType.NORMAL, operatorType, predicates, children);
    }

    private Pattern(PatternType patternType, Pattern... children) {
        this(patternType, OperatorType.UNKNOWN, children);
    }

    /**
     * Constructor for Pattern.
     *
     * @param patternType pattern type to matching
     * @param children sub pattern
     */
    private Pattern(PatternType patternType, OperatorType operatorType, Pattern... children) {
        super(NodeType.PATTERN, children);
        this.patternType = patternType;
        this.operatorType = operatorType;
        this.predicates = ImmutableList.of();
    }

    /**
     * Constructor for Pattern.
     *
     * @param patternType pattern type to matching
     * @param operatorType operator type to matching
     * @param predicates custom matching predicate
     * @param children sub pattern
     */
    private Pattern(PatternType patternType, OperatorType operatorType,
                   List<Predicate<TYPE>> predicates, Pattern... children) {
        super(NodeType.PATTERN, children);
        this.patternType = patternType;
        this.operatorType = operatorType;
        this.predicates = ImmutableList.copyOf(predicates);

        for (int i = 0; i + 1 < children.length; ++i) {
            if (children[i].isMulti()) {
                throw new IllegalStateException("Pattern.MULTI must be last child of current pattern");
            } else if (children[i].isMultiGroup()) {
                throw new IllegalStateException("Pattern.MULTI_GROUP must be last child of current pattern");
            }
        }
    }

    /**
     * get current type in Operator.
     *
     * @return operator type in pattern
     */
    public OperatorType getOperatorType() {
        return operatorType;
    }

    /**
     * get current type in Pattern.
     *
     * @return pattern type
     */
    public PatternType getPatternType() {
        return patternType;
    }

    /**
     * get all predicates in Pattern.
     *
     * @return all predicates
     */
    public List<Predicate<TYPE>> getPredicates() {
        return predicates;
    }

    public boolean isGroup() {
        return patternType == PatternType.GROUP;
    }

    public boolean isMultiGroup() {
        return patternType == PatternType.MULTI_GROUP;
    }

    public boolean isAny() {
        return patternType == PatternType.ANY;
    }

    public boolean isMulti() {
        return patternType == PatternType.MULTI;
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
        switch (patternType) {
            case ANY:
            case MULTI:
            case GROUP:
            case MULTI_GROUP:
                return true;
            default:
                return operatorType == operator.getType();
        }
    }

    /**
     * match all predicates.
     * @param root root plan
     * @return true if all predicates matched
     */
    public boolean matchPredicates(TYPE root) {
        return predicates.stream().allMatch(predicate -> predicate.test(root));
    }

    @Override
    public Pattern<? extends NODE_TYPE, NODE_TYPE> withChildren(
            List<Pattern<? extends NODE_TYPE, NODE_TYPE>> children) {
        throw new IllegalStateException("Pattern can not invoke withChildren");
    }

    public Pattern<TYPE, NODE_TYPE> withPredicates(List<Predicate<TYPE>> predicates) {
        return new Pattern(patternType, operatorType, predicates, children.toArray(new Pattern[0]));
    }

    @Override
    public Optional<GroupExpression> getGroupExpression() {
        return Optional.empty();
    }

    public boolean hasMultiChild() {
        return !children.isEmpty() && children.get(children.size() - 1).isMulti();
    }

    public boolean hasMultiGroupChild() {
        return !children.isEmpty() && children.get(children.size() - 1).isMultiGroup();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pattern<?, ?> pattern = (Pattern<?, ?>) o;
        return predicates.equals(pattern.predicates)
                && patternType == pattern.patternType
                && operatorType == pattern.operatorType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicates, patternType, operatorType);
    }
}
