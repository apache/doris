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

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryPredicate;
import org.apache.doris.nereids.trees.expressions.BinaryPredicate.Operator;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBroadcastHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;

/**
 * An interface provided some PatternDescriptor.
 * Child Interface(RuleFactory) can use to declare a pattern shape, then convert to a rule.
 * In the future, we will generate this interface by codegen.
 */
public interface Patterns {
    // need implement
    RulePromise defaultPromise();

    // logical pattern descriptors

    default <T extends RULE_TYPE, RULE_TYPE extends TreeNode> PatternDescriptor<T, RULE_TYPE> any() {
        return new PatternDescriptor<>(Pattern.ANY, defaultPromise());
    }

    default <T extends RULE_TYPE, RULE_TYPE extends TreeNode> PatternDescriptor<T, RULE_TYPE> multi() {
        return new PatternDescriptor<>(Pattern.MULTI, defaultPromise());
    }

    /**
     * create a unboundRelation pattern.
     */
    default PatternDescriptor<UnboundRelation, Plan> unboundRelation() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_UNBOUND_RELATION),
                defaultPromise()
        );
    }

    /**
     * create a logicalFilter pattern.
     */
    default PatternDescriptor<LogicalFilter<Plan>, Plan> logicalFilter() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_FILTER),
                defaultPromise()
        );
    }

    /**
     * create a logicalFilter pattern with child pattern.
     */
    default <T extends Plan> PatternDescriptor<LogicalFilter<T>, Plan>
            logicalFilter(PatternDescriptor<T, Plan> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_FILTER, childPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a logicalProject pattern.
     */
    default PatternDescriptor<LogicalProject<Plan>, Plan> logicalProject() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_PROJECT),
                defaultPromise()
        );
    }

    /**
     * create a logicalProject pattern.
     */
    default <T extends Plan> PatternDescriptor<LogicalProject, Plan>
            logicalProject(PatternDescriptor<T, Plan> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_PROJECT, childPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a logicalJoin pattern.
     */
    default PatternDescriptor<LogicalJoin<Plan, Plan>, Plan> logicalJoin() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_JOIN),
                defaultPromise()
        );
    }

    /**
     * create a logicalJoin pattern with join type.
     */
    default PatternDescriptor<LogicalJoin<Plan, Plan>, Plan> logicalJoin(JoinType joinType) {
        return new PatternDescriptor<LogicalJoin<Plan, Plan>, Plan>(
                new Pattern<>(NodeType.LOGICAL_JOIN),
                defaultPromise()
        ).when(j -> j.getJoinType() == joinType);
    }

    /**
     * create a logicalJoin pattern with joinType and children patterns.
     */
    default <C1 extends Plan, C2 extends Plan> PatternDescriptor<LogicalJoin<C1, C2>, Plan> logicalJoin(
            JoinType joinType, PatternDescriptor<C1, Plan> leftChildPattern,
            PatternDescriptor<C2, Plan> rightChildPattern) {
        return new PatternDescriptor<LogicalJoin<C1, C2>, Plan>(
                new Pattern<>(NodeType.LOGICAL_JOIN, leftChildPattern.pattern, rightChildPattern.pattern),
                defaultPromise()
        ).when(j -> j.getJoinType() == joinType);
    }

    /**
     * create a logicalJoin pattern with children patterns.
     */
    default <C1 extends Plan, C2 extends Plan> PatternDescriptor<LogicalJoin<C1, C2>, Plan> logicalJoin(
            PatternDescriptor<C1, Plan> leftChildPattern, PatternDescriptor<C2, Plan> rightChildPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_JOIN, leftChildPattern.pattern, rightChildPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a logicalJoin pattern with joinType is inner.
     */
    default PatternDescriptor<LogicalJoin<Plan, Plan>, Plan> innerLogicalJoin() {
        return new PatternDescriptor<LogicalJoin<Plan, Plan>, Plan>(
                new Pattern<>(NodeType.LOGICAL_JOIN),
                defaultPromise()
        ).when(j -> j.getJoinType() == JoinType.INNER_JOIN);
    }

    /**
     * create a logical join pattern with join type is inner and children patterns.
     */
    default <C1 extends Plan, C2 extends Plan> PatternDescriptor<LogicalJoin<C1, C2>, Plan> innerLogicalJoin(
            PatternDescriptor<C1, Plan> leftChildPattern, PatternDescriptor<C2, Plan> rightChildPattern) {
        return new PatternDescriptor<LogicalJoin<C1, C2>, Plan>(
                new Pattern<>(NodeType.LOGICAL_JOIN, leftChildPattern.pattern, rightChildPattern.pattern),
                defaultPromise()
        ).when(j -> j.getJoinType() == JoinType.INNER_JOIN);
    }

    /**
     * create a logicalRelation pattern.
     */
    default PatternDescriptor<LogicalRelation, Plan> logicalRelation() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_BOUND_RELATION),
                defaultPromise()
        );
    }

    // physical pattern descriptors

    /**
     * create a physicalFilter pattern.
     */
    default PatternDescriptor<PhysicalFilter<Plan>, Plan> physicalFilter() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_FILTER),
                defaultPromise()
        );
    }

    /**
     * create a physicalFilter pattern with child pattern.
     */
    default <T extends Plan> PatternDescriptor<PhysicalFilter<T>, Plan>
            physicalFilter(PatternDescriptor<T, Plan> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_FILTER, childPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a physicalProject pattern.
     */
    default PatternDescriptor<PhysicalProject<Plan>, Plan> physicalProject() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_PROJECT),
                defaultPromise()
        );
    }

    /**
     * create a physicalProject pattern with child pattern.
     */
    default <T extends Plan> PatternDescriptor<PhysicalProject<T>, Plan>
            physicalProject(PatternDescriptor<T, Plan> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_PROJECT, childPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a physicalBroadcastHashJoin pattern.
     */
    default PatternDescriptor<PhysicalBroadcastHashJoin<Plan, Plan>, Plan> physicalBroadcastHashJoin() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_BROADCAST_HASH_JOIN),
                defaultPromise()
        );
    }

    /**
     * create a physicalBroadcastHashJoin pattern with children patterns.
     */
    default <C1 extends Plan, C2 extends Plan> PatternDescriptor<PhysicalBroadcastHashJoin<C1, C2>, Plan>
            physicalBroadcastHashJoin(PatternDescriptor<C1, Plan> leftChildPattern,
                PatternDescriptor<C2, Plan> rightChildPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_BROADCAST_HASH_JOIN,
                        leftChildPattern.pattern,
                        rightChildPattern.pattern
                ),
                defaultPromise()
        );
    }

    /**
     * create a physicalOlapScan pattern.
     */
    default PatternDescriptor<PhysicalOlapScan, Plan> physicalOlapScan() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_OLAP_SCAN),
                defaultPromise()
        );
    }

    // expression pattern descriptors

    /**
     * create a unboundAlias pattern.
     */
    default PatternDescriptor<UnboundAlias<Expression>, Expression> unboundAlias() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.UNBOUND_ALIAS),
                defaultPromise()
        );
    }

    /**
     * create a unboundAlias pattern.
     */
    default <T extends Expression> PatternDescriptor<UnboundAlias<T>, Expression>
            unboundAlias(PatternDescriptor<T, Expression> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.UNBOUND_ALIAS, childPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a unboundSlot pattern.
     */
    default PatternDescriptor<UnboundSlot, Expression> unboundSlot() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.UNBOUND_SLOT),
                defaultPromise()
        );
    }

    /**
     * create a unboundStar pattern.
     */
    default PatternDescriptor<UnboundStar, Expression> unboundStar() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.UNBOUND_STAR),
                defaultPromise()
        );
    }

    /**
     * create a literal pattern.
     */
    default PatternDescriptor<Literal, Expression> literal() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LITERAL),
                defaultPromise()
        );
    }

    /**
     * create a slotReference pattern.
     */
    default PatternDescriptor<SlotReference, Expression> slotReference() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.SLOT_REFERENCE),
                defaultPromise()
        );
    }

    /**
     * create a binaryPredicate pattern.
     */
    default PatternDescriptor<BinaryPredicate<Expression, Expression>, Expression> binaryPredicate() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.BINARY_PREDICATE),
                defaultPromise()
        );
    }

    /**
     * create a binaryPredicate pattern with operator type.
     */
    default PatternDescriptor<BinaryPredicate<Expression, Expression>, Expression> binaryPredicate(Operator operator) {
        return new PatternDescriptor<BinaryPredicate<Expression, Expression>, Expression>(
                new Pattern<>(NodeType.BINARY_PREDICATE),
                defaultPromise()
        ).when(p -> p.getOperator() == operator);
    }

    /**
     * create a binaryPredicate pattern with children patterns.
     */
    default <C1 extends Expression, C2 extends Expression> PatternDescriptor<BinaryPredicate<C1, C2>, Expression>
            binaryPredicate(PatternDescriptor<C1, Expression> leftChildPattern,
                PatternDescriptor<C2, Expression> rightChildPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.BINARY_PREDICATE,
                        leftChildPattern.pattern,
                        rightChildPattern.pattern
                ),
                defaultPromise()
        );
    }

    /**
     * create a alias pattern.
     */
    default PatternDescriptor<Alias<Expression>, Expression> alias() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.ALIAS),
                defaultPromise()
        );
    }

    /**
     * create a alias pattern with child pattern.
     */
    default <T extends Expression> PatternDescriptor<Alias<T>, Expression>
            alias(PatternDescriptor<T, Expression> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.ALIAS, childPattern.pattern),
                defaultPromise()
        );
    }
}
