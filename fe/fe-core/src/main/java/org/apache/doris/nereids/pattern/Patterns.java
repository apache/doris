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

    default <T extends TreeNode> PatternDescriptor<T> any() {
        return new PatternDescriptor<>(Pattern.ANY, defaultPromise());
    }

    default <T extends TreeNode> PatternDescriptor<T> multi() {
        return new PatternDescriptor<>(Pattern.MULTI, defaultPromise());
    }

    default PatternDescriptor<UnboundRelation> unboundRelation() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_UNBOUND_RELATION),
                defaultPromise()
        );
    }

    default PatternDescriptor<LogicalFilter<Plan>> logicalFilter() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_FILTER),
                defaultPromise()
        );
    }

    default <T extends Plan> PatternDescriptor<LogicalFilter<T>> logicalFilter(PatternDescriptor<T> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_FILTER, childPattern.pattern),
                defaultPromise()
        );
    }

    default PatternDescriptor<LogicalProject<Plan>> logicalProject() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_PROJECT),
                defaultPromise()
        );
    }

    default <T extends Plan> PatternDescriptor<LogicalProject> logicalProject(PatternDescriptor<T> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_PROJECT, childPattern.pattern),
                defaultPromise()
        );
    }

    default PatternDescriptor<LogicalJoin<Plan, Plan>> logicalJoin() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_JOIN),
                defaultPromise()
        );
    }

    default PatternDescriptor<LogicalJoin<Plan, Plan>> logicalJoin(JoinType joinType) {
        return new PatternDescriptor<LogicalJoin<Plan, Plan>>(
                new Pattern<>(NodeType.LOGICAL_JOIN),
                defaultPromise()
        ).when(j -> j.getJoinType() == joinType);
    }

    default <C1 extends Plan, C2 extends Plan> PatternDescriptor<LogicalJoin<C1, C2>> logicalJoin(
            JoinType joinType, PatternDescriptor<C1> leftChildPattern, PatternDescriptor<C2> rightChildPattern) {
        return new PatternDescriptor<LogicalJoin<C1, C2>>(
                new Pattern<>(NodeType.LOGICAL_JOIN, leftChildPattern.pattern, rightChildPattern.pattern),
                defaultPromise()
        ).when(j -> j.getJoinType() == joinType);
    }

    default <C1 extends Plan, C2 extends Plan> PatternDescriptor<LogicalJoin<C1, C2>> logicalJoin(
            PatternDescriptor<C1> leftChildPattern, PatternDescriptor<C2> rightChildPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_JOIN, leftChildPattern.pattern, rightChildPattern.pattern),
                defaultPromise()
        );
    }

    default PatternDescriptor<LogicalJoin<Plan, Plan>> innerLogicalJoin() {
        return new PatternDescriptor<LogicalJoin<Plan, Plan>>(
                new Pattern<>(NodeType.LOGICAL_JOIN),
                defaultPromise()
        ).when(j -> j.getJoinType() == JoinType.INNER_JOIN);
    }

    default <C1 extends Plan, C2 extends Plan> PatternDescriptor<LogicalJoin<C1, C2>> innerLogicalJoin(
            PatternDescriptor<C1> leftChildPattern, PatternDescriptor<C2> rightChildPattern) {
        return new PatternDescriptor<LogicalJoin<C1, C2>>(
                new Pattern<>(NodeType.LOGICAL_JOIN, leftChildPattern.pattern, rightChildPattern.pattern),
                defaultPromise()
        ).when(j -> j.getJoinType() == JoinType.INNER_JOIN);
    }

    default PatternDescriptor<LogicalRelation> logicalRelation() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LOGICAL_BOUND_RELATION),
                defaultPromise()
        );
    }

    // physical pattern descriptors

    default PatternDescriptor<PhysicalFilter<Plan>> physicalFilter() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_FILTER),
                defaultPromise()
        );
    }

    default <T extends Plan> PatternDescriptor<PhysicalFilter<T>> physicalFilter(PatternDescriptor<T> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_FILTER, childPattern.pattern),
                defaultPromise()
        );
    }

    default PatternDescriptor<PhysicalProject<Plan>> physicalProject() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_PROJECT),
                defaultPromise()
        );
    }

    default <T extends Plan> PatternDescriptor<PhysicalProject<T>> physicalProject(PatternDescriptor<T> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_PROJECT, childPattern.pattern),
                defaultPromise()
        );
    }

    default PatternDescriptor<PhysicalBroadcastHashJoin<Plan, Plan>> physicalBroadcastHashJoin() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_BROADCAST_HASH_JOIN),
                defaultPromise()
        );
    }

    default <C1 extends Plan, C2 extends Plan> PatternDescriptor<PhysicalBroadcastHashJoin<C1, C2>>
            physicalBroadcastHashJoin(PatternDescriptor<C1> leftChildPattern, PatternDescriptor<C2> rightChildPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_BROADCAST_HASH_JOIN,
                        leftChildPattern.pattern,
                        rightChildPattern.pattern
                ),
                defaultPromise()
        );
    }

    default PatternDescriptor<PhysicalOlapScan> physicalOlapScan() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.PHYSICAL_OLAP_SCAN),
                defaultPromise()
        );
    }

    // expression pattern descriptors

    default PatternDescriptor<UnboundAlias<Expression>> unboundAlias() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.UNBOUND_ALIAS),
                defaultPromise()
        );
    }

    default <T extends Expression> PatternDescriptor<UnboundAlias<T>>
            unboundAlias(PatternDescriptor<T> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.UNBOUND_ALIAS, childPattern.pattern),
                defaultPromise()
        );
    }

    default PatternDescriptor<UnboundSlot> unboundSlot() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.UNBOUND_SLOT),
                defaultPromise()
        );
    }

    default PatternDescriptor<UnboundStar> unboundStar() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.UNBOUND_STAR),
                defaultPromise()
        );
    }

    default PatternDescriptor<Literal> literal() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.LITERAL),
                defaultPromise()
        );
    }

    default PatternDescriptor<SlotReference> slotReference() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.SLOT_REFERENCE),
                defaultPromise()
        );
    }

    default PatternDescriptor<BinaryPredicate<Expression, Expression>> binaryPredicate() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.BINARY_PREDICATE),
                defaultPromise()
        );
    }

    default PatternDescriptor<BinaryPredicate<Expression, Expression>> binaryPredicate(Operator operator) {
        return new PatternDescriptor<BinaryPredicate<Expression, Expression>>(
                new Pattern<>(NodeType.BINARY_PREDICATE),
                defaultPromise()
        ).when(p -> p.getOperator() == operator);
    }

    default <C1 extends Expression, C2 extends Expression> PatternDescriptor<BinaryPredicate<C1, C2>>
            binaryPredicate(PatternDescriptor<C1> leftChildPattern, PatternDescriptor<C2> rightChildPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.BINARY_PREDICATE,
                        leftChildPattern.pattern,
                        rightChildPattern.pattern
                ),
                defaultPromise()
        );
    }

    default PatternDescriptor<Alias<Expression>> alias() {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.ALIAS),
                defaultPromise()
        );
    }

    default <T extends Expression> PatternDescriptor<Alias<T>> alias(PatternDescriptor<T> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(NodeType.ALIAS, childPattern.pattern),
                defaultPromise()
        );
    }
}
