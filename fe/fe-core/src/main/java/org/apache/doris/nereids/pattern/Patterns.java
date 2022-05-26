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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.operators.plans.logical.LogicalFilter;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.operators.plans.logical.LogicalRelation;
import org.apache.doris.nereids.operators.plans.physical.PhysicalBroadcastHashJoin;
import org.apache.doris.nereids.operators.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.operators.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.operators.plans.physical.PhysicalProject;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinary;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeaf;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnary;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBinary;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLeaf;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnary;

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

    default <T extends RULE_TYPE, RULE_TYPE extends TreeNode> PatternDescriptor<T, RULE_TYPE> fixed() {
        return new PatternDescriptor<>(Pattern.FIXED, defaultPromise());
    }

    /**
     * create a unboundRelation pattern.
     */
    default PatternDescriptor<LogicalLeaf<UnboundRelation>, Plan> unboundRelation() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.LOGICAL_UNBOUND_RELATION),
                defaultPromise()
        );
    }

    /**
     * create a logicalFilter pattern.
     */
    default PatternDescriptor<LogicalUnary<LogicalFilter, Plan>, Plan> logicalFilter() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.LOGICAL_FILTER),
                defaultPromise()
        );
    }

    /**
     * create a logicalFilter pattern with child pattern.
     */
    default <T extends Plan> PatternDescriptor<LogicalUnary<LogicalFilter, T>, Plan>
            logicalFilter(PatternDescriptor<T, Plan> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.LOGICAL_FILTER, childPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a logicalProject pattern.
     */
    default PatternDescriptor<LogicalUnary<LogicalProject, Plan>, Plan> logicalProject() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.LOGICAL_PROJECT),
                defaultPromise()
        );
    }

    /**
     * create a logicalProject pattern.
     */
    default <T extends Plan> PatternDescriptor<LogicalUnary<LogicalProject, T>, Plan>
            logicalProject(PatternDescriptor<T, Plan> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.LOGICAL_PROJECT, childPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a logicalJoin pattern.
     */
    default PatternDescriptor<LogicalBinary<LogicalJoin, Plan, Plan>, Plan> logicalJoin() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.LOGICAL_JOIN),
                defaultPromise()
        );
    }

    /**
     * create a logicalJoin pattern with join type.
     */
    default PatternDescriptor<LogicalBinary<LogicalJoin, Plan, Plan>, Plan> logicalJoin(JoinType joinType) {
        return new PatternDescriptor<LogicalBinary<LogicalJoin, Plan, Plan>, Plan>(
                new Pattern<>(OperatorType.LOGICAL_JOIN),
                defaultPromise()
        ).when(j -> j.operator.getJoinType() == joinType);
    }

    /**
     * create a logicalJoin pattern with joinType and children patterns.
     */
    default <C1 extends Plan, C2 extends Plan>
            PatternDescriptor<LogicalBinary<LogicalJoin, C1, C2>, Plan> logicalJoin(
                JoinType joinType, PatternDescriptor<C1, Plan> leftChildPattern,
                PatternDescriptor<C2, Plan> rightChildPattern) {
        return new PatternDescriptor<LogicalBinary<LogicalJoin, C1, C2>, Plan>(
                new Pattern<>(OperatorType.LOGICAL_JOIN, leftChildPattern.pattern, rightChildPattern.pattern),
                defaultPromise()
        ).when(j -> j.operator.getJoinType() == joinType);
    }

    /**
     * create a logicalJoin pattern with children patterns.
     */
    default <C1 extends Plan, C2 extends Plan>
            PatternDescriptor<LogicalBinary<LogicalJoin, C1, C2>, Plan> logicalJoin(
                PatternDescriptor<C1, Plan> leftChildPattern, PatternDescriptor<C2, Plan> rightChildPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.LOGICAL_JOIN, leftChildPattern.pattern, rightChildPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a logicalJoin pattern with joinType is inner.
     */
    default PatternDescriptor<LogicalBinary<LogicalJoin, Plan, Plan>, Plan> innerLogicalJoin() {
        return new PatternDescriptor<LogicalBinary<LogicalJoin, Plan, Plan>, Plan>(
                new Pattern<>(OperatorType.LOGICAL_JOIN),
                defaultPromise()
        ).when(j -> j.operator.getJoinType() == JoinType.INNER_JOIN);
    }

    /**
     * create a logical join pattern with join type is inner and children patterns.
     */
    default <C1 extends Plan, C2 extends Plan>
            PatternDescriptor<LogicalBinary<LogicalJoin, C1, C2>, Plan> innerLogicalJoin(
                PatternDescriptor<C1, Plan> leftChildPattern, PatternDescriptor<C2, Plan> rightChildPattern) {
        return new PatternDescriptor<LogicalBinary<LogicalJoin, C1, C2>, Plan>(
                new Pattern<>(OperatorType.LOGICAL_JOIN, leftChildPattern.pattern, rightChildPattern.pattern),
                defaultPromise()
        ).when(j -> j.operator.getJoinType() == JoinType.INNER_JOIN);
    }

    /**
     * create a logicalRelation pattern.
     */
    default PatternDescriptor<LogicalLeaf<LogicalRelation>, Plan> logicalRelation() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.LOGICAL_BOUND_RELATION),
                defaultPromise()
        );
    }

    // physical pattern descriptors

    /**
     * create a physicalFilter pattern.
     */
    default PatternDescriptor<PhysicalUnary<PhysicalFilter, Plan>, Plan> physicalFilter() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.PHYSICAL_FILTER),
                defaultPromise()
        );
    }

    /**
     * create a physicalFilter pattern with child pattern.
     */
    default <T extends Plan> PatternDescriptor<PhysicalUnary<PhysicalFilter, T>, Plan>
            physicalFilter(PatternDescriptor<T, Plan> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.PHYSICAL_FILTER, childPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a physicalProject pattern.
     */
    default PatternDescriptor<PhysicalUnary<PhysicalProject, Plan>, Plan> physicalProject() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.PHYSICAL_PROJECT),
                defaultPromise()
        );
    }

    /**
     * create a physicalProject pattern with child pattern.
     */
    default <T extends Plan> PatternDescriptor<PhysicalUnary<PhysicalProject, T>, Plan>
            physicalProject(PatternDescriptor<T, Plan> childPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.PHYSICAL_PROJECT, childPattern.pattern),
                defaultPromise()
        );
    }

    /**
     * create a physicalBroadcastHashJoin pattern.
     */
    default PatternDescriptor<PhysicalBinary<PhysicalBroadcastHashJoin, Plan, Plan>, Plan>
            physicalBroadcastHashJoin() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.PHYSICAL_BROADCAST_HASH_JOIN),
                defaultPromise()
        );
    }

    /**
     * create a physicalBroadcastHashJoin pattern with children patterns.
     */
    default <C1 extends Plan, C2 extends Plan>
            PatternDescriptor<PhysicalBinary<PhysicalBroadcastHashJoin, C1, C2>, Plan>
                physicalBroadcastHashJoin(PatternDescriptor<C1, Plan> leftChildPattern,
                        PatternDescriptor<C2, Plan> rightChildPattern) {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.PHYSICAL_BROADCAST_HASH_JOIN,
                        leftChildPattern.pattern,
                        rightChildPattern.pattern
                ),
                defaultPromise()
        );
    }

    /**
     * create a physicalOlapScan pattern.
     */
    default PatternDescriptor<PhysicalLeaf<PhysicalOlapScan>, Plan> physicalOlapScan() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.PHYSICAL_OLAP_SCAN),
                defaultPromise()
        );
    }
}
