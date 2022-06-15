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

import org.apache.doris.nereids.operators.plans.BinaryPlanOperator;
import org.apache.doris.nereids.operators.plans.LeafPlanOperator;
import org.apache.doris.nereids.operators.plans.UnaryPlanOperator;
import org.apache.doris.nereids.operators.plans.logical.LogicalBinaryOperator;
import org.apache.doris.nereids.operators.plans.logical.LogicalLeafOperator;
import org.apache.doris.nereids.operators.plans.logical.LogicalUnaryOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalBinaryOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalLeafOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalScan;
import org.apache.doris.nereids.operators.plans.physical.PhysicalUnaryOperator;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.BinaryPlan;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnaryPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLeafPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnaryPlan;

/**
 * An interface provided some PatternDescriptor.
 * Child Interface(RuleFactory) can use to declare a pattern shape, then convert to a rule.
 * In the future, we will generate this interface by codegen.
 */
public interface Patterns {
    // need implement
    RulePromise defaultPromise();

    /* special pattern descriptors */

    default <T extends RULE_TYPE, RULE_TYPE extends TreeNode<RULE_TYPE>> PatternDescriptor<T, RULE_TYPE> any() {
        return new PatternDescriptor<>(Pattern.ANY, defaultPromise());
    }

    default <T extends RULE_TYPE, RULE_TYPE extends TreeNode<RULE_TYPE>> PatternDescriptor<T, RULE_TYPE> multi() {
        return new PatternDescriptor<>(Pattern.MULTI, defaultPromise());
    }

    default <T extends RULE_TYPE, RULE_TYPE extends TreeNode<RULE_TYPE>> PatternDescriptor<T, RULE_TYPE> fixed() {
        return new PatternDescriptor<>(Pattern.FIXED, defaultPromise());
    }

    default <T extends RULE_TYPE, RULE_TYPE extends TreeNode<RULE_TYPE>> PatternDescriptor<T, RULE_TYPE> multiFixed() {
        return new PatternDescriptor<>(Pattern.MULTI_FIXED, defaultPromise());
    }

    /* abstract plan operator patterns */

    /**
     * create a leafPlan pattern.
     */
    default PatternDescriptor<LeafPlan, Plan> leafPlan() {
        return new PatternDescriptor(new TypePattern(LeafPlanOperator.class), defaultPromise());
    }

    /**
     * create a unaryPlan pattern.
     */
    default PatternDescriptor<UnaryPlan<Plan>, Plan> unaryPlan() {
        return new PatternDescriptor(new TypePattern(UnaryPlanOperator.class, Pattern.FIXED), defaultPromise());
    }

    /**
     * create a unaryPlan pattern.
     */
    default <C extends Plan> PatternDescriptor<UnaryPlan<C>, Plan>
            unaryPlan(PatternDescriptor<C, Plan> child) {
        return new PatternDescriptor(new TypePattern(UnaryPlanOperator.class, child.pattern), defaultPromise());
    }

    /**
     * create a binaryPlan pattern.
     */
    default PatternDescriptor<BinaryPlan<Plan, Plan>, Plan> binaryPlan() {
        return new PatternDescriptor(
                new TypePattern(BinaryPlanOperator.class, Pattern.FIXED, Pattern.FIXED),
                defaultPromise()
        );
    }

    /**
     * create a binaryPlan pattern.
     */
    default <LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
            PatternDescriptor<BinaryPlan<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>, Plan> binaryPlan(
                PatternDescriptor<LEFT_CHILD_TYPE, Plan> leftChild,
                PatternDescriptor<RIGHT_CHILD_TYPE, Plan> rightChild) {
        return new PatternDescriptor(
                new TypePattern(BinaryPlanOperator.class, leftChild.pattern, rightChild.pattern),
                defaultPromise()
        );
    }

    /* abstract logical operator patterns */

    /**
     * create a logicalLeaf pattern.
     */
    default PatternDescriptor<LogicalLeafPlan<LogicalLeafOperator>, Plan> logicalLeaf() {
        return new PatternDescriptor(new TypePattern(LogicalLeafPlan.class), defaultPromise());
    }

    /**
     * create a logicalUnary pattern.
     */
    default PatternDescriptor<LogicalUnaryPlan<LogicalUnaryOperator, Plan>, Plan> logicalUnary() {
        return new PatternDescriptor(new TypePattern(LogicalUnaryPlan.class, Pattern.FIXED), defaultPromise());
    }

    /**
     * create a logicalUnary pattern.
     */
    default <C extends Plan> PatternDescriptor<LogicalUnaryPlan<LogicalUnaryOperator, C>, Plan>
            logicalUnary(PatternDescriptor<C, Plan> child) {
        return new PatternDescriptor(new TypePattern(LogicalUnaryPlan.class, child.pattern), defaultPromise());
    }

    /**
     * create a logicalBinary pattern.
     */
    default PatternDescriptor<LogicalBinaryPlan<LogicalBinaryOperator, Plan, Plan>, Plan> logicalBinary() {
        return new PatternDescriptor(
                new TypePattern(LogicalBinaryPlan.class, Pattern.FIXED, Pattern.FIXED),
                defaultPromise()
        );
    }

    /**
     * create a logicalBinary pattern.
     */
    default <LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
            PatternDescriptor<LogicalBinaryPlan<LogicalBinaryOperator, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>, Plan>
            logicalBinary(
                PatternDescriptor<LEFT_CHILD_TYPE, Plan> leftChild,
                PatternDescriptor<RIGHT_CHILD_TYPE, Plan> rightChild) {
        return new PatternDescriptor(
                new TypePattern(LogicalBinaryPlan.class, leftChild.pattern, rightChild.pattern),
                defaultPromise()
        );
    }

    /* abstract physical operator patterns */

    /**
     * create a physicalLeaf pattern.
     */
    default PatternDescriptor<PhysicalLeafPlan<PhysicalLeafOperator>, Plan> physicalLeaf() {
        return new PatternDescriptor(new TypePattern(PhysicalLeafPlan.class), defaultPromise());
    }

    /**
     * create a physicalUnary pattern.
     */
    default PatternDescriptor<PhysicalUnaryPlan<PhysicalUnaryOperator, Plan>, Plan> physicalUnary() {
        return new PatternDescriptor(new TypePattern(PhysicalUnaryPlan.class, Pattern.FIXED), defaultPromise());
    }

    /**
     * create a physicalUnary pattern.
     */
    default <C extends Plan> PatternDescriptor<PhysicalUnaryPlan<PhysicalUnaryOperator, C>, Plan>
            physicalUnary(PatternDescriptor<C, Plan> child) {
        return new PatternDescriptor(new TypePattern(PhysicalUnaryPlan.class, child.pattern), defaultPromise());
    }

    /**
     * create a physicalBinary pattern.
     */
    default PatternDescriptor<PhysicalBinaryPlan<PhysicalBinaryOperator, Plan, Plan>, Plan> physicalBinary() {
        return new PatternDescriptor(
                new TypePattern(PhysicalBinaryPlan.class, Pattern.FIXED, Pattern.FIXED),
                defaultPromise()
        );
    }

    /**
     * create a physicalBinary pattern.
     */
    default <LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
            PatternDescriptor<PhysicalBinaryPlan<PhysicalBinaryOperator, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>, Plan>
            physicalBinary(
                PatternDescriptor<LEFT_CHILD_TYPE, Plan> leftChild,
                PatternDescriptor<RIGHT_CHILD_TYPE, Plan> rightChild) {
        return new PatternDescriptor(
                new TypePattern(PhysicalBinaryPlan.class, leftChild.pattern, rightChild.pattern),
                defaultPromise()
        );
    }

    /**
     * create a physicalScan pattern.
     */
    default PatternDescriptor<PhysicalLeafPlan<PhysicalScan>, Plan> physicalScan() {
        return new PatternDescriptor(new TypePattern(PhysicalScan.class), defaultPromise());
    }
}
