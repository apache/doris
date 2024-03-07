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

import org.apache.doris.nereids.trees.plans.BinaryPlan;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinary;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeaf;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnary;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBinary;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLeaf;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnary;

import java.util.Arrays;

/** PlanPatterns */
public interface PlanPatterns extends Patterns {

    /* abstract plan operator patterns */

    /**
     * create a leafPlan pattern.
     */
    default PatternDescriptor<LeafPlan> leafPlan() {
        return new PatternDescriptor(new TypePattern(LeafPlan.class), defaultPromise());
    }

    /**
     * create a unaryPlan pattern.
     */
    default PatternDescriptor<UnaryPlan<Plan>> unaryPlan() {
        return new PatternDescriptor(new TypePattern(UnaryPlan.class, Pattern.ANY), defaultPromise());
    }

    /**
     * create a unaryPlan pattern.
     */
    default <C extends Plan> PatternDescriptor<UnaryPlan<C>>
            unaryPlan(PatternDescriptor<C> child) {
        return new PatternDescriptor(new TypePattern(UnaryPlan.class, child.pattern), defaultPromise());
    }

    /**
     * create a binaryPlan pattern.
     */
    default PatternDescriptor<BinaryPlan<Plan, Plan>> binaryPlan() {
        return new PatternDescriptor(
                new TypePattern(BinaryPlan.class, Pattern.ANY, Pattern.ANY),
                defaultPromise()
        );
    }

    /**
     * create a binaryPlan pattern.
     */
    default <LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
            PatternDescriptor<BinaryPlan<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>> binaryPlan(
                PatternDescriptor<LEFT_CHILD_TYPE> leftChild,
                PatternDescriptor<RIGHT_CHILD_TYPE> rightChild) {
        return new PatternDescriptor(
                new TypePattern(BinaryPlan.class, leftChild.pattern, rightChild.pattern),
                defaultPromise()
        );
    }

    /* abstract logical plan patterns */

    /**
     * create a logicalPlan pattern.
     */
    default PatternDescriptor<LogicalPlan> logicalPlan() {
        return new PatternDescriptor(new TypePattern(LogicalPlan.class, multi().pattern), defaultPromise());
    }

    /**
     * create a logicalLeaf pattern.
     */
    default PatternDescriptor<LogicalLeaf> logicalLeaf() {
        return new PatternDescriptor(new TypePattern(LogicalLeaf.class), defaultPromise());
    }

    /**
     * create a logicalUnary pattern.
     */
    default PatternDescriptor<LogicalUnary<Plan>> logicalUnary() {
        return new PatternDescriptor(new TypePattern(LogicalUnary.class, Pattern.ANY), defaultPromise());
    }

    /**
     * create a logicalUnary pattern.
     */
    default <C extends Plan> PatternDescriptor<LogicalUnary<C>>
            logicalUnary(PatternDescriptor<C> child) {
        return new PatternDescriptor(new TypePattern(LogicalUnary.class, child.pattern), defaultPromise());
    }

    /**
     * create a logicalBinary pattern.
     */
    default PatternDescriptor<LogicalBinary<Plan, Plan>> logicalBinary() {
        return new PatternDescriptor(
                new TypePattern(LogicalBinary.class, Pattern.ANY, Pattern.ANY),
                defaultPromise()
        );
    }

    /**
     * create a logicalBinary pattern.
     */
    default <LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
            PatternDescriptor<LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>>
            logicalBinary(
                PatternDescriptor<LEFT_CHILD_TYPE> leftChild,
                PatternDescriptor<RIGHT_CHILD_TYPE> rightChild) {
        return new PatternDescriptor(
                new TypePattern(LogicalBinary.class, leftChild.pattern, rightChild.pattern),
                defaultPromise()
        );
    }

    /**
     * create a logicalRelation pattern.
     */
    default PatternDescriptor<LogicalRelation> logicalRelation() {
        return new PatternDescriptor(new TypePattern(LogicalRelation.class), defaultPromise());
    }

    /**
     * create a logicalSetOperation pattern.
     */
    default PatternDescriptor<LogicalSetOperation>
            logicalSetOperation(
                PatternDescriptor... children) {
        return new PatternDescriptor(
                new TypePattern(LogicalSetOperation.class,
                        Arrays.stream(children)
                                .map(PatternDescriptor::getPattern)
                                .toArray(Pattern[]::new)),
                defaultPromise());
    }

    /**
     * create a logicalSetOperation multi.
     */
    default PatternDescriptor<LogicalSetOperation> logicalSetOperation() {
        return new PatternDescriptor(
                new TypePattern(LogicalSetOperation.class, multi().pattern),
                defaultPromise());
    }

    /**
     * create a logicalUnion pattern.
     */
    default PatternDescriptor<LogicalUnion>
            logicalUnion(
                PatternDescriptor... children) {
        return new PatternDescriptor(
                new TypePattern(LogicalUnion.class,
                        Arrays.stream(children)
                                .map(PatternDescriptor::getPattern)
                                .toArray(Pattern[]::new)),
                defaultPromise());
    }

    /**
     * create a logicalUnion multi.
     */
    default PatternDescriptor<LogicalUnion> logicalUnion() {
        return new PatternDescriptor(
                new TypePattern(LogicalUnion.class, multi().pattern),
                defaultPromise());
    }

    /**
     * create a logicalExcept pattern.
     */
    default PatternDescriptor<LogicalExcept>
            logicalExcept(
                PatternDescriptor... children) {
        return new PatternDescriptor(
                new TypePattern(LogicalExcept.class,
                        Arrays.stream(children)
                                .map(PatternDescriptor::getPattern)
                                .toArray(Pattern[]::new)),
                defaultPromise());
    }

    /**
     * create a logicalExcept multi.
     */
    default PatternDescriptor<LogicalExcept> logicalExcept() {
        return new PatternDescriptor(
                new TypePattern(LogicalExcept.class, multi().pattern),
                defaultPromise());
    }

    /**
     * create a logicalUnion pattern.
     */
    default PatternDescriptor<LogicalIntersect>
            logicalIntersect(
                PatternDescriptor... children) {
        return new PatternDescriptor(
                new TypePattern(LogicalIntersect.class,
                        Arrays.stream(children)
                                .map(PatternDescriptor::getPattern)
                                .toArray(Pattern[]::new)),
                defaultPromise());
    }

    /**
     * create a logicalUnion multi.
     */
    default PatternDescriptor<LogicalIntersect> logicalIntersect() {
        return new PatternDescriptor(
                new TypePattern(LogicalIntersect.class, multi().pattern),
                defaultPromise());
    }

    /* abstract physical plan patterns */

    /**
     * create a physicalLeaf pattern.
     */
    default PatternDescriptor<PhysicalLeaf> physicalLeaf() {
        return new PatternDescriptor(new TypePattern(PhysicalLeaf.class), defaultPromise());
    }

    /**
     * create a physicalUnary pattern.
     */
    default PatternDescriptor<PhysicalUnary<Plan>> physicalUnary() {
        return new PatternDescriptor(new TypePattern(PhysicalUnary.class, Pattern.ANY), defaultPromise());
    }

    /**
     * create a physicalUnary pattern.
     */
    default <C extends Plan> PatternDescriptor<PhysicalUnary<C>>
            physicalUnary(PatternDescriptor<C> child) {
        return new PatternDescriptor(new TypePattern(PhysicalUnary.class, child.pattern), defaultPromise());
    }

    /**
     * create a physicalBinary pattern.
     */
    default PatternDescriptor<PhysicalBinary<Plan, Plan>> physicalBinary() {
        return new PatternDescriptor(
                new TypePattern(PhysicalBinary.class, Pattern.ANY, Pattern.ANY),
                defaultPromise()
        );
    }

    /**
     * create a physicalBinary pattern.
     */
    default <LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
            PatternDescriptor<PhysicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>>
            physicalBinary(
                PatternDescriptor<LEFT_CHILD_TYPE> leftChild,
                PatternDescriptor<RIGHT_CHILD_TYPE> rightChild) {
        return new PatternDescriptor(
                new TypePattern(PhysicalBinary.class, leftChild.pattern, rightChild.pattern),
                defaultPromise()
        );
    }

    /**
     * create a physicalRelation pattern.
     */
    default PatternDescriptor<PhysicalRelation> physicalRelation() {
        return new PatternDescriptor(new TypePattern(PhysicalRelation.class), defaultPromise());
    }

    /**
     * create a aggregate pattern.
     */
    default PatternDescriptor<Aggregate<Plan>> aggregate() {
        return new PatternDescriptor(new TypePattern(Aggregate.class, Pattern.ANY), defaultPromise());
    }

    /**
     * create a aggregate pattern.
     */
    default <C extends Plan> PatternDescriptor<Aggregate<C>> aggregate(PatternDescriptor<C> child) {
        return new PatternDescriptor(new TypePattern(Aggregate.class, child.pattern), defaultPromise());
    }
}
