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

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.logical.LogicalBinaryOperator;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinary;

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

    default PatternDescriptor<LogicalBinary<LogicalBinaryOperator, Plan, Plan>, Plan> logicalBinary() {
        return new PatternDescriptor(new TypePattern(LogicalBinary.class), defaultPromise());
    }

    /**
     * create a logicalJoin pattern.
     */
    default PatternDescriptor<LogicalBinary<LogicalJoin, Plan, Plan>, Plan> logicalJoin() {
        return new PatternDescriptor<>(
                new Pattern<>(OperatorType.LOGICAL_JOIN,
                        new Pattern<>(OperatorType.FIXED), new Pattern<>(OperatorType.FIXED)),
                defaultPromise()
        );
    }

    default <LEFT_CHILD_TYPE extends Plan,
            RIGHT_CHILD_TYPE extends Plan>
            PatternDescriptor<LogicalBinary<LogicalBinaryOperator, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>, Plan>
            logicalBinary(
                PatternDescriptor<LEFT_CHILD_TYPE, Plan> leftChild,
                PatternDescriptor<RIGHT_CHILD_TYPE, Plan> rightChild) {
        return new PatternDescriptor(
                new TypePattern(LogicalBinary.class, leftChild.pattern, rightChild.pattern),
                defaultPromise()
        );
    }
}
