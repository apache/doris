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

package org.apache.doris.nereids.trees;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.plans.logical.LogicalBinaryOperator;
import org.apache.doris.nereids.operators.plans.logical.LogicalLeafOperator;
import org.apache.doris.nereids.operators.plans.logical.LogicalUnaryOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalBinaryOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalLeafOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalUnaryOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinary;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeaf;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnary;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBinary;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLeaf;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnary;

/**
 * Utility class for creating trees.
 */
public class Utils {
    /**
     * Generate a {@link TreeNode} subclass from a {@link GroupExpression}.
     *
     * @param groupExpression source to generate TreeNode
     * @return generated TreeNode subclass
     */
    public static TreeNode treeNodeWithoutChildren(GroupExpression groupExpression) {
        Operator operator = groupExpression.getOperator();
        LogicalProperties logicalProperties = groupExpression.getParent().getLogicalProperties();
        if (operator instanceof LogicalLeafOperator) {
            LogicalLeafOperator logicalLeafOperator = (LogicalLeafOperator) operator;
            return new LogicalLeaf(logicalLeafOperator, groupExpression, logicalProperties);
        }
        if (operator instanceof LogicalUnaryOperator) {
            LogicalUnaryOperator logicalUnaryOperator = (LogicalUnaryOperator) operator;
            return new LogicalUnary(logicalUnaryOperator, groupExpression, logicalProperties, null);
        }
        if (operator instanceof LogicalBinaryOperator) {
            LogicalBinaryOperator logicalBinaryOperator = (LogicalBinaryOperator) operator;
            return new LogicalBinary(logicalBinaryOperator, groupExpression, logicalProperties, null, null);
        }
        if (operator instanceof PhysicalLeafOperator) {
            PhysicalLeafOperator physicalLeafOperator = (PhysicalLeafOperator) operator;
            return new PhysicalLeaf(physicalLeafOperator, groupExpression, logicalProperties);
        }
        if (operator instanceof PhysicalUnaryOperator) {
            PhysicalUnaryOperator physicalUnaryOperator = (PhysicalUnaryOperator) operator;
            return new PhysicalUnary(physicalUnaryOperator, groupExpression, logicalProperties, null);
        }
        if (operator instanceof PhysicalBinaryOperator) {
            PhysicalBinaryOperator physicalBinaryOperator = (PhysicalBinaryOperator) operator;
            return new PhysicalBinary(physicalBinaryOperator, groupExpression, logicalProperties, null, null);
        }
        throw new RuntimeException("Unknown operator type: " + operator.getClass().getName());
    }
}
