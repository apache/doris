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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test for Expression hasUnbound assignment logic.
 */
public class ExpressionUnboundTest {

    /**
     * Mock bound expression for testing.
     */
    private static class MockBoundExpression extends Expression {
        public MockBoundExpression() {
            super();
        }

        public MockBoundExpression(Expression... children) {
            super(children);
        }

        public MockBoundExpression(List<Expression> children) {
            super(children);
        }

        @Override
        public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
            return null;
        }

        @Override
        public DataType getDataType() {
            return IntegerType.INSTANCE;
        }

        @Override
        public boolean nullable() {
            return false;
        }

        @Override
        public Expression withChildren(List<Expression> children) {
            return new MockBoundExpression(children);
        }

        @Override
        protected String computeToSql() {
            return toSql();
        }
    }

    /**
     * Mock unbound expression for testing.
     */
    private static class MockUnboundExpression extends Expression {
        public MockUnboundExpression() {
            super();
        }

        public MockUnboundExpression(Expression... children) {
            super(children);
        }

        public MockUnboundExpression(List<Expression> children) {
            super(children);
        }

        @Override
        public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
            return null;
        }

        @Override
        public DataType getDataType() {
            return IntegerType.INSTANCE;
        }

        @Override
        public boolean nullable() {
            return false;
        }

        @Override
        public Expression withChildren(List<Expression> children) {
            return new MockUnboundExpression(children);
        }

        @Override
        protected String computeToSql() {
            return toSql();
        }

        // Override hasUnbound to return true for testing
        @Override
        public boolean hasUnbound() {
            return true;
        }
    }

    /**
     * Test case 1: Single bound child (line 115 logic).
     * When there's one bound child, parent should be bound.
     */
    @Test
    public void testSingleBoundChild() {
        // Create a bound child expression
        MockBoundExpression boundChild = new MockBoundExpression();
        Assertions.assertFalse(boundChild.hasUnbound(), "Bound child should not have unbound");

        // Create parent with single bound child
        MockBoundExpression parent = new MockBoundExpression(boundChild);

        // Verify line 115: hasUnbound = child.hasUnbound();
        Assertions.assertFalse(parent.hasUnbound(), "Parent with bound child should not have unbound");
    }

    /**
     * Test case 2: Single unbound child (line 115 logic).
     * When there's one unbound child, parent should be unbound.
     */
    @Test
    public void testSingleUnboundChild() {
        // Create an unbound child expression
        MockUnboundExpression unboundChild = new MockUnboundExpression();
        Assertions.assertTrue(unboundChild.hasUnbound(), "Unbound child should have unbound");

        // Create parent with single unbound child
        MockBoundExpression parent = new MockBoundExpression(unboundChild);

        // Verify line 115: hasUnbound = child.hasUnbound();
        Assertions.assertTrue(parent.hasUnbound(), "Parent with unbound child should have unbound");
    }

    /**
     * Test case 3: Two bound children (line 125 logic).
     * When both children are bound, parent should be bound.
     */
    @Test
    public void testTwoBoundChildren() {
        // Create two bound child expressions
        MockBoundExpression leftChild = new MockBoundExpression();
        MockBoundExpression rightChild = new MockBoundExpression();
        Assertions.assertFalse(leftChild.hasUnbound(), "Left bound child should not have unbound");
        Assertions.assertFalse(rightChild.hasUnbound(), "Right bound child should not have unbound");

        // Create parent with two bound children
        MockBoundExpression parent = new MockBoundExpression(leftChild, rightChild);

        // Verify line 125: hasUnbound = left.hasUnbound() || right.hasUnbound();
        Assertions.assertFalse(parent.hasUnbound(), "Parent with two bound children should not have unbound");
    }

    /**
     * Test case 4: Left unbound, right bound (line 125 logic).
     * When left child is unbound, parent should be unbound.
     */
    @Test
    public void testLeftUnboundRightBound() {
        // Create unbound left and bound right child
        MockUnboundExpression leftChild = new MockUnboundExpression();
        MockBoundExpression rightChild = new MockBoundExpression();
        Assertions.assertTrue(leftChild.hasUnbound(), "Left unbound child should have unbound");
        Assertions.assertFalse(rightChild.hasUnbound(), "Right bound child should not have unbound");

        // Create parent with left unbound, right bound
        MockBoundExpression parent = new MockBoundExpression(leftChild, rightChild);

        // Verify line 125: hasUnbound = left.hasUnbound() || right.hasUnbound();
        Assertions.assertTrue(parent.hasUnbound(), "Parent with left unbound child should have unbound");
    }

    /**
     * Test case 5: Left bound, right unbound (line 125 logic).
     * When right child is unbound, parent should be unbound.
     */
    @Test
    public void testLeftBoundRightUnbound() {
        // Create bound left and unbound right child
        MockBoundExpression leftChild = new MockBoundExpression();
        MockUnboundExpression rightChild = new MockUnboundExpression();
        Assertions.assertFalse(leftChild.hasUnbound(), "Left bound child should not have unbound");
        Assertions.assertTrue(rightChild.hasUnbound(), "Right unbound child should have unbound");

        // Create parent with left bound, right unbound
        MockBoundExpression parent = new MockBoundExpression(leftChild, rightChild);

        // Verify line 125: hasUnbound = left.hasUnbound() || right.hasUnbound();
        Assertions.assertTrue(parent.hasUnbound(), "Parent with right unbound child should have unbound");
    }

    /**
     * Test case 6: Both children unbound (line 125 logic).
     * When both children are unbound, parent should be unbound.
     */
    @Test
    public void testTwoUnboundChildren() {
        // Create two unbound child expressions
        MockUnboundExpression leftChild = new MockUnboundExpression();
        MockUnboundExpression rightChild = new MockUnboundExpression();
        Assertions.assertTrue(leftChild.hasUnbound(), "Left unbound child should have unbound");
        Assertions.assertTrue(rightChild.hasUnbound(), "Right unbound child should have unbound");

        // Create parent with two unbound children
        MockBoundExpression parent = new MockBoundExpression(leftChild, rightChild);

        // Verify line 125: hasUnbound = left.hasUnbound() || right.hasUnbound();
        Assertions.assertTrue(parent.hasUnbound(), "Parent with two unbound children should have unbound");
    }
}
