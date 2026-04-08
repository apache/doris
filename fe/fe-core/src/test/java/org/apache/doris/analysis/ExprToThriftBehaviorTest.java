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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Pre-refactoring tests capturing current toThrift() / treeToThrift() behavior
 * for representative Expr subclasses.  These tests act as a safety net so the
 * visitor-pattern refactor can be verified against the existing behavior.
 */
public class ExprToThriftBehaviorTest {

    // ======================== Simple Literals ========================

    @Test
    public void testBoolLiteralTrue() {
        BoolLiteral expr = new BoolLiteral(true);
        TExprNode node = firstNode(expr);

        Assertions.assertEquals(TExprNodeType.BOOL_LITERAL, node.node_type);
        Assertions.assertTrue(node.bool_literal.isValue());
    }

    @Test
    public void testBoolLiteralFalse() {
        BoolLiteral expr = new BoolLiteral(false);
        TExprNode node = firstNode(expr);

        Assertions.assertEquals(TExprNodeType.BOOL_LITERAL, node.node_type);
        Assertions.assertFalse(node.bool_literal.isValue());
    }

    @Test
    public void testIntLiteral() {
        IntLiteral expr = new IntLiteral(42);
        TExprNode node = firstNode(expr);

        Assertions.assertEquals(TExprNodeType.INT_LITERAL, node.node_type);
        Assertions.assertEquals(42, node.int_literal.getValue());
    }

    @Test
    public void testFloatLiteral() {
        FloatLiteral expr = new FloatLiteral(3.14);
        TExprNode node = firstNode(expr);

        Assertions.assertEquals(TExprNodeType.FLOAT_LITERAL, node.node_type);
        Assertions.assertEquals(3.14, node.float_literal.getValue(), 1e-15);
    }

    @Test
    public void testStringLiteral() {
        StringLiteral expr = new StringLiteral("hello");
        TExprNode node = firstNode(expr);

        Assertions.assertEquals(TExprNodeType.STRING_LITERAL, node.node_type);
        Assertions.assertEquals("hello", node.string_literal.getValue());
    }

    @Test
    public void testNullLiteral() {
        NullLiteral expr = new NullLiteral();
        TExprNode node = firstNode(expr);

        Assertions.assertEquals(TExprNodeType.NULL_LITERAL, node.node_type);
    }

    @Test
    public void testLargeIntLiteral() throws AnalysisException {
        LargeIntLiteral expr = new LargeIntLiteral("12345678901234567890");
        TExprNode node = firstNode(expr);

        Assertions.assertEquals(TExprNodeType.LARGE_INT_LITERAL, node.node_type);
        Assertions.assertEquals("12345678901234567890", node.large_int_literal.getValue());
    }

    // ======================== Predicates / Expressions ========================

    @Test
    public void testBinaryPredicateEq() {
        IntLiteral left = new IntLiteral(1);
        IntLiteral right = new IntLiteral(2);
        BinaryPredicate expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, left, right);
        expr.setType(Type.BOOLEAN);

        TExpr texpr = ExprToThriftVisitor.treeToThrift(expr);
        List<TExprNode> nodes = texpr.getNodes();

        TExprNode predNode = nodes.get(0);
        Assertions.assertEquals(TExprNodeType.BINARY_PRED, predNode.node_type);
        Assertions.assertEquals(TExprOpcode.EQ, predNode.getOpcode());
        Assertions.assertEquals(2, predNode.num_children);

        // Children follow in pre-order
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, nodes.get(1).node_type);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, nodes.get(2).node_type);
    }

    @Test
    public void testCompoundPredicateAnd() {
        BoolLiteral left = new BoolLiteral(true);
        BoolLiteral right = new BoolLiteral(false);
        CompoundPredicate expr = new CompoundPredicate(CompoundPredicate.Operator.AND, left, right);
        expr.setType(Type.BOOLEAN);

        TExpr texpr = ExprToThriftVisitor.treeToThrift(expr);
        List<TExprNode> nodes = texpr.getNodes();

        TExprNode predNode = nodes.get(0);
        Assertions.assertEquals(TExprNodeType.COMPOUND_PRED, predNode.node_type);
        Assertions.assertEquals(TExprOpcode.COMPOUND_AND, predNode.getOpcode());
        Assertions.assertEquals(2, predNode.num_children);

        Assertions.assertEquals(TExprNodeType.BOOL_LITERAL, nodes.get(1).node_type);
        Assertions.assertEquals(TExprNodeType.BOOL_LITERAL, nodes.get(2).node_type);
    }

    @Test
    public void testCompoundPredicateOr() {
        BoolLiteral left = new BoolLiteral(true);
        BoolLiteral right = new BoolLiteral(false);
        CompoundPredicate expr = new CompoundPredicate(CompoundPredicate.Operator.OR, left, right);
        expr.setType(Type.BOOLEAN);

        TExprNode node = firstNode(expr);
        Assertions.assertEquals(TExprNodeType.COMPOUND_PRED, node.node_type);
        Assertions.assertEquals(TExprOpcode.COMPOUND_OR, node.getOpcode());
    }

    @Test
    public void testArithmeticExprAdd() {
        IntLiteral left = new IntLiteral(1);
        IntLiteral right = new IntLiteral(2);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, left, right,
                Type.BIGINT, NullableMode.DEPEND_ON_ARGUMENT, false);

        TExpr texpr = ExprToThriftVisitor.treeToThrift(expr);
        List<TExprNode> nodes = texpr.getNodes();

        TExprNode arithNode = nodes.get(0);
        Assertions.assertEquals(TExprNodeType.ARITHMETIC_EXPR, arithNode.node_type);
        Assertions.assertEquals(TExprOpcode.ADD, arithNode.getOpcode());
        Assertions.assertEquals(2, arithNode.num_children);
    }

    @Test
    public void testArithmeticExprDecimalNoOpcode() {
        IntLiteral left = new IntLiteral(1);
        IntLiteral right = new IntLiteral(2);
        ArithmeticExpr expr = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, left, right,
                Type.DECIMALV2, NullableMode.DEPEND_ON_ARGUMENT, false);

        TExprNode node = firstNode(expr);
        Assertions.assertEquals(TExprNodeType.ARITHMETIC_EXPR, node.node_type);
        // For decimal types, opcode should NOT be set
        Assertions.assertFalse(node.isSetOpcode());
    }

    @Test
    public void testIsNullPredicate() {
        IntLiteral child = new IntLiteral(1);
        IsNullPredicate expr = new IsNullPredicate(child, false);
        expr.setType(Type.BOOLEAN);

        TExpr texpr = ExprToThriftVisitor.treeToThrift(expr);
        List<TExprNode> nodes = texpr.getNodes();

        TExprNode predNode = nodes.get(0);
        Assertions.assertEquals(TExprNodeType.FUNCTION_CALL, predNode.node_type);
        Assertions.assertEquals(1, predNode.num_children);
    }

    @Test
    public void testInPredicateIn() {
        IntLiteral compareExpr = new IntLiteral(1);
        List<Expr> inList = Lists.newArrayList(new IntLiteral(2), new IntLiteral(3));
        InPredicate expr = new InPredicate(compareExpr, inList, false, true, false);

        TExprNode node = firstNode(expr);
        Assertions.assertEquals(TExprNodeType.IN_PRED, node.node_type);
        Assertions.assertFalse(node.in_predicate.isIsNotIn());
        Assertions.assertEquals(TExprOpcode.FILTER_IN, node.getOpcode());
    }

    @Test
    public void testInPredicateNotIn() {
        IntLiteral compareExpr = new IntLiteral(1);
        List<Expr> inList = Lists.newArrayList(new IntLiteral(2), new IntLiteral(3));
        InPredicate expr = new InPredicate(compareExpr, inList, true, true, false);

        TExprNode node = firstNode(expr);
        Assertions.assertEquals(TExprNodeType.IN_PRED, node.node_type);
        Assertions.assertTrue(node.in_predicate.isIsNotIn());
        Assertions.assertEquals(TExprOpcode.FILTER_NOT_IN, node.getOpcode());
    }

    // ======================== CastExpr ========================

    @Test
    public void testCastExprNoOpFalse() {
        // Cast TINYINT to BIGINT -> noOp should be false (different types)
        IntLiteral child = new IntLiteral(42);
        CastExpr expr = new CastExpr(Type.BIGINT, child, false);

        TExpr texpr = ExprToThriftVisitor.treeToThrift(expr);
        List<TExprNode> nodes = texpr.getNodes();

        // Should produce a CAST_EXPR node followed by the child
        Assertions.assertEquals(2, nodes.size());
        Assertions.assertEquals(TExprNodeType.CAST_EXPR, nodes.get(0).node_type);
        Assertions.assertEquals(TExprOpcode.CAST, nodes.get(0).getOpcode());
        Assertions.assertEquals(1, nodes.get(0).num_children);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, nodes.get(1).node_type);
    }

    @Test
    public void testCastExprNoOpTrue() {
        // Cast TINYINT to TINYINT -> noOp should be true (same type)
        IntLiteral child = new IntLiteral(42);
        // IntLiteral(42) gets type TINYINT from init()
        CastExpr expr = new CastExpr(Type.TINYINT, child, false);

        TExpr texpr = ExprToThriftVisitor.treeToThrift(expr);
        List<TExprNode> nodes = texpr.getNodes();

        // noOp=true means the cast node is skipped; only the child node appears
        Assertions.assertEquals(1, nodes.size());
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, nodes.get(0).node_type);
    }

    // ======================== treesToThrift ========================

    @Test
    public void testTreesToThrift() {
        BoolLiteral b = new BoolLiteral(true);
        IntLiteral i = new IntLiteral(7);
        StringLiteral s = new StringLiteral("test");

        List<TExpr> result = ExprToThriftVisitor.treesToThrift(Lists.newArrayList(b, i, s));

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(TExprNodeType.BOOL_LITERAL, result.get(0).getNodes().get(0).node_type);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, result.get(1).getNodes().get(0).node_type);
        Assertions.assertEquals(TExprNodeType.STRING_LITERAL, result.get(2).getNodes().get(0).node_type);
    }

    // ======================== Helpers ========================

    private static TExprNode firstNode(Expr expr) {
        TExpr texpr = ExprToThriftVisitor.treeToThrift(expr);
        Assertions.assertNotNull(texpr.getNodes());
        Assertions.assertFalse(texpr.getNodes().isEmpty());
        return texpr.getNodes().get(0);
    }
}
