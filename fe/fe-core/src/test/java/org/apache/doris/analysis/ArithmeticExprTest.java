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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.datasource.InternalDataSource;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ArithmeticExprTest {
    private static final String internalCtl = InternalDataSource.INTERNAL_DS_NAME;

    @Test
    public void testDecimalArithmetic(@Mocked VectorizedUtil vectorizedUtil) {
        Expr lhsExpr = new SlotRef(new TableName(internalCtl, "db", "table"), "c0");
        Expr rhsExpr = new SlotRef(new TableName(internalCtl, "db", "table"), "c1");
        ScalarType t1;
        ScalarType t2;
        ScalarType res;
        ArithmeticExpr arithmeticExpr;
        boolean hasException = false;
        new Expectations() {
            {
                vectorizedUtil.isVectorized();
                result = true;
            }
        };
        List<ArithmeticExpr.Operator> operators = Arrays.asList(ArithmeticExpr.Operator.ADD,
                ArithmeticExpr.Operator.SUBTRACT, ArithmeticExpr.Operator.MOD,
                ArithmeticExpr.Operator.MULTIPLY, ArithmeticExpr.Operator.DIVIDE);
        try {
            for (ArithmeticExpr.Operator operator : operators) {
                t1 = ScalarType.createDecimalType(9, 4);
                t2 = ScalarType.createDecimalType(19, 6);
                lhsExpr.setType(t1);
                rhsExpr.setType(t2);
                arithmeticExpr = new ArithmeticExpr(operator, lhsExpr, rhsExpr);
                res = ScalarType.createDecimalType(38, 6);
                if (operator == ArithmeticExpr.Operator.MULTIPLY) {
                    res = ScalarType.createDecimalType(38, 10);
                }
                if (operator == ArithmeticExpr.Operator.DIVIDE) {
                    res = ScalarType.createDecimalType(38, 4);
                }
                arithmeticExpr.analyzeImpl(null);
                Assert.assertEquals(arithmeticExpr.type, res);

                t1 = ScalarType.createDecimalType(9, 4);
                t2 = ScalarType.createDecimalType(18, 5);
                lhsExpr.setType(t1);
                rhsExpr.setType(t2);
                arithmeticExpr = new ArithmeticExpr(operator, lhsExpr, rhsExpr);
                res = ScalarType.createDecimalType(18, 5);
                if (operator == ArithmeticExpr.Operator.MULTIPLY) {
                    res = ScalarType.createDecimalType(18, 9);
                }
                if (operator == ArithmeticExpr.Operator.DIVIDE) {
                    res = ScalarType.createDecimalType(18, 4);
                }
                arithmeticExpr.analyzeImpl(null);
                Assert.assertEquals(arithmeticExpr.type, res);

                t1 = ScalarType.createDecimalType(9, 4);
                t2 = ScalarType.createType(PrimitiveType.BIGINT);
                lhsExpr.setType(t1);
                rhsExpr.setType(t2);
                arithmeticExpr = new ArithmeticExpr(operator, lhsExpr, rhsExpr);
                res = ScalarType.createDecimalType(18, 4);
                arithmeticExpr.analyzeImpl(null);
                Assert.assertEquals(arithmeticExpr.type, res);

                t1 = ScalarType.createDecimalType(9, 4);
                t2 = ScalarType.createType(PrimitiveType.LARGEINT);
                lhsExpr.setType(t1);
                rhsExpr.setType(t2);
                arithmeticExpr = new ArithmeticExpr(operator, lhsExpr, rhsExpr);
                res = ScalarType.createDecimalType(38, 4);
                arithmeticExpr.analyzeImpl(null);
                Assert.assertEquals(arithmeticExpr.type, res);

                t1 = ScalarType.createDecimalType(9, 4);
                t2 = ScalarType.createType(PrimitiveType.INT);
                lhsExpr.setType(t1);
                rhsExpr.setType(t2);
                arithmeticExpr = new ArithmeticExpr(operator, lhsExpr, rhsExpr);
                res = ScalarType.createDecimalType(9, 4);
                arithmeticExpr.analyzeImpl(null);
                Assert.assertEquals(arithmeticExpr.type, res);

                t1 = ScalarType.createDecimalType(9, 4);
                t2 = ScalarType.createType(PrimitiveType.FLOAT);
                lhsExpr.setType(t1);
                rhsExpr.setType(t2);
                arithmeticExpr = new ArithmeticExpr(operator, lhsExpr, rhsExpr);
                res = ScalarType.createType(PrimitiveType.DOUBLE);
                arithmeticExpr.analyzeImpl(null);
                Assert.assertEquals(arithmeticExpr.type, res);

                t1 = ScalarType.createDecimalType(9, 4);
                t2 = ScalarType.createType(PrimitiveType.DOUBLE);
                lhsExpr.setType(t1);
                rhsExpr.setType(t2);
                arithmeticExpr = new ArithmeticExpr(operator, lhsExpr, rhsExpr);
                res = ScalarType.createType(PrimitiveType.DOUBLE);
                arithmeticExpr.analyzeImpl(null);
                Assert.assertEquals(arithmeticExpr.type, res);
            }
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testDecimalBitOperation(@Mocked VectorizedUtil vectorizedUtil) {
        Expr lhsExpr = new SlotRef(new TableName(internalCtl, "db", "table"), "c0");
        Expr rhsExpr = new SlotRef(new TableName(internalCtl, "db", "table"), "c1");
        ScalarType t1;
        ScalarType t2;
        ScalarType res;
        ArithmeticExpr arithmeticExpr;
        boolean hasException = false;
        new Expectations() {
            {
                vectorizedUtil.isVectorized();
                result = true;
            }
        };
        List<ArithmeticExpr.Operator> operators = Arrays.asList(ArithmeticExpr.Operator.BITAND,
                ArithmeticExpr.Operator.BITOR, ArithmeticExpr.Operator.BITXOR);
        try {
            for (ArithmeticExpr.Operator operator : operators) {
                t1 = ScalarType.createDecimalType(9, 4);
                t2 = ScalarType.createDecimalType(19, 6);
                lhsExpr.setType(t1);
                rhsExpr.setType(t2);
                arithmeticExpr = new ArithmeticExpr(operator, lhsExpr, rhsExpr);
                res = ScalarType.createType(PrimitiveType.BIGINT);
                arithmeticExpr.analyzeImpl(null);
                Assert.assertEquals(arithmeticExpr.type, res);
                Assert.assertTrue(arithmeticExpr.getChild(0) instanceof CastExpr);
                Assert.assertTrue(arithmeticExpr.getChild(1) instanceof CastExpr);
            }
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }
}
