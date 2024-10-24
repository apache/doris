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

package org.apache.doris.datasource.lakesoul;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import io.substrait.expression.Expression;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LakeSoulPredicateTest {

    public static Schema schema;

    @BeforeClass
    public static void before() throws AnalysisException, IOException {
        schema = new Schema(
                Arrays.asList(
                    new Field("c_int", FieldType.nullable(new ArrowType.Int(32, true)), null),
                    new Field("c_long", FieldType.nullable(new ArrowType.Int(64, true)), null),
                    new Field("c_bool", FieldType.nullable(new ArrowType.Bool()), null),
                    new Field("c_float", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
                    new Field("c_double", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                    new Field("c_dec", FieldType.nullable(new ArrowType.Decimal(20, 10)), null),
                    new Field("c_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                    new Field("c_ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")), null),
                    new Field("c_str", FieldType.nullable(new ArrowType.Utf8()), null)
                ));
    }

    @Test
    public void testBinaryPredicate() throws AnalysisException, IOException {
        List<LiteralExpr> literalList = new ArrayList<LiteralExpr>() {{
                add(new BoolLiteral(true));
                add(new DateLiteral("2023-01-02", Type.DATEV2));
                add(new DateLiteral("2024-01-02 12:34:56.123456", Type.DATETIMEV2));
                add(new DecimalLiteral(new BigDecimal("1.23")));
                add(new FloatLiteral(1.23, Type.FLOAT));
                add(new FloatLiteral(3.456, Type.DOUBLE));
                add(new IntLiteral(1, Type.TINYINT));
                add(new IntLiteral(1, Type.SMALLINT));
                add(new IntLiteral(1, Type.INT));
                add(new IntLiteral(1, Type.BIGINT));
                add(new StringLiteral("abc"));
                add(new StringLiteral("2023-01-02"));
                add(new StringLiteral("2023-01-02 01:02:03.456789"));
            }};

        List<SlotRef> slotRefs = new ArrayList<SlotRef>() {{
                add(new SlotRef(new TableName(), "c_int"));
                add(new SlotRef(new TableName(), "c_long"));
                add(new SlotRef(new TableName(), "c_bool"));
                add(new SlotRef(new TableName(), "c_float"));
                add(new SlotRef(new TableName(), "c_double"));
                add(new SlotRef(new TableName(), "c_dec"));
                add(new SlotRef(new TableName(), "c_date"));
                add(new SlotRef(new TableName(), "c_ts"));
                add(new SlotRef(new TableName(), "c_str"));
            }};

        // true indicates support for pushdown
        Boolean[][] expects = new Boolean[][] {
                { // int
                        false, false, false, false, false, false, true, true, true, true, false, false, false
                },
                { // long
                        false, false, false, false, false, false, true, true, true, true, false, false, false
                },
                { // boolean
                        true, false, false, false, false, false, false, false, false, false, false, false, false
                },
                { // float
                        false, false, false, false, true, false, true, true, true, true, false, false, false
                },
                { // double
                        false, false, false, true, true, true, true, true, true, true, false, false, false
                },
                { // decimal
                        false, false, false, true, true, true, true, true, true, true, false, false, false
                },
                { // date
                        false, true, false, false, false, false, true, true, true, true, false, true, false
                },
                { // timestamp
                        false, true, true, false, false, false, false, false, false, true, false, false, false
                },
                { // string
                        true, true, true, true, false, false, false, false, false, false, true, true, true
                }
        };

        ArrayListMultimap<Boolean, Expr> validPredicateMap = ArrayListMultimap.create();

        // binary predicate
        for (int i = 0; i < slotRefs.size(); i++) {
            final int loc = i;
            List<Boolean> ret = literalList.stream().map(literal -> {
                BinaryPredicate expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRefs.get(loc), literal);
                Expression expression = null;
                try {
                    expression = LakeSoulUtils.convertToSubstraitExpr(expr, schema);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                validPredicateMap.put(expression != null, expr);
                return expression != null;
            }).collect(Collectors.toList());
            Assert.assertArrayEquals(expects[i], ret.toArray());
        }

        // in predicate
        for (int i = 0; i < slotRefs.size(); i++) {
            final int loc = i;
            List<Boolean> ret = literalList.stream().map(literal -> {
                InPredicate expr = new InPredicate(slotRefs.get(loc), Lists.newArrayList(literal), false);
                Expression expression = null;
                try {
                    expression = LakeSoulUtils.convertToSubstraitExpr(expr, schema);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                validPredicateMap.put(expression != null, expr);
                return expression != null;
            }).collect(Collectors.toList());
            Assert.assertArrayEquals(expects[i], ret.toArray());
        }

        // not in predicate
        for (int i = 0; i < slotRefs.size(); i++) {
            final int loc = i;
            List<Boolean> ret = literalList.stream().map(literal -> {
                InPredicate expr = new InPredicate(slotRefs.get(loc), Lists.newArrayList(literal), true);
                Expression expression = null;
                try {
                    expression = LakeSoulUtils.convertToSubstraitExpr(expr, schema);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                validPredicateMap.put(expression != null, expr);
                return expression != null;
            }).collect(Collectors.toList());
            Assert.assertArrayEquals(expects[i], ret.toArray());
        }

        // bool literal

        Expression trueExpr = LakeSoulUtils.convertToSubstraitExpr(new BoolLiteral(true), schema);
        Expression falseExpr = LakeSoulUtils.convertToSubstraitExpr(new BoolLiteral(false), schema);
        Assert.assertEquals(SubstraitUtil.CONST_TRUE, trueExpr);
        Assert.assertEquals(SubstraitUtil.CONST_FALSE, falseExpr);
        validPredicateMap.put(true, new BoolLiteral(true));
        validPredicateMap.put(true, new BoolLiteral(false));

        List<Expr> validExprs = validPredicateMap.get(true);
        List<Expr> invalidExprs = validPredicateMap.get(false);
        // OR predicate
        // both valid
        for (int i = 0; i < validExprs.size(); i++) {
            for (int j = 0; j < validExprs.size(); j++) {
                CompoundPredicate orPredicate = new CompoundPredicate(Operator.OR,
                        validExprs.get(i), validExprs.get(j));
                Expression expression = LakeSoulUtils.convertToSubstraitExpr(orPredicate, schema);
                Assert.assertNotNull("pred: " + orPredicate.toSql(), expression);
            }
        }
        // both invalid
        for (int i = 0; i < invalidExprs.size(); i++) {
            for (int j = 0; j < invalidExprs.size(); j++) {
                CompoundPredicate orPredicate = new CompoundPredicate(Operator.OR,
                        invalidExprs.get(i), invalidExprs.get(j));
                Expression expression = LakeSoulUtils.convertToSubstraitExpr(orPredicate, schema);
                Assert.assertNull("pred: " + orPredicate.toSql(), expression);
            }
        }
        // valid or invalid
        for (int i = 0; i < validExprs.size(); i++) {
            for (int j = 0; j < invalidExprs.size(); j++) {
                CompoundPredicate orPredicate = new CompoundPredicate(Operator.OR,
                        validExprs.get(i), invalidExprs.get(j));
                Expression expression = LakeSoulUtils.convertToSubstraitExpr(orPredicate, schema);
                Assert.assertNull("pred: " + orPredicate.toSql(), expression);
            }
        }

        // AND predicate
        // both valid
        for (int i = 0; i < validExprs.size(); i++) {
            for (int j = 0; j < validExprs.size(); j++) {
                CompoundPredicate andPredicate = new CompoundPredicate(Operator.AND,
                        validExprs.get(i), validExprs.get(j));
                Expression expression = LakeSoulUtils.convertToSubstraitExpr(andPredicate, schema);
                Assert.assertNotNull("pred: " + andPredicate.toSql(), expression);
            }
        }
        // both invalid
        for (int i = 0; i < invalidExprs.size(); i++) {
            for (int j = 0; j < invalidExprs.size(); j++) {
                CompoundPredicate andPredicate = new CompoundPredicate(Operator.AND,
                        invalidExprs.get(i), invalidExprs.get(j));
                Expression expression = LakeSoulUtils.convertToSubstraitExpr(andPredicate, schema);
                Assert.assertNull("pred: " + andPredicate.toSql(), expression);
            }
        }
        // valid and invalid
        for (int i = 0; i < validExprs.size(); i++) {
            for (int j = 0; j < invalidExprs.size(); j++) {
                CompoundPredicate andPredicate = new CompoundPredicate(Operator.AND,
                        validExprs.get(i), invalidExprs.get(j));
                Expression expression = LakeSoulUtils.convertToSubstraitExpr(andPredicate, schema);
                Assert.assertNotNull("pred: " + andPredicate.toSql(), expression);
                Assert.assertEquals(SubstraitUtil.substraitExprToProto(LakeSoulUtils.convertToSubstraitExpr(validExprs.get(i), schema), "table"),
                        SubstraitUtil.substraitExprToProto(expression, "table"));
            }
        }

        // NOT predicate
        // valid
        for (int i = 0; i < validExprs.size(); i++) {
            CompoundPredicate notPredicate = new CompoundPredicate(Operator.NOT,
                    validExprs.get(i), null);
            Expression expression = LakeSoulUtils.convertToSubstraitExpr(notPredicate, schema);
            Assert.assertNotNull("pred: " + notPredicate.toSql(), expression);
        }
        // invalid
        for (int i = 0; i < invalidExprs.size(); i++) {
            CompoundPredicate notPredicate = new CompoundPredicate(Operator.NOT,
                    invalidExprs.get(i), null);
            Expression expression = LakeSoulUtils.convertToSubstraitExpr(notPredicate, schema);
            Assert.assertNull("pred: " + notPredicate.toSql(), expression);
        }
    }
}
