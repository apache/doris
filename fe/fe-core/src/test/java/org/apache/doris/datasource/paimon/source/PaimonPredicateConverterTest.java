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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class PaimonPredicateConverterTest {

    private PaimonPredicateConverter converter;
    private RowType schema;

    @Before
    public void setUp() {
        // Create a simple Paimon schema for testing
        schema = RowType.of(
                new org.apache.paimon.types.DataType[] {
                        DataTypes.STRING(),
                        DataTypes.INT(),
                        DataTypes.DATE(),
                        DataTypes.TIMESTAMP(),
                        DataTypes.FLOAT(),
                        DataTypes.DECIMAL(10, 2)
                },
                new String[] { "c_str", "c_int", "c_date", "c_timestamp", "c_float", "c_dec" });

        converter = new PaimonPredicateConverter(schema);
    }

    @Test
    public void testCastExpressionNotPushedDown() throws AnalysisException {
        // Test that expressions containing CAST on columns are not pushed down to
        // Paimon.

        // Test 1: Basic CAST predicate - CAST(string_col AS datetime) = literal
        SlotRef stringColumn = new SlotRef(new TableName(), "c_str");
        stringColumn.setType(Type.STRING);
        CastExpr castToDatetime = new CastExpr(Type.DATETIMEV2, stringColumn);
        StringLiteral datetimeLiteral = new StringLiteral("2025-06-10 00:00:00");
        BinaryPredicate castPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, castToDatetime,
                datetimeLiteral);

        List<Predicate> predicates = converter.convertToPaimonExpr(Lists.newArrayList(castPredicate));
        Assert.assertTrue("CAST(string_col AS datetime) = literal should not be pushed down", predicates.isEmpty());

        // Test 2: CAST with comparison operator - CAST(string_col AS date) >= literal
        CastExpr castToDate = new CastExpr(Type.DATEV2, stringColumn);
        StringLiteral dateLiteral = new StringLiteral("2025-06-10");
        BinaryPredicate castGePredicate = new BinaryPredicate(BinaryPredicate.Operator.GE, castToDate, dateLiteral);

        predicates = converter.convertToPaimonExpr(Lists.newArrayList(castGePredicate));
        Assert.assertTrue("CAST(string_col AS date) >= literal should not be pushed down", predicates.isEmpty());

        // Test 3: CAST with IN predicate - CAST(string_col AS int) IN (1, 2, 3)
        CastExpr castToInt = new CastExpr(Type.INT, stringColumn);
        List<Expr> inList = Lists.newArrayList(
                new IntLiteral(1, Type.INT),
                new IntLiteral(2, Type.INT),
                new IntLiteral(3, Type.INT));
        InPredicate castInPredicate = new InPredicate(castToInt, inList, false);

        predicates = converter.convertToPaimonExpr(Lists.newArrayList(castInPredicate));
        Assert.assertTrue("CAST(string_col AS int) IN (...) should not be pushed down", predicates.isEmpty());

        // Test 4: CAST with NOT IN predicate - CAST(int_col AS string) NOT IN ('a',
        // 'b')
        SlotRef intColumn = new SlotRef(new TableName(), "c_int");
        intColumn.setType(Type.INT);
        CastExpr castIntToString = new CastExpr(Type.STRING, intColumn);
        List<Expr> notInList = Lists.newArrayList(
                new StringLiteral("a"),
                new StringLiteral("b"),
                new StringLiteral("c"));
        InPredicate castNotInPredicate = new InPredicate(castIntToString, notInList, true);

        predicates = converter.convertToPaimonExpr(Lists.newArrayList(castNotInPredicate));
        Assert.assertTrue("CAST(int_col AS string) NOT IN (...) should not be pushed down", predicates.isEmpty());

        // Test 5: Reverse operand order - literal = CAST(date_col AS string)
        SlotRef dateColumn = new SlotRef(new TableName(), "c_date");
        dateColumn.setType(Type.DATEV2);
        CastExpr castDateToString = new CastExpr(Type.STRING, dateColumn);
        StringLiteral stringLiteral = new StringLiteral("2025-06-10");
        BinaryPredicate reversePredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, stringLiteral,
                castDateToString);

        predicates = converter.convertToPaimonExpr(Lists.newArrayList(reversePredicate));
        Assert.assertTrue("literal = CAST(date_col AS string) should not be pushed down", predicates.isEmpty());

        // Test 6: Mixed predicates - combine normal predicate with cast predicate
        BinaryPredicate normalPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, intColumn,
                new IntLiteral(100, Type.INT));

        predicates = converter.convertToPaimonExpr(Lists.newArrayList(normalPredicate, castPredicate));
        // Should only push down the normal predicate, not the cast predicate
        Assert.assertEquals("Should push down only the normal predicate", 1, predicates.size());

        // Test 7: Only normal predicate (for comparison)
        predicates = converter.convertToPaimonExpr(Lists.newArrayList(normalPredicate));
        Assert.assertEquals("Normal predicate should be pushed down", 1, predicates.size());

        // Test 8: Nested CAST - CAST(CAST(string_col AS int) AS bigint) = literal
        CastExpr nestedCast = new CastExpr(Type.BIGINT, castToInt);
        BinaryPredicate nestedCastPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, nestedCast,
                new IntLiteral(1000, Type.BIGINT));

        predicates = converter.convertToPaimonExpr(Lists.newArrayList(nestedCastPredicate));
        Assert.assertTrue("Nested CAST should not be pushed down", predicates.isEmpty());

        // Test 9: Different types of CAST operations
        // CAST float to int
        SlotRef floatColumn = new SlotRef(new TableName(), "c_float");
        floatColumn.setType(Type.FLOAT);
        CastExpr castFloatToInt = new CastExpr(Type.INT, floatColumn);
        BinaryPredicate floatCastPredicate = new BinaryPredicate(BinaryPredicate.Operator.LT, castFloatToInt,
                new IntLiteral(10, Type.INT));

        predicates = converter.convertToPaimonExpr(Lists.newArrayList(floatCastPredicate));
        Assert.assertTrue("CAST(float_col AS int) should not be pushed down", predicates.isEmpty());

        // CAST decimal to string
        SlotRef decimalColumn = new SlotRef(new TableName(), "c_dec");
        decimalColumn.setType(Type.DECIMALV2);
        CastExpr castDecimalToString = new CastExpr(Type.STRING, decimalColumn);
        BinaryPredicate decimalCastPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, castDecimalToString,
                new StringLiteral("123.45"));

        predicates = converter.convertToPaimonExpr(Lists.newArrayList(decimalCastPredicate));
        Assert.assertTrue("CAST(decimal_col AS string) should not be pushed down", predicates.isEmpty());
    }

    @Test
    public void testConvertDorisExprToSlotRefRejectsCast() {
        // Test that convertDorisExprToSlotRef returns null for any CAST expression

        SlotRef stringColumn = new SlotRef(new TableName(), "c_str");
        stringColumn.setType(Type.STRING);

        // Test 1: Direct SlotRef should work
        SlotRef result = PaimonPredicateConverter.convertDorisExprToSlotRef(stringColumn);
        Assert.assertNotNull("Direct SlotRef should be returned", result);
        Assert.assertEquals("Should return the same SlotRef", stringColumn, result);

        // Test 2: CAST expression should return null
        CastExpr castExpr = new CastExpr(Type.INT, stringColumn);
        result = PaimonPredicateConverter.convertDorisExprToSlotRef(castExpr);
        Assert.assertNull("CAST expression should return null", result);

        // Test 3: Nested CAST should also return null
        CastExpr nestedCast = new CastExpr(Type.BIGINT, castExpr);
        result = PaimonPredicateConverter.convertDorisExprToSlotRef(nestedCast);
        Assert.assertNull("Nested CAST expression should return null", result);
    }
}