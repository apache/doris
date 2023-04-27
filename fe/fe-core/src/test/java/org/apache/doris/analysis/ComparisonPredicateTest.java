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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;


/**
 * Comparison Predicate unit test.
 */
public class ComparisonPredicateTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    @Mocked
    Analyzer analyzer;

    @Test
    public void testMultiColumnSubquery(@Injectable Expr child0,
                                        @Injectable Subquery child1) {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, child0, child1);
        new Expectations() {
            {
                child1.returnsScalarColumn();
                result = false;
            }
        };

        try {
            binaryPredicate.analyzeImpl(analyzer);
            Assert.fail();
        } catch (AnalysisException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
    }

    @Test
    public void testSingleColumnSubquery(@Injectable Expr child0,
                                         @Injectable QueryStmt subquery,
            @Injectable SlotRef slotRef) {
        Subquery child1 = new Subquery(subquery);
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, child0, child1);
        new Expectations() {
            {
                subquery.getResultExprs();
                result = Lists.newArrayList(slotRef);
                slotRef.getType();
                result = Type.INT;
            }
        };

        try {
            binaryPredicate.analyzeImpl(analyzer);
            Assert.assertSame(null, Deencapsulation.getField(binaryPredicate, "fn"));
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testWrongOperand(@Injectable Expr child0, @Injectable Expr child1) {
        BinaryPredicate predicate1 = new BinaryPredicate(
                BinaryPredicate.Operator.EQ, child0, new StringLiteral("test"));
        BinaryPredicate predicate2 = new BinaryPredicate(
                BinaryPredicate.Operator.EQ, child1, new StringLiteral("test"));

        new Expectations() {
            {
                child0.getType();
                result = ScalarType.createType("HLL");

                child1.getType();
                result = ScalarType.createType("BITMAP");
            }
        };

        try {
            predicate1.analyzeImpl(analyzer);
            Assert.fail();
        } catch (AnalysisException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        try {
            predicate2.analyzeImpl(analyzer);
            Assert.fail();
        } catch (AnalysisException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
    }

    @Test
    public void testConvertToRange() {
        SlotRef slotRef = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr literalExpr = new IntLiteral(1);
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.LE, slotRef, literalExpr);
        Range<LiteralExpr> range = binaryPredicate.convertToRange();
        Assert.assertEquals(literalExpr, range.upperEndpoint());
        Assert.assertEquals(BoundType.CLOSED, range.upperBoundType());
        Assert.assertFalse(range.hasLowerBound());
    }

    @Test
    public void testConvertToRangeForDateV2() {
        SlotRef slotRef = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr dateExpr = new DateLiteral(2022, 5, 19, Type.DATE);
        LiteralExpr dateV2Expr = new DateLiteral(2022, 5, 19, Type.DATEV2);
        BinaryPredicate binaryPredicate1 = new BinaryPredicate(BinaryPredicate.Operator.LE, slotRef, dateExpr);
        BinaryPredicate binaryPredicate2 = new BinaryPredicate(BinaryPredicate.Operator.LE, slotRef, dateV2Expr);
        Range<LiteralExpr> range1 = binaryPredicate1.convertToRange();
        Range<LiteralExpr> range2 = binaryPredicate2.convertToRange();
        Assert.assertEquals(dateExpr, range1.upperEndpoint());
        Assert.assertEquals(dateV2Expr, range1.upperEndpoint());
        Assert.assertEquals(BoundType.CLOSED, range1.upperBoundType());
        Assert.assertFalse(range1.hasLowerBound());
        Assert.assertEquals(dateExpr, range2.upperEndpoint());
        Assert.assertEquals(dateV2Expr, range2.upperEndpoint());
        Assert.assertEquals(BoundType.CLOSED, range2.upperBoundType());
        Assert.assertFalse(range2.hasLowerBound());
    }

    @Test
    public void testConvertToRangeForDateTimeV2() {
        SlotRef slotRef = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr dateTimeExpr = new DateLiteral(2022, 5, 19, 0, 0, 0, Type.DATETIME);
        LiteralExpr dateTimeV2Expr1 = new DateLiteral(2022, 5, 19, 0, 0, 0, Type.DEFAULT_DATETIMEV2);
        LiteralExpr dateTimeV2Expr2 = new DateLiteral(2022, 5, 19, 0, 0, 0, ScalarType.createDatetimeV2Type(6));
        BinaryPredicate binaryPredicate1 = new BinaryPredicate(BinaryPredicate.Operator.LE, slotRef, dateTimeExpr);
        BinaryPredicate binaryPredicate2 = new BinaryPredicate(BinaryPredicate.Operator.LE, slotRef, dateTimeV2Expr1);
        Range<LiteralExpr> range1 = binaryPredicate1.convertToRange();
        Range<LiteralExpr> range2 = binaryPredicate2.convertToRange();

        Assert.assertEquals(dateTimeExpr, range1.upperEndpoint());
        Assert.assertEquals(dateTimeV2Expr1, range1.upperEndpoint());
        Assert.assertEquals(dateTimeV2Expr2, range1.upperEndpoint());
        Assert.assertEquals(BoundType.CLOSED, range1.upperBoundType());
        Assert.assertFalse(range1.hasLowerBound());

        Assert.assertEquals(dateTimeExpr, range2.upperEndpoint());
        Assert.assertEquals(dateTimeV2Expr1, range2.upperEndpoint());
        Assert.assertEquals(BoundType.CLOSED, range2.upperBoundType());
        Assert.assertFalse(range2.hasLowerBound());
        Assert.assertEquals(dateTimeV2Expr2, range2.upperEndpoint());
    }
}
