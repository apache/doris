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

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class BinaryPredicateTest {

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
        }

        try {
            predicate2.analyzeImpl(analyzer);
            Assert.fail();
        } catch (AnalysisException e) {
        }
    }

    @Test
    public void testConvertToRange() {
        SlotRef slotRef = new SlotRef(new TableName("db1", "tb1"), "k1");
        LiteralExpr literalExpr = new IntLiteral(1);
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.LE, slotRef, literalExpr);
        Range<LiteralExpr> range = binaryPredicate.convertToRange();
        Assert.assertEquals(literalExpr, range.upperEndpoint());
        Assert.assertEquals(BoundType.CLOSED, range.upperBoundType());
        Assert.assertFalse(range.hasLowerBound());
    }

}