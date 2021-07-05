// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package org.apache.doris.rewrite;

import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.BoundType;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.clearspring.analytics.util.Lists;
import org.junit.Assert;
import org.junit.Test;

public class ExtractCommonFactorsRuleTest {

    // Input: k1 in (k2, 1)
    // Result: false
    @Test
    public void testSingleColumnPredicateInColumn() {
        SlotRef child0 = new SlotRef(new TableName("db1", "tb1"), "k1");
        SlotRef inColumn = new SlotRef(new TableName("db1", "tb1"), "k2");
        IntLiteral intLiteral = new IntLiteral(1);
        List<Expr> inExprList = Lists.newArrayList();
        inExprList.add(inColumn);
        inExprList.add(intLiteral);
        InPredicate inPredicate = new InPredicate(child0, inExprList, false);
        ExtractCommonFactorsRule extractCommonFactorsRule = new ExtractCommonFactorsRule();
        boolean result = Deencapsulation.invoke(extractCommonFactorsRule, "singleColumnPredicate", inPredicate);
        Assert.assertFalse(result);
    }

    // Clause1: 1<k1<3, 3>k2
    // Clause2: 2<k1<4
    // Result: 1<k1<4
    @Test
    public void testMergeTwoClauseRange() {
        // Clause1
        SlotRef k1SlotRef = new SlotRef(new TableName("db1", "tb1"), "k1");
        SlotRef k2SlotRef = new SlotRef(new TableName("db1", "tb1"), "k2");
        Range k1Range1 = Range.range(new IntLiteral(1), BoundType.OPEN, new IntLiteral(3), BoundType.OPEN);
        Range k2Range = Range.greaterThan(new IntLiteral(3));
        RangeSet<LiteralExpr> k1RangeSet1 = TreeRangeSet.create();
        k1RangeSet1.add(k1Range1);
        RangeSet<LiteralExpr> k2RangeSet = TreeRangeSet.create();
        k2RangeSet.add(k2Range);
        Map<SlotRef, RangeSet<LiteralExpr>> clause1 = Maps.newHashMap();
        clause1.put(k1SlotRef, k1RangeSet1);
        clause1.put(k2SlotRef, k2RangeSet);
        // Clause2
        Range k1Range2 = Range.range(new IntLiteral(2), BoundType.OPEN, new IntLiteral(4), BoundType.OPEN);
        Map<SlotRef, Range<LiteralExpr>> clause2 = Maps.newHashMap();
        clause2.put(k1SlotRef, k1Range2);

        ExtractCommonFactorsRule extractCommonFactorsRule = new ExtractCommonFactorsRule();
        Map<SlotRef, RangeSet<LiteralExpr>> result = Deencapsulation.invoke(extractCommonFactorsRule,
                "mergeTwoClauseRange", clause1, clause2);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey(k1SlotRef));
        Set<Range<LiteralExpr>> k1ResultRangeSet = result.get(k1SlotRef).asRanges();
        Assert.assertEquals(1, k1ResultRangeSet.size());
        Range<LiteralExpr> k1ResultRange = k1ResultRangeSet.iterator().next();
        Assert.assertEquals(new IntLiteral(1), k1ResultRange.lowerEndpoint());
        Assert.assertEquals(new IntLiteral(4), k1ResultRange.upperEndpoint());
    }

    // Clause1: k1 in (1), k2 in (1)
    // Clause2: k1 in (1, 2)
    // Result: k1 in (1, 2)
    @Test
    public void testMergeTwoClauseIn() {
        // Clause1
        SlotRef k1SlotRef = new SlotRef(new TableName("db1", "tb1"), "k1");
        SlotRef k2SlotRef = new SlotRef(new TableName("db1", "tb1"), "k2");
        IntLiteral intLiteral1 = new IntLiteral(1);
        IntLiteral intLiteral2 = new IntLiteral(2);
        List<Expr> k1Values1 = Lists.newArrayList();
        k1Values1.add(intLiteral1);
        List<Expr> k2Values1 = Lists.newArrayList();
        k2Values1.add(intLiteral1);
        InPredicate k1InPredicate1 = new InPredicate(k1SlotRef, k1Values1, false);
        InPredicate k2InPredicate = new InPredicate(k2SlotRef, k2Values1, false);
        Map<SlotRef, InPredicate> clause1 = Maps.newHashMap();
        clause1.put(k1SlotRef, k1InPredicate1);
        clause1.put(k2SlotRef, k2InPredicate);
        // Clause2
        List<Expr> k1Values2 = Lists.newArrayList();
        k1Values2.add(intLiteral1);
        k1Values2.add(intLiteral2);
        InPredicate k1InPredicate2 = new InPredicate(k1SlotRef, k1Values2, false);
        Map<SlotRef, InPredicate> clause2 = Maps.newHashMap();
        clause2.put(k1SlotRef, k1InPredicate2);

        ExtractCommonFactorsRule extractCommonFactorsRule = new ExtractCommonFactorsRule();
        Map<SlotRef, InPredicate> result = Deencapsulation.invoke(extractCommonFactorsRule,
                "mergeTwoClauseIn", clause1, clause2);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey(k1SlotRef));
        InPredicate k1Result = result.get(k1SlotRef);
        Assert.assertEquals(2, k1Result.getListChildren().size());
        List<Expr> k1ResultValues = k1Result.getListChildren();
        k1ResultValues.contains(intLiteral1);
        k1ResultValues.contains(intLiteral2);
    }

    // RangeSet: {(1,3], (6,7)}
    // SlotRef: k1
    // Result: (k1>1 and k1<=3) or (k1>6 and k1<7)
    @Test
    public void testRangeSetToCompoundPredicate() {
        Range<LiteralExpr> range1 = Range.range(new IntLiteral(1), BoundType.OPEN, new IntLiteral(3), BoundType.CLOSED);
        Range<LiteralExpr> range2 = Range.range(new IntLiteral(6), BoundType.OPEN, new IntLiteral(7), BoundType.OPEN);
        RangeSet<LiteralExpr> rangeSet = TreeRangeSet.create();
        rangeSet.add(range1);
        rangeSet.add(range2);
        SlotRef slotRef = new SlotRef(new TableName("db1", "tb1"), "k1");

        ExtractCommonFactorsRule extractCommonFactorsRule = new ExtractCommonFactorsRule();
        Expr result = Deencapsulation.invoke(extractCommonFactorsRule,
                "rangeSetToCompoundPredicate", slotRef, rangeSet);
        Assert.assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate compoundPredicate = (CompoundPredicate) result;
        Assert.assertEquals(CompoundPredicate.Operator.OR, compoundPredicate.getOp());
        Assert.assertTrue(compoundPredicate.getChild(0) instanceof CompoundPredicate);
        Assert.assertTrue(compoundPredicate.getChild(1) instanceof CompoundPredicate);
        CompoundPredicate clause1 = (CompoundPredicate) compoundPredicate.getChild(0);
        CompoundPredicate clause2 = (CompoundPredicate) compoundPredicate.getChild(1);
        Assert.assertEquals(CompoundPredicate.Operator.AND, clause1.getOp());
        Assert.assertEquals(CompoundPredicate.Operator.AND, clause2.getOp());
    }
}
