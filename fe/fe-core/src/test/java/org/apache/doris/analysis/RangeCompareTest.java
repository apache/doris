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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class RangeCompareTest {

    /*
    Range1: a>=1
    Range2: a<2
    Intersection: 1<=a<2
     */
    @Test
    public void testIntersection() {
        LiteralExpr lowerBoundOfRange1 = new IntLiteral(1);
        Range<LiteralExpr> range1 = Range.atLeast(lowerBoundOfRange1);
        LiteralExpr upperBoundOfRange2 = new IntLiteral(2);
        Range<LiteralExpr> range2 = Range.lessThan(upperBoundOfRange2);
        Range<LiteralExpr> intersectionRange = range1.intersection(range2);
        Assert.assertTrue(intersectionRange.hasLowerBound());
        Assert.assertEquals(lowerBoundOfRange1, intersectionRange.lowerEndpoint());
        Assert.assertEquals(BoundType.CLOSED, intersectionRange.lowerBoundType());
        Assert.assertTrue(intersectionRange.hasUpperBound());
        Assert.assertEquals(upperBoundOfRange2, intersectionRange.upperEndpoint());
        Assert.assertEquals(BoundType.OPEN, intersectionRange.upperBoundType());
    }

    /*
    Range1: a>1
    Range2: a<0
    Intersection: null
    */
    @Test
    public void testWithoutIntersection() {
        LiteralExpr lowerBoundOfRange1 = new IntLiteral(1);
        Range<LiteralExpr> range1 = Range.greaterThan(lowerBoundOfRange1);
        LiteralExpr upperBoundOfRange2 = new IntLiteral(1);
        Range<LiteralExpr> range2 = Range.lessThan(upperBoundOfRange2);
        try {
            range1.intersection(range2);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            System.out.println(e);
        }
    }

    // Range1: a>=1
    // Range2: a<0.1
    // Intersection: null
    @Test
    public void testIntersectionInvalidRange() throws AnalysisException {
        LiteralExpr lowerBoundOfRange1 = new IntLiteral(1);
        Range<LiteralExpr> range1 = Range.atLeast(lowerBoundOfRange1);
        LiteralExpr upperBoundOfRange2 = new DecimalLiteral("0.1");
        Range<LiteralExpr> range2 = Range.lessThan(upperBoundOfRange2);
        try {
            range1.intersection(range2);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            System.out.println(e);
        }
    }

    // Range1: a>=3.0
    // Range2: a<6
    // Intersection: 3.0<=a<=6
    @Test
    public void testIntersectionWithDifferentType() throws AnalysisException {
        LiteralExpr lowerBoundOfRange1 = new DecimalLiteral("3.0");
        Range<LiteralExpr> range1 = Range.atLeast(lowerBoundOfRange1);
        LiteralExpr upperBoundOfRange2 = new IntLiteral(6);
        Range<LiteralExpr> range2 = Range.lessThan(upperBoundOfRange2);
        try {
            Range<LiteralExpr> intersectionRange = range1.intersection(range2);
            Assert.assertEquals(lowerBoundOfRange1, intersectionRange.lowerEndpoint());
            Assert.assertEquals(upperBoundOfRange2, intersectionRange.upperEndpoint());
        } catch (ClassCastException e) {
            Assert.fail(e.getMessage());
        }
    }

    // Range1: a>=3.0
    // Range2: a<true
    // Intersection: IllegalArgumentException
    @Test
    public void testIntersectionWithDifferentTypeDecimalToBool() throws AnalysisException {
        LiteralExpr lowerBoundOfRange1 = new DecimalLiteral("3.0");
        Range<LiteralExpr> range1 = Range.atLeast(lowerBoundOfRange1);
        LiteralExpr upperBoundOfRange2 = new BoolLiteral(true);
        Range<LiteralExpr> range2 = Range.lessThan(upperBoundOfRange2);
        try {
            range1.intersection(range2);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            System.out.println(e);
        }
    }

    /*
    Range1: 1<a<10
    Range2: a>5
    Intersection: 5<a<10
     */
    @Test
    public void testIntersectionWithBothBound() {
        LiteralExpr lowerBoundOfRange1 = new IntLiteral(1);
        LiteralExpr upperBoundOfRange1 = new IntLiteral(10);
        Range<LiteralExpr> range1 = Range.range(lowerBoundOfRange1, BoundType.OPEN, upperBoundOfRange1, BoundType.OPEN);
        LiteralExpr lowerBoundOfRange2 = new IntLiteral(5);
        Range<LiteralExpr> range2 = Range.greaterThan(lowerBoundOfRange2);
        Range<LiteralExpr> intersection = range1.intersection(range2);
        Assert.assertEquals(lowerBoundOfRange2, intersection.lowerEndpoint());
        Assert.assertEquals(BoundType.OPEN, intersection.lowerBoundType());
        Assert.assertEquals(upperBoundOfRange1, intersection.upperEndpoint());
        Assert.assertEquals(BoundType.OPEN, intersection.upperBoundType());
    }

    /*
    Range1: a>=1
    Range2: a<0
    Merge Range Set: a >=1, a <0
     */
    @Test
    public void testMergeRangeWithoutIntersection() {
        LiteralExpr lowerBoundOfRange1 = new IntLiteral(1);
        Range<LiteralExpr> range1 = Range.atLeast(lowerBoundOfRange1);
        LiteralExpr upperBoundOfRange2 = new IntLiteral(0);
        Range<LiteralExpr> range2 = Range.lessThan(upperBoundOfRange2);
        RangeSet<LiteralExpr> rangeSet = TreeRangeSet.create();
        rangeSet.add(range1);
        rangeSet.add(range2);
        Set<Range<LiteralExpr>> rangeList = rangeSet.asRanges();
        Assert.assertEquals(2, rangeList.size());
        Assert.assertTrue(rangeList.contains(range1));
        Assert.assertTrue(rangeList.contains(range2));
    }

    /*
    Range1: 1<=a<=10
    Range2: 10<=a<=20
    Merge Range Set: 1<=a<=20
     */
    @Test
    public void testMergeRangeWithIntersection1() {
        LiteralExpr lowerBoundOfRange1 = new IntLiteral(1);
        LiteralExpr upperBoundOfRange1 = new IntLiteral(10);
        Range<LiteralExpr> range1 = Range.range(lowerBoundOfRange1, BoundType.CLOSED, upperBoundOfRange1, BoundType.CLOSED);
        LiteralExpr lowerBoundOfRange2 = new IntLiteral(10);
        LiteralExpr upperBoundOfRange2 = new IntLiteral(20);
        Range<LiteralExpr> range2 = Range.range(lowerBoundOfRange2, BoundType.CLOSED, upperBoundOfRange2, BoundType.CLOSED);
        RangeSet<LiteralExpr> rangeSet = TreeRangeSet.create();
        rangeSet.add(range1);
        rangeSet.add(range2);
        Set<Range<LiteralExpr>> rangeList = rangeSet.asRanges();
        Assert.assertEquals(1, rangeList.size());
        Range<LiteralExpr> intersection = rangeList.iterator().next();
        Assert.assertEquals(lowerBoundOfRange1, intersection.lowerEndpoint());
        Assert.assertEquals(upperBoundOfRange2, intersection.upperEndpoint());
        Assert.assertEquals(BoundType.CLOSED, intersection.lowerBoundType());
        Assert.assertEquals(BoundType.CLOSED, intersection.upperBoundType());
    }

    /*
    Range1: 1<=a<10
    Range2: 5<=a<=20
    Merge Range Set: 1<=a<=20
     */
    @Test
    public void testMergeRangeWithIntersection2() {
        LiteralExpr lowerBoundOfRange1 = new IntLiteral(1);
        LiteralExpr upperBoundOfRange1 = new IntLiteral(10);
        Range<LiteralExpr> range1 = Range.range(lowerBoundOfRange1, BoundType.CLOSED, upperBoundOfRange1, BoundType.OPEN);
        LiteralExpr lowerBoundOfRange2 = new IntLiteral(5);
        LiteralExpr upperBoundOfRange2 = new IntLiteral(20);
        Range<LiteralExpr> range2 = Range.range(lowerBoundOfRange2, BoundType.CLOSED, upperBoundOfRange2, BoundType.CLOSED);
        RangeSet<LiteralExpr> rangeSet = TreeRangeSet.create();
        rangeSet.add(range1);
        rangeSet.add(range2);
        Set<Range<LiteralExpr>> rangeList = rangeSet.asRanges();
        Assert.assertEquals(1, rangeList.size());
        Range<LiteralExpr> intersection = rangeList.iterator().next();
        Assert.assertEquals(lowerBoundOfRange1, intersection.lowerEndpoint());
        Assert.assertEquals(upperBoundOfRange2, intersection.upperEndpoint());
        Assert.assertEquals(BoundType.CLOSED, intersection.lowerBoundType());
        Assert.assertEquals(BoundType.CLOSED, intersection.upperBoundType());
    }

    /*
    Range1: a<=3.0
    Range2: a>2021-01-01
    Merge: ClassCastException
     */
    @Test
    public void testMergeRangeWithDifferentType() throws AnalysisException {
        LiteralExpr lowerBoundOfRange1 = new DecimalLiteral("3.0");
        Range<LiteralExpr> range1 = Range.lessThan(lowerBoundOfRange1);
        LiteralExpr upperBoundOfRange2 = new DateLiteral("2021-01-01", Type.DATE);
        Range<LiteralExpr> range2 = Range.atLeast(upperBoundOfRange2);
        RangeSet<LiteralExpr> rangeSet = TreeRangeSet.create();
        rangeSet.add(range1);
        try {
            rangeSet.add(range2);
            Assert.fail();
        } catch (ClassCastException e) {
            System.out.println(e);
        }
    }

    @Test
    public void testMergeRangeWithDifferentType2() throws AnalysisException {
        LiteralExpr lowerBoundOfRange1 = new DecimalLiteral("3.0");
        Range<LiteralExpr> range1 = Range.lessThan(lowerBoundOfRange1);
        LiteralExpr upperBoundOfRange2 = new DateLiteral("2021-01-01", Type.DATEV2);
        Range<LiteralExpr> range2 = Range.atLeast(upperBoundOfRange2);
        RangeSet<LiteralExpr> rangeSet = TreeRangeSet.create();
        rangeSet.add(range1);
        try {
            rangeSet.add(range2);
            Assert.fail();
        } catch (ClassCastException e) {
            System.out.println(e);
        }
    }

}
