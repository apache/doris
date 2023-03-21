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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.Objects;
import java.util.Set;

/** ColumnRange */
public class ColumnRange {
    private final RangeSet<ColumnBound> rangeSet;

    public ColumnRange() {
        rangeSet = ImmutableRangeSet.of();
    }

    public ColumnRange(Range<ColumnBound> range) {
        this.rangeSet = ImmutableRangeSet.of(range);
    }

    public ColumnRange(RangeSet<ColumnBound> rangeSet) {
        this.rangeSet = Objects.requireNonNull(rangeSet);
    }

    public ColumnRange intersect(ColumnRange range) {
        RangeSet<ColumnBound> newSet = TreeRangeSet.create();
        range.rangeSet.asRanges().forEach(r -> newSet.addAll(rangeSet.subRangeSet(r)));
        return new ColumnRange(newSet);
    }

    public ColumnRange union(ColumnRange range) {
        RangeSet<ColumnBound> newSet = TreeRangeSet.create();
        newSet.addAll(this.rangeSet);
        newSet.addAll(range.rangeSet);
        return new ColumnRange(newSet);
    }

    public Set<Range<ColumnBound>> asRanges() {
        return rangeSet.asRanges();
    }

    public ColumnRange complete() {
        return new ColumnRange(rangeSet.complement());
    }

    public boolean isEmptyRange() {
        return rangeSet.isEmpty();
    }

    /** isSingleton */
    public boolean isSingleton() {
        Set<Range<ColumnBound>> ranges = rangeSet.asRanges();
        if (ranges.size() != 1) {
            return false;
        }
        Range<ColumnBound> range = ranges.iterator().next();
        if (!range.hasLowerBound() || !range.hasUpperBound()) {
            return false;
        }
        return range.lowerEndpoint().equals(range.upperEndpoint());
    }

    public Range<ColumnBound> span() {
        return rangeSet.span();
    }

    public ColumnBound getLowerBound() {
        return rangeSet.span().lowerEndpoint();
    }

    public ColumnBound getUpperBound() {
        return rangeSet.span().upperEndpoint();
    }

    @Override
    public String toString() {
        return rangeSet.toString();
    }

    // <
    public static ColumnRange lessThen(Literal value) {
        return new ColumnRange(ColumnBound.lessThen(value));
    }

    // <=
    public static ColumnRange atMost(Literal value) {
        return new ColumnRange(ColumnBound.atMost(value));
    }

    // >
    public static ColumnRange greaterThan(Literal value) {
        return new ColumnRange(ColumnBound.greaterThan(value));
    }

    // >=
    public static ColumnRange atLeast(Literal value) {
        return new ColumnRange(ColumnBound.atLeast(value));
    }

    public static ColumnRange all() {
        return new ColumnRange(ColumnBound.all());
    }

    public static ColumnRange empty() {
        return new ColumnRange();
    }

    public static ColumnRange singleton(Literal value) {
        return new ColumnRange(ColumnBound.singleton(value));
    }

    public static ColumnRange between(Literal lower, Literal upper) {
        return new ColumnRange(ColumnBound.between(lower, upper));
    }

    public static ColumnRange range(Literal lower, BoundType lowerType, Literal upper, BoundType upperType) {
        return new ColumnRange(ColumnBound.range(lower, lowerType, upper, upperType));
    }

    /** main */
    public static void main(String[] args) {
        ColumnRange columnRange = new ColumnRange(ColumnBound.atMost(new IntegerLiteral(3)));
        ColumnRange intersect = columnRange.intersect(ColumnRange.empty());
        System.out.println(intersect);

        ColumnRange intersect2 = columnRange.intersect(new ColumnRange(ColumnBound.singleton(new IntegerLiteral(3))));
        System.out.println(intersect2);

        ColumnRange intersect3 = columnRange.intersect(new ColumnRange(ColumnBound.singleton(new IntegerLiteral(4))));
        System.out.println(intersect3);

        ColumnRange union = columnRange.union(ColumnRange.empty());
        System.out.println(union);

        ColumnRange union2 = columnRange.union(new ColumnRange(ColumnBound.singleton(new IntegerLiteral(3))));
        System.out.println(union2);

        ColumnRange union3 = columnRange.union(new ColumnRange(ColumnBound.singleton(new IntegerLiteral(4))));
        System.out.println(union3);

        ColumnRange union4 = columnRange.union(new ColumnRange(ColumnBound.range(
                new IntegerLiteral(3), BoundType.OPEN,
                new IntegerLiteral(4), BoundType.CLOSED)));
        System.out.println(union4);
    }
}
