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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MaxLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.hadoop.util.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/** PartitionPredicateToRange */
public class PartitionPredicateToRange extends DefaultExpressionVisitor<RangeSet<MultiColumnBound>, Void> {
    private List<Slot> columns;
    private Set<Integer> slotIds;

    /** PartitionPredicateToRange */
    public PartitionPredicateToRange(List<Slot> columns) {
        this.columns = Utils.fastToImmutableList(
                Objects.requireNonNull(columns, "columns can not be null")
        );

        ImmutableSet.Builder<Integer> slotIds = ImmutableSet.builderWithExpectedSize(columns.size());
        for (Slot column : columns) {
            slotIds.add(column.getExprId().asInt());
        }
        this.slotIds = slotIds.build();
    }

    @Override
    public RangeSet<MultiColumnBound> visitAnd(And and, Void context) {
        boolean first = true;
        RangeSet<MultiColumnBound> intersects = null;
        for (Expression child : and.children()) {
            RangeSet<MultiColumnBound> childRanges = child.accept(this, context);

            // if some conjunct not supported, just skip it safety because the big ranges contains
            // all partitions the predicates need
            if (childRanges == null) {
                continue;
            } else if (first) {
                first = false;
                intersects = childRanges;
                continue;
            }
            intersects = ColumnRange.intersect(childRanges, intersects);
            if (intersects.isEmpty()) {
                break;
            }
        }
        return intersects;
    }

    @Override
    public RangeSet<MultiColumnBound> visitOr(Or or, Void context) {
        RangeSet<MultiColumnBound> intersects = TreeRangeSet.create();
        for (Expression child : or.children()) {
            RangeSet<MultiColumnBound> childRanges = child.accept(this, context);

            // if any predicate can not parse to range, we can not do binary search
            if (childRanges == null) {
                return null;
            }
            intersects.addAll(childRanges);
        }
        return intersects;
    }

    @Override
    public RangeSet<MultiColumnBound> visitNot(Not not, Void context) {
        Expression child = not.child();
        if (child instanceof IsNull && ((IsNull) child).child() instanceof SlotReference) {
            SlotReference slot = (SlotReference) ((IsNull) child).child();
            int slotId = slot.getExprId().asInt();
            if (slotIds.contains(slotId)) {
                DataType dataType = child.getDataType();
                return toRangeSet(slot,
                        new NullLiteral(dataType), BoundType.OPEN,
                        new MaxLiteral(dataType), BoundType.CLOSED
                );
            }
        }
        return null;
    }

    @Override
    public RangeSet<MultiColumnBound> visitIsNull(IsNull isNull, Void context) {
        Expression child = isNull.child();
        if (child instanceof SlotReference && slotIds.contains(((SlotReference) child).getExprId().asInt())) {
            NullLiteral nullLiteral = new NullLiteral(child.getDataType());
            return toRangeSet((SlotReference) child, nullLiteral, BoundType.CLOSED, nullLiteral, BoundType.CLOSED);
        }
        return null;
    }

    @Override
    public RangeSet<MultiColumnBound> visitEqualTo(EqualTo equalTo, Void context) {
        Expression left = equalTo.left();
        Expression right = equalTo.right();
        if (left instanceof SlotReference && right instanceof Literal) {
            if (slotIds.contains(((SlotReference) left).getExprId().asInt())) {
                Literal literal = (Literal) right;
                return toRangeSet((SlotReference) left, literal, BoundType.CLOSED, literal, BoundType.CLOSED);
            }
        }
        return null;
    }

    @Override
    public RangeSet<MultiColumnBound> visitNullSafeEqual(NullSafeEqual nullSafeEqual, Void context) {
        Expression left = nullSafeEqual.left();
        Expression right = nullSafeEqual.right();
        if (left instanceof SlotReference && right instanceof Literal) {
            if (slotIds.contains(((SlotReference) left).getExprId().asInt())) {
                Literal literal = (Literal) right;
                return toRangeSet((SlotReference) left, literal, BoundType.CLOSED, literal, BoundType.CLOSED);
            }
        }
        return null;
    }

    @Override
    public RangeSet<MultiColumnBound> visitInPredicate(InPredicate inPredicate, Void context) {
        Expression compareExpr = inPredicate.getCompareExpr();
        if (compareExpr instanceof SlotReference) {
            SlotReference slot = (SlotReference) compareExpr;
            if (slotIds.contains((slot).getExprId().asInt()) && inPredicate.optionsAreLiterals()) {
                RangeSet<MultiColumnBound> union = TreeRangeSet.create();
                for (Expression option : inPredicate.getOptions()) {
                    Literal literal = (Literal) option;
                    union.addAll(
                            toRangeSet(slot, literal, BoundType.CLOSED, literal, BoundType.CLOSED)
                    );
                }
                return union;
            }
        }
        return null;
    }

    @Override
    public RangeSet<MultiColumnBound> visitLessThan(LessThan lessThan, Void context) {
        Expression left = lessThan.left();
        Expression right = lessThan.right();
        if (left instanceof SlotReference && right instanceof Literal) {
            if (slotIds.contains(((SlotReference) left).getExprId().asInt())) {
                NullLiteral nullLiteral = new NullLiteral(right.getDataType());
                Literal literal = (Literal) right;
                return toRangeSet((SlotReference) left, nullLiteral, BoundType.OPEN, literal, BoundType.OPEN);
            }
        }
        return null;
    }

    @Override
    public RangeSet<MultiColumnBound> visitLessThanEqual(LessThanEqual lessThanEqual, Void context) {
        Expression left = lessThanEqual.left();
        Expression right = lessThanEqual.right();
        if (left instanceof SlotReference && right instanceof Literal) {
            if (slotIds.contains(((SlotReference) left).getExprId().asInt())) {
                NullLiteral nullLiteral = new NullLiteral(right.getDataType()); // Refrain : why NULL?
                Literal literal = (Literal) right;
                return toRangeSet((SlotReference) left, nullLiteral, BoundType.OPEN, literal, BoundType.CLOSED);
            }
        }
        return null;
    }

    @Override
    public RangeSet<MultiColumnBound> visitGreaterThan(GreaterThan greaterThan, Void context) {
        Expression left = greaterThan.left();
        Expression right = greaterThan.right();
        if (left instanceof SlotReference && right instanceof Literal) {
            if (slotIds.contains(((SlotReference) left).getExprId().asInt())) {
                Literal literal = (Literal) right;
                MaxLiteral maxLiteral = new MaxLiteral(right.getDataType());
                return toRangeSet((SlotReference) left, literal, BoundType.OPEN, maxLiteral, BoundType.CLOSED);
            }
        }
        return null;
    }

    @Override
    public RangeSet<MultiColumnBound> visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, Void context) {
        Expression left = greaterThanEqual.left();
        Expression right = greaterThanEqual.right();
        if (left instanceof SlotReference && right instanceof Literal) {
            if (slotIds.contains(((SlotReference) left).getExprId().asInt())) {
                Literal literal = (Literal) right;
                MaxLiteral maxLiteral = new MaxLiteral(right.getDataType());
                return toRangeSet((SlotReference) left, literal, BoundType.CLOSED, maxLiteral, BoundType.CLOSED);
            }
        }
        return null;
    }

    private RangeSet<MultiColumnBound> toRangeSet(SlotReference slotReference,
            Literal columnLowerBound, BoundType lowerBoundType,
            Literal columnUpperBound, BoundType upperBoundType) {
        List<ColumnBound> lowerBounds = Lists.newArrayListWithCapacity(columns.size());
        List<ColumnBound> upperBounds = Lists.newArrayListWithCapacity(columns.size());
        for (Slot column : columns) {
            if (column.getExprId().asInt() == slotReference.getExprId().asInt()) {
                lowerBounds.add(ColumnBound.of(columnLowerBound));
                upperBounds.add(ColumnBound.of(columnUpperBound));
            } else {
                lowerBounds.add(ColumnBound.of(new NullLiteral(slotReference.getDataType())));
                upperBounds.add(ColumnBound.of(new MaxLiteral(slotReference.getDataType())));
            }
        }
        MultiColumnBound lowerBound = new MultiColumnBound(lowerBounds);
        MultiColumnBound upperBound = new MultiColumnBound(upperBounds);

        Range<MultiColumnBound> range = Range.range(lowerBound, lowerBoundType, upperBound, upperBoundType);
        return ImmutableRangeSet.of(range);
    }
}
