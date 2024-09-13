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

package org.apache.doris.planner.normalize;

import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionKey;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/** PredicateToRange */
public class PredicateToRange {
    private final Column partitionColumn;
    private final PartitionKey minKey;
    private final PartitionKey maxKey;

    public PredicateToRange(Column partitionColumn) {
        this.partitionColumn = partitionColumn;

        try {
            minKey = PartitionKey.createInfinityPartitionKey(ImmutableList.of(partitionColumn), false);
            maxKey = PartitionKey.createInfinityPartitionKey(ImmutableList.of(partitionColumn), true);
        } catch (Throwable t) {
            throw new IllegalStateException(
                    "Can not create min/max partition key by the column: " + partitionColumn, t);
        }
    }

    /** supportedPartitionPredicate */
    public static boolean supportedToRange(Expr conjunct) {
        if (conjunct instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) conjunct;
            switch (binaryPredicate.getOp()) {
                case EQ:
                case LT:
                case LE:
                case GT:
                case GE:
                case NE:
                    break;
                default:
                    return false;
            }
            return conjunct.getChild(0) instanceof SlotRef && conjunct.getChild(1) instanceof LiteralExpr;
        } else if (conjunct instanceof BetweenPredicate) {
            return conjunct.getChild(0) instanceof SlotRef
                    && conjunct.getChild(1) instanceof LiteralExpr
                    && conjunct.getChild(2) instanceof LiteralExpr;
        } else if (conjunct instanceof InPredicate) {
            if (!(conjunct.getChild(0) instanceof SlotRef)) {
                return false;
            }
            for (int i = 1; i < conjunct.getChildren().size(); i++) {
                if (!(conjunct.getChild(i) instanceof LiteralExpr)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    /** exprToRange */
    public RangeSet<PartitionKey> exprToRange(Expr predicate) {
        if (predicate instanceof BinaryPredicate) {
            return compareToRangeSet((BinaryPredicate) predicate);
        } else if (predicate instanceof BetweenPredicate) {
            return betweenToRange((BetweenPredicate) predicate);
        } else if (predicate instanceof InPredicate) {
            return inToRange((InPredicate) predicate);
        } else {
            throw new IllegalStateException("Unsupported to Range: " + predicate);
        }
    }

    private RangeSet<PartitionKey> compareToRangeSet(BinaryPredicate predicate) {
        Operator op = predicate.getOp();
        Expr rightChild = predicate.getChild(1);
        PartitionKey rightPartitionKey = toPartitionKey(rightChild);

        TreeRangeSet<PartitionKey> rangeSet = TreeRangeSet.create();
        switch (op) {
            case EQ:
                rangeSet.add(Range.closed(rightPartitionKey, rightPartitionKey));
                break;
            case NE:
                rangeSet.add(Range.open(minKey, rightPartitionKey));
                rangeSet.add(Range.open(rightPartitionKey, maxKey));
                break;
            case LT:
                rangeSet.add(Range.open(minKey, rightPartitionKey));
                break;
            case LE:
                rangeSet.add(Range.openClosed(minKey, rightPartitionKey));
                break;
            case GT:
                rangeSet.add(Range.open(rightPartitionKey, maxKey));
                break;
            case GE:
                rangeSet.add(Range.closedOpen(rightPartitionKey, maxKey));
                break;
            default:
        }
        return rangeSet;
    }

    private RangeSet<PartitionKey> betweenToRange(BetweenPredicate predicate) {
        PartitionKey lowerBound = toPartitionKey(predicate.getChild(0));
        PartitionKey upperBound = toPartitionKey(predicate.getChild(1));
        TreeRangeSet<PartitionKey> rangeSet = TreeRangeSet.create();
        if (predicate.isNotBetween()) {
            rangeSet.add(Range.open(minKey, lowerBound));
            rangeSet.add(Range.open(upperBound, maxKey));
        } else {
            rangeSet.add(Range.closed(lowerBound, upperBound));
        }
        return rangeSet;
    }

    private RangeSet<PartitionKey> inToRange(InPredicate predicate) {
        boolean isNotIn = predicate.isNotIn();
        RangeSet<PartitionKey> rangeSet = TreeRangeSet.create();
        for (Expr item : predicate.getListChildren()) {
            PartitionKey itemKey = toPartitionKey(item);
            if (isNotIn) {
                RangeSet<PartitionKey> completeRangeSet = TreeRangeSet.create();
                completeRangeSet.add(Range.open(minKey, itemKey));
                completeRangeSet.add(Range.open(itemKey, maxKey));

                RangeSet<PartitionKey> intersect = TreeRangeSet.create();
                for (Range<PartitionKey> completeRange : completeRangeSet.asRanges()) {
                    intersect.addAll(rangeSet.subRangeSet(completeRange));
                }
                rangeSet = intersect;
            } else {
                rangeSet.add(Range.closed(itemKey, itemKey));
            }
        }

        if (isNotIn) {
            rangeSet = rangeSet.complement();
        }
        return rangeSet;
    }

    private PartitionKey toPartitionKey(Expr expr) {
        // try to cast to literal, if wrong, query cache will be skipped
        LiteralExpr literalExpr = (LiteralExpr) expr;
        PartitionKey partitionKey = new PartitionKey();
        partitionKey.pushColumn(literalExpr, partitionColumn.getDataType());
        return partitionKey;
    }
}
