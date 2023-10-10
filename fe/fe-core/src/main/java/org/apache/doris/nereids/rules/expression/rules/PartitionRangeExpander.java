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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.math.BigInteger;
import java.text.ParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * PartitionRangeExpander
 *
 * try to expand range partition if the partition is enumerable
 * example:
 * partition column type: int, range [1, 10), return [1, 2, 3, 4, 5, 6, 7, 8, 9).
 *
 * after expand range, we can replace partition slot to the literal in expression tree and evaluate it.
 */
public class PartitionRangeExpander {
    /** PartitionSlotType */
    public enum PartitionSlotType {
        // e.g. the first partition column is const '1' in partition [('1', '2', '5'), ('1', '3', '5')),
        // we can substitute the slot in the expression tree and evaluate.
        CONST,
        // e.g. the second partition column is range ['2', '3'] in partition [('1', '2', '5'), ('1', '3', '5'))
        // if the partition column is discrete type(int, date), we expand and iterate it and substitute the slot
        // in the expression tree and evaluate, else use range set to check whether the partition is valid range
        RANGE,
        // e.g. the third partition column other type in partition [('1', '2', '5'), ('1', '3', '5')),
        // every partition column after the first range type column is other type.
        // we can check the range if the previous partition column equals to the bound, e.g.
        // if first_part_column = '1' and second_part_column = '2', then we can infer third_part_column >= '5'.
        // if first_part_column = '1' and second_part_column = '3', then we can infer third_part_column < '5'.
        OTHER;
    }

    /** expandRangeLiterals */
    public final List<List<Expression>> tryExpandRange(
            List<Slot> partitionSlots, List<Literal> lowers, List<Literal> uppers,
            List<PartitionSlotType> partitionSlotTypes, int expandThreshold, boolean allowMerged) {

        long expandedCount = 1;
        List<List<Expression>> expandedLists = Lists.newArrayListWithCapacity(lowers.size());
        for (int i = 0; i < partitionSlotTypes.size(); i++) {
            Slot slot = partitionSlots.get(i);
            PartitionSlotType partitionSlotType = partitionSlotTypes.get(i);
            List<Expression> expandedList = Lists.newArrayList();
            Literal lower = lowers.get(i);
            switch (partitionSlotType) {
                case CONST:
                    // don't need expanded, just replace to literal as input
                    expandedList.add(lower);
                    break;
                case RANGE:
                    // try to expand range to literals as input
                    // e.g. [1, 5) will be expand to [1, 2, 3, 4] if the data type is integer like type.
                    // some types can not expend, like varchar type
                    Literal upper = uppers.get(i);
                    try {
                        boolean isLastColumn = i + 1 == partitionSlots.size();
                        if (!allowMerged && canExpandRange(slot, lower, upper, expandedCount, expandThreshold)) {
                            expandedList.addAll(ImmutableList.copyOf(
                                    enumerableIterator(slot, lower, upper, isLastColumn))
                            );
                        } else {
                            expandedList.add(slot);
                        }
                    } catch (Throwable t) {
                        // catch for safety, should not invoke here
                        expandedList.add(slot);
                    }
                    break;
                case OTHER:
                    // can't expend other slots, keep slot as input
                    expandedList.add(slot);
                    break;
                default:
                    throw new AnalysisException("Unknown partition slot type: " + partitionSlotType);
            }
            expandedCount *= expandedList.size();
            expandedLists.add(expandedList);
        }
        return expandedLists;
    }

    private final boolean canExpandRange(Slot slot, Literal lower, Literal upper,
            long expandedCount, int expandThreshold) {
        DataType type = slot.getDataType();
        if (!type.isIntegerLikeType() && !type.isDateType() && !type.isDateV2Type()) {
            return false;
        }
        try {
            long count = enumerableCount(slot.getDataType(), lower, upper);
            if (count <= 0) {
                return false;
            }
            // too much expanded will consuming resources of frontend,
            // e.g. [1, 100000000), we should skip expand it
            return (expandedCount * count) <= expandThreshold;
        } catch (Throwable t) {
            // e.g. max_value can not expand
            return false;
        }
    }

    /** the types will like this: [CONST, CONST, ..., RANGE, OTHER, OTHER, ...] */
    public List<PartitionSlotType> computePartitionSlotTypes(List<Literal> lowers, List<Literal> uppers) {
        PartitionSlotType previousType = PartitionSlotType.CONST;
        List<PartitionSlotType> types = Lists.newArrayListWithCapacity(lowers.size());
        for (int i = 0; i < lowers.size(); ++i) {
            if (previousType == PartitionSlotType.RANGE || previousType == PartitionSlotType.OTHER) {
                types.add(PartitionSlotType.OTHER);
                continue;
            }
            Literal lower = lowers.get(i);
            Literal upper = uppers.get(i);

            PartitionSlotType type = lower.toLegacyLiteral().equals(upper.toLegacyLiteral())
                    ? PartitionSlotType.CONST
                    : PartitionSlotType.RANGE;
            types.add(type);
            previousType = type;
        }
        return types;
    }

    private final long enumerableCount(DataType dataType, Literal startInclusive, Literal endExclusive) throws
            ParseException {
        if (dataType.isIntegerLikeType()) {
            BigInteger start = new BigInteger(startInclusive.getStringValue());
            BigInteger end = new BigInteger(endExclusive.getStringValue());
            return end.subtract(start).longValue();
        } else if (dataType.isDateType() || dataType.isDateV2Type()) {
            Date start = DateUtils.parseDate(startInclusive.toString(), DateLiteral.JAVA_DATE_FORMAT);
            Date end = DateUtils.parseDate(endExclusive.toString(), DateLiteral.JAVA_DATE_FORMAT);
            return ChronoUnit.DAYS.between(start.toInstant(), end.toInstant());
        }

        // not enumerable
        return -1;
    }

    private final Iterator<? extends Expression> enumerableIterator(
            Slot slot, Literal startInclusive, Literal endLiteral, boolean endExclusive) throws ParseException {
        DataType dataType = slot.getDataType();
        if (dataType.isIntegerLikeType()) {
            BigInteger start = new BigInteger(startInclusive.getStringValue());
            BigInteger end = new BigInteger(endLiteral.getStringValue());
            if (dataType.isTinyIntType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, endExclusive, value -> new TinyIntLiteral(value.byteValue()));
            } else if (dataType.isSmallIntType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, endExclusive, value -> new SmallIntLiteral(value.shortValue()));
            } else if (dataType.isIntegerType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, endExclusive, value -> new IntegerLiteral(value.intValue()));
            } else if (dataType.isBigIntType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, endExclusive, value -> new BigIntLiteral(value.longValue()));
            } else if (dataType.isLargeIntType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, endExclusive, LargeIntLiteral::new);
            }
        } else if (dataType.isDateType()) {
            Date startDate = DateUtils.parseDate(startInclusive.toString(), DateLiteral.JAVA_DATE_FORMAT);
            Date endDate = DateUtils.parseDate(endLiteral.toString(), DateLiteral.JAVA_DATE_FORMAT);
            return new DateLikeRangePartitionValueIterator<>(startDate, endDate, endExclusive,
                    date -> new DateLiteral(DateFormatUtils.format(date, DateLiteral.JAVA_DATE_FORMAT)));
        } else if (dataType.isDateV2Type()) {
            Date startDate = DateUtils.parseDate(startInclusive.toString(), DateLiteral.JAVA_DATE_FORMAT);
            Date endDate = DateUtils.parseDate(endLiteral.toString(), DateLiteral.JAVA_DATE_FORMAT);
            return new DateLikeRangePartitionValueIterator<>(startDate, endDate, endExclusive,
                    date -> new DateV2Literal(DateFormatUtils.format(date, DateLiteral.JAVA_DATE_FORMAT)));
        }
        // unsupported type
        return Iterators.singletonIterator(slot);
    }

    private class IntegerLikeRangePartitionValueIterator<L extends IntegerLikeLiteral>
            extends RangePartitionValueIterator<BigInteger, L> {

        public IntegerLikeRangePartitionValueIterator(BigInteger startInclusive, BigInteger end,
                boolean endExclusive, Function<BigInteger, L> toLiteral) {
            super(startInclusive, end, endExclusive, toLiteral);
        }

        @Override
        protected BigInteger doGetNext(BigInteger current) {
            return current.add(BigInteger.ONE);
        }
    }

    private class DateLikeRangePartitionValueIterator<L extends Literal>
            extends RangePartitionValueIterator<Date, L> {

        public DateLikeRangePartitionValueIterator(
                Date startInclusive, Date finish, boolean endExclusive, Function<Date, L> toLiteral) {
            super(startInclusive, finish, endExclusive, toLiteral);
        }

        @Override
        protected Date doGetNext(Date current) {
            return DateUtils.addDays(current, 1);
        }
    }

    private abstract class RangePartitionValueIterator<C extends Comparable, L extends Literal>
            implements Iterator<L> {
        private final C startInclusive;
        private final C end;
        private final boolean endExclusive;
        private C current;

        private final Function<C, L> toLiteral;

        public RangePartitionValueIterator(C startInclusive, C end, boolean endExclusive, Function<C, L> toLiteral) {
            this.startInclusive = startInclusive;
            this.end = end;
            this.endExclusive = endExclusive;
            this.current = this.startInclusive;
            this.toLiteral = toLiteral;
        }

        @Override
        public boolean hasNext() {
            if (endExclusive) {
                return current.compareTo(end) < 0;
            } else {
                return current.compareTo(end) <= 0;
            }
        }

        @Override
        public L next() {
            if (hasNext()) {
                C value = current;
                current = doGetNext(current);
                return toLiteral.apply(value);
            }
            throw new NoSuchElementException();
        }

        protected abstract C doGetNext(C current);
    }
}
