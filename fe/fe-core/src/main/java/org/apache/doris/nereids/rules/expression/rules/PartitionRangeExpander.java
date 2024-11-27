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
import org.apache.commons.collections.iterators.SingletonIterator;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
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
    public static final long ONE_DAY_MILLIS_SECOND = 1000L * 60 * 60 * 24;

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
    public static final List<List<Expression>> tryExpandRange(
            List<Slot> partitionSlots, List<Literal> lowers, List<Literal> uppers,
            List<PartitionSlotType> partitionSlotTypes, int expandThreshold) {
        if (partitionSlots.size() == 1) {
            return tryExpandSingleColumnRange(partitionSlots.get(0), lowers.get(0),
                    uppers.get(0), expandThreshold);
        } else {
            // slow path
            return commonTryExpandRange(partitionSlots, lowers, uppers, partitionSlotTypes, expandThreshold);
        }
    }

    private static List<List<Expression>> tryExpandSingleColumnRange(Slot partitionSlot, Literal lower,
            Literal upper, int expandThreshold) {
        // must be range slot
        try {
            if (canExpandRange(partitionSlot, lower, upper, 1, expandThreshold)) {
                Iterator<? extends Expression> iterator = enumerableIterator(
                        partitionSlot, lower, upper, true);
                if (iterator instanceof SingletonIterator) {
                    return ImmutableList.of(ImmutableList.of(iterator.next()));
                } else {
                    return ImmutableList.of(
                            ImmutableList.copyOf(iterator)
                    );
                }
            } else {
                return ImmutableList.of(ImmutableList.of(partitionSlot));
            }
        } catch (Throwable t) {
            // catch for safety, should not invoke here
            return ImmutableList.of(ImmutableList.of(partitionSlot));
        }
    }

    private static List<List<Expression>> commonTryExpandRange(
            List<Slot> partitionSlots, List<Literal> lowers, List<Literal> uppers,
            List<PartitionSlotType> partitionSlotTypes, int expandThreshold) {
        long expandedCount = 1;
        List<List<Expression>> expandedLists = Lists.newArrayListWithCapacity(lowers.size());
        for (int i = 0; i < partitionSlotTypes.size(); i++) {
            Slot slot = partitionSlots.get(i);
            PartitionSlotType partitionSlotType = partitionSlotTypes.get(i);
            List<Expression> expandedList = Lists.newArrayListWithCapacity(2);
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
                        if (canExpandRange(slot, lower, upper, expandedCount, expandThreshold)) {
                            Iterator<? extends Expression> iterator = enumerableIterator(
                                    slot, lower, upper, isLastColumn);
                            if (iterator instanceof SingletonIterator) {
                                expandedList.add(iterator.next());
                            } else {
                                expandedList.addAll(ImmutableList.copyOf(iterator));
                            }
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

    private static boolean canExpandRange(Slot slot, Literal lower, Literal upper,
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
            return count == 1 || (expandedCount * count) <= expandThreshold;
        } catch (Throwable t) {
            // e.g. max_value can not expand
            return false;
        }
    }

    /** the types will like this: [CONST, CONST, ..., RANGE, OTHER, OTHER, ...] */
    public static List<PartitionSlotType> computePartitionSlotTypes(List<Literal> lowers, List<Literal> uppers) {
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

    private static long enumerableCount(DataType dataType, Literal startInclusive, Literal endExclusive) {
        if (dataType.isIntegerLikeType()) {
            BigInteger start = new BigInteger(startInclusive.getStringValue());
            BigInteger end = new BigInteger(endExclusive.getStringValue());
            return end.subtract(start).longValue();
        } else if (dataType.isDateType()) {
            DateLiteral startInclusiveDate = (DateLiteral) startInclusive;
            DateLiteral endExclusiveDate = (DateLiteral) endExclusive;

            if (startInclusiveDate.getYear() == endExclusiveDate.getYear()
                    && startInclusiveDate.getMonth() == endExclusiveDate.getMonth()) {
                return endExclusiveDate.getDay() - startInclusiveDate.getDay();
            }

            LocalDate startDate = LocalDate.of(
                    (int) startInclusiveDate.getYear(),
                    (int) startInclusiveDate.getMonth(),
                    (int) startInclusiveDate.getDay()
            );

            LocalDate endDate = LocalDate.of(
                    (int) endExclusiveDate.getYear(),
                    (int) endExclusiveDate.getMonth(),
                    (int) endExclusiveDate.getDay()
            );
            long diffMillisSecond = endDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
                    - startDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            return (diffMillisSecond + ONE_DAY_MILLIS_SECOND + ONE_DAY_MILLIS_SECOND - 1) / ONE_DAY_MILLIS_SECOND;
        } else if (dataType.isDateV2Type()) {
            DateV2Literal startInclusiveDate = (DateV2Literal) startInclusive;
            DateV2Literal endExclusiveDate = (DateV2Literal) endExclusive;

            if (startInclusiveDate.getYear() == endExclusiveDate.getYear()
                    && startInclusiveDate.getMonth() == endExclusiveDate.getMonth()) {
                return endExclusiveDate.getDay() - startInclusiveDate.getDay();
            }

            LocalDate startDate = LocalDate.of(
                    (int) startInclusiveDate.getYear(),
                    (int) startInclusiveDate.getMonth(),
                    (int) startInclusiveDate.getDay()
            );

            LocalDate endDate = LocalDate.of(
                    (int) endExclusiveDate.getYear(),
                    (int) endExclusiveDate.getMonth(),
                    (int) endExclusiveDate.getDay()
            );
            long diffMillisSecond = endDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
                    - startDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            return (diffMillisSecond + ONE_DAY_MILLIS_SECOND + ONE_DAY_MILLIS_SECOND - 1) / ONE_DAY_MILLIS_SECOND;
        }

        // not enumerable
        return -1;
    }

    private static Iterator<? extends Expression> enumerableIterator(
            Slot slot, Literal startInclusive, Literal endLiteral, boolean endExclusive) {
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
            DateLiteral startInclusiveDate = (DateLiteral) startInclusive;
            DateLiteral endLiteralDate = (DateLiteral) endLiteral;
            if (endExclusive && startInclusiveDate.getYear() == endLiteralDate.getYear()
                    && startInclusiveDate.getMonth() == endLiteralDate.getMonth()
                    && startInclusiveDate.getDay() + 1 == endLiteralDate.getDay()) {
                return new SingletonIterator(startInclusive);
            }

            LocalDate startDate = LocalDate.of(
                    (int) startInclusiveDate.getYear(),
                    (int) startInclusiveDate.getMonth(),
                    (int) startInclusiveDate.getDay()
            );

            LocalDate endDate = LocalDate.of(
                    (int) endLiteralDate.getYear(),
                    (int) endLiteralDate.getMonth(),
                    (int) endLiteralDate.getDay()
            );
            if (endExclusive
                    && startDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() + ONE_DAY_MILLIS_SECOND
                        >= endDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()) {
                return new SingletonIterator(startInclusive);
            }
            return new DateLikeRangePartitionValueIterator<>(startDate, endDate, endExclusive,
                    date -> new DateLiteral(date.getYear(), date.getMonthValue(), date.getDayOfMonth()));
        } else if (dataType.isDateV2Type()) {
            DateV2Literal startInclusiveDate = (DateV2Literal) startInclusive;
            DateV2Literal endLiteralDate = (DateV2Literal) endLiteral;

            if (endExclusive && startInclusiveDate.getYear() == endLiteralDate.getYear()
                    && startInclusiveDate.getMonth() == endLiteralDate.getMonth()
                    && startInclusiveDate.getDay() + 1 == endLiteralDate.getDay()) {
                return new SingletonIterator(startInclusive);
            }

            LocalDate startDate = LocalDate.of(
                    (int) startInclusiveDate.getYear(),
                    (int) startInclusiveDate.getMonth(),
                    (int) startInclusiveDate.getDay()
            );

            LocalDate endDate = LocalDate.of(
                    (int) endLiteralDate.getYear(),
                    (int) endLiteralDate.getMonth(),
                    (int) endLiteralDate.getDay()
            );
            if (endExclusive
                    && startDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() + ONE_DAY_MILLIS_SECOND
                    >= endDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()) {
                return new SingletonIterator(startInclusive);
            }

            return new DateLikeRangePartitionValueIterator<>(startDate, endDate, endExclusive,
                    date -> new DateV2Literal(date.getYear(), date.getMonthValue(), date.getDayOfMonth()));
        }
        // unsupported type
        return Iterators.singletonIterator(slot);
    }

    private static class IntegerLikeRangePartitionValueIterator<L extends IntegerLikeLiteral>
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

    private static class DateLikeRangePartitionValueIterator<L extends Literal>
            extends RangePartitionValueIterator<LocalDate, L> {

        public DateLikeRangePartitionValueIterator(
                LocalDate startInclusive, LocalDate finish, boolean endExclusive, Function<LocalDate, L> toLiteral) {
            super(startInclusive, finish, endExclusive, toLiteral);
        }

        @Override
        protected LocalDate doGetNext(LocalDate current) {
            return current.plusDays(1);
        }
    }
}
