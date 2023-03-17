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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.math.BigInteger;
import java.text.ParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/** RangePartitionLiteral */
public class RangePartitionLiteral extends PartitionLiteral {
    private final Literal startInclusive;
    private final Literal endExclusive;

    /** RangePartitionLiteral */
    public RangePartitionLiteral(Column partitionColumn, Slot partitionSlot, RangePartitionItem partitionItem,
            int partitionColumnIndex) {
        super(partitionColumn, partitionSlot, partitionItem, partitionColumnIndex);
        Range<PartitionKey> partitionRange = partitionItem.getItems();
        this.startInclusive = Literal.fromLegacyLiteral(
                partitionRange.lowerEndpoint().getKeys().get(partitionColumnIndex)
        );
        this.endExclusive = Literal.fromLegacyLiteral(
                partitionRange.upperEndpoint().getKeys().get(partitionColumnIndex)
        );
    }

    /**
     * example
     * type: int, range [1, 10) is enumerable
     * type: datetime, range['2022-01-01 00:00:00', '2022-01-10 00:00:00') is not enumerable
     */
    @Override
    public boolean enumerable() {
        DataType dataType = getDataType();

        // if previous partition maybe contains more than one value, this literal is not enumerable,
        // e.g. [(1, 5), (2, 6))
        // the first partition column maybe 1 or 2, so the second partition column can be any int,
        // like (1, 100), (2, -100) and so on
        //
        // but if previous partition only has one value, then this literal maybe enumerable,
        // e.g. [(1, 10), (1, 20))
        // first and second partition column both are enumerable
        return (dataType.isIntegerLikeType() || dataType.isDateType() || dataType.isDateV2Type());
    }

    @Override
    public long valueCount() throws ParseException {
        if (!enumerable()) {
            return -1;
        }
        DataType dataType = getDataType();
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

    /**
     * if enumerable, return complete set of the partition value
     * example
     * type: int, range [1, 10), return [1, 2, 3, 4, 5, 6, 7, 8, 9)
     */
    @Override
    public Iterator<? extends Literal> expendLiterals() throws ParseException {
        if (!enumerable()) {
            return Iterators.singletonIterator(this);
        }
        DataType dataType = getDataType();
        if (dataType.isIntegerLikeType()) {
            BigInteger start = new BigInteger(startInclusive.getStringValue());
            BigInteger end = new BigInteger(endExclusive.getStringValue());
            if (dataType.isTinyIntType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, value -> new TinyIntLiteral(value.byteValue()));
            } else if (dataType.isSmallIntType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, value -> new SmallIntLiteral(value.shortValue()));
            } else if (dataType.isIntegerType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, value -> new IntegerLiteral(value.intValue()));
            } else if (dataType.isBigIntType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, value -> new BigIntLiteral(value.longValue()));
            } else if (dataType.isLargeIntType()) {
                return new IntegerLikeRangePartitionValueIterator<>(
                        start, end, LargeIntLiteral::new);
            }
        } else if (dataType.isDateType()) {
            Date start = DateUtils.parseDate(startInclusive.toString(), DateLiteral.JAVA_DATE_FORMAT);
            Date end = DateUtils.parseDate(endExclusive.toString(), DateLiteral.JAVA_DATE_FORMAT);
            return new DateLikeRangePartitionValueIterator<>(start, end,
                    date -> new DateLiteral(DateFormatUtils.format(date, DateLiteral.JAVA_DATE_FORMAT)));
        } else if (dataType.isDateV2Type()) {
            Date start = DateUtils.parseDate(startInclusive.toString(), DateLiteral.JAVA_DATE_FORMAT);
            Date end = DateUtils.parseDate(endExclusive.toString(), DateLiteral.JAVA_DATE_FORMAT);
            return new DateLikeRangePartitionValueIterator<>(start, end,
                    date -> new DateV2Literal(DateFormatUtils.format(date, DateLiteral.JAVA_DATE_FORMAT)));
        }
        // unsupported type
        return Iterators.singletonIterator(this);
    }

    private class IntegerLikeRangePartitionValueIterator<L extends IntegerLikeLiteral>
            extends RangePartitionValueIterator<BigInteger, L> {

        public IntegerLikeRangePartitionValueIterator(
                BigInteger startInclusive, BigInteger endExclusive, Function<BigInteger, L> toLiteral) {
            super(startInclusive, endExclusive, toLiteral);
        }

        @Override
        protected BigInteger doGetNext(BigInteger current) {
            return current.add(BigInteger.ONE);
        }
    }

    private class DateLikeRangePartitionValueIterator<L extends Literal>
            extends RangePartitionValueIterator<Date, L> {

        public DateLikeRangePartitionValueIterator(
                Date startInclusive, Date endExclusive, Function<Date, L> toLiteral) {
            super(startInclusive, endExclusive, toLiteral);
        }

        @Override
        protected Date doGetNext(Date current) {
            return DateUtils.addDays(current, 1);
        }
    }

    private abstract class RangePartitionValueIterator<C extends Comparable, L extends Literal>
            implements Iterator<L> {
        private final C startInclusive;
        private final C endExclusive;
        private C current;

        private final Function<C, L> toLiteral;

        public RangePartitionValueIterator(C startInclusive, C endExclusive, Function<C, L> toLiteral) {
            this.startInclusive = startInclusive;
            this.endExclusive = endExclusive;
            this.current = this.startInclusive;
            this.toLiteral = toLiteral;
        }

        @Override
        public boolean hasNext() {
            return current.compareTo(endExclusive) < 0;
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
