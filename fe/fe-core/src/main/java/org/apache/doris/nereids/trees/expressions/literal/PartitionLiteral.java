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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/** PartitionLiteral */
public abstract class PartitionLiteral extends Literal {
    protected final Slot partitionSlot;
    protected final Column partitionColumn;
    protected final PartitionItem partitionItem;
    protected final int partitionColumnIndex;

    public PartitionLiteral(Column partitionColumn, Slot partitionSlot,
            PartitionItem partitionItem, int partitionColumnIndex) {
        super(partitionSlot.getDataType());
        this.partitionColumn = Objects.requireNonNull(partitionColumn, "partitionColumn cannot be null");
        this.partitionSlot = Objects.requireNonNull(partitionSlot, "partitionSlot cannot be null");
        this.partitionItem = Objects.requireNonNull(partitionItem, "partitionItem cannot be null");
        this.partitionColumnIndex = partitionColumnIndex;
    }

    /**
     * toLiterals
     * you need provide partitionSlots in the order defined by the partition field
     *
     * example.
     *
     * for list partition part column slot#1, exist partitions: [
     *   partition p1 (('1'), ('4'), ('7'))
     *   partition p2 (('8'), ('9'), ('5'))
     * ]
     *
     * return [
     *   [ListPartitionLiteral(slot#1, values=[1, 4, 7])],
     *   [ListPartitionLiteral(slot#1, values=[8, 9, 5])]
     * ]
     */
    public static List<PartitionWithLiteral> toPartitionLiterals(
            PartitionInfo partitionInfo, List<Slot> partitionSlots) {
        List<Column> partitionColumns = partitionInfo.getPartitionColumns();
        Map<Long, PartitionItem> idToItem = partitionInfo.getIdToItem(false);
        boolean isListPartition = partitionInfo instanceof ListPartitionInfo;
        boolean isRangePartition = partitionInfo instanceof RangePartitionInfo;
        if (isListPartition || isRangePartition) {
            return idToItem.entrySet()
                .stream()
                .map(idToPartitionItem -> new PartitionWithLiteral(
                    idToPartitionItem.getKey(),
                    idToPartitionItem.getValue(),
                    IntStream.range(0, partitionColumns.size())
                            .mapToObj(index -> {
                                if (isListPartition) {
                                    return new ListPartitionLiteral(
                                            partitionColumns.get(index), partitionSlots.get(index),
                                            (ListPartitionItem) idToPartitionItem.getValue(), index);
                                } else {
                                    return new RangePartitionLiteral(
                                            partitionColumns.get(index), partitionSlots.get(index),
                                            (RangePartitionItem) idToPartitionItem.getValue(), index);
                                }
                            })
                            .collect(ImmutableList.toImmutableList())
                )).collect(ImmutableList.toImmutableList());
        } else {
            throw new AnalysisException("Unsupported partition: " + partitionInfo.getClass().getSimpleName());
        }
    }

    @Override
    public boolean nullable() {
        return partitionSlot.nullable();
    }

    @Override
    public Object getValue() {
        throw new IllegalStateException("Can not convert PartitionLiteral to legacy literal");
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        throw new IllegalStateException("Can not convert PartitionLiteral to legacy literal");
    }

    @Override
    public String toSql() {
        return partitionSlot.toSql();
    }

    @Override
    public String toString() {
        return partitionSlot.toString();
    }

    public boolean maybeNullable() {
        return !partitionSlot.nullable();
    }

    /**
     * example
     * type: int, range [1, 10) is enumerable
     * type: datetime, range['2022-01-01 00:00:00', '2022-01-10 00:00:00') is not enumerable
     */
    public abstract boolean enumerable();

    /**
     * return value count if enumerable or else return -1
     */
    public abstract long valueCount() throws Exception;

    public abstract Iterator<? extends Literal> expendLiterals() throws Exception;

    /** PartitionWithLiteral */
    public static class PartitionWithLiteral {
        public final long partitionId;
        public final PartitionItem partitionItem;
        public final List<PartitionLiteral> partitionLiterals;

        public PartitionWithLiteral(
                long partitionId, PartitionItem partitionItem, List<PartitionLiteral> partitionLiterals) {
            this.partitionId = partitionId;
            this.partitionItem = partitionItem;
            this.partitionLiterals = partitionLiterals;
        }
    }
}
