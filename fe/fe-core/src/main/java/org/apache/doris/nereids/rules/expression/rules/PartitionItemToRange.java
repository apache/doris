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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import java.util.List;

/** PartitionItemToRange */
public class PartitionItemToRange {
    /** toRangeSets */
    public static List<Range<MultiColumnBound>> toRanges(PartitionItem partitionItem) {
        if (partitionItem instanceof RangePartitionItem) {
            Range<PartitionKey> range = partitionItem.getItems();
            PartitionKey lowerKey = range.lowerEndpoint();
            ImmutableList.Builder<ColumnBound> lowerBounds
                    = ImmutableList.builderWithExpectedSize(lowerKey.getKeys().size());
            for (LiteralExpr key : lowerKey.getKeys()) {
                Literal literal = toNereidsLiteral(key);
                lowerBounds.add(ColumnBound.of(literal));
            }

            PartitionKey upperKey = range.upperEndpoint();
            ImmutableList.Builder<ColumnBound> upperBounds
                    = ImmutableList.builderWithExpectedSize(lowerKey.getKeys().size());
            for (LiteralExpr key : upperKey.getKeys()) {
                Literal literal = Literal.fromLegacyLiteral(key, key.getType());
                upperBounds.add(ColumnBound.of(literal));
            }

            return ImmutableList.of(Range.closedOpen(
                    new MultiColumnBound(lowerBounds.build()),
                    new MultiColumnBound(upperBounds.build())
            ));
        } else if (partitionItem instanceof ListPartitionItem) {
            List<PartitionKey> partitionKeys = partitionItem.getItems();
            ImmutableList.Builder<Range<MultiColumnBound>> ranges
                    = ImmutableList.builderWithExpectedSize(partitionKeys.size());
            for (PartitionKey partitionKey : partitionKeys) {
                ImmutableList.Builder<ColumnBound> bounds
                        = ImmutableList.builderWithExpectedSize(partitionKeys.size());
                for (LiteralExpr key : partitionKey.getKeys()) {
                    Literal literal = toNereidsLiteral(key);
                    bounds.add(ColumnBound.of(literal));
                }
                MultiColumnBound bound = new MultiColumnBound(bounds.build());
                ranges.add(Range.singleton(bound));
            }
            return ranges.build();
        } else {
            throw new UnsupportedOperationException(partitionItem.getClass().getName());
        }
    }

    private static Literal toNereidsLiteral(LiteralExpr partitionKeyLiteral) {
        if (!partitionKeyLiteral.isMinValue()) {
            return Literal.fromLegacyLiteral(partitionKeyLiteral, partitionKeyLiteral.getType());
        } else {
            return new NullLiteral(DataType.fromCatalogType(partitionKeyLiteral.getType()));
        }
    }
}
