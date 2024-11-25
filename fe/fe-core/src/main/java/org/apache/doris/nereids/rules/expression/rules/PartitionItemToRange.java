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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;

import java.util.List;

/** PartitionItemToRange */
public class PartitionItemToRange {
    /** toRangeSets */
    public static TreeRangeSet<MultiColumnBound> toRangeSets(PartitionItem partitionItem) {
        if (partitionItem instanceof RangePartitionItem) {
            Range<PartitionKey> range = partitionItem.getItems();
            TreeRangeSet<MultiColumnBound> oneRangePartitionRanges = TreeRangeSet.create();
            PartitionKey lowerKey = range.lowerEndpoint();
            ImmutableList.Builder<ColumnBound> lowerBounds
                    = ImmutableList.builderWithExpectedSize(lowerKey.getKeys().size());
            for (LiteralExpr key : lowerKey.getKeys()) {
                Literal literal = Literal.fromLegacyLiteral(key, key.getType());
                lowerBounds.add(ColumnBound.of(literal));
            }

            PartitionKey upperKey = range.upperEndpoint();
            ImmutableList.Builder<ColumnBound> upperBounds
                    = ImmutableList.builderWithExpectedSize(lowerKey.getKeys().size());
            for (LiteralExpr key : upperKey.getKeys()) {
                Literal literal = Literal.fromLegacyLiteral(key, key.getType());
                upperBounds.add(ColumnBound.of(literal));
            }

            oneRangePartitionRanges.add(Range.closedOpen(
                    new MultiColumnBound(lowerBounds.build()),
                    new MultiColumnBound(upperBounds.build())));
            return oneRangePartitionRanges;
        } else if (partitionItem instanceof ListPartitionItem) {
            TreeRangeSet<MultiColumnBound> oneListPartitionRanges = TreeRangeSet.create();
            List<PartitionKey> partitionKeys = partitionItem.getItems();
            for (PartitionKey partitionKey : partitionKeys) {
                ImmutableList.Builder<ColumnBound> bounds
                        = ImmutableList.builderWithExpectedSize(partitionKeys.size());
                for (LiteralExpr key : partitionKey.getKeys()) {
                    Literal literal = Literal.fromLegacyLiteral(key, key.getType());
                    bounds.add(ColumnBound.of(literal));
                }
                MultiColumnBound bound = new MultiColumnBound(bounds.build());
                oneListPartitionRanges.add(Range.singleton(bound));
            }
            return oneListPartitionRanges;
        } else {
            throw new UnsupportedOperationException(partitionItem.getClass().getName());
        }
    }
}
