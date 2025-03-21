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

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Range;

import java.util.List;
import java.util.Objects;

/** SortedPartitionRanges */
public class SortedPartitionRanges<K> {
    public final List<PartitionItemAndRange<K>> sortedPartitions;
    public final List<PartitionItemAndId<K>> defaultPartitions;

    /** SortedPartitionRanges */
    public SortedPartitionRanges(
            List<PartitionItemAndRange<K>> sortedPartitions, List<PartitionItemAndId<K>> defaultPartitions) {
        this.sortedPartitions = Utils.fastToImmutableList(
                Objects.requireNonNull(sortedPartitions, "sortedPartitions bounds can not be null")
        );
        this.defaultPartitions = Utils.fastToImmutableList(
                Objects.requireNonNull(defaultPartitions, "defaultPartitions bounds can not be null")
        );
    }

    /** PartitionItemAndRange */
    public static class PartitionItemAndRange<K> {
        public final K id;
        public final PartitionItem partitionItem;
        public final Range<MultiColumnBound> range;

        public PartitionItemAndRange(K id, PartitionItem partitionItem, Range<MultiColumnBound> range) {
            this.id = id;
            this.partitionItem = Objects.requireNonNull(partitionItem, "partitionItem can not be null");
            this.range = Objects.requireNonNull(range, "range can not be null");
        }

        @Override
        public String toString() {
            return range.toString();
        }
    }

    /** PartitionItemAndId */
    public static class PartitionItemAndId<K> {
        public final K id;
        public final PartitionItem partitionItem;

        public PartitionItemAndId(K id, PartitionItem partitionItem) {
            this.id = id;
            this.partitionItem = Objects.requireNonNull(partitionItem, "partitionItem can not be null");
        }
    }
}
