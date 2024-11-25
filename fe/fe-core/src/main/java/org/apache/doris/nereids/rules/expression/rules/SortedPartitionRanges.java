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

import com.google.common.collect.RangeSet;

import java.util.List;

/** SortedPartitionRanges */
public class SortedPartitionRanges<K> {
    public final List<PartitionItemAndRange<K>> sortedPartitions;

    public SortedPartitionRanges(List<PartitionItemAndRange<K>> sortedPartitions) {
        this.sortedPartitions = sortedPartitions;
    }

    /** PartitionItemAndRange */
    public static class PartitionItemAndRange<K> {
        public final K id;
        public final PartitionItem partitionItem;
        public final RangeSet<MultiColumnBound> ranges;

        public PartitionItemAndRange(K id, PartitionItem partitionItem, RangeSet<MultiColumnBound> ranges) {
            this.id = id;
            this.partitionItem = partitionItem;
            this.ranges = ranges;
        }
    }
}
