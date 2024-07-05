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

package org.apache.doris.catalog;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVUtil;

import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;

import java.util.Optional;

public class RangePartitionItem extends PartitionItem {
    @SerializedName(value = "range")
    private Range<PartitionKey> partitionKeyRange;
    public static final Range<PartitionKey> DUMMY_RANGE;
    public static final RangePartitionItem DUMMY_ITEM;

    static {
        DUMMY_RANGE = Range.closed(new PartitionKey(), new PartitionKey());
        DUMMY_ITEM = new RangePartitionItem(Range.closed(new PartitionKey(), PartitionKey.createMaxPartitionKey()));
    }

    public RangePartitionItem(Range<PartitionKey> range) {
        this.partitionKeyRange = range;
    }

    public Range<PartitionKey> getItems() {
        return partitionKeyRange;
    }

    public String getItemsString() {
        return toString();
    }

    @Override
    public boolean isDefaultPartition() {
        return false;
    }

    @Override
    public PartitionKeyDesc toPartitionKeyDesc() {
        return PartitionKeyDesc.createFixed(
                PartitionInfo.toPartitionValue(partitionKeyRange.lowerEndpoint()),
                PartitionInfo.toPartitionValue(partitionKeyRange.upperEndpoint()));
    }

    @Override
    public PartitionKeyDesc toPartitionKeyDesc(int pos) {
        // MTMV do not allow base tables with partition type range to have multiple partition columns,
        // so pos is ignored here
        return toPartitionKeyDesc();
    }

    @Override
    public boolean isGreaterThanSpecifiedTime(int pos, Optional<String> dateFormatOptional, long nowTruncSubSec)
            throws AnalysisException {
        PartitionKey partitionKey = partitionKeyRange.upperEndpoint();
        if (partitionKey.getKeys().size() <= pos) {
            throw new AnalysisException(
                    String.format("toPartitionKeyDesc IndexOutOfBounds, partitionKey: %s, pos: %d",
                            partitionKey.toString(),
                            pos));
        }
        // If the upper limit of the partition range meets the requirements, this partition needs to be retained
        return !isDefaultPartition() && MTMVUtil.getExprTimeSec(partitionKey.getKeys().get(pos), dateFormatOptional)
                > nowTruncSubSec;
    }

    @Override
    public int compareTo(PartitionItem other) {
        if (partitionKeyRange.contains(other.getItems())) {
            return 1;
        }
        if (partitionKeyRange.equals(((RangePartitionItem) other).getItems())) {
            return 0;
        }
        return -1;
    }

    @Override
    public PartitionItem getIntersect(PartitionItem newItem) {
        Range<PartitionKey> newRange = newItem.getItems();
        if (partitionKeyRange.isConnected(newRange)) {
            Range<PartitionKey> intersection = partitionKeyRange.intersection(newRange);
            if (!intersection.isEmpty()) {
                return new RangePartitionItem(intersection);
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RangePartitionItem) {
            return partitionKeyRange.equals(((RangePartitionItem) obj).getItems());
        }
        return false;
    }

    @Override
    public String toString() {
        // ATTN: DO NOT EDIT unless unless you explicitly guarantee compatibility
        // between different versions.
        //
        // the ccr syncer depends on this string to identify partitions between two
        // clusters (cluster versions may be different).
        return partitionKeyRange.toString();
    }
}
