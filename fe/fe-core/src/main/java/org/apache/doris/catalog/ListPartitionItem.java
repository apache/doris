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
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ListPartitionItem extends PartitionItem {
    public static final ListPartitionItem DUMMY_ITEM = new ListPartitionItem(Lists.newArrayList());

    private final List<PartitionKey> partitionKeys;
    private boolean isDefaultPartition = false;

    public ListPartitionItem(List<PartitionKey> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public static ListPartitionItem read(DataInput input) throws IOException {
        int counter = input.readInt();
        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (int i = 0; i < counter; i++) {
            PartitionKey partitionKey = PartitionKey.read(input);
            partitionKeys.add(partitionKey);
        }
        return new ListPartitionItem(partitionKeys);
    }

    public List<PartitionKey> getItems() {
        return partitionKeys;
    }

    public String getItemsString() {
        // ATTN: DO NOT EDIT unless unless you explicitly guarantee compatibility
        // between different versions.
        //
        // the ccr syncer depends on this string to identify partitions between two
        // clusters (cluster versions may be different).
        return getItems().toString();
    }

    public String getItemsSql() {
        return toSql();
    }

    @Override
    public boolean isDefaultPartition() {
        return isDefaultPartition;
    }

    public void setDefaultPartition(boolean isDefaultPartition) {
        this.isDefaultPartition = isDefaultPartition;
    }

    @Override
    public PartitionItem getIntersect(PartitionItem newItem) {
        List<PartitionKey> newKeys = newItem.getItems();
        for (PartitionKey newKey : newKeys) {
            if (partitionKeys.contains(newKey)) {
                return newItem;
            }
        }
        return null;
    }

    @Override
    public PartitionKeyDesc toPartitionKeyDesc() {
        List<List<PartitionValue>> inValues = partitionKeys.stream().map(PartitionInfo::toPartitionValue)
                .collect(Collectors.toList());
        return PartitionKeyDesc.createIn(inValues);
    }

    @Override
    public PartitionKeyDesc toPartitionKeyDesc(int pos) throws AnalysisException {
        List<List<PartitionValue>> inValues = partitionKeys.stream().map(PartitionInfo::toPartitionValue)
                .collect(Collectors.toList());
        Set<List<PartitionValue>> res = Sets.newHashSet();
        for (List<PartitionValue> values : inValues) {
            if (values.size() <= pos) {
                throw new AnalysisException(
                        String.format("toPartitionKeyDesc IndexOutOfBounds, values: %s, pos: %d", values.toString(),
                                pos));
            }
            res.add(Lists.newArrayList(values.get(pos)));
        }
        return PartitionKeyDesc.createIn(Lists.newArrayList(res));
    }

    @Override
    public boolean isGreaterThanSpecifiedTime(int pos, Optional<String> dateFormatOptional, long nowTruncSubSec)
            throws AnalysisException {
        for (PartitionKey partitionKey : partitionKeys) {
            if (partitionKey.getKeys().size() <= pos) {
                throw new AnalysisException(
                        String.format("toPartitionKeyDesc IndexOutOfBounds, partitionKey: %s, pos: %d",
                                partitionKey.toString(),
                                pos));
            }
            if (!isDefaultPartition()
                    && MTMVUtil.getExprTimeSec(partitionKey.getKeys().get(pos), dateFormatOptional) >= nowTruncSubSec) {
                // As long as one of the partitionKeys meets the requirements, this partition
                // needs to be retained
                return true;
            }
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(partitionKeys.size());
        for (PartitionKey partitionKey : partitionKeys) {
            partitionKey.write(out);
        }
    }

    @Override
    public int compareTo(PartitionItem other) {
        int thisKeyLen = this.partitionKeys.size();
        int otherKeyLen = ((ListPartitionItem) other).getItems().size();
        int minLen = Math.min(thisKeyLen, otherKeyLen);
        for (int i = 0; i < minLen; i++) {
            int ret = this.getItems().get(i).compareTo(((ListPartitionItem) other).getItems().get(i));
            if (0 != ret) {
                return ret;
            }
        }
        return Integer.compare(thisKeyLen, otherKeyLen);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof ListPartitionItem)) {
            return false;
        }

        ListPartitionItem other = (ListPartitionItem) obj;
        // check keys
        return partitionKeys.size() == other.partitionKeys.size()
                && partitionKeys.equals(other.partitionKeys);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + partitionKeys.size();
        for (PartitionKey partitionKey : partitionKeys) {
            result = 31 * result + partitionKey.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        // ATTN: DO NOT EDIT unless unless you explicitly guarantee compatibility
        // between different versions.
        //
        // the ccr syncer depends on this string to identify partitions between two
        // clusters (cluster versions may be different).
        StringBuilder builder = new StringBuilder();
        builder.append("partitionKeys: [");
        for (PartitionKey partitionKey : partitionKeys) {
            builder.append(partitionKey.toString());
        }
        builder.append("]; ");
        return builder.toString();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");

        int i = 0;
        for (PartitionKey partitionKey : partitionKeys) {
            sb.append(partitionKey.toSql());
            if (i < partitionKeys.size() - 1) {
                sb.append(",");
            }
            i++;
        }

        sb.append(")");

        return sb.toString();
    }
}
