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
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.RangeUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangePartitionInfo extends PartitionInfo {
    private static final Logger LOG = LogManager.getLogger(RangePartitionInfo.class);

    private List<Column> partitionColumns;
    // partition id -> partition range
    private Map<Long, Range<PartitionKey>> idToRange;

    public RangePartitionInfo() {
        // for persist
        super();
        this.partitionColumns = new LinkedList<Column>();
        this.idToRange = new HashMap<Long, Range<PartitionKey>>();
    }

    public RangePartitionInfo(List<Column> partitionColumns) {
        super(PartitionType.RANGE);
        this.partitionColumns = partitionColumns;
        this.idToRange = new HashMap<Long, Range<PartitionKey>>();
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    @Override
    public void dropPartition(long partitionId) {
        super.dropPartition(partitionId);
        idToRange.remove(partitionId);
    }

    public void addPartition(long partitionId, Range<PartitionKey> range, DataProperty dataProperty,
                             short replicationNum, boolean isInMemory) {
        addPartition(partitionId, dataProperty, replicationNum, isInMemory);
        idToRange.put(partitionId, range);
    }

    public Range<PartitionKey> checkAndCreateRange(SingleRangePartitionDesc desc) throws DdlException {
        Range<PartitionKey> newRange = null;
        PartitionKeyDesc partitionKeyDesc = desc.getPartitionKeyDesc();
        // check range
        try {
            newRange = createAndCheckNewRange(partitionKeyDesc);
        } catch (AnalysisException e) {
            throw new DdlException("Invalid range value format： " + e.getMessage());
        }

        Preconditions.checkNotNull(newRange);
        return newRange;
    }

    // create a new range and check it.
    private Range<PartitionKey> createAndCheckNewRange(PartitionKeyDesc partKeyDesc)
            throws AnalysisException, DdlException {
        Range<PartitionKey> newRange = null;
        // generate and sort the existing ranges
        List<Map.Entry<Long, Range<PartitionKey>>> sortedRanges = getSortedRangeMap();

        // create upper values for new range
        PartitionKey newRangeUpper = null;
        if (partKeyDesc.isMax()) {
            newRangeUpper = PartitionKey.createInfinityPartitionKey(partitionColumns, true);
        } else {
            newRangeUpper = PartitionKey.createPartitionKey(partKeyDesc.getUpperValues(), partitionColumns);
        }
        if (newRangeUpper.isMinValue()) {
            throw new DdlException("Partition's upper value should not be MIN VALUE: " + partKeyDesc.toSql());
        }

        Range<PartitionKey> lastRange = null;
        Range<PartitionKey> currentRange = null;
        for (Map.Entry<Long, Range<PartitionKey>> entry : sortedRanges) {
            currentRange = entry.getValue();
            // check if equals to upper bound
            PartitionKey upperKey = currentRange.upperEndpoint();
            if (upperKey.compareTo(newRangeUpper) >= 0) {
                newRange = checkNewRange(partKeyDesc, newRangeUpper, lastRange, currentRange);
                break;
            } else {
                lastRange = currentRange;
            }
        } // end for ranges

        if (newRange == null) /* the new range's upper value is larger than any existing ranges */ {
            newRange = checkNewRange(partKeyDesc, newRangeUpper, lastRange, currentRange);
        }
        return newRange;
    }

    private Range<PartitionKey> checkNewRange(PartitionKeyDesc partKeyDesc, PartitionKey newRangeUpper,
            Range<PartitionKey> lastRange, Range<PartitionKey> currentRange) throws AnalysisException, DdlException {
        Range<PartitionKey> newRange;
        PartitionKey lowKey = null;
        if (partKeyDesc.hasLowerValues()) {
            lowKey = PartitionKey.createPartitionKey(partKeyDesc.getLowerValues(), partitionColumns);
        } else {
            if (lastRange == null) {
                lowKey = PartitionKey.createInfinityPartitionKey(partitionColumns, false);
            } else {
                lowKey = lastRange.upperEndpoint();
            }
        }
        // check: [left, right), error if left equal right
        if (lowKey.compareTo(newRangeUpper) >= 0) {
            throw new AnalysisException("The lower values must smaller than upper values");
        }
        newRange = Range.closedOpen(lowKey, newRangeUpper);

        if (currentRange != null) {
            // check if range intersected
            RangeUtils.checkRangeIntersect(newRange, currentRange);
        }
        return newRange;
    }

    public Range<PartitionKey> handleNewSinglePartitionDesc(SingleRangePartitionDesc desc, 
            long partitionId) throws DdlException {
        Preconditions.checkArgument(desc.isAnalyzed());
        Range<PartitionKey> range = null;
        try {
            range = checkAndCreateRange(desc);
            idToRange.put(partitionId, range);
        } catch (IllegalArgumentException e) {
            // Range.closedOpen may throw this if (lower > upper)
            throw new DdlException("Invalid key range: " + e.getMessage());
        }
        idToDataProperty.put(partitionId, desc.getPartitionDataProperty());
        idToReplicationNum.put(partitionId, desc.getReplicationNum());
        idToInMemory.put(partitionId, desc.isInMemory());
        return range;
    }

    public void unprotectHandleNewSinglePartitionDesc(long partitionId, Range<PartitionKey> range,
                                                      DataProperty dataProperty, short replicationNum,
                                                      boolean isInMemory) {
        idToRange.put(partitionId, range);
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicationNum.put(partitionId, replicationNum);
        idToInMemory.put(partitionId, isInMemory);
    }

    public void setRange(long partitionId, Range<PartitionKey> range) {
        idToRange.put(partitionId, range);
    }

    public Map<Long, Range<PartitionKey>> getIdToRange() {
        return idToRange;
    }

    public Range<PartitionKey> getRange(long partitionId) {
        return idToRange.get(partitionId);
    }

    public static void checkRangeColumnType(Column column) throws AnalysisException {
        PrimitiveType type = column.getDataType();
        if (!type.isFixedPointType() && !type.isDateType()) {
            throw new AnalysisException("Column[" + column.getName() + "] type[" + type
                    + "] cannot be a range partition key.");
        }
    }

    public List<Map.Entry<Long, Range<PartitionKey>>> getSortedRangeMap() {
        List<Map.Entry<Long, Range<PartitionKey>>> sortedList = Lists.newArrayList(this.idToRange.entrySet());
        Collections.sort(sortedList, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);
        return sortedList;
    }

    // get a sorted range list, exclude partitions which ids are in 'excludePartitionIds'
    public List<Range<PartitionKey>> getRangeList(Set<Long> excludePartitionIds) {
        List<Range<PartitionKey>> resultList = Lists.newArrayList();
        for (Map.Entry<Long, Range<PartitionKey>> entry : idToRange.entrySet()) {
            if (!excludePartitionIds.contains(entry.getKey())) {
                resultList.add(entry.getValue());
            }
        }
        return resultList;
    }

    public boolean checkRange(Range<PartitionKey> newRange) {
        for (Range<PartitionKey> range : idToRange.values()) {
            if (range.isConnected(newRange)) {
                Range<PartitionKey> intersection = range.intersection(newRange);
                if (!intersection.isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        PartitionInfo partitionInfo = new RangePartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        // partition columns
        out.writeInt(partitionColumns.size());
        for (Column column : partitionColumns) {
            column.write(out);
        }

        out.writeInt(idToRange.size());
        for (Map.Entry<Long, Range<PartitionKey>> entry : idToRange.entrySet()) {
            out.writeLong(entry.getKey());
            RangeUtils.writeRange(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            Column column = Column.read(in);
            partitionColumns.add(column);
        }

        this.isMultiColumnPartition = partitionColumns.size() > 1;

        counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            Range<PartitionKey> range = RangeUtils.readRange(in);
            idToRange.put(partitionId, range);
        }
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY RANGE(");
        int idx = 0;
        for (Column column : partitionColumns) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append("`").append(column.getName()).append("`");
            idx++;
        }
        sb.append(")\n(");

        // sort range
        List<Map.Entry<Long, Range<PartitionKey>>> entries =
                new ArrayList<Map.Entry<Long, Range<PartitionKey>>>(this.idToRange.entrySet());
        Collections.sort(entries, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);

        idx = 0;
        for (Map.Entry<Long, Range<PartitionKey>> entry : entries) {
            Partition partition = table.getPartition(entry.getKey());
            String partitionName = partition.getName();
            Range<PartitionKey> range = entry.getValue();

            // print all partitions' range is fixed range, even if some of them is created by less than range
            sb.append("PARTITION ").append(partitionName).append(" VALUES [");
            sb.append(range.lowerEndpoint().toSql());
            sb.append(", ").append(range.upperEndpoint().toSql()).append(")");

            if (partitionId != null) {
                partitionId.add(entry.getKey());
                break;
            }

            if (idx != entries.size() - 1) {
                sb.append(",\n");
            }
            idx++;
        }
        sb.append(")");
        return sb.toString();
    }
}

