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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RangePartitionInfo extends PartitionInfo {
    private static final Logger LOG = LogManager.getLogger(RangePartitionInfo.class);

    private List<Column> partitionColumns;
    // partition id -> partition range
    private Map<Long, Range<PartitionKey>> idToRange;

    private static final Comparator<Map.Entry<Long, Range<PartitionKey>>> RANGE_MAP_ENTRY_COMPARATOR;

    static {
        RANGE_MAP_ENTRY_COMPARATOR = new Comparator<Map.Entry<Long, Range<PartitionKey>>>() {
            @Override
            public int compare(Map.Entry<Long, Range<PartitionKey>> o1,
                               Map.Entry<Long, Range<PartitionKey>> o2) {
                return o1.getValue().lowerEndpoint().compareTo(o2.getValue().lowerEndpoint());
            }
        };
    }

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
            short replicationNum) {
        addPartition(partitionId, dataProperty, replicationNum);
        idToRange.put(partitionId, range);
    }

    public Range<PartitionKey> checkAndCreateRange(SingleRangePartitionDesc desc) throws DdlException {
        Range<PartitionKey> newRange = null;
        // check range
        try {
            // create single value partition key
            PartitionKeyDesc partKeyDesc = desc.getPartitionKeyDesc();
            PartitionKey singlePartitionKey  = null;
            if (partKeyDesc.isMax()) {
                singlePartitionKey = PartitionKey.createInfinityPartitionKey(partitionColumns, true);
            } else {
                singlePartitionKey = PartitionKey.createPartitionKey(partKeyDesc.getUpperValues(), partitionColumns);
            }

            if (singlePartitionKey.isMinValue()) {
                throw new DdlException("Partition value should not be MIN VALUE: " + singlePartitionKey.toSql());
            }

            List<Map.Entry<Long, Range<PartitionKey>>> entries =
                    new ArrayList<Map.Entry<Long, Range<PartitionKey>>>(this.idToRange.entrySet());
            Collections.sort(entries, RANGE_MAP_ENTRY_COMPARATOR);

            Range<PartitionKey> lastRange = null;
            Range<PartitionKey> nextRange = null;
            for (Map.Entry<Long, Range<PartitionKey>> entry : entries) {
                nextRange = entry.getValue();

                // check if equals to upper bound
                PartitionKey upperKey = nextRange.upperEndpoint();
                if (upperKey.compareTo(singlePartitionKey) >= 0) {
                    PartitionKey lowKey = null;
                    if (!partKeyDesc.getLowerValues().isEmpty()) {
                        lowKey = PartitionKey.createPartitionKey(partKeyDesc.getLowerValues(), partitionColumns);
                    } else {
                        if (lastRange == null) {
                            lowKey = PartitionKey.createInfinityPartitionKey(partitionColumns, false);
                        } else {
                            lowKey = lastRange.upperEndpoint();
                        }
                    }

                    newRange = Range.closedOpen(lowKey, singlePartitionKey);

                    // check if range intersected
                    checkRangeIntersect(newRange, nextRange);
                    break;
                }
                lastRange = nextRange;
            } // end for ranges

            if (newRange == null) {
                PartitionKey lowKey = null;
                if (!partKeyDesc.getLowerValues().isEmpty()) {
                    lowKey = PartitionKey.createPartitionKey(partKeyDesc.getLowerValues(), partitionColumns);
                } else {
                    if (lastRange == null) {
                        // add first partition to this table. so the lower key is MIN
                        lowKey = PartitionKey.createInfinityPartitionKey(partitionColumns, false);
                    } else {
                        lowKey = lastRange.upperEndpoint();
                    }
                }

                newRange = Range.closedOpen(lowKey, singlePartitionKey);
            }
        } catch (AnalysisException e) {
            throw new DdlException("Invalid range value format： " + e.getMessage());
        }

        Preconditions.checkNotNull(newRange);
        return newRange;
    }

    public static void checkRangeIntersect(Range<PartitionKey> range1, Range<PartitionKey> range2) throws DdlException {
        if (range2.isConnected(range1)) {
            if (!range2.intersection(range1).isEmpty()) {
                throw new DdlException("Range " + range1 + " is intersected with range: " + range2);
            }
        }
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
            throw new DdlException("Invalid key range", e);
        }
        idToDataProperty.put(partitionId, desc.getPartitionDataProperty());
        idToReplicationNum.put(partitionId, desc.getReplicationNum());
        return range;
    }

    // for catalog restore
    public void unprotectHandleNewSinglePartitionDesc(long partitionId, Range<PartitionKey> range,
                                                      DataProperty dataProperty, short replicationNum)
            throws DdlException {
        idToRange.put(partitionId, range);
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicationNum.put(partitionId, replicationNum);
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
        Collections.sort(sortedList, RANGE_MAP_ENTRY_COMPARATOR);
        return sortedList;
    }

    public static void writeRange(DataOutput out, Range<PartitionKey> range) throws IOException {
        boolean hasLowerBound = false;
        boolean hasUpperBound = false;

        // write lower bound if lower bound exists
        hasLowerBound = range.hasLowerBound();
        out.writeBoolean(hasLowerBound);
        if (hasLowerBound) {
            PartitionKey lowerBound = range.lowerEndpoint();
            out.writeBoolean(range.lowerBoundType() == BoundType.CLOSED);
            lowerBound.write(out);
        }

        // write upper bound if upper bound exists
        hasUpperBound = range.hasUpperBound();
        out.writeBoolean(hasUpperBound);
        if (hasUpperBound) {
            PartitionKey upperBound = range.upperEndpoint();
            out.writeBoolean(range.upperBoundType() == BoundType.CLOSED);
            upperBound.write(out);
        }
    }

    public static Range<PartitionKey> readRange(DataInput in) throws IOException {
        boolean hasLowerBound = false;
        boolean hasUpperBound = false;
        boolean lowerBoundClosed = false;
        boolean upperBoundClosed = false;
        PartitionKey lowerBound = null;
        PartitionKey upperBound = null;

        hasLowerBound = in.readBoolean();
        if (hasLowerBound) {
            lowerBoundClosed = in.readBoolean();
            lowerBound = PartitionKey.read(in);
        }

        hasUpperBound = in.readBoolean();
        if (hasUpperBound) {
            upperBoundClosed = in.readBoolean();
            upperBound = PartitionKey.read(in);
        }

        // Totally 9 cases. Both lower bound and upper bound could be open, closed or not exist
        if (hasLowerBound && lowerBoundClosed && hasUpperBound && upperBoundClosed) {
            return Range.closed(lowerBound, upperBound);
        }
        if (hasLowerBound && lowerBoundClosed && hasUpperBound && !upperBoundClosed) {
            return Range.closedOpen(lowerBound, upperBound);
        }
        if (hasLowerBound && !lowerBoundClosed && hasUpperBound && upperBoundClosed) {
            return Range.openClosed(lowerBound, upperBound);
        }
        if (hasLowerBound && !lowerBoundClosed && hasUpperBound && !upperBoundClosed) {
            return Range.open(lowerBound, upperBound);
        }
        if (hasLowerBound && lowerBoundClosed && !hasUpperBound) {
            return Range.atLeast(lowerBound);
        }
        if (hasLowerBound && !lowerBoundClosed && !hasUpperBound) {
            return Range.greaterThan(lowerBound);
        }
        if (!hasLowerBound && hasUpperBound && upperBoundClosed) {
            return Range.atMost(upperBound);
        }
        if (!hasLowerBound && hasUpperBound && !upperBoundClosed) {
            return Range.lessThan(upperBound);
        }
        // Neither lower bound nor upper bound exists, return null. This means just one partition
        return null;
    }

    public SingleRangePartitionDesc toSingleRangePartitionDesc(long partitionId, String partitionName,
                                                               Map<String, String> properties) {
        Range<PartitionKey> range = idToRange.get(partitionId);
        List<String> upperValues = Lists.newArrayList();
        List<String> lowerValues = Lists.newArrayList();
        // FIXME(cmy): check here(getStringValue)
        lowerValues.add(range.lowerEndpoint().getKeys().get(0).getStringValue());

        PartitionKey upperKey = range.upperEndpoint();
        PartitionKeyDesc keyDesc = null;
        if (upperKey.isMaxValue()) {
            keyDesc = PartitionKeyDesc.createMaxKeyDesc();
            keyDesc.setLowerValues(lowerValues);
        } else {
            upperValues.add(range.upperEndpoint().getKeys().get(0).getStringValue());
            keyDesc = new PartitionKeyDesc(lowerValues, upperValues);
        }

        SingleRangePartitionDesc singleDesc = new SingleRangePartitionDesc(false, partitionName, keyDesc, properties);

        if (properties != null) {
            // properties
            Short replicationNum = getReplicationNum(partitionId);
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, replicationNum.toString());
        }
        return singleDesc;
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
            RangePartitionInfo.writeRange(out, entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            Column column = Column.read(in);
            partitionColumns.add(column);
        }

        counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            Range<PartitionKey> range = RangePartitionInfo.readRange(in);
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
        Collections.sort(entries, RANGE_MAP_ENTRY_COMPARATOR);

        Range<PartitionKey> lastRange = null;
        idx = 0;
        for (Map.Entry<Long, Range<PartitionKey>> entry : entries) {
            Partition partition = table.getPartition(entry.getKey());
            String partitionName = partition.getName();
            Range<PartitionKey> range = entry.getValue();

            if (idx == 0) {
                // first partition
                if (!range.lowerEndpoint().isMinValue()) {
                    sb.append("PARTITION ").append(FeNameFormat.FORBIDDEN_PARTITION_NAME).append(idx)
                            .append(" VALUES LESS THAN ").append(range.lowerEndpoint().toSql());
                    sb.append(",\n");
                }
            } else {
                Preconditions.checkNotNull(lastRange);
                if (!lastRange.upperEndpoint().equals(range.lowerEndpoint())) {
                    sb.append("PARTITION ").append(FeNameFormat.FORBIDDEN_PARTITION_NAME).append(idx)
                            .append(" VALUES LESS THAN ").append(range.lowerEndpoint().toSql());
                    sb.append(",\n");
                }
            }

            sb.append("PARTITION ").append(partitionName).append(" VALUES LESS THAN ");
            sb.append(range.upperEndpoint().toSql());

            if (partitionId != null) {
                partitionId.add(entry.getKey());
                break;
            }

            if (idx != entries.size() - 1) {
                sb.append(",\n");
            }
            idx++;

            lastRange = range;
        }
        sb.append(")");
        return sb.toString();
    }
}

