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

import org.apache.doris.analysis.AllPartitionDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class RangePartitionInfo extends PartitionInfo {

    public RangePartitionInfo() {
        // for persist
        super();
    }

    public RangePartitionInfo(List<Column> partitionColumns) {
        super(PartitionType.RANGE);
        this.partitionColumns = partitionColumns;
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    public RangePartitionInfo(boolean isAutoCreatePartitions, ArrayList<Expr> exprs, List<Column> partitionColumns) {
        super(PartitionType.RANGE, partitionColumns);
        this.isAutoCreatePartitions = isAutoCreatePartitions;
        if (exprs != null) {
            this.partitionExprs.addAll(exprs);
        }
    }

    public Map<Long, PartitionItem> getPartitionItems(Collection<Long> partitionIds) {
        Map<Long, PartitionItem> columnRanges = Maps.newLinkedHashMapWithExpectedSize(partitionIds.size());
        for (Long partitionId : partitionIds) {
            PartitionItem partitionItem = idToItem.get(partitionId);
            if (partitionItem == null) {
                partitionItem = idToTempItem.get(partitionId);
            }
            if (partitionItem == null) {
                throw new IllegalStateException("Can not found partition item: " + partitionId);
            }
            columnRanges.put(partitionId, partitionItem);
        }
        return columnRanges;
    }

    @Override
    public PartitionItem createAndCheckPartitionItem(SinglePartitionDesc desc, boolean isTemp) throws DdlException {
        Range<PartitionKey> newRange = null;
        PartitionKeyDesc partitionKeyDesc = desc.getPartitionKeyDesc();
        // check range
        try {
            newRange = createAndCheckNewRange(partitionKeyDesc, isTemp);
        } catch (AnalysisException e) {
            throw new DdlException("Invalid range value format： " + e.getMessage());
        }

        Preconditions.checkNotNull(newRange);
        return new RangePartitionItem(newRange);
    }

    @Override
    public List<Map.Entry<Long, PartitionItem>> getPartitionItemEntryList(boolean isTemp, boolean isSorted) {
        Map<Long, PartitionItem> tmpMap = idToItem;
        if (isTemp) {
            tmpMap = idToTempItem;
        }
        List<Map.Entry<Long, PartitionItem>> itemEntryList = Lists.newArrayList(tmpMap.entrySet());
        if (isSorted) {
            Collections.sort(itemEntryList, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);
        }
        return itemEntryList;
    }

    public List<Map.Entry<Long, PartitionItem>> getAllPartitionItemEntryList(boolean isSorted) {
        Map<Long, PartitionItem> tmpMap = Maps.newHashMap();

        tmpMap.putAll(idToItem);
        tmpMap.putAll(idToTempItem);

        List<Map.Entry<Long, PartitionItem>> itemEntryList = Lists.newArrayList(tmpMap.entrySet());
        if (isSorted) {
            Collections.sort(itemEntryList, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);
        }
        return itemEntryList;
    }

    // create a new range and check it.
    private Range<PartitionKey> createAndCheckNewRange(PartitionKeyDesc partKeyDesc, boolean isTemp)
            throws AnalysisException, DdlException {
        boolean isFixedPartitionKeyValueType
                = partKeyDesc.getPartitionType() == PartitionKeyDesc.PartitionKeyValueType.FIXED;

        // generate partitionItemEntryList
        List<Map.Entry<Long, PartitionItem>> partitionItemEntryList = isFixedPartitionKeyValueType
                        ? getPartitionItemEntryList(isTemp, false) : getPartitionItemEntryList(isTemp, true);

        if (isFixedPartitionKeyValueType) {
            return createNewRangeForFixedPartitionValueType(partKeyDesc, partitionItemEntryList);
        } else {
            Range<PartitionKey> newRange = null;
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
            for (Map.Entry<Long, PartitionItem> entry : partitionItemEntryList) {
                currentRange = entry.getValue().getItems();
                // check if equals to upper bound
                PartitionKey upperKey = currentRange.upperEndpoint();
                if (upperKey.compareTo(newRangeUpper) >= 0) {
                    newRange = createNewRangeForLessThanPartitionValueType(newRangeUpper, lastRange, currentRange);
                    break;
                } else {
                    lastRange = currentRange;
                }
            } // end for ranges

            if (newRange == null) /* the new range's upper value is larger than any existing ranges */ {
                newRange = createNewRangeForLessThanPartitionValueType(newRangeUpper, lastRange, currentRange);
            }
            return newRange;
        }
    }

    private Range<PartitionKey> createNewRangeForFixedPartitionValueType(PartitionKeyDesc partKeyDesc,
            List<Map.Entry<Long, PartitionItem>> partitionItemEntryList)
            throws AnalysisException, DdlException {
        PartitionKey lowKey = PartitionKey.createPartitionKey(partKeyDesc.getLowerValues(), partitionColumns);
        PartitionKey upperKey =  PartitionKey.createPartitionKey(partKeyDesc.getUpperValues(), partitionColumns);
        if (lowKey.compareTo(upperKey) >= 0) {
            throw new AnalysisException("The lower values must smaller than upper values");
        }
        Range<PartitionKey> newRange = Range.closedOpen(lowKey, upperKey);
        for (Map.Entry<Long, PartitionItem> partitionItemEntry : partitionItemEntryList) {
            RangeUtils.checkRangeIntersect(newRange, partitionItemEntry.getValue().getItems());
        }
        return newRange;
    }

    private Range<PartitionKey> createNewRangeForLessThanPartitionValueType(PartitionKey newRangeUpper,
            Range<PartitionKey> lastRange, Range<PartitionKey> currentRange) throws AnalysisException, DdlException {
        PartitionKey lowKey = lastRange == null
                ? PartitionKey.createInfinityPartitionKey(partitionColumns, false) : lastRange.upperEndpoint();

        // check: [left, right), error if left equal right
        if (lowKey.compareTo(newRangeUpper) >= 0) {
            throw new AnalysisException("The lower values must smaller than upper values");
        }
        Range<PartitionKey> newRange = Range.closedOpen(lowKey, newRangeUpper);

        if (currentRange != null) {
            // check if range intersected
            RangeUtils.checkRangeIntersect(newRange, currentRange);
        }
        return newRange;
    }

    public static void checkPartitionColumn(Column column) throws AnalysisException {
        PrimitiveType type = column.getDataType();
        if (!type.isFixedPointType() && !type.isDateType()) {
            throw new AnalysisException("Column[" + column.getName() + "] type[" + type
                    + "] cannot be a range partition key.");
        }
    }

    @Override
    public void checkPartitionItemListsMatch(List<PartitionItem> list1, List<PartitionItem> list2)
            throws DdlException {
        RangeUtils.checkPartitionItemListsMatch(list1, list2);
    }

    @Override
    public void checkPartitionItemListsConflict(List<PartitionItem> list1, List<PartitionItem> list2)
            throws DdlException {
        RangeUtils.checkRangeConflict(list1, list2);
    }

    @Deprecated
    public static PartitionInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_136) {
            return GsonUtils.GSON.fromJson(Text.readString(in), RangePartitionInfo.class);
        }

        PartitionInfo partitionInfo = new RangePartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

    @Deprecated
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
            idToItem.put(partitionId, new RangePartitionItem(range));
        }

        counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            Range<PartitionKey> range = RangeUtils.readRange(in);
            idToTempItem.put(partitionId, new RangePartitionItem(range));
        }
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        StringBuilder sb = new StringBuilder();
        int idx = 0;
        if (enableAutomaticPartition()) {
            sb.append("AUTO PARTITION BY RANGE ");
            for (Expr e : partitionExprs) {
                sb.append("(");
                sb.append(e.toSql());
                sb.append(")");
            }
            sb.append("\n(");
        } else {
            sb.append("PARTITION BY RANGE(");
            for (Column column : partitionColumns) {
                if (idx != 0) {
                    sb.append(", ");
                }
                sb.append("`").append(column.getName()).append("`");
                idx++;
            }
            sb.append(")\n(");
        }

        // sort range
        List<Map.Entry<Long, PartitionItem>> entries = new ArrayList<>(this.idToItem.entrySet());
        Collections.sort(entries, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);

        idx = 0;
        for (Map.Entry<Long, PartitionItem> entry : entries) {
            Partition partition = table.getPartition(entry.getKey());
            String partitionName = partition.getName();
            Range<PartitionKey> range = entry.getValue().getItems();

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

    @Override
    public PartitionDesc toPartitionDesc(OlapTable table) throws AnalysisException {
        List<String> partitionColumnNames = partitionColumns.stream().map(Column::getName).collect(Collectors.toList());
        List<AllPartitionDesc> allPartitionDescs = Lists.newArrayListWithCapacity(this.idToItem.size());

        // sort range
        List<Map.Entry<Long, PartitionItem>> entries = new ArrayList<>(this.idToItem.entrySet());
        Collections.sort(entries, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);
        for (Map.Entry<Long, PartitionItem> entry : entries) {
            Partition partition = table.getPartition(entry.getKey());
            String partitionName = partition.getName();

            Range<PartitionKey> range = entry.getValue().getItems();
            PartitionKeyDesc partitionKeyDesc = PartitionKeyDesc.createFixed(
                    PartitionInfo.toPartitionValue(range.lowerEndpoint()),
                    PartitionInfo.toPartitionValue(range.upperEndpoint()));

            Map<String, String> properties = Maps.newHashMap();
            Optional.ofNullable(this.idToStoragePolicy.get(entry.getKey())).ifPresent(p -> {
                if (!p.equals("")) {
                    properties.put("STORAGE POLICY", p);
                }
            });

            allPartitionDescs.add(new SinglePartitionDesc(false, partitionName, partitionKeyDesc, properties));
        }
        return new RangePartitionDesc(this.partitionExprs, partitionColumnNames, allPartitionDescs);
    }
}
