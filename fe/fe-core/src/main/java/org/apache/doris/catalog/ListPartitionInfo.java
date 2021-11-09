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
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.ListUtil;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ListPartitionInfo extends PartitionInfo{

    public ListPartitionInfo() {
        // for persist
        super();
    }

    public ListPartitionInfo(List<Column> partitionColumns) {
        super(PartitionType.LIST);
        this.partitionColumns = partitionColumns;
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        PartitionInfo partitionInfo = new ListPartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

    @Override
    public PartitionItem createAndCheckPartitionItem(SinglePartitionDesc desc, boolean isTemp) throws DdlException {
        // get partition key
        PartitionKeyDesc partitionKeyDesc = desc.getPartitionKeyDesc();

        for (List<PartitionValue> values : partitionKeyDesc.getInValues()) {
            Preconditions.checkArgument(values.size() == partitionColumns.size(),
                    "partition key desc list size[" + values.size() + "] is not equal to " +
                            "partition column size[" + partitionColumns.size() + "]");
        }
        List<PartitionKey> partitionKeys = new ArrayList<>();
        try {
            for (List<PartitionValue> values : partitionKeyDesc.getInValues()) {
                PartitionKey partitionKey = PartitionKey.createListPartitionKey(values, partitionColumns);
                checkNewPartitionKey(partitionKey, partitionKeyDesc, isTemp);
                if (partitionKeys.contains(partitionKey)) {
                    throw new AnalysisException("The partition key[" + partitionKeyDesc.toSql() + "] has duplicate item ["
                            + partitionKey.toSql() + "].");
                }
                partitionKeys.add(partitionKey);
            }
        } catch (AnalysisException e) {
            throw new DdlException("Invalid list value format: " + e.getMessage());
        }
        return new ListPartitionItem(partitionKeys);
    }

    private void checkNewPartitionKey(PartitionKey newKey, PartitionKeyDesc keyDesc, boolean isTemp) throws AnalysisException {
        Map<Long, PartitionItem> id2Item = idToItem;
        if (isTemp) {
             id2Item = idToTempItem;
        }
        // check new partition key not exists.
        for (Map.Entry<Long, PartitionItem> entry : id2Item.entrySet()) {
            if (((ListPartitionItem)entry.getValue()).getItems().contains(newKey)) {
                StringBuilder sb = new StringBuilder();
                sb.append("The partition key[").append(newKey.toSql()).append("] in partition item[")
                        .append(keyDesc.toSql()).append("] is conflict with current partitionKeys[")
                        .append(((ListPartitionItem) entry.getValue()).toSql()).append("]");
                throw new AnalysisException(sb.toString());
            }
        }
    }

    @Override
    public void checkPartitionItemListsMatch(List<PartitionItem> list1, List<PartitionItem> list2) throws DdlException {
        ListUtil.checkPartitionKeyListsMatch(list1, list2);
    }

    @Override
    public void checkPartitionItemListsConflict(List<PartitionItem> list1, List<PartitionItem> list2) throws DdlException {
        ListUtil.checkListsConflict(list1, list2);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        // partition columns
        out.writeInt(partitionColumns.size());
        for (Column column : partitionColumns) {
            column.write(out);
        }

        out.writeInt(idToItem.size());
        for (Map.Entry<Long, PartitionItem> entry : idToItem.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().write(out);
        }

        out.writeInt(idToTempItem.size());
        for (Map.Entry<Long, PartitionItem> entry : idToTempItem.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().write(out);
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
            ListPartitionItem partitionItem = ListPartitionItem.read(in);
            idToItem.put(partitionId, partitionItem);
        }

        counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            ListPartitionItem partitionItem = ListPartitionItem.read(in);
            idToTempItem.put(partitionId, partitionItem);
        }
    }

    public static void checkPartitionColumn(Column column) throws AnalysisException {
        PrimitiveType type = column.getDataType();
        if (!type.isFixedPointType() && !type.isDateType()
                && !type.isCharFamily() && type != PrimitiveType.BOOLEAN) {
            throw new AnalysisException("Column[" + column.getName() + "] type[" + type
                    + "] cannot be a list partition key.");
        }
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY LIST(");
        int idx = 0;
        for (Column column : partitionColumns) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append("`").append(column.getName()).append("`");
            idx++;
        }
        sb.append(")\n(");

        // sort list
        List<Map.Entry<Long, PartitionItem>> entries = new ArrayList<>(this.idToItem.entrySet());
        Collections.sort(entries, ListUtil.LIST_MAP_ENTRY_COMPARATOR);
        idx = 0;
        for (Map.Entry<Long, PartitionItem> entry : entries) {
            Partition partition = table.getPartition(entry.getKey());
            String partitionName = partition.getName();
            List<PartitionKey> partitionKeys = entry.getValue().getItems();

            sb.append("PARTITION ").append(partitionName).append(" VALUES IN ");
            sb.append("(");
            int idxInternal = 0;
            for (PartitionKey partitionKey : partitionKeys) {
                String partitionKeyStr = partitionKey.toSql();
                if (!isMultiColumnPartition) {
                    partitionKeyStr = partitionKeyStr.substring(1, partitionKeyStr.length() - 1);
                }
                sb.append(partitionKeyStr);
                if (partitionKeys.size() > 1 && idxInternal != partitionKeys.size() - 1) {
                    sb.append(",");
                }
                idxInternal++;
            }
            sb.append(")");

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
