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

import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * Repository of a partition's related infos
 */
public class PartitionInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(PartitionInfo.class);

    protected PartitionType type;
    // partition columns for list and range partitions
    protected List<Column> partitionColumns = Lists.newArrayList();
    // formal partition id -> partition item
    protected Map<Long, PartitionItem> idToItem = Maps.newHashMap();
    // temp partition id -> partition item
    protected Map<Long, PartitionItem> idToTempItem = Maps.newHashMap();
    // partition id -> data property
    protected Map<Long, DataProperty> idToDataProperty;
    // partition id -> replication allocation
    protected Map<Long, ReplicaAllocation> idToReplicaAllocation;
    // true if the partition has multi partition columns
    protected boolean isMultiColumnPartition = false;

    protected Map<Long, Boolean> idToInMemory;

    // partition id -> tablet type
    // Note: currently it's only used for testing, it may change/add more meta field later,
    // so we defer adding meta serialization until memory engine feature is more complete.
    protected Map<Long, TTabletType> idToTabletType;

    public PartitionInfo() {
        this.idToDataProperty = new HashMap<>();
        this.idToReplicaAllocation = new HashMap<>();
        this.idToInMemory = new HashMap<>();
        this.idToTabletType = new HashMap<>();
    }

    public PartitionInfo(PartitionType type) {
        this.type = type;
        this.idToDataProperty = new HashMap<>();
        this.idToReplicaAllocation = new HashMap<>();
        this.idToInMemory = new HashMap<>();
        this.idToTabletType = new HashMap<>();
    }

    public PartitionInfo(PartitionType type, List<Column> partitionColumns) {
        this(type);
        this.partitionColumns = partitionColumns;
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    public PartitionType getType() {
        return type;
    }

    public List<Column> getPartitionColumns(){
        return partitionColumns;
    }

    public Map<Long, PartitionItem> getIdToItem(boolean isTemp) {
        if (isTemp) {
            return idToTempItem;
        } else {
            return idToItem;
        }
    }

    public PartitionItem getItem(long partitionId) {
        PartitionItem item = idToItem.get(partitionId);
        if (item == null) {
            item = idToTempItem.get(partitionId);
        }
        return item;
    }

    public void setItem(long partitionId, boolean isTemp, PartitionItem item) {
        setItemInternal(partitionId, isTemp, item);
    }

    private void setItemInternal(long partitionId, boolean isTemp, PartitionItem item) {
        if (isTemp) {
            idToTempItem.put(partitionId, item);
        } else {
            idToItem.put(partitionId, item);
        }
    }

    public PartitionItem handleNewSinglePartitionDesc(SinglePartitionDesc desc,
                                              long partitionId, boolean isTemp) throws DdlException {
        Preconditions.checkArgument(desc.isAnalyzed());
        PartitionItem partitionItem = createAndCheckPartitionItem(desc, isTemp);
        setItemInternal(partitionId, isTemp, partitionItem);

        idToDataProperty.put(partitionId, desc.getPartitionDataProperty());
        idToReplicaAllocation.put(partitionId, desc.getReplicaAlloc());
        idToInMemory.put(partitionId, desc.isInMemory());

        return partitionItem;
    }

    public PartitionItem createAndCheckPartitionItem(SinglePartitionDesc desc, boolean isTemp) throws DdlException {
        return null;
    }

    public void unprotectHandleNewSinglePartitionDesc(long partitionId, boolean isTemp, PartitionItem partitionItem,
                                                      DataProperty dataProperty, ReplicaAllocation replicaAlloc,
                                                      boolean isInMemory) {
        setItemInternal(partitionId, isTemp, partitionItem);
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicaAllocation.put(partitionId, replicaAlloc);
        idToInMemory.put(partitionId, isInMemory);
    }

    public List<Map.Entry<Long, PartitionItem>> getSortedItemMap(boolean isTemp) {
        Map<Long, PartitionItem> tmpMap = idToItem;
        if (isTemp) {
            tmpMap = idToTempItem;
        }
        List<Map.Entry<Long, PartitionItem>> sortedList = Lists.newArrayList(tmpMap.entrySet());
        Collections.sort(sortedList, PartitionItem.ITEM_MAP_ENTRY_COMPARATOR);
        return sortedList;
    }

    // get sorted item list, exclude partitions which ids are in 'excludePartitionIds'
    public List<PartitionItem> getItemList(Set<Long> excludePartitionIds, boolean isTemp) {
        Map<Long, PartitionItem> tempMap = idToItem;
        if (isTemp) {
            tempMap = idToTempItem;
        }
        List<PartitionItem> resultList = Lists.newArrayList();
        for (Map.Entry<Long, PartitionItem> entry : tempMap.entrySet()) {
            if (!excludePartitionIds.contains(entry.getKey())) {
                resultList.add(entry.getValue());
            }
        }
        return resultList;
    }

    // return any item intersect with the newItem.
    // return null if no item intersect.
    public PartitionItem getAnyIntersectItem(PartitionItem newItem, boolean isTemp) {
        Map<Long, PartitionItem> tmpMap = idToItem;
        if (isTemp) {
            tmpMap = idToTempItem;
        }
        PartitionItem retItem;
        for (PartitionItem item : tmpMap.values()) {
            retItem = item.getIntersect(newItem);
            if (null != retItem) {
                return retItem;
            }
        }
        return null;
    }

    public void checkPartitionItemListsMatch(List<PartitionItem> list1, List<PartitionItem> list2) throws DdlException {
    }

    public void checkPartitionItemListsConflict(List<PartitionItem> list1, List<PartitionItem> list2) throws DdlException {
    }

    public DataProperty getDataProperty(long partitionId) {
        return idToDataProperty.get(partitionId);
    }

    public void setDataProperty(long partitionId, DataProperty newDataProperty) {
        idToDataProperty.put(partitionId, newDataProperty);
    }

    public ReplicaAllocation getReplicaAllocation(long partitionId) {
        if (!idToReplicaAllocation.containsKey(partitionId)) {
            LOG.debug("failed to get replica allocation for partition: {}", partitionId);
        }
        return idToReplicaAllocation.get(partitionId);
    }

    public void setReplicaAllocation(long partitionId, ReplicaAllocation replicaAlloc) {
        this.idToReplicaAllocation.put(partitionId, replicaAlloc);
    }

    public boolean getIsInMemory(long partitionId) {
        return idToInMemory.get(partitionId);
    }

    public void setIsInMemory(long partitionId, boolean isInMemory) {
        idToInMemory.put(partitionId, isInMemory);
    }

    public TTabletType getTabletType(long partitionId) {
        if (!idToTabletType.containsKey(partitionId)) {
            return TTabletType.TABLET_TYPE_DISK;
        }
        return idToTabletType.get(partitionId);
    }

    public void setTabletType(long partitionId, TTabletType tabletType) {
        idToTabletType.put(partitionId, tabletType);
    }

    public void dropPartition(long partitionId) {
        idToDataProperty.remove(partitionId);
        idToReplicaAllocation.remove(partitionId);
        idToInMemory.remove(partitionId);
        idToItem.remove(partitionId);
        idToTempItem.remove(partitionId);
    }

    public void addPartition(long partitionId, boolean isTemp, PartitionItem item, DataProperty dataProperty,
                             ReplicaAllocation replicaAlloc, boolean isInMemory) {
        addPartition(partitionId, dataProperty, replicaAlloc, isInMemory);
        setItemInternal(partitionId, isTemp, item);
    }

    public void addPartition(long partitionId, DataProperty dataProperty,
                             ReplicaAllocation replicaAlloc,
                             boolean isInMemory) {
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicaAllocation.put(partitionId, replicaAlloc);
        idToInMemory.put(partitionId, isInMemory);
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

    public boolean isMultiColumnPartition() {
        return isMultiColumnPartition;
    }

    public String toSql(OlapTable table, List<Long> partitionId) {
        return "";
    }

    public void moveFromTempToFormal(long tempPartitionId) {
        PartitionItem item = idToTempItem.remove(tempPartitionId);
        if (item != null) {
            idToItem.put(tempPartitionId, item);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());

        Preconditions.checkState(idToDataProperty.size() == idToReplicaAllocation.size());
        Preconditions.checkState(idToInMemory.keySet().equals(idToReplicaAllocation.keySet()));
        out.writeInt(idToDataProperty.size());
        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            out.writeLong(entry.getKey());
            if (entry.getValue().equals(new DataProperty(TStorageMedium.HDD))) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                entry.getValue().write(out);
            }

            idToReplicaAllocation.get(entry.getKey()).write(out);
            out.writeBoolean(idToInMemory.get(entry.getKey()));
        }
    }

    public void readFields(DataInput in) throws IOException {
        type = PartitionType.valueOf(Text.readString(in));

        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            boolean isDefaultHddDataProperty = in.readBoolean();
            if (isDefaultHddDataProperty) {
                idToDataProperty.put(partitionId, new DataProperty(TStorageMedium.HDD));
            } else {
                idToDataProperty.put(partitionId, DataProperty.read(in));
            }

            if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_100) {
                short replicationNum = in.readShort();
                ReplicaAllocation replicaAlloc = new ReplicaAllocation(replicationNum);
                idToReplicaAllocation.put(partitionId, replicaAlloc);
            } else {
                ReplicaAllocation replicaAlloc = ReplicaAllocation.read(in);
                idToReplicaAllocation.put(partitionId, replicaAlloc);
            }

            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_72) {
                idToInMemory.put(partitionId, in.readBoolean());
            } else {
                // for compatibility, default is false
                idToInMemory.put(partitionId, false);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("type: ").append(type.typeString).append("; ");

        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            buff.append(entry.getKey()).append(" is HDD: ");
            if (entry.getValue().equals(new DataProperty(TStorageMedium.HDD))) {
                buff.append(true);
            } else {
                buff.append(false);
            }
            buff.append("; ");
            buff.append("data_property: ").append(entry.getValue().toString()).append("; ");
            buff.append("replica number: ").append(idToReplicaAllocation.get(entry.getKey())).append("; ");
            buff.append("in memory: ").append(idToInMemory.get(entry.getKey()));
        }

        return buff.toString();
    }

}
