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

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.MaxLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Repository of a partition's related infos
 */
public class PartitionInfo {
    private static final Logger LOG = LogManager.getLogger(PartitionInfo.class);

    @SerializedName("Type")
    protected PartitionType type;
    // partition columns for list and range partitions
    @SerializedName("pc")
    protected List<Column> partitionColumns = Lists.newArrayList();
    // formal partition id -> partition item
    @SerializedName("IdToItem")
    protected Map<Long, PartitionItem> idToItem = Maps.newHashMap();
    @SerializedName("IdToTempItem")
    // temp partition id -> partition item
    protected Map<Long, PartitionItem> idToTempItem = Maps.newHashMap();
    // partition id -> data property
    @SerializedName("IdToDataProperty")
    protected Map<Long, DataProperty> idToDataProperty;
    // partition id -> storage policy
    protected Map<Long, String> idToStoragePolicy;
    // partition id -> replication allocation
    @SerializedName("IdToReplicaAllocation")
    protected Map<Long, ReplicaAllocation> idToReplicaAllocation;
    // true if the partition has multi partition columns
    @SerializedName("isM")
    protected boolean isMultiColumnPartition = false;

    @SerializedName("IdToInMemory")
    protected Map<Long, Boolean> idToInMemory;

    // partition id -> tablet type
    // Note: currently it's only used for testing, it may change/add more meta field later,
    // so we defer adding meta serialization until memory engine feature is more complete.
    protected Map<Long, TTabletType> idToTabletType;

    // the enable automatic partition will hold this, could create partition by expr result
    @SerializedName("PartitionExprs")
    protected ArrayList<Expr> partitionExprs;

    @SerializedName("IsAutoCreatePartitions")
    protected boolean isAutoCreatePartitions;

    public PartitionInfo() {
        this.type = PartitionType.UNPARTITIONED;
        this.idToDataProperty = new HashMap<>();
        this.idToReplicaAllocation = new HashMap<>();
        this.idToInMemory = new HashMap<>();
        this.idToTabletType = new HashMap<>();
        this.idToStoragePolicy = new HashMap<>();
        this.partitionExprs = new ArrayList<>();
    }

    public PartitionInfo(PartitionType type) {
        this.type = type;
        this.idToDataProperty = new HashMap<>();
        this.idToReplicaAllocation = new HashMap<>();
        this.idToInMemory = new HashMap<>();
        this.idToTabletType = new HashMap<>();
        this.idToStoragePolicy = new HashMap<>();
        this.partitionExprs = new ArrayList<>();
    }

    public PartitionInfo(PartitionType type, List<Column> partitionColumns) {
        this(type);
        this.partitionColumns = partitionColumns;
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    public PartitionType getType() {
        return type;
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    public Map<Long, PartitionItem> getIdToItem(boolean isTemp) {
        if (isTemp) {
            return idToTempItem;
        } else {
            return idToItem;
        }
    }

    /**
     * @return both normal partition and temp partition
     */
    public Map<Long, PartitionItem> getAllPartitions() {
        HashMap all = new HashMap<>();
        all.putAll(idToTempItem);
        all.putAll(idToItem);
        return all;
    }

    public PartitionItem getItem(long partitionId) {
        PartitionItem item = idToItem.get(partitionId);
        if (item == null) {
            item = idToTempItem.get(partitionId);
        }
        return item;
    }

    // Get the unique string of the partition range.
    public String getPartitionRangeString(long partitionId) {
        String partitionRange = "";
        if (getType() == PartitionType.RANGE || getType() == PartitionType.LIST) {
            PartitionItem item = getItem(partitionId);
            partitionRange = item.getItemsString();
        }
        return partitionRange;
    }

    public PartitionItem getItemOrAnalysisException(long partitionId) throws AnalysisException {
        PartitionItem item = idToItem.get(partitionId);
        if (item == null) {
            item = idToTempItem.get(partitionId);
        }
        if (item == null) {
            throw new AnalysisException("PartitionItem not found: " + partitionId);
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
        idToStoragePolicy.put(partitionId, desc.getStoragePolicy());

        return partitionItem;
    }

    public PartitionItem createAndCheckPartitionItem(SinglePartitionDesc desc, boolean isTemp) throws DdlException {
        return null;
    }

    public void unprotectHandleNewSinglePartitionDesc(long partitionId, boolean isTemp, PartitionItem partitionItem,
                                                      DataProperty dataProperty, ReplicaAllocation replicaAlloc,
                                                      boolean isInMemory, boolean isMutable) {
        setItemInternal(partitionId, isTemp, partitionItem);
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicaAllocation.put(partitionId, replicaAlloc);
        idToInMemory.put(partitionId, isInMemory);
        idToStoragePolicy.put(partitionId, "");
        //TODO
        //idToMutable.put(partitionId, isMutable);
    }

    public List<Map.Entry<Long, PartitionItem>> getPartitionItemEntryList(boolean isTemp, boolean isSorted) {
        Map<Long, PartitionItem> tmpMap = idToItem;
        if (isTemp) {
            tmpMap = idToTempItem;
        }
        List<Map.Entry<Long, PartitionItem>> itemEntryList = Lists.newArrayList(tmpMap.entrySet());
        if (isSorted) {
            Collections.sort(itemEntryList, PartitionItem.ITEM_MAP_ENTRY_COMPARATOR);
        }
        return itemEntryList;
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

    public boolean enableAutomaticPartition() {
        return isAutoCreatePartitions;
    }

    // forbid change metadata.
    public ArrayList<Expr> getPartitionExprs() {
        return Expr.cloneList(this.partitionExprs);
    }

    public void checkPartitionItemListsMatch(List<PartitionItem> list1, List<PartitionItem> list2) throws DdlException {
    }

    public void checkPartitionItemListsConflict(List<PartitionItem> list1,
            List<PartitionItem> list2) throws DdlException {
    }

    public DataProperty getDataProperty(long partitionId) {
        return idToDataProperty.get(partitionId);
    }

    public void setDataProperty(long partitionId, DataProperty newDataProperty) {
        idToDataProperty.put(partitionId, newDataProperty);
    }

    public void refreshTableStoragePolicy(String storagePolicy) {
        idToStoragePolicy.replaceAll((k, v) -> storagePolicy);
        idToDataProperty.entrySet().forEach(entry -> {
            entry.getValue().setStoragePolicy(storagePolicy);
        });
    }

    public String getStoragePolicy(long partitionId) {
        return idToStoragePolicy.getOrDefault(partitionId, "");
    }

    public void setStoragePolicy(long partitionId, String storagePolicy) {
        idToStoragePolicy.put(partitionId, storagePolicy);
    }

    public Map<Long, ReplicaAllocation> getPartitionReplicaAllocations() {
        return idToReplicaAllocation;
    }

    public ReplicaAllocation getReplicaAllocation(long partitionId) {
        if (!idToReplicaAllocation.containsKey(partitionId)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("failed to get replica allocation for partition: {}", partitionId);
            }
            return ReplicaAllocation.DEFAULT_ALLOCATION;
        }
        return idToReplicaAllocation.get(partitionId);
    }

    public void setReplicaAllocation(long partitionId, ReplicaAllocation replicaAlloc) {
        this.idToReplicaAllocation.put(partitionId, replicaAlloc);
    }

    public boolean getIsInMemory(long partitionId) {
        return idToInMemory.get(partitionId);
    }

    public boolean getIsMutable(long partitionId) {
        return idToDataProperty.get(partitionId).isMutable();
    }

    public void setIsMutable(long partitionId, boolean isMutable) {
        idToDataProperty.get(partitionId).setMutable(isMutable);
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
                             ReplicaAllocation replicaAlloc, boolean isInMemory, boolean isMutable) {
        addPartition(partitionId, dataProperty, replicaAlloc, isInMemory, isMutable);
        setItemInternal(partitionId, isTemp, item);
    }

    public void addPartition(long partitionId, DataProperty dataProperty,
                             ReplicaAllocation replicaAlloc,
                             boolean isInMemory, boolean isMutable) {
        dataProperty.setMutable(isMutable);
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicaAllocation.put(partitionId, replicaAlloc);
        idToInMemory.put(partitionId, isInMemory);
    }

    public boolean isMultiColumnPartition() {
        return isMultiColumnPartition;
    }

    public String toSql(OlapTable table, List<Long> partitionId) {
        return "";
    }

    public PartitionDesc toPartitionDesc(OlapTable olapTable) throws AnalysisException {
        throw new RuntimeException("Should implement it in derived classes.");
    }

    public static List<PartitionValue> toPartitionValue(PartitionKey partitionKey) {
        return partitionKey.getKeys().stream().map(expr -> {
            if (expr == MaxLiteral.MAX_VALUE) {
                return PartitionValue.MAX_VALUE;
            } else if (expr instanceof DateLiteral) {
                return new PartitionValue(expr.getStringValue());
            } else if (expr instanceof NullLiteral) {
                return new PartitionValue("NULL", true);
            } else {
                return new PartitionValue(expr.getRealValue().toString());
            }
        }).collect(Collectors.toList());
    }

    public void moveFromTempToFormal(long tempPartitionId) {
        PartitionItem item = idToTempItem.remove(tempPartitionId);
        if (item != null) {
            idToItem.put(tempPartitionId, item);
        }
    }

    public void resetPartitionIdForRestore(
            Map<Long, Long> partitionIdMap,
            ReplicaAllocation restoreReplicaAlloc, boolean isSinglePartitioned) {
        Map<Long, DataProperty> origIdToDataProperty = idToDataProperty;
        Map<Long, ReplicaAllocation> origIdToReplicaAllocation = idToReplicaAllocation;
        Map<Long, PartitionItem> origIdToItem = idToItem;
        Map<Long, Boolean> origIdToInMemory = idToInMemory;
        Map<Long, String> origIdToStoragePolicy = idToStoragePolicy;
        idToDataProperty = Maps.newHashMap();
        idToReplicaAllocation = Maps.newHashMap();
        idToItem = Maps.newHashMap();
        idToInMemory = Maps.newHashMap();
        idToStoragePolicy = Maps.newHashMap();

        for (Map.Entry<Long, Long> entry : partitionIdMap.entrySet()) {
            idToDataProperty.put(entry.getKey(), origIdToDataProperty.get(entry.getValue()));
            idToReplicaAllocation.put(entry.getKey(),
                    restoreReplicaAlloc == null ? origIdToReplicaAllocation.get(entry.getValue())
                            : restoreReplicaAlloc);
            if (!isSinglePartitioned) {
                idToItem.put(entry.getKey(), origIdToItem.get(entry.getValue()));
            }
            idToInMemory.put(entry.getKey(), origIdToInMemory.get(entry.getValue()));
            idToStoragePolicy.put(entry.getKey(), origIdToStoragePolicy.get(entry.getValue()));
        }
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        type = PartitionType.valueOf(Text.readString(in));

        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            boolean isDefaultHddDataProperty = in.readBoolean();
            if (isDefaultHddDataProperty) {
                idToDataProperty.put(partitionId, new DataProperty(DataProperty.DEFAULT_HDD_DATA_PROPERTY));
            } else {
                idToDataProperty.put(partitionId, DataProperty.read(in));
            }

            if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_105) {
                short replicationNum = in.readShort();
                ReplicaAllocation replicaAlloc = new ReplicaAllocation(replicationNum);
                idToReplicaAllocation.put(partitionId, replicaAlloc);
            } else {
                ReplicaAllocation replicaAlloc = ReplicaAllocation.read(in);
                idToReplicaAllocation.put(partitionId, replicaAlloc);
            }

            idToInMemory.put(partitionId, in.readBoolean());
            if (Config.isCloudMode()) {
                // HACK: the origin implementation of the cloud mode has code likes:
                //
                //     idToPersistent.put(partitionId, in.readBoolean());
                //
                // keep the compatibility here.
                in.readBoolean();
            }
        }
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_125) {
            int size = in.readInt();
            for (int i = 0; i < size; ++i) {
                Expr e = Expr.readIn(in);
                this.partitionExprs.add(e);
            }
            this.isAutoCreatePartitions = in.readBoolean();
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
            buff.append("is mutable: ").append(idToDataProperty.get(entry.getKey()).isMutable());
        }

        return buff.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionInfo that = (PartitionInfo) o;
        return isMultiColumnPartition == that.isMultiColumnPartition && type == that.type && Objects.equals(
                partitionColumns, that.partitionColumns) && Objects.equals(idToItem, that.idToItem)
                && Objects.equals(idToTempItem, that.idToTempItem) && Objects.equals(idToDataProperty,
                that.idToDataProperty) && Objects.equals(idToStoragePolicy, that.idToStoragePolicy)
                && Objects.equals(idToReplicaAllocation, that.idToReplicaAllocation) && Objects.equals(
                idToInMemory, that.idToInMemory) && Objects.equals(idToTabletType, that.idToTabletType)
                && Objects.equals(partitionExprs, that.partitionExprs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, partitionColumns, idToItem, idToTempItem, idToDataProperty, idToStoragePolicy,
                idToReplicaAllocation, isMultiColumnPartition, idToInMemory, idToTabletType, partitionExprs);
    }
}
