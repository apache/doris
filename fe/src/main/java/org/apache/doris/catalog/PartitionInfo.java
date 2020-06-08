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

import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Preconditions;

import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Repository of a partition's related infos
 */
public class PartitionInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(PartitionInfo.class);

    protected PartitionType type;
    // partition id -> data property
    protected Map<Long, DataProperty> idToDataProperty;
    // partition id -> replication num
    protected Map<Long, Short> idToReplicationNum;
    // true if the partition has multi partition columns
    protected boolean isMultiColumnPartition = false;

    protected Map<Long, Boolean> idToInMemory;

    // partition id -> tablet type
    // Note: currently it's only used for testing, it may change/add more meta field later,
    // so we defer adding meta serialization until memory engine feature is more complete.
    protected Map<Long, TTabletType> idToTabletType;

    public PartitionInfo() {
        this.idToDataProperty = new HashMap<>();
        this.idToReplicationNum = new HashMap<>();
        this.idToInMemory = new HashMap<>();
        this.idToTabletType = new HashMap<>();
    }

    public PartitionInfo(PartitionType type) {
        this.type = type;
        this.idToDataProperty = new HashMap<>();
        this.idToReplicationNum = new HashMap<>();
        this.idToInMemory = new HashMap<>();
        this.idToTabletType = new HashMap<>();
    }

    public PartitionType getType() {
        return type;
    }

    public DataProperty getDataProperty(long partitionId) {
        return idToDataProperty.get(partitionId);
    }

    public void setDataProperty(long partitionId, DataProperty newDataProperty) {
        idToDataProperty.put(partitionId, newDataProperty);
    }

    public short getReplicationNum(long partitionId) {
        if (!idToReplicationNum.containsKey(partitionId)) {
            LOG.debug("failed to get replica num for partition: {}", partitionId);
        }
        return idToReplicationNum.get(partitionId);
    }

    public void setReplicationNum(long partitionId, short replicationNum) {
        idToReplicationNum.put(partitionId, replicationNum);
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
        idToReplicationNum.remove(partitionId);
        idToInMemory.remove(partitionId);
    }

    public void addPartition(long partitionId, DataProperty dataProperty,
                             short replicationNum,
                             boolean isInMemory) {
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicationNum.put(partitionId, replicationNum);
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

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());

        Preconditions.checkState(idToDataProperty.size() == idToReplicationNum.size());
        Preconditions.checkState(idToInMemory.keySet().equals(idToReplicationNum.keySet()));
        out.writeInt(idToDataProperty.size());
        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            out.writeLong(entry.getKey());
            if (entry.getValue().equals(new DataProperty(TStorageMedium.HDD))) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                entry.getValue().write(out);
            }

            out.writeShort(idToReplicationNum.get(entry.getKey()));
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

            short replicationNum = in.readShort();
            idToReplicationNum.put(partitionId, replicationNum);
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
            buff.append(entry.getKey()).append("is HDD: ");;
            if (entry.getValue().equals(new DataProperty(TStorageMedium.HDD))) {
                buff.append(true);
            } else {
                buff.append(false);

            }
            buff.append("data_property: ").append(entry.getValue().toString());
            buff.append("replica number: ").append(idToReplicationNum.get(entry.getKey()));
            buff.append("in memory: ").append(idToInMemory.get(entry.getKey()));
        }

        return buff.toString();
    }
}
