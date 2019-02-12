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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Preconditions;

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
    protected PartitionType type;
    // partition id -> data property
    protected Map<Long, DataProperty> idToDataProperty;
    // partition id -> replication num
    protected Map<Long, Short> idToReplicationNum;

    public PartitionInfo() {
        // for persist
        this.idToDataProperty = new HashMap<Long, DataProperty>();
        this.idToReplicationNum = new HashMap<Long, Short>();
    }

    public PartitionInfo(PartitionType type) {
        this.type = type;
        this.idToDataProperty = new HashMap<Long, DataProperty>();
        this.idToReplicationNum = new HashMap<Long, Short>();
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
        return idToReplicationNum.get(partitionId);
    }

    public void setReplicationNum(long partitionId, short replicationNum) {
        idToReplicationNum.put(partitionId, replicationNum);
    }

    public void dropPartition(long partitionId) {
        idToDataProperty.remove(partitionId);
        idToReplicationNum.remove(partitionId);
    }

    public void addPartition(long partitionId, DataProperty dataProperty, short replicationNum) {
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicationNum.put(partitionId, replicationNum);
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

    public String toSql(OlapTable table, List<Long> partitionId) {
        return "";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());

        Preconditions.checkState(idToDataProperty.size() == idToReplicationNum.size());
        out.writeInt(idToDataProperty.size());
        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            out.writeLong(entry.getKey());
            if (entry.getValue() == DataProperty.DEFAULT_HDD_DATA_PROPERTY) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                entry.getValue().write(out);
            }

            out.writeShort(idToReplicationNum.get(entry.getKey()));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = PartitionType.valueOf(Text.readString(in));

        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            boolean isDefaultDataProperty = in.readBoolean();
            if (isDefaultDataProperty) {
                idToDataProperty.put(partitionId, DataProperty.DEFAULT_HDD_DATA_PROPERTY);
            } else {
                idToDataProperty.put(partitionId, DataProperty.read(in));
            }

            short replicationNum = in.readShort();
            idToReplicationNum.put(partitionId, replicationNum);
        }
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("type: ").append(type.typeString).append("; ");

        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            buff.append(entry.getKey()).append("is HDD: ");;
            if (entry.getValue() == DataProperty.DEFAULT_HDD_DATA_PROPERTY) {
                buff.append(true);
            } else {
                buff.append(false);

            }
            buff.append("data_property: ").append(entry.getValue().toString());
            buff.append("replica number: ").append(idToReplicationNum.get(entry.getKey()));
        }

        return buff.toString();
    }
}
