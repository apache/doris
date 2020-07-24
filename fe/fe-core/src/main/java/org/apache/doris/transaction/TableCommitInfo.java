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

package org.apache.doris.transaction;

import org.apache.doris.common.io.Writable;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class TableCommitInfo implements Writable {
    
    private long tableId;
    private Map<Long, PartitionCommitInfo> idToPartitionCommitInfo;

    public TableCommitInfo() {
        
    }
    
    public TableCommitInfo(long tableId) {
        this.tableId = tableId;
        idToPartitionCommitInfo = Maps.newHashMap();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tableId);
        if (idToPartitionCommitInfo == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(idToPartitionCommitInfo.size());
            for (PartitionCommitInfo partitionCommitInfo : idToPartitionCommitInfo.values()) {
                partitionCommitInfo.write(out);
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
        tableId = in.readLong();
        boolean hasPartitionInfo = in.readBoolean();
        idToPartitionCommitInfo = Maps.newHashMap();
        if (hasPartitionInfo) {
            int elementNum = in.readInt();
            for (int i = 0; i < elementNum; ++i) {
                PartitionCommitInfo partitionCommitInfo = PartitionCommitInfo.read(in);
                idToPartitionCommitInfo.put(partitionCommitInfo.getPartitionId(), partitionCommitInfo);
            }
        }
    }

    public long getTableId() {
        return tableId;
    }

    public Map<Long, PartitionCommitInfo> getIdToPartitionCommitInfo() {
        return idToPartitionCommitInfo;
    }
    
    public void addPartitionCommitInfo(PartitionCommitInfo info) {
        this.idToPartitionCommitInfo.put(info.getPartitionId(), info);
    }
    
    public void removePartition(long partitionId) {
        this.idToPartitionCommitInfo.remove(partitionId);
    }
    
    public PartitionCommitInfo getPartitionCommitInfo(long partitionId) {
        return this.idToPartitionCommitInfo.get(partitionId);
    }
}
