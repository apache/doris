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

package org.apache.doris.load;

import org.apache.doris.common.io.Writable;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TableLoadInfo implements Writable {
    private Map<Long, PartitionLoadInfo> idToPartitionLoadInfo;
    private Map<Long, Integer> indexIdToSchemaHash;

    public TableLoadInfo() {
        this(new HashMap<Long, PartitionLoadInfo>());
    }
    
    public TableLoadInfo(Map<Long, PartitionLoadInfo> idToPartitionLoadInfo) {
        this.idToPartitionLoadInfo = idToPartitionLoadInfo;
        this.indexIdToSchemaHash = Maps.newHashMap();
    }
    
    public boolean containsIndex(long indexId) {
        if (indexIdToSchemaHash.containsKey(indexId)) {
            return true;
        }
        return false;
    }

    public Map<Long, PartitionLoadInfo> getIdToPartitionLoadInfo() {
        return idToPartitionLoadInfo;
    }
    
    public PartitionLoadInfo getPartitionLoadInfo(long partitionId) {
        return idToPartitionLoadInfo.get(partitionId);
    }
    
    public void addIndexSchemaHash(long indexId, int schemaHash) {
        indexIdToSchemaHash.put(indexId, schemaHash);
    }

    public void addAllSchemaHash(Map<Long, Integer> m) {
        indexIdToSchemaHash.putAll(m);
    }
    
    public int getIndexSchemaHash(long indexId) {
        if (indexIdToSchemaHash.containsKey(indexId)) {
            return indexIdToSchemaHash.get(indexId);
        }
        return -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = idToPartitionLoadInfo.size();
        out.writeInt(count);
        for (Entry<Long, PartitionLoadInfo> entry : idToPartitionLoadInfo.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().write(out);
        }
        
        count = indexIdToSchemaHash.size();
        out.writeInt(count);
        for (Entry<Long, Integer> entry : indexIdToSchemaHash.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }
 
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            long key = in.readLong();
            PartitionLoadInfo value = new PartitionLoadInfo();
            value.readFields(in);
            idToPartitionLoadInfo.put(key, value);
        }
        
        count = in.readInt();
        for (int i = 0; i < count; i++) {
            long key = in.readLong();
            int value = in.readInt();
            indexIdToSchemaHash.put(key, value);
        }
    }

    @Override
    public int hashCode() {
        return idToPartitionLoadInfo.size() ^ indexIdToSchemaHash.size();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof TableLoadInfo)) {
            return false;
        }
        
        TableLoadInfo tableLoadInfo = (TableLoadInfo) obj;
        
        // check idToPartitionLoadInfo
        if (idToPartitionLoadInfo != tableLoadInfo.idToPartitionLoadInfo) {
            if (idToPartitionLoadInfo.size() != tableLoadInfo.idToPartitionLoadInfo.size()) {
                return false;
            }
            for (Entry<Long, PartitionLoadInfo> entry : idToPartitionLoadInfo.entrySet()) {
                long key = entry.getKey();
                if (!tableLoadInfo.idToPartitionLoadInfo.containsKey(key)) {
                    return false;
                }
                if (!entry.getValue().equals(tableLoadInfo.idToPartitionLoadInfo.get(key))) {
                    return false;
                }
            }
        }

        // check indexIdToSchemaHash
        if (indexIdToSchemaHash != tableLoadInfo.indexIdToSchemaHash) {
            if (indexIdToSchemaHash.size() != tableLoadInfo.indexIdToSchemaHash.size()) {
                return false;
            }
            for (Entry<Long, Integer> entry : indexIdToSchemaHash.entrySet()) {
                if (!tableLoadInfo.indexIdToSchemaHash.containsKey(entry.getKey())) {
                    return false;
                }
                if (entry.getValue() != tableLoadInfo.indexIdToSchemaHash.get(entry.getKey())) {
                    return false;
                }
            }
        }
 
        return true;
    }

}
