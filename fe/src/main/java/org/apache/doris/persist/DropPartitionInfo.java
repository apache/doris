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

package org.apache.doris.persist;

import org.apache.doris.common.io.Writable;
import org.apache.doris.common.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropPartitionInfo implements Writable {
    private Long dbId;
    private Long tableId;
    private String partitionName;
    
    public DropPartitionInfo() {
    }

    public DropPartitionInfo(Long dbId, Long tableId, String partitionName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionName = partitionName;
    }
    
    public Long getDbId() {
        return dbId;
    }
    
    public Long getTableId() {
        return tableId;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        Text.writeString(out, partitionName);
    }
 
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partitionName = Text.readString(in);
    }
    
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DropPartitionInfo)) {
            return false;
        }
        
        DropPartitionInfo info = (DropPartitionInfo) obj;
        
        return (dbId.equals(info.dbId))
                && (tableId.equals(info.tableId))
                && (partitionName.equals(info.partitionName));
    }
}
