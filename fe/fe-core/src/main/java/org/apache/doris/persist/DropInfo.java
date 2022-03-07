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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropInfo implements Writable {
    private long dbId;
    private long tableId;
    
    private long indexId;
    private boolean forceDrop = false;

    public DropInfo() {
    }
    
    public DropInfo(long dbId, long tableId, long indexId, boolean forceDrop) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.indexId = indexId;
        this.forceDrop = forceDrop;
    }
    
    public long getDbId() {
        return this.dbId;
    }
    
    public long getTableId() {
        return this.tableId;
    }

    public long getIndexId() {
        return this.indexId;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeBoolean(forceDrop);
        if (indexId == -1L) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(indexId);
        }
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        forceDrop = in.readBoolean();
        boolean hasIndexId = in.readBoolean();
        if (hasIndexId) {
            indexId = in.readLong();
        } else {
            indexId = -1L;
        }
    }

    public static DropInfo read(DataInput in) throws IOException {
        DropInfo dropInfo = new DropInfo();
        dropInfo.readFields(in);
        return dropInfo;
    }
    
    public boolean equals (Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (!(obj instanceof DropInfo)) {
            return false;
        }
        
        DropInfo info = (DropInfo) obj;
        
        return (dbId == info.dbId) && (tableId == info.tableId) && (indexId == info.indexId)
                && (forceDrop == info.forceDrop);
    }
}
