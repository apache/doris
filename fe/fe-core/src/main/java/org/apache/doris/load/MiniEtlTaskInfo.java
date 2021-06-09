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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MiniEtlTaskInfo implements Writable {
    private long id;
    private long backendId;
    private long tableId;
    private final EtlStatus taskStatus;
    
    public MiniEtlTaskInfo() {
        this(-1L, -1L, -1L);
    }

    public MiniEtlTaskInfo(long id, long backendId, long tableId) {
        this.id = id;
        this.backendId = backendId;
        this.tableId = tableId;
        this.taskStatus = new EtlStatus();
    }

    public long getId() {
        return id;
    }

    public long getBackendId() {
        return backendId;
    }

    public long getTableId() {
        return tableId;
    }

    public EtlStatus getTaskStatus() {
        return taskStatus;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(backendId);
        out.writeLong(tableId);
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        backendId = in.readLong();
        tableId = in.readLong();
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
 
        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof MiniEtlTaskInfo)) {
            return false;
        }
        
        MiniEtlTaskInfo taskInfo = (MiniEtlTaskInfo) obj;
 
        return id == taskInfo.id 
                && backendId == taskInfo.backendId 
                && tableId == taskInfo.tableId;
    }
}
