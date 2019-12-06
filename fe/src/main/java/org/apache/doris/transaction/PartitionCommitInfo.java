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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.doris.common.io.Writable;

public class PartitionCommitInfo implements Writable {

    private long partitionId;
    private long version;
    private long versionHash;

    public PartitionCommitInfo() {
        
    }

    public PartitionCommitInfo(long partitionId, long version, long versionHash) {
        super();
        this.partitionId = partitionId;
        this.version = version;
        this.versionHash = versionHash;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(partitionId);
        out.writeLong(version);
        out.writeLong(versionHash);
    }

    public void readFields(DataInput in) throws IOException {
        partitionId = in.readLong();
        version = in.readLong();
        versionHash = in.readLong();
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getVersion() {
        return version;
    }

    public long getVersionHash() {
        return versionHash;
    }
    
    @Override
    public String toString() {
        StringBuffer strBuffer = new StringBuffer("partitionid=");
        strBuffer.append(partitionId);
        strBuffer.append(", version=");
        strBuffer.append(version);
        strBuffer.append(", versionHash=");
        strBuffer.append(versionHash);
        return strBuffer.toString();
    }
}
