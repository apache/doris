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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TabletLoadInfo implements Writable {
    private String filePath;
    private long fileSize;
    private Set<Long> sentReplicas;
    
    public TabletLoadInfo() {
        this("", -1);
    }

    public TabletLoadInfo(String filePath, long fileSize) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.sentReplicas = new HashSet<Long>();
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public boolean addSentReplica(long replicaId) {
        sentReplicas.add(replicaId);
        return true;
    }
    
    public boolean isReplicaSent(long replicaId) {
        return sentReplicas.contains(replicaId);
    }
    
    public void write(DataOutput out) throws IOException {
        if (filePath == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, filePath);
            out.writeLong(fileSize);
        }
    }

    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            filePath = Text.readString(in).intern();
            fileSize = in.readLong();
        } else {
            filePath = null;
            fileSize = -1;
        }
    }
    
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof TabletLoadInfo)) {
            return false;
        }
        
        TabletLoadInfo info = (TabletLoadInfo) obj;
        
        if (sentReplicas != info.sentReplicas) {
            if (sentReplicas == null || info.sentReplicas == null) {
                return false;
            }
            if (sentReplicas.size() != info.sentReplicas.size()) {
                return false;
            }
            for (long id : sentReplicas) {
                if (!info.sentReplicas.contains(id)) {
                    return false;
                }
            }
        }
        
        if (filePath != info.filePath) {
            if (filePath == null || info.filePath == null) {
                return false;
            }
        }
        
        return filePath.equals(info.filePath);
    }
    
    public int hashCode() {
        int ret = filePath.length();
        return ret;
    }
}
