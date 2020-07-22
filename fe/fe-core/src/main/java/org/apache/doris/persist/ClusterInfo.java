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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import com.google.common.collect.Lists;

public class ClusterInfo implements Writable {

    private String clusterName;
    private long clusterId;
    private int instanceNum;

    private String newClusterName;
    private long newClusterId;
    private int newInstanceNum;
    
    private List<Long> expandBackendIds = Lists.newArrayList();

    public ClusterInfo() {
        this.clusterName = "";
        this.clusterId = 0L;
        this.instanceNum = 0;
        this.newClusterName = "";
        this.newClusterId = 0L;
        this.newInstanceNum = 0;
    }
    
    public ClusterInfo(String clusterName, long clusterId) {
        this.clusterName = clusterName;
        this.clusterId = clusterId;
        this.instanceNum = 0;
        this.newClusterName = "";
        this.newClusterId = 0L;
        this.newInstanceNum = 0;
    }

    public ClusterInfo(String clusterName, long clusterId, List<Long> expandBackendIds) {
        this.clusterName = clusterName;
        this.clusterId = clusterId;
        this.expandBackendIds = expandBackendIds;
        this.instanceNum = 0;
        this.newClusterName = "";
        this.newClusterId = 0L;
        this.newInstanceNum = 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, clusterName);
        out.writeLong(clusterId);
        out.writeInt(instanceNum);
        out.writeInt(expandBackendIds.size());
        for (long id : expandBackendIds) {
            out.writeLong(id);
        }
    }

    public void readFields(DataInput in) throws IOException {
        clusterName = Text.readString(in);
        clusterId = in.readLong();
        instanceNum = in.readInt();
        int count = in.readInt();
        while (count-- > 0) {
            expandBackendIds.add(in.readLong());
        }
    }

    public String getClusterName() {
        return clusterName;
    }

    public long getClusterId() {
        return clusterId;
    }

    public int getInstanceNum() {
        return instanceNum;
    }

    public String getNewClusterName() {
        return newClusterName;
    }

    public int getNewInstanceNum() {
        return newInstanceNum;
    }

    public List<Long> getBackendIdList() {
        return expandBackendIds;
    }
}
