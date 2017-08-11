// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.persist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.google.common.collect.Lists;

public class ClusterInfo implements Writable {

    private String clusterName;
    private long clusterId;
    private int instanceNum;

    private String newClusterName;
    private long newClusterId;
    private int newInstanceNum;
    
    private List<Long> backendIdList = Lists.newArrayList();

    public ClusterInfo() {
        this.clusterName = "";
        this.clusterId = 0L;
        this.instanceNum = 0;
        this.newClusterName = "";
        this.newClusterId = 0L;
        this.newInstanceNum = 0;
    }
    
    public ClusterInfo(String cluster, long id) {
        this.clusterName = cluster;
        this.clusterId = id;
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
        out.writeInt(backendIdList.size());
        for (long id : backendIdList) {
            out.writeLong(id);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clusterName = Text.readString(in);
        clusterId = in.readLong();
        instanceNum = in.readInt();
        int count = in.readInt();
        while (count-- > 0) {
            backendIdList.add(in.readLong());
        }
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public int getInstanceNum() {
        return instanceNum;
    }

    public void setInstanceNum(int instanceNum) {
        this.instanceNum = instanceNum;
    }

    public String getNewClusterName() {
        return newClusterName;
    }

    public void setNewClusterName(String newClusterName) {
        this.newClusterName = newClusterName;
    }

    public long getNewClusterId() {
        return newClusterId;
    }

    public void setNewClusterId(long newClusterId) {
        this.newClusterId = newClusterId;
    }

    public int getNewInstanceNum() {
        return newInstanceNum;
    }

    public void setNewInstanceNum(int newInstanceNum) {
        this.newInstanceNum = newInstanceNum;
    }

    public List<Long> getBackendIdList() {
        return backendIdList;
    }

    public void setBackendIdList(List<Long> backendIdList) {
        if (backendIdList == null) {
            return;
        }
        this.backendIdList = backendIdList;
    }

}
