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

package org.apache.doris.clone;

public class BackendInfo {
    private long backendId;
    private String host;

    private long totalCapacityB;
    private long availableCapacityB;
    // capacity for clone
    private long cloneCapacityB;

    private int tableReplicaNum;
    // replica num for clone
    private int cloneReplicaNum;

    public BackendInfo(String host, long backendId, long totalCapacityB, long availableCapacityB) {
        this.backendId = backendId;
        this.totalCapacityB = totalCapacityB;
        this.availableCapacityB = availableCapacityB;
        this.host = host;
        this.cloneCapacityB = 0L;
        this.tableReplicaNum = 0;
        this.cloneReplicaNum = 0;
    }

    public String getHost() {
        return host;
    }

    public long getBackendId() {
        return backendId;
    }

    public long getTotalCapacityB() {
        return totalCapacityB;
    }

    public long getAvailableCapacityB() {
        return availableCapacityB;
    }

    public void setCloneCapacityB(long cloneCapacityB) {
        this.cloneCapacityB = cloneCapacityB;
    }

    public boolean canCloneByCapacity(long tabletSizeB) {
        if (cloneCapacityB <= tabletSizeB) {
            return false;
        }
        return true;
    }

    public void decreaseCloneCapacityB(long tabletSizeB) {
        cloneCapacityB -= tabletSizeB;
    }

    public int getTableReplicaNum() {
        return tableReplicaNum;
    }

    public void setTableReplicaNum(int tableReplicaNum) {
        this.tableReplicaNum = tableReplicaNum;
    }

    public void setCloneReplicaNum(int cloneReplicaNum) {
        this.cloneReplicaNum = cloneReplicaNum;
    }

    public boolean canCloneByDistribution() {
        if (cloneReplicaNum <= 1) {
            return false;
        }
        return true;
    }

    public void decreaseCloneReplicaNum() {
        cloneReplicaNum -= 1;
    }
}
