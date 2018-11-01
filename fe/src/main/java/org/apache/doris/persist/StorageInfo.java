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

/**
 * This class is designed for sending storage information from master to standby master.
 * StorageInfo is easier to serialize to a Json String than class Storage
 */
public class StorageInfo {
    private int clusterID;
    private long imageSeq;
    private long editsSeq;

    public StorageInfo() {
        this(-1, 0, 0);
    }

    public StorageInfo(int clusterID, long imageSeq, long editsSeq) {
        this.clusterID = clusterID;
        this.editsSeq = editsSeq;
        this.imageSeq = imageSeq;
    }

    public int getClusterID() {
        return clusterID;
    }

    public void setClusterID(int clusterID) {
        this.clusterID = clusterID;
    }
    
    public long getEditsSeq() {
        return editsSeq;
    }

    public void setEditsSeq(long editsSeq) {
        this.editsSeq = editsSeq;
    }

    public long getImageSeq() {
        return imageSeq;
    }

    public void setImageSeq(long imageSeq) {
        this.imageSeq = imageSeq;
    }
}
