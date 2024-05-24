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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.util.StructProjection;

public class IcebergPartition {
    private StructProjection partitionData;
    private int specId;
    private long recordCount;
    private int fileCount;
    private long totalDataFileSizeInBytes;
    private long positionDeleteRecordCount;
    private int positionDeleteFileCount;
    private long equalityDeleteRecordCount;
    private int equalityDeleteFileCount;
    private long lastUpdatedAt;
    private long lastUpdatedSnapshotId;


    public StructProjection getPartitionData() {
        return partitionData;
    }

    public void setPartitionData(StructProjection partitionData) {
        this.partitionData = partitionData;
    }

    public int getSpecId() {
        return specId;
    }

    public void setSpecId(int specId) {
        this.specId = specId;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public int getFileCount() {
        return fileCount;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public long getTotalDataFileSizeInBytes() {
        return totalDataFileSizeInBytes;
    }

    public void setTotalDataFileSizeInBytes(long totalDataFileSizeInBytes) {
        this.totalDataFileSizeInBytes = totalDataFileSizeInBytes;
    }

    public long getPositionDeleteRecordCount() {
        return positionDeleteRecordCount;
    }

    public void setPositionDeleteRecordCount(long positionDeleteRecordCount) {
        this.positionDeleteRecordCount = positionDeleteRecordCount;
    }

    public int getPositionDeleteFileCount() {
        return positionDeleteFileCount;
    }

    public void setPositionDeleteFileCount(int positionDeleteFileCount) {
        this.positionDeleteFileCount = positionDeleteFileCount;
    }

    public long getEqualityDeleteRecordCount() {
        return equalityDeleteRecordCount;
    }

    public void setEqualityDeleteRecordCount(long equalityDeleteRecordCount) {
        this.equalityDeleteRecordCount = equalityDeleteRecordCount;
    }

    public int getEqualityDeleteFileCount() {
        return equalityDeleteFileCount;
    }

    public void setEqualityDeleteFileCount(int equalityDeleteFileCount) {
        this.equalityDeleteFileCount = equalityDeleteFileCount;
    }

    public long getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    public void setLastUpdatedAt(long lastUpdatedAt) {
        this.lastUpdatedAt = lastUpdatedAt;
    }

    public long getLastUpdatedSnapshotId() {
        return lastUpdatedSnapshotId;
    }

    public void setLastUpdatedSnapshotId(long lastUpdatedSnapshotId) {
        this.lastUpdatedSnapshotId = lastUpdatedSnapshotId;
    }
}
