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

import java.util.List;

public class IcebergPartition {
    private final String partitionName;
    private final List<String> partitionValues;
    private final int specId;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final long fileCount;
    private final long lastUpdateTime;
    private final long lastSnapshotId;
    private final List<String> transforms;

    public IcebergPartition(String partitionName, int specId, long recordCount, long fileSizeInBytes, long fileCount,
                            long lastUpdateTime, long lastSnapshotId, List<String> partitionValues,
                            List<String> transforms) {
        this.partitionName = partitionName;
        this.specId = specId;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastUpdateTime = lastUpdateTime;
        this.lastSnapshotId = lastSnapshotId;
        this.partitionValues = partitionValues;
        this.transforms = transforms;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public int getSpecId() {
        return specId;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }

    public long getFileCount() {
        return fileCount;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getLastSnapshotId() {
        return lastSnapshotId;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    public List<String> getTransforms() {
        return transforms;
    }
}
