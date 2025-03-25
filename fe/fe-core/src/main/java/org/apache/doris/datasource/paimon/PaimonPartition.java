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

package org.apache.doris.datasource.paimon;

// https://paimon.apache.org/docs/0.9/maintenance/system-tables/#partitions-table
public class PaimonPartition {
    // Partition values, for example: [1, dd]
    private final String partitionValues;
    // The amount of data in the partition
    private final long recordCount;
    // Partition file size
    private final long fileSizeInBytes;
    // Number of partition files
    private final long fileCount;
    // Last update time of partition
    private final long lastUpdateTime;

    public PaimonPartition(String partitionValues, long recordCount, long fileSizeInBytes, long fileCount,
            long lastUpdateTime) {
        this.partitionValues = partitionValues;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getPartitionValues() {
        return partitionValues;
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
}
