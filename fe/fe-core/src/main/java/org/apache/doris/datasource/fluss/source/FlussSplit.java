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

package org.apache.doris.datasource.fluss.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.TableFormatType;

public class FlussSplit extends FileSplit {
    private final String databaseName;
    private final String tableName;
    private final long tableId;
    private final int bucketId;
    private final String partitionName;
    private final long snapshotId;
    private final String bootstrapServers;
    private final TableFormatType tableFormatType;

    public FlussSplit(String databaseName, String tableName, long tableId, int bucketId,
                      String partitionName, long snapshotId, String bootstrapServers,
                      String filePath, long fileSize) {
        super(LocationPath.of(filePath != null ? filePath : "/fluss/" + databaseName + "/" + tableName),
                0, fileSize, fileSize, 0, null, null);
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableId = tableId;
        this.bucketId = bucketId;
        this.partitionName = partitionName;
        this.snapshotId = snapshotId;
        this.bootstrapServers = bootstrapServers;
        this.tableFormatType = TableFormatType.FLUSS;
    }

    public FlussSplit(String databaseName, String tableName, long tableId) {
        this(databaseName, tableName, tableId, 0, null, -1, null, null, 0);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTableId() {
        return tableId;
    }

    public int getBucketId() {
        return bucketId;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public TableFormatType getTableFormatType() {
        return tableFormatType;
    }

    public boolean isPartitioned() {
        return partitionName != null && !partitionName.isEmpty();
    }

    @Override
    public String getConsistentHashString() {
        StringBuilder sb = new StringBuilder();
        sb.append(databaseName).append(".").append(tableName);
        if (partitionName != null) {
            sb.append(".").append(partitionName);
        }
        sb.append(".bucket").append(bucketId);
        return sb.toString();
    }

    @Override
    public String toString() {
        return "FlussSplit{"
                + "db='" + databaseName + '\''
                + ", table='" + tableName + '\''
                + ", tableId=" + tableId
                + ", bucketId=" + bucketId
                + ", partition='" + partitionName + '\''
                + ", snapshotId=" + snapshotId
                + '}';
    }
}
