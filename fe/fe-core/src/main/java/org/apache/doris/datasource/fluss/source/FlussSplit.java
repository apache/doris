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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlussSplit extends FileSplit {

    public enum SplitTier {
        LAKE_ONLY,      // Data only in lake (Parquet/ORC files) - can be read directly
        LOG_ONLY,       // Data only in log (Fluss native format) - requires Fluss SDK
        HYBRID          // Data in both tiers - read lake first, then log
    }

    private final String databaseName;
    private final String tableName;
    private final long tableId;
    private final int bucketId;
    private final String partitionName;
    private final long snapshotId;
    private final String bootstrapServers;
    private final TableFormatType tableFormatType;

    private final SplitTier tier;
    private final List<String> lakeFilePaths;
    private final String lakeFormat;
    private final long lakeSnapshotId;
    private final long logStartOffset;
    private final long logEndOffset;

    public FlussSplit(String databaseName, String tableName, long tableId, int bucketId,
                      String partitionName, long snapshotId, String bootstrapServers,
                      String filePath, long fileSize) {
        this(databaseName, tableName, tableId, bucketId, partitionName, snapshotId,
                bootstrapServers, filePath, fileSize, SplitTier.LAKE_ONLY,
                Collections.emptyList(), "parquet", -1, -1, -1);
    }

    public FlussSplit(String databaseName, String tableName, long tableId, int bucketId,
                      String partitionName, long snapshotId, String bootstrapServers,
                      String filePath, long fileSize, SplitTier tier,
                      List<String> lakeFilePaths, String lakeFormat, long lakeSnapshotId,
                      long logStartOffset, long logEndOffset) {
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
        this.tier = tier;
        this.lakeFilePaths = lakeFilePaths != null ? new ArrayList<>(lakeFilePaths) : new ArrayList<>();
        this.lakeFormat = lakeFormat != null ? lakeFormat : "parquet";
        this.lakeSnapshotId = lakeSnapshotId;
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
    }

    public FlussSplit(String databaseName, String tableName, long tableId) {
        this(databaseName, tableName, tableId, 0, null, -1, null, null, 0);
    }

    public static FlussSplit createLakeSplit(String databaseName, String tableName, long tableId,
                                              int bucketId, String partitionName, String bootstrapServers,
                                              List<String> lakeFilePaths, String lakeFormat, long lakeSnapshotId) {
        String primaryPath = lakeFilePaths != null && !lakeFilePaths.isEmpty() ? lakeFilePaths.get(0) : null;
        return new FlussSplit(databaseName, tableName, tableId, bucketId, partitionName,
                lakeSnapshotId, bootstrapServers, primaryPath, 0, SplitTier.LAKE_ONLY,
                lakeFilePaths, lakeFormat, lakeSnapshotId, -1, -1);
    }

    public static FlussSplit createLogSplit(String databaseName, String tableName, long tableId,
                                             int bucketId, String partitionName, String bootstrapServers,
                                             long logStartOffset, long logEndOffset) {
        return new FlussSplit(databaseName, tableName, tableId, bucketId, partitionName,
                -1, bootstrapServers, null, 0, SplitTier.LOG_ONLY,
                Collections.emptyList(), null, -1, logStartOffset, logEndOffset);
    }

    public static FlussSplit createHybridSplit(String databaseName, String tableName, long tableId,
                                                int bucketId, String partitionName, String bootstrapServers,
                                                List<String> lakeFilePaths, String lakeFormat, long lakeSnapshotId,
                                                long logStartOffset, long logEndOffset) {
        String primaryPath = lakeFilePaths != null && !lakeFilePaths.isEmpty() ? lakeFilePaths.get(0) : null;
        return new FlussSplit(databaseName, tableName, tableId, bucketId, partitionName,
                lakeSnapshotId, bootstrapServers, primaryPath, 0, SplitTier.HYBRID,
                lakeFilePaths, lakeFormat, lakeSnapshotId, logStartOffset, logEndOffset);
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

    public SplitTier getTier() {
        return tier;
    }

    public List<String> getLakeFilePaths() {
        return Collections.unmodifiableList(lakeFilePaths);
    }

    public String getLakeFormat() {
        return lakeFormat;
    }

    public long getLakeSnapshotId() {
        return lakeSnapshotId;
    }

    public long getLogStartOffset() {
        return logStartOffset;
    }

    public long getLogEndOffset() {
        return logEndOffset;
    }

    public boolean isLakeSplit() {
        return tier == SplitTier.LAKE_ONLY || tier == SplitTier.HYBRID;
    }

    public boolean isLogSplit() {
        return tier == SplitTier.LOG_ONLY || tier == SplitTier.HYBRID;
    }

    public boolean isHybridSplit() {
        return tier == SplitTier.HYBRID;
    }

    public boolean hasLakeData() {
        return lakeFilePaths != null && !lakeFilePaths.isEmpty();
    }

    public boolean hasLogData() {
        return logStartOffset >= 0 && (logEndOffset < 0 || logEndOffset > logStartOffset);
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
                + ", tier=" + tier
                + ", lakeFiles=" + lakeFilePaths.size()
                + ", lakeSnapshotId=" + lakeSnapshotId
                + ", logOffsets=[" + logStartOffset + "," + logEndOffset + "]"
                + '}';
    }
}
