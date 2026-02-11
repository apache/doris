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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.spi.Split;
import org.apache.doris.datasource.fluss.FlussExternalCatalog;
import org.apache.doris.datasource.fluss.FlussExternalTable;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFlussFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlussScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(FlussScanNode.class);

    private FlussSource source;

    public FlussScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
            SessionVariable sv) {
        super(id, desc, "FLUSS_SCAN_NODE", needCheckColumnPriv, sv);
        source = new FlussSource(desc);
    }

    @VisibleForTesting
    public FlussScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        super(id, desc, "FLUSS_SCAN_NODE", false, sv);
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        ExternalUtil.initSchemaInfo(params, -1L, source.getTargetTable().getColumns());
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof FlussSplit) {
            setFlussParams(rangeDesc, (FlussSplit) split);
        }
    }

    private void setFlussParams(TFileRangeDesc rangeDesc, FlussSplit flussSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(TableFormatType.FLUSS.value());

        TFlussFileDesc flussFileDesc = new TFlussFileDesc();
        flussFileDesc.setDatabaseName(flussSplit.getDatabaseName());
        flussFileDesc.setTableName(flussSplit.getTableName());
        flussFileDesc.setTableId(flussSplit.getTableId());
        flussFileDesc.setBucketId(flussSplit.getBucketId());
        if (flussSplit.getPartitionName() != null) {
            flussFileDesc.setPartitionName(flussSplit.getPartitionName());
        }
        flussFileDesc.setSnapshotId(flussSplit.getSnapshotId());
        if (flussSplit.getBootstrapServers() != null) {
            flussFileDesc.setBootstrapServers(flussSplit.getBootstrapServers());
        }

        String fileFormat = flussSplit.getLakeFormat() != null ? flussSplit.getLakeFormat() : "parquet";
        flussFileDesc.setFileFormat(fileFormat);

        flussFileDesc.setLakeSnapshotId(flussSplit.getLakeSnapshotId());
        if (flussSplit.hasLakeData()) {
            flussFileDesc.setLakeFilePaths(flussSplit.getLakeFilePaths());
        }
        flussFileDesc.setLogStartOffset(flussSplit.getLogStartOffset());
        flussFileDesc.setLogEndOffset(flussSplit.getLogEndOffset());

        if (fileFormat.equals("orc")) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
        } else {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        }

        tableFormatFileDesc.setFlussParams(flussFileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        List<Split> splits = new ArrayList<>();

        try {
            FlussExternalTable flussTable = source.getTargetTable();
            Table table = source.getFlussTable();
            TableInfo tableInfo = table.getTableInfo();
            long tableId = tableInfo.getTableId();
            int numBuckets = flussTable.getNumBuckets();
            List<String> partitionKeys = flussTable.getPartitionKeys();
            String bootstrapServers = flussTable.getBootstrapServers();

            LakeSnapshot lakeSnapshot = getLakeSnapshot(flussTable);
            Map<TableBucket, Long> bucketOffsets = lakeSnapshot != null 
                    ? lakeSnapshot.getTableBucketsOffset() 
                    : new HashMap<>();
            long lakeSnapshotId = lakeSnapshot != null ? lakeSnapshot.getSnapshotId() : -1;

            Map<TableBucket, List<String>> bucketLakeFiles = getLakeFilesPerBucket(flussTable, lakeSnapshotId);

            String lakeFormat = determineLakeFormat(tableInfo);

            if (partitionKeys == null || partitionKeys.isEmpty()) {
                splits.addAll(generateSplitsForPartition(
                        flussTable, tableId, numBuckets, null, null,
                        bootstrapServers, bucketOffsets, bucketLakeFiles, lakeFormat, lakeSnapshotId));
            } else {
                List<String> partitions = getPartitions(table);
                for (String partition : partitions) {
                    Long partitionId = getPartitionId(table, partition);
                    splits.addAll(generateSplitsForPartition(
                            flussTable, tableId, numBuckets, partition, partitionId,
                            bootstrapServers, bucketOffsets, bucketLakeFiles, lakeFormat, lakeSnapshotId));
                }
            }

            if (splits.isEmpty()) {
                FlussSplit fallbackSplit = FlussSplit.createLakeSplit(
                        flussTable.getRemoteDbName(),
                        flussTable.getRemoteName(),
                        tableId, 0, null, bootstrapServers,
                        Collections.singletonList(buildFilePath(flussTable, null, 0)),
                        lakeFormat, lakeSnapshotId);
                splits.add(fallbackSplit);
            }

            // Use session variable for split size
            long targetSplitSize = sessionVariable.getFileSplitSize();
            splits.forEach(s -> s.setTargetSplitSize(targetSplitSize));

            LOG.info("Created {} Fluss splits for table {}.{} (lake={}, log={}, hybrid={})",
                    splits.size(), flussTable.getRemoteDbName(), flussTable.getRemoteName(),
                    countSplitsByTier(splits, FlussSplit.SplitTier.LAKE_ONLY),
                    countSplitsByTier(splits, FlussSplit.SplitTier.LOG_ONLY),
                    countSplitsByTier(splits, FlussSplit.SplitTier.HYBRID));

        } catch (Exception e) {
            LOG.error("Failed to get Fluss splits", e);
            throw new UserException("Failed to get Fluss splits: " + e.getMessage(), e);
        }

        return splits;
    }

    private List<FlussSplit> generateSplitsForPartition(
            FlussExternalTable flussTable, long tableId, int numBuckets,
            String partitionName, Long partitionId, String bootstrapServers,
            Map<TableBucket, Long> bucketOffsets, Map<TableBucket, List<String>> bucketLakeFiles,
            String lakeFormat, long lakeSnapshotId) {

        List<FlussSplit> splits = new ArrayList<>();
        String dbName = flussTable.getRemoteDbName();
        String tableName = flussTable.getRemoteName();

        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

            Long lakeOffset = bucketOffsets.get(tableBucket);
            List<String> lakeFiles = bucketLakeFiles.getOrDefault(tableBucket, Collections.emptyList());
            boolean hasLakeData = lakeFiles != null && !lakeFiles.isEmpty();

            FlussSplit split;
            if (hasLakeData) {
                split = FlussSplit.createLakeSplit(
                        dbName, tableName, tableId, bucketId, partitionName,
                        bootstrapServers, lakeFiles, lakeFormat, lakeSnapshotId);
            } else {
                split = new FlussSplit(dbName, tableName, tableId, bucketId,
                        partitionName, lakeSnapshotId, bootstrapServers,
                        buildFilePath(flussTable, partitionName, bucketId), 0);
            }
            splits.add(split);
        }
        return splits;
    }

    private LakeSnapshot getLakeSnapshot(FlussExternalTable flussTable) {
        try {
            FlussExternalCatalog catalog = (FlussExternalCatalog) flussTable.getCatalog();
            Admin admin = catalog.getFlussAdmin();
            TablePath tablePath = TablePath.of(flussTable.getRemoteDbName(), flussTable.getRemoteName());
            return admin.getLatestLakeSnapshot(tablePath).get();
        } catch (Exception e) {
            LOG.warn("Failed to get lake snapshot for {}.{}, will use log-only splits",
                    flussTable.getRemoteDbName(), flussTable.getRemoteName(), e);
            return null;
        }
    }

    private Map<TableBucket, List<String>> getLakeFilesPerBucket(FlussExternalTable flussTable, long lakeSnapshotId) {
        Map<TableBucket, List<String>> result = new HashMap<>();
        if (lakeSnapshotId < 0) {
            return result;
        }

        try {
            String dbName = flussTable.getRemoteDbName();
            String tableName = flussTable.getRemoteName();
            int numBuckets = flussTable.getNumBuckets();
            long tableId = 0;

            for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                TableBucket bucket = new TableBucket(tableId, null, bucketId);
                List<String> files = new ArrayList<>();
                files.add(buildLakeFilePath(flussTable, null, bucketId, lakeSnapshotId));
                result.put(bucket, files);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get lake files for table, will discover at read time", e);
        }
        return result;
    }

    private String buildLakeFilePath(FlussExternalTable table, String partition, int bucketId, long snapshotId) {
        StringBuilder path = new StringBuilder();
        // Use S3 path for lake storage (MinIO or other S3-compatible storage)
        path.append("s3://fluss-lake/").append(table.getRemoteDbName())
                .append("/").append(table.getRemoteName());
        if (partition != null) {
            path.append("/").append(partition);
        }
        path.append("/bucket-").append(bucketId)
                .append("/snapshot-").append(snapshotId)
                .append("/data.parquet");
        return path.toString();
    }

    private String determineLakeFormat(TableInfo tableInfo) {
        try {
            Map<String, String> options = tableInfo.getProperties().toMap();
            String format = options.getOrDefault("lake.format", "parquet");
            return format.toLowerCase();
        } catch (Exception e) {
            return "parquet";
        }
    }

    private Long getPartitionId(Table table, String partitionName) {
        try {
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private long countSplitsByTier(List<Split> splits, FlussSplit.SplitTier tier) {
        return splits.stream()
                .filter(s -> s instanceof FlussSplit && ((FlussSplit) s).getTier() == tier)
                .count();
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        // Return Fluss connection properties
        Map<String, String> properties = new HashMap<>();
        FlussExternalTable table = (FlussExternalTable) desc.getTable();
        FlussExternalCatalog catalog = (FlussExternalCatalog) table.getCatalog();
        properties.put("bootstrap.servers", catalog.getBootstrapServers());
        return properties;
    }

    @Override
    protected org.apache.doris.datasource.ExternalTable getTargetTable() {
        return (FlussExternalTable) desc.getTable();
    }

    @Override
    protected List<String> getPathPartitionKeys() {
        // Fluss handles partitioning internally
        // Return empty list as path-based partitioning is not used
        return new ArrayList<>();
    }

    private long getLatestSnapshotId(Table table) {
        // TODO: Fluss API changed - getLatestSnapshot() no longer available
        // This method is used for tiered storage support
        // For now, returning -1 to indicate no snapshot available
        return -1L;
    }

    private List<String> getPartitions(Table table) {
        // TODO: Fluss API changed - listPartitions() no longer available on Table
        // Partition information should be obtained from TableInfo metadata
        // For now, returning empty list
        return new ArrayList<>();
    }

    private String buildFilePath(FlussExternalTable table, String partition, int bucketId) {
        StringBuilder path = new StringBuilder();
        path.append("/fluss/").append(table.getRemoteDbName()).append("/").append(table.getRemoteName());
        if (partition != null) {
            path.append("/").append(partition);
        }
        path.append("/bucket-").append(bucketId);
        return path.toString();
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        // Fluss uses Parquet as default format, but can also support ORC
        // The actual format is determined per-split in setFlussParams
        return TFileFormatType.FORMAT_PARQUET;
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
    }
}
