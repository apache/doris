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
import org.apache.doris.datasource.Split;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.fluss.FlussExternalTable;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFlussFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import org.apache.fluss.client.scanner.ScanRecord;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanBucket;
import org.apache.fluss.client.table.snapshot.BucketSnapshot;
import org.apache.fluss.client.table.snapshot.TableSnapshot;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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
        flussFileDesc.setDatabase_name(flussSplit.getDatabaseName());
        flussFileDesc.setTable_name(flussSplit.getTableName());
        flussFileDesc.setTable_id(flussSplit.getTableId());
        flussFileDesc.setBucket_id(flussSplit.getBucketId());
        if (flussSplit.getPartitionName() != null) {
            flussFileDesc.setPartition_name(flussSplit.getPartitionName());
        }
        flussFileDesc.setSnapshot_id(flussSplit.getSnapshotId());
        if (flussSplit.getBootstrapServers() != null) {
            flussFileDesc.setBootstrap_servers(flussSplit.getBootstrapServers());
        }

        String fileFormat = "parquet";
        flussFileDesc.setFile_format(fileFormat);

        if (fileFormat.equals("orc")) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
        } else {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        }

        tableFormatFileDesc.setFluss_params(flussFileDesc);
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

            long snapshotId = getLatestSnapshotId(table);

            if (partitionKeys == null || partitionKeys.isEmpty()) {
                for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                    FlussSplit split = new FlussSplit(
                            flussTable.getRemoteDbName(),
                            flussTable.getRemoteName(),
                            tableId,
                            bucketId,
                            null,
                            snapshotId,
                            bootstrapServers,
                            buildFilePath(flussTable, null, bucketId),
                            0
                    );
                    splits.add(split);
                }
            } else {
                List<String> partitions = getPartitions(table);
                for (String partition : partitions) {
                    for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                        FlussSplit split = new FlussSplit(
                                flussTable.getRemoteDbName(),
                                flussTable.getRemoteName(),
                                tableId,
                                bucketId,
                                partition,
                                snapshotId,
                                bootstrapServers,
                                buildFilePath(flussTable, partition, bucketId),
                                0
                        );
                        splits.add(split);
                    }
                }
            }

            if (splits.isEmpty()) {
                FlussSplit fallbackSplit = new FlussSplit(
                        flussTable.getRemoteDbName(),
                        flussTable.getRemoteName(),
                        tableId,
                        0,
                        null,
                        snapshotId,
                        bootstrapServers,
                        buildFilePath(flussTable, null, 0),
                        0
                );
                splits.add(fallbackSplit);
            }

            long targetSplitSize = getRealFileSplitSize(0);
            splits.forEach(s -> s.setTargetSplitSize(targetSplitSize));

            LOG.info("Created {} Fluss splits for table {}.{}", splits.size(),
                    flussTable.getRemoteDbName(), flussTable.getRemoteName());

        } catch (Exception e) {
            LOG.error("Failed to get Fluss splits", e);
            throw new UserException("Failed to get Fluss splits: " + e.getMessage(), e);
        }

        return splits;
    }

    private long getLatestSnapshotId(Table table) {
        try {
            TableSnapshot snapshot = table.getLatestSnapshot();
            if (snapshot != null) {
                return snapshot.getSnapshotId();
            }
        } catch (Exception e) {
            LOG.warn("Failed to get latest snapshot, using -1", e);
        }
        return -1L;
    }

    private List<String> getPartitions(Table table) {
        List<String> partitions = new ArrayList<>();
        try {
            List<String> partitionNames = table.listPartitions();
            if (partitionNames != null) {
                partitions.addAll(partitionNames);
            }
        } catch (Exception e) {
            LOG.warn("Failed to list partitions, returning empty list", e);
        }
        return partitions;
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
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
    }
}
