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

package org.apache.doris.datasource.deltalake.source;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.deltalake.AbstractDeltaLakeMetadataOps;
import org.apache.doris.datasource.deltalake.DeletionVectorDescriptorInfo;
import org.apache.doris.datasource.deltalake.DeltaLakeExternalTable;
import org.apache.doris.datasource.deltalake.DeltaLakePredicateConverter;
import org.apache.doris.datasource.deltalake.DeltaLakeUnityExternalCatalog;
import org.apache.doris.datasource.deltalake.DeltaLakeUnityMetadataOps;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TDeltaLakeDeletionVectorDesc;
import org.apache.doris.thrift.TDeltaLakeFileDesc;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Scan node for Delta Lake tables.
 * Extends FileQueryScanNode (like IcebergScanNode) to leverage the existing
 * Parquet reader infrastructure on the BE side.
 *
 * <p>Works with both HMS-backed and Unity Catalog-backed Delta Lake catalogs.
 * When using Unity Catalog with Credential Vending, temporary cloud storage
 * credentials are automatically injected into the location properties.
 */
public class DeltaLakeScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeScanNode.class);

    private DeltaLakeExternalTable deltaLakeTable;
    private ExternalCatalog deltaLakeCatalog;

    public DeltaLakeScanNode(PlanNodeId id,
            org.apache.doris.analysis.TupleDescriptor desc,
            boolean needCheckColumnPriv,
            SessionVariable sv,
            ScanContext scanContext) {
        super(id, desc, "DELTALAKE_SCAN_NODE", scanContext, needCheckColumnPriv, sv);
    }

    @Override
    protected void doInitialize() throws UserException {
        deltaLakeTable = (DeltaLakeExternalTable) desc.getTable();
        deltaLakeCatalog = (ExternalCatalog) deltaLakeTable.getCatalog();
        super.doInitialize();
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        List<Split> splits = new ArrayList<>();

        try {
            AbstractDeltaLakeMetadataOps metadataOps =
                    (AbstractDeltaLakeMetadataOps) deltaLakeCatalog.getMetadataOps();
            Engine engine = metadataOps.getEngine();
            String tableLocation = deltaLakeTable.getTableLocation();

            Table deltaTable = Table.forPath(engine, tableLocation);
            Snapshot snapshot = deltaTable.getLatestSnapshot(engine);

            // Build scan with optional predicate pushdown
            ScanBuilder scanBuilder = snapshot.getScanBuilder();

            // Convert Doris conjuncts to Delta Kernel Predicate for data skipping
            Optional<Predicate> deltaPredicate = DeltaLakePredicateConverter.convertToKernelPredicate(conjuncts);
            if (deltaPredicate.isPresent()) {
                scanBuilder = scanBuilder.withFilter(deltaPredicate.get());
            }

            Scan scan = scanBuilder.build();
            CloseableIterator<FilteredColumnarBatch> scanFilesIter = scan.getScanFiles(engine);

            while (scanFilesIter.hasNext()) {
                FilteredColumnarBatch batch = scanFilesIter.next();
                CloseableIterator<Row> rowIter = batch.getRows();
                while (rowIter.hasNext()) {
                    Row scanFileRow = rowIter.next();
                    DeltaLakeSplit split = buildSplit(scanFileRow, tableLocation, engine);
                    if (split != null) {
                        splits.add(split);
                    }
                }
                rowIter.close();
            }
            scanFilesIter.close();
        } catch (Exception e) {
            throw new UserException("Failed to get splits for Delta Lake table: "
                    + deltaLakeTable.getName(), e);
        }

        return splits;
    }

    private DeltaLakeSplit buildSplit(Row scanFileRow, String tableLocation, Engine engine) {
        try {
            // Extract file path (relative to table location)
            String filePath = InternalScanFileUtils.getAddFileStatus(scanFileRow).getPath();
            // Delta Lake stores relative paths, make absolute
            if (!filePath.startsWith("/") && !filePath.startsWith("s3://")
                    && !filePath.startsWith("hdfs://") && !filePath.startsWith("gs://")) {
                filePath = tableLocation + "/" + filePath;
            }

            long fileSize = InternalScanFileUtils.getAddFileStatus(scanFileRow).getSize();

            // Extract partition values
            Map<String, String> partitionValues = new HashMap<>();
            // Partition values are extracted from the scan file row by Delta Kernel
            // They will be set via columnsFromPath in setScanParams

            // Extract Deletion Vector descriptor if present
            DeletionVectorDescriptorInfo dvInfo = DeletionVectorDescriptorInfo.fromScanFileRow(
                    scanFileRow, tableLocation);

            return new DeltaLakeSplit(
                    LocationPath.of(filePath),
                    0, fileSize, fileSize,
                    0, new String[0], partitionValues, dvInfo);
        } catch (Exception e) {
            LOG.warn("Failed to build split from scan file row", e);
            return null;
        }
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof DeltaLakeSplit) {
            DeltaLakeSplit deltaSplit = (DeltaLakeSplit) split;
            TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
            tableFormatFileDesc.setTableFormatType("deltalake");

            // Set Delta Lake specific params including Deletion Vector info
            TDeltaLakeFileDesc deltaFileDesc = new TDeltaLakeFileDesc();
            if (deltaSplit.hasDeletionVector()) {
                DeletionVectorDescriptorInfo dvInfo = deltaSplit.getDvInfo();
                if (!dvInfo.isInline() && dvInfo.getResolvedPath() != null) {
                    TDeltaLakeDeletionVectorDesc dvDesc = new TDeltaLakeDeletionVectorDesc();
                    dvDesc.setFilePath(dvInfo.getResolvedPath());
                    dvDesc.setOffset(dvInfo.getResolvedOffset());
                    dvDesc.setSizeInBytes(dvInfo.getSizeInBytes());
                    dvDesc.setCardinality(dvInfo.getCardinality());
                    deltaFileDesc.setDeletionVectorDesc(dvDesc);
                }
            }
            tableFormatFileDesc.setDeltaLakeParams(deltaFileDesc);
            rangeDesc.setTableFormatParams(tableFormatFileDesc);

            // Set partition values via columnsFromPath
            Map<String, String> partitionValues = deltaSplit.getDeltaPartitionValues();
            if (partitionValues != null && !partitionValues.isEmpty()) {
                List<String> fromPathKeys = new ArrayList<>(partitionValues.keySet());
                List<String> fromPathValues = new ArrayList<>();
                for (String key : fromPathKeys) {
                    String value = partitionValues.get(key);
                    fromPathValues.add(value != null ? value : "");
                }
                rangeDesc.setColumnsFromPathKeys(fromPathKeys);
                rangeDesc.setColumnsFromPath(fromPathValues);
            }
        }
    }

    @Override
    protected TFileFormatType getFileFormatType() {
        // Delta Lake data files are always Parquet
        return TFileFormatType.FORMAT_PARQUET;
    }

    @Override
    protected List<String> getPathPartitionKeys() {
        return deltaLakeTable.getPartitionColumnNames();
    }

    @Override
    protected TableIf getTargetTable() {
        return deltaLakeTable;
    }

    @Override
    protected Map<String, String> getLocationProperties() {
        Map<String, String> props = new HashMap<>(
                deltaLakeCatalog.getCatalogProperty().getHadoopProperties());

        // If using Unity Catalog with Credential Vending, inject temporary credentials
        if (deltaLakeCatalog instanceof DeltaLakeUnityExternalCatalog) {
            DeltaLakeUnityExternalCatalog unityCatalog =
                    (DeltaLakeUnityExternalCatalog) deltaLakeCatalog;
            if (unityCatalog.isCredentialVendingEnabled()) {
                DeltaLakeUnityMetadataOps unityOps =
                        (DeltaLakeUnityMetadataOps) unityCatalog.getMetadataOps();
                Map<String, String> tempCredentials = unityOps.getStorageCredentials(
                        deltaLakeTable.getDbName(), deltaLakeTable.getRemoteName());
                props.putAll(tempCredentials);
            }
        }

        return props;
    }
}
