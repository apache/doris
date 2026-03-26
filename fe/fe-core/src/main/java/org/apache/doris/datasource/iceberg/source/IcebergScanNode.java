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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.CollectingSplitSink;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.PlanningSplitMetadata;
import org.apache.doris.datasource.PlanningSplitProducer;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

public class IcebergScanNode extends FileQueryScanNode {

    public static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergSource source;
    private Table icebergTable;
    private int formatVersion;
    private ExecutionAuthenticator preExecutionAuthenticator;
    // Store PropertiesMap, including vended credentials or static credentials
    // get them in doInitialize() to ensure internal consistency of ScanNode
    private Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;
    private Map<String, String> backendStorageProperties;
    private PlanningSplitProducer planningSplitProducer;

    // for test
    @VisibleForTesting
    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv, ScanContext scanContext) {
        super(id, desc, "ICEBERG_SCAN_NODE", scanContext, false, sv);
    }

    /**
     * External file scan node for Query iceberg table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext) {
        super(id, desc, "ICEBERG_SCAN_NODE", scanContext, needCheckColumnPriv, sv);

        ExternalTable table = (ExternalTable) desc.getTable();
        if (table instanceof HMSExternalTable) {
            source = new IcebergHMSSource((HMSExternalTable) table, desc);
        } else if (table instanceof IcebergExternalTable) {
            String catalogType = ((IcebergExternalTable) table).getIcebergCatalogType();
            switch (catalogType) {
                case IcebergExternalCatalog.ICEBERG_HMS:
                case IcebergExternalCatalog.ICEBERG_REST:
                case IcebergExternalCatalog.ICEBERG_DLF:
                case IcebergExternalCatalog.ICEBERG_GLUE:
                case IcebergExternalCatalog.ICEBERG_HADOOP:
                case IcebergExternalCatalog.ICEBERG_JDBC:
                case IcebergExternalCatalog.ICEBERG_S3_TABLES:
                    source = new IcebergApiSource((IcebergExternalTable) table, desc, columnNameToRange);
                    break;
                default:
                    Preconditions.checkState(false, "Unknown iceberg catalog type: " + catalogType);
                    break;
            }
        }
        Preconditions.checkNotNull(source);
    }

    @Override
    protected void doInitialize() throws UserException {
        icebergTable = source.getIcebergTable();
        // Metadata tables (system tables) are not BaseTable instances, so we need to handle this case
        if (icebergTable instanceof BaseTable) {
            formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
        } else {
            // For metadata tables (e.g., snapshots, history), use a default format version
            // These tables are always readable regardless of format version
            formatVersion = MIN_DELETE_FILE_SUPPORT_VERSION;
        }
        preExecutionAuthenticator = source.getCatalog().getExecutionAuthenticator();
        storagePropertiesMap = VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
                source.getCatalog().getCatalogProperty().getMetastoreProperties(),
                source.getCatalog().getCatalogProperty().getStoragePropertiesMap(),
                icebergTable
        );
        backendStorageProperties = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);
        super.doInitialize();
    }

    /**
     * Extract name mapping from Iceberg table properties.
     * Returns a map from field ID to list of mapped names.
     */
    private Map<Integer, List<String>> extractNameMapping() {
        Map<Integer, List<String>> result = new HashMap<>();
        try {
            String nameMappingJson = icebergTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
            if (nameMappingJson != null && !nameMappingJson.isEmpty()) {
                NameMapping mapping = NameMappingParser.fromJson(nameMappingJson);
                if (mapping != null) {
                    // Extract mappings from NameMapping
                    // NameMapping contains field mappings, we need to convert them to our format
                    extractMappingsFromNameMapping(mapping.asMappedFields(), result);
                }
            }
        } catch (Exception e) {
            // If name mapping parsing fails, continue without it
            LOG.warn("Failed to parse name mapping from Iceberg table properties", e);
        }
        return result;
    }

    private void extractMappingsFromNameMapping(MappedFields mappingFields, Map<Integer, List<String>> result) {
        if (mappingFields == null) {
            return;
        }
        for (MappedField mappedField : mappingFields.fields()) {
            result.put(mappedField.id(), new ArrayList<>(mappedField.names()));
            extractMappingsFromNameMapping(mappedField.nestedMapping(), result);
        }

    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof IcebergSplit) {
            setIcebergParams(rangeDesc, (IcebergSplit) split);
        }
    }

    private void setIcebergParams(TFileRangeDesc rangeDesc, IcebergSplit icebergSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(icebergSplit.getTableFormatType().value());
        if (icebergSplit.getTableLevelRowCount() >= 0) {
            tableFormatFileDesc.setTableLevelRowCount(icebergSplit.getTableLevelRowCount());
        } else {
            // MUST explicitly set to -1, to be distinct from valid row count >= 0
            tableFormatFileDesc.setTableLevelRowCount(-1);
        }
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        fileDesc.setFormatVersion(formatVersion);
        fileDesc.setOriginalFilePath(icebergSplit.getOriginalPath());
        if (icebergSplit.getPartitionSpecId() != null) {
            fileDesc.setPartitionSpecId(icebergSplit.getPartitionSpecId());
        }
        if (icebergSplit.getPartitionDataJson() != null) {
            fileDesc.setPartitionDataJson(icebergSplit.getPartitionDataJson());
        }
        if (formatVersion < MIN_DELETE_FILE_SUPPORT_VERSION) {
            fileDesc.setContent(FileContent.DATA.id());
        } else {
            for (IcebergDeleteFileFilter filter : icebergSplit.getDeleteFileFilters()) {
                TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
                String deleteFilePath = filter.getDeleteFilePath();
                LocationPath locationPath = LocationPath.of(deleteFilePath, icebergSplit.getConfig());
                deleteFileDesc.setPath(locationPath.toStorageLocation().toString());
                if (filter instanceof IcebergDeleteFileFilter.PositionDelete) {
                    IcebergDeleteFileFilter.PositionDelete positionDelete =
                            (IcebergDeleteFileFilter.PositionDelete) filter;
                    OptionalLong lowerBound = positionDelete.getPositionLowerBound();
                    OptionalLong upperBound = positionDelete.getPositionUpperBound();
                    if (lowerBound.isPresent()) {
                        deleteFileDesc.setPositionLowerBound(lowerBound.getAsLong());
                    }
                    if (upperBound.isPresent()) {
                        deleteFileDesc.setPositionUpperBound(upperBound.getAsLong());
                    }
                    deleteFileDesc.setContent(IcebergDeleteFileFilter.PositionDelete.type());

                    if (filter instanceof IcebergDeleteFileFilter.DeletionVector) {
                        IcebergDeleteFileFilter.DeletionVector dv = (IcebergDeleteFileFilter.DeletionVector) filter;
                        deleteFileDesc.setContentOffset((int) dv.getContentOffset());
                        deleteFileDesc.setContentSizeInBytes((int) dv.getContentLength());
                        deleteFileDesc.setContent(IcebergDeleteFileFilter.DeletionVector.type());
                    }
                } else {
                    IcebergDeleteFileFilter.EqualityDelete equalityDelete =
                            (IcebergDeleteFileFilter.EqualityDelete) filter;
                    deleteFileDesc.setFieldIds(equalityDelete.getFieldIds());
                    deleteFileDesc.setContent(IcebergDeleteFileFilter.EqualityDelete.type());
                }
                fileDesc.addToDeleteFiles(deleteFileDesc);
            }
        }
        tableFormatFileDesc.setIcebergParams(fileDesc);
        Map<String, String> partitionValues = icebergSplit.getIcebergPartitionValues();
        if (partitionValues != null) {
            List<String> fromPathKeys = new ArrayList<>();
            List<String> fromPathValues = new ArrayList<>();
            List<Boolean> fromPathIsNull = new ArrayList<>();
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                fromPathKeys.add(entry.getKey());
                fromPathValues.add(entry.getValue() != null ? entry.getValue() : "");
                fromPathIsNull.add(entry.getValue() == null);
            }
            rangeDesc.setColumnsFromPathKeys(fromPathKeys);
            rangeDesc.setColumnsFromPath(fromPathValues);
            rangeDesc.setColumnsFromPathIsNull(fromPathIsNull);
        }
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    protected List<String> getDeleteFiles(TFileRangeDesc rangeDesc) {
        List<String> deleteFiles = new ArrayList<>();
        if (rangeDesc == null || !rangeDesc.isSetTableFormatParams()) {
            return deleteFiles;
        }
        TTableFormatFileDesc tableFormatParams = rangeDesc.getTableFormatParams();
        if (tableFormatParams == null || !tableFormatParams.isSetIcebergParams()) {
            return deleteFiles;
        }
        TIcebergFileDesc icebergParams = tableFormatParams.getIcebergParams();
        if (icebergParams == null || !icebergParams.isSetDeleteFiles()) {
            return deleteFiles;
        }
        List<TIcebergDeleteFileDesc> icebergDeleteFiles = icebergParams.getDeleteFiles();
        if (icebergDeleteFiles == null) {
            return deleteFiles;
        }
        for (TIcebergDeleteFileDesc deleteFile : icebergDeleteFiles) {
            if (deleteFile != null && deleteFile.isSetPath()) {
                deleteFiles.add(deleteFile.getPath());
            }
        }
        return deleteFiles;
    }

    private String getDeleteFileContentType(int content) {
        // Iceberg file type: 0: data, 1: position delete, 2: equality delete, 3: deletion vector
        switch (content) {
            case 1:
                return "position_delete";
            case 2:
                return "equality_delete";
            case 3:
                return "deletion_vector";
            default:
                return "unknown";
        }
    }

    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
        // Extract name mapping from Iceberg table properties
        Map<Integer, List<String>> nameMapping = extractNameMapping();

        boolean haveTopnLazyMatCol = false;
        for (SlotDescriptor slot : desc.getSlots()) {
            String colName = slot.getColumn().getName();
            if (colName.startsWith(Column.GLOBAL_ROWID_COL)) {
                haveTopnLazyMatCol = true;
                break;
            }
        }
        if (haveTopnLazyMatCol) {
            ExternalUtil.initSchemaInfoForAllColumn(params, -1L, source.getTargetTable().getColumns(), nameMapping);
        } else {
            // Use new initSchemaInfo method that only includes needed columns based on slots and pruned type
            ExternalUtil.initSchemaInfoForPrunedColumn(params, -1L, desc.getSlots(), nameMapping);
        }
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        PlanningSplitProducer producer = getPlanningSplitProducer();
        CollectingSplitSink splitSink = new CollectingSplitSink();
        producer.start(numBackends, splitSink);
        splitSink.awaitFinished();
        syncPlanningMetadataFromProducer();
        return splitSink.getSplits();
    }

    @Override
    public void startSplit(int numBackends) throws UserException {
        Preconditions.checkNotNull(splitAssignment);
        getPlanningSplitProducer().start(numBackends, splitAssignment);
    }

    PlanningSplitProducer createPlanningSplitProducer() {
        return new LocalParallelPlanningSplitProducer(new IcebergSplitPlanningSupport(
                this,
                source,
                icebergTable,
                preExecutionAuthenticator,
                sessionVariable,
                scanContext,
                storagePropertiesMap,
                formatVersion,
                icebergTable.spec().isPartitioned()));
    }

    @Override
    public boolean isBatchMode() {
        return getPlanningSplitProducer().isBatchMode();
    }

    public IcebergTableQueryInfo getSpecifiedSnapshot() throws UserException {
        TableSnapshot tableSnapshot = getQueryTableSnapshot();
        TableScanParams scanParams = getScanParams();
        Optional<TableScanParams> params = Optional.ofNullable(scanParams);
        if (tableSnapshot != null || IcebergUtils.isIcebergBranchOrTag(params)) {
            return IcebergUtils.getQuerySpecSnapshot(
                icebergTable,
                Optional.ofNullable(tableSnapshot),
                params);
        }
        return null;
    }

    @Override
    public PlanningSplitProducer getPlanningSplitProducer() {
        if (planningSplitProducer == null) {
            planningSplitProducer = createPlanningSplitProducer();
        }
        return planningSplitProducer;
    }

    @Override
    public TFileFormatType getFileFormatType() throws UserException {
        TFileFormatType type;
        String icebergFormat = source.getFileFormat();
        if (icebergFormat.equalsIgnoreCase("parquet")) {
            type = TFileFormatType.FORMAT_PARQUET;
        } else if (icebergFormat.equalsIgnoreCase("orc")) {
            type = TFileFormatType.FORMAT_ORC;
        } else {
            throw new DdlException(String.format("Unsupported format name: %s for iceberg table.", icebergFormat));
        }
        return type;
    }

    @Override
    public List<String> getPathPartitionKeys() throws UserException {
        // return icebergTable.spec().fields().stream().map(PartitionField::name).map(String::toLowerCase)
        //         .collect(Collectors.toList());
        /**First, iceberg partition columns are based on existing fields, which will be stored in the actual data file.
         * Second, iceberg partition columns support Partition transforms. In this case, the path partition key is not
         * equal to the column name of the partition column, so remove this code and get all the columns you want to
         * read from the file.
         * Related code:
         *  be/src/vec/exec/scan/vfile_scanner.cpp:
         *      VFileScanner::_init_expr_ctxes()
         *          if (slot_info.is_file_slot) {
         *              xxxx
         *          }
         */
        return new ArrayList<>();
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws UserException {
        return backendStorageProperties;
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        super.toThrift(planNode);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        syncPlanningMetadataFromProducer();
        String base = super.getNodeExplainString(prefix, detailLevel);
        StringBuilder builder = new StringBuilder(base);
        IcebergPlanningMetadata metadata = getExistingIcebergPlanningMetadata();

        if (metadata != null && detailLevel == TExplainLevel.VERBOSE
                && IcebergUtils.isManifestCacheEnabled(source.getCatalog())) {
            builder.append(prefix).append("manifest cache: hits=").append(metadata.getManifestCacheHits())
                    .append(", misses=").append(metadata.getManifestCacheMisses())
                    .append(", failures=").append(metadata.getManifestCacheFailures()).append("\n");
        }

        if (metadata != null && !metadata.getPushdownIcebergPredicates().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String predicate : metadata.getPushdownIcebergPredicates()) {
                sb.append(prefix).append(prefix).append(predicate).append("\n");
            }
            builder.append(String.format("%sicebergPredicatePushdown=\n%s\n", prefix, sb));
        }
        return builder.toString();
    }

    @Override
    public int numApproximateSplits() {
        return getPlanningSplitProducer().numApproximateSplits();
    }

    @Override
    public long getSelectedPartitionNum() {
        syncPlanningMetadataFromProducer();
        return super.getSelectedPartitionNum();
    }

    private IcebergPlanningMetadata getExistingIcebergPlanningMetadata() {
        PlanningSplitProducer producer = planningSplitProducer;
        if (producer == null) {
            return null;
        }
        PlanningSplitMetadata metadata = producer.getPlanningMetadata();
        Preconditions.checkState(metadata instanceof IcebergPlanningMetadata,
                "Unexpected planning metadata type for iceberg scan node: %s", metadata.getClass().getName());
        return (IcebergPlanningMetadata) metadata;
    }

    private void syncPlanningMetadataFromProducer() {
        IcebergPlanningMetadata metadata = getExistingIcebergPlanningMetadata();
        if (metadata == null) {
            return;
        }
        selectedPartitionNum = metadata.getSelectedPartitionNum();
        if (metadata.hasTableLevelPushDownCount()) {
            setPushDownCount(metadata.getCountFromSnapshot());
        }
    }
}
