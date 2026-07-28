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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMvccSnapshot;
import org.apache.doris.datasource.iceberg.IcebergSysExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.iceberg.cache.IcebergManifestCacheLoader;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;
import org.apache.doris.datasource.iceberg.profile.IcebergMetricsReporter;
import org.apache.doris.datasource.iceberg.source.IcebergDeleteFileFilter.EqualityDelete;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TColumnCategory;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFileIndex;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SplittableScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ScanTaskUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class IcebergScanNode extends FileQueryScanNode {

    public static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;
    static final int ICEBERG_SCAN_SEMANTICS_VERSION = 1;
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergSource source;
    private Table icebergTable;
    private List<String> pushdownIcebergPredicates = Lists.newArrayList();
    // If tableLevelPushDownCount is true, means we can do count push down opt at table level.
    // which means all splits have no position/equality delete files,
    // so for query like "select count(*) from ice_tbl", we can get count from snapshot row count info directly.
    // If tableLevelPushDownCount is false, means we can't do count push down opt at table level,
    // But for part of splits which have no position/equality delete files, we can still do count push down opt.
    // And for split level count push down opt, the flag is set in each split.
    private boolean tableLevelPushDownCount = false;
    private long countFromSnapshot;
    private static final long COUNT_WITH_PARALLEL_SPLITS = 10000;
    private long targetSplitSize = 0;
    // This is used to avoid repeatedly calculating partition info map for the same partition data.
    private Map<PartitionData, Map<String, String>> partitionMapInfos;
    private boolean isPartitionedTable;
    private int formatVersion;
    private ExecutionAuthenticator preExecutionAuthenticator;
    private TableScan icebergTableScan;
    // Store PropertiesMap, including vended credentials or static credentials
    // get them in doInitialize() to ensure internal consistency of ScanNode
    private Map<StorageTypeId, StorageAdapter> storagePropertiesMap;
    private Map<String, String> backendStorageProperties;
    private long manifestCacheHits;
    private long manifestCacheMisses;
    private long manifestCacheFailures;

    // Cached values for LocationPath creation optimization
    // These are lazily initialized on first use to avoid parsing overhead for each file
    private boolean locationPathCacheInitialized = false;
    private StorageAdapter cachedStorageProperties;
    private String cachedSchema;
    private String cachedFsIdPrefix;
    // Cache for path prefix transformation to avoid repeated S3URI parsing
    // Maps original path prefix (e.g., "https://bucket.s3.amazonaws.com/") to normalized prefix (e.g., "s3://bucket/")
    private String cachedOriginalPathPrefix;
    private String cachedNormalizedPathPrefix;
    private String cachedFsIdentifier;

    private Boolean isBatchMode = null;
    private boolean isSystemTable = false;

    // ReferencedDataFile path -> List<DeleteFile> / List<TIcebergDeleteFileDesc> (exclude equal delete)
    public Map<String, List<DeleteFile>> deleteFilesByReferencedDataFile = new HashMap<>();
    public Map<String, List<TIcebergDeleteFileDesc>> deleteFilesDescByReferencedDataFile = new HashMap<>();

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
        initIcebergSource(table);
    }

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, IcebergSysExternalTable sysExternalTable,
            SessionVariable sv, ScanContext scanContext) {
        super(id, desc, "ICEBERG_SCAN_NODE", scanContext, false, sv);
        isSystemTable = true;
        initIcebergSource(sysExternalTable);
    }

    private void initIcebergSource(ExternalTable table) {
        if (table instanceof HMSExternalTable) {
            source = new IcebergHMSSource((HMSExternalTable) table, desc);
        } else if (table instanceof IcebergExternalTable || table instanceof IcebergSysExternalTable) {
            if (table instanceof IcebergSysExternalTable) {
                isSystemTable = true;
            }
            String catalogType = table instanceof IcebergExternalTable
                    ? ((IcebergExternalTable) table).getIcebergCatalogType()
                    : ((IcebergSysExternalTable) table).getSourceTable().getIcebergCatalogType();
            switch (catalogType) {
                case IcebergExternalCatalog.ICEBERG_HMS:
                case IcebergExternalCatalog.ICEBERG_REST:
                case IcebergExternalCatalog.ICEBERG_DLF:
                case IcebergExternalCatalog.ICEBERG_GLUE:
                case IcebergExternalCatalog.ICEBERG_HADOOP:
                case IcebergExternalCatalog.ICEBERG_JDBC:
                case IcebergExternalCatalog.ICEBERG_S3_TABLES:
                    source = new IcebergApiSource(table, desc, columnNameToRange);
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
        long startTime = System.currentTimeMillis();
        try {
            icebergTable = source.getIcebergTable();
            partitionMapInfos = new HashMap<>();
            isPartitionedTable = icebergTable.spec().isPartitioned();
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
                    source.getCatalog().getCatalogProperty().getStorageAdaptersMap(),
                    icebergTable
            );
            backendStorageProperties = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);
        } finally {
            if (getSummaryProfile() != null) {
                getSummaryProfile().addExternalTableGetTableMetaTime(System.currentTimeMillis() - startTime);
            }
        }
        super.doInitialize();
    }

    private Optional<Map<Integer, List<String>>> extractNameMapping() {
        Optional<MvccSnapshot> snapshot = MvccUtil.getSnapshotFromContext(source.getTargetTable());
        if (snapshot.isPresent() && snapshot.get() instanceof IcebergMvccSnapshot) {
            // The mapping must come from the same metadata generation as the pinned schema; a
            // property-only refresh can otherwise change alias semantics within one statement.
            return ((IcebergMvccSnapshot) snapshot.get()).getSnapshotCacheValue().getNameMapping();
        }
        return IcebergUtils.getNameMapping(icebergTable);
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
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        if (isSystemTable && icebergSplit.isPositionDeleteSystemTableSplit()) {
            setIcebergPositionDeleteSysTableParams(rangeDesc, icebergSplit, tableFormatFileDesc, fileDesc);
            return;
        }
        if (isSystemTable) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
            tableFormatFileDesc.setTableLevelRowCount(-1);
            fileDesc.setSerializedSplit(icebergSplit.getSerializedSplit());
            tableFormatFileDesc.setIcebergParams(fileDesc);
            rangeDesc.setTableFormatParams(tableFormatFileDesc);
            rangeDesc.unsetColumnsFromPath();
            rangeDesc.unsetColumnsFromPathKeys();
            rangeDesc.unsetColumnsFromPathIsNull();
            return;
        }
        // update for every split file format
        rangeDesc.setFormatType(toTFileFormatType(icebergSplit.getSplitFileFormat()));
        if (tableLevelPushDownCount) {
            tableFormatFileDesc.setTableLevelRowCount(icebergSplit.getTableLevelRowCount());
        } else {
            // MUST explicitly set to -1, to be distinct from valid row count >= 0
            tableFormatFileDesc.setTableLevelRowCount(-1);
        }
        fileDesc.setFormatVersion(formatVersion);
        fileDesc.setOriginalFilePath(icebergSplit.getOriginalPath());
        if (icebergSplit.getPartitionSpecId() != null) {
            fileDesc.setPartitionSpecId(icebergSplit.getPartitionSpecId());
        }
        if (icebergSplit.getPartitionDataJson() != null) {
            fileDesc.setPartitionDataJson(icebergSplit.getPartitionDataJson());
        }
        if (formatVersion >= 3) {
            fileDesc.setFirstRowId(icebergSplit.getFirstRowId());
            fileDesc.setLastUpdatedSequenceNumber(icebergSplit.getLastUpdatedSequenceNumber());
        }
        if (formatVersion < MIN_DELETE_FILE_SUPPORT_VERSION) {
            fileDesc.setContent(FileContent.DATA.id());
        } else {
            fileDesc.setDeleteFiles(new ArrayList<>());
            for (IcebergDeleteFileFilter filter : icebergSplit.getDeleteFileFilters()) {
                TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
                String deleteFilePath = filter.getDeleteFilePath();
                LocationPath locationPath = LocationPath.ofAdapters(deleteFilePath, icebergSplit.getConfig());
                deleteFileDesc.setPath(locationPath.toStorageLocation().toString());
                setDeleteFileFormat(deleteFileDesc, filter.getFileformat());
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
                        deleteFileDesc.setContentOffset(dv.getContentOffset());
                        deleteFileDesc.setContentSizeInBytes(dv.getContentLength());
                        deleteFileDesc.setContent(IcebergDeleteFileFilter.DeletionVector.type());
                    }
                } else {
                    IcebergDeleteFileFilter.EqualityDelete equalityDelete =
                            (IcebergDeleteFileFilter.EqualityDelete) filter;
                    deleteFileDesc.setFieldIds(equalityDelete.getFieldIds());
                    deleteFileDesc.setContent(EqualityDelete.type());
                }
                fileDesc.addToDeleteFiles(deleteFileDesc);
            }

            // Filter out equality delete files from deleteFilesByReferencedDataFile as well.
            List<DeleteFile> nonEqualityDeleteFiles = new ArrayList<>();
            for (DeleteFile df : icebergSplit.getDeleteFiles()) {
                if (df.content() != FileContent.EQUALITY_DELETES) {
                    nonEqualityDeleteFiles.add(df);
                }
            }
            deleteFilesByReferencedDataFile.put(icebergSplit.getOriginalPath(), nonEqualityDeleteFiles);
            List<TIcebergDeleteFileDesc> nonEqualityDeleteFileDesc = new ArrayList<>();
            for (TIcebergDeleteFileDesc df : fileDesc.getDeleteFiles()) {
                if (df.getContent() != EqualityDelete.type()) {
                    nonEqualityDeleteFileDesc.add(df);
                }
            }
            deleteFilesDescByReferencedDataFile.put(icebergSplit.getOriginalPath(), nonEqualityDeleteFileDesc);
        }
        tableFormatFileDesc.setIcebergParams(fileDesc);
        rangeDesc.unsetColumnsFromPath();
        rangeDesc.unsetColumnsFromPathKeys();
        rangeDesc.unsetColumnsFromPathIsNull();
        Map<String, String> partitionValues = icebergSplit.getIcebergPartitionValues();
        List<String> orderedPartitionKeys = getOrderedPathPartitionKeys();
        if (partitionValues != null && !orderedPartitionKeys.isEmpty()) {
            List<String> fromPathKeys = new ArrayList<>();
            List<String> fromPathValues = new ArrayList<>();
            List<Boolean> fromPathIsNull = new ArrayList<>();
            for (String partitionKey : orderedPartitionKeys) {
                if (!partitionValues.containsKey(partitionKey)) {
                    continue;
                }
                String partitionValue = partitionValues.get(partitionKey);
                fromPathKeys.add(partitionKey);
                fromPathValues.add(partitionValue != null ? partitionValue : "");
                fromPathIsNull.add(partitionValue == null);
            }
            if (!fromPathKeys.isEmpty()) {
                rangeDesc.setColumnsFromPathKeys(fromPathKeys);
                rangeDesc.setColumnsFromPath(fromPathValues);
                rangeDesc.setColumnsFromPathIsNull(fromPathIsNull);
            }
        }
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    private void setIcebergPositionDeleteSysTableParams(TFileRangeDesc rangeDesc, IcebergSplit icebergSplit,
            TTableFormatFileDesc tableFormatFileDesc, TIcebergFileDesc fileDesc) {
        rangeDesc.setFormatType(icebergSplit.getPositionDeleteFileFormat());
        tableFormatFileDesc.setTableLevelRowCount(-1);
        fileDesc.setContent(icebergSplit.getPositionDeleteContent());

        if (icebergSplit.getPartitionSpecId() != null) {
            fileDesc.setPartitionSpecId(icebergSplit.getPartitionSpecId());
        }
        if (icebergSplit.getPartitionDataJson() != null) {
            fileDesc.setPartitionDataJson(icebergSplit.getPartitionDataJson());
        }

        TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
        deleteFileDesc.setPath(rangeDesc.getPath());
        deleteFileDesc.setOriginalPath(icebergSplit.getPositionDeleteOriginalPath());
        deleteFileDesc.setFileFormat(icebergSplit.getPositionDeleteFileFormat());
        deleteFileDesc.setContent(icebergSplit.getPositionDeleteContent());
        if (icebergSplit.getPositionDeleteContentOffset() != null) {
            deleteFileDesc.setContentOffset(icebergSplit.getPositionDeleteContentOffset());
        }
        if (icebergSplit.getPositionDeleteContentSizeInBytes() != null) {
            deleteFileDesc.setContentSizeInBytes(icebergSplit.getPositionDeleteContentSizeInBytes());
        }
        if (icebergSplit.getPositionDeleteReferencedDataFilePath() != null) {
            deleteFileDesc.setReferencedDataFilePath(icebergSplit.getPositionDeleteReferencedDataFilePath());
        }
        fileDesc.setDeleteFiles(Lists.newArrayList(deleteFileDesc));
        tableFormatFileDesc.setIcebergParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
        rangeDesc.unsetColumnsFromPath();
        rangeDesc.unsetColumnsFromPathKeys();
        rangeDesc.unsetColumnsFromPathIsNull();
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

    private void setDeleteFileFormat(TIcebergDeleteFileDesc deleteFileDesc, FileFormat fileFormat) {
        if (fileFormat == FileFormat.PARQUET) {
            deleteFileDesc.setFileFormat(TFileFormatType.FORMAT_PARQUET);
        } else if (fileFormat == FileFormat.ORC) {
            deleteFileDesc.setFileFormat(TFileFormatType.FORMAT_ORC);
        }
    }

    private TFileFormatType toTFileFormatType(FileFormat fileFormat) {
        if (fileFormat == FileFormat.PARQUET) {
            return TFileFormatType.FORMAT_PARQUET;
        } else if (fileFormat == FileFormat.ORC) {
            return TFileFormatType.FORMAT_ORC;
        }
        throw new UnsupportedOperationException("Unsupported Iceberg data file format: " + fileFormat);
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

    private List<String> getOrderedPathPartitionKeys() {
        if (icebergTable == null) {
            return Collections.emptyList();
        }
        return IcebergUtils.getIdentityPartitionColumns(icebergTable);
    }

    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
        enableCurrentIcebergScanSemantics();
        // Extract name mapping from Iceberg table properties
        Optional<Map<Integer, List<String>>> nameMapping = extractNameMapping();

        // Equality-delete keys are hidden scan dependencies and need not appear in the query
        // projection. Both scanners need the complete current schema to resolve field ids,
        // historical names, types, and initial defaults when an old data file lacks such a key.
        ExternalUtil.initSchemaInfoForAllColumn(params, -1L, source.getTargetTable().getColumns(),
                nameMapping.orElse(Collections.emptyMap()), nameMapping.isPresent(),
                getBase64EncodedInitialDefaultsForScan());
    }

    @VisibleForTesting
    void enableCurrentIcebergScanSemantics() {
        // This explicit capability is the rollout boundary: old FE plans must keep legacy values
        // when fragments run on a mixture of old and new BEs.
        params.setIcebergScanSemanticsVersion(ICEBERG_SCAN_SEMANTICS_VERSION);
    }

    @VisibleForTesting
    Map<Integer, String> getBase64EncodedInitialDefaultsForScan() throws UserException {
        if (isSystemTable) {
            // System-table columns are derived from the metadata table schema. Some metadata
            // tables, such as position_deletes, do not support Table.newScan(). Use the same
            // schema that produced source.getTargetTable().getColumns() to keep defaults aligned.
            return IcebergUtils.getBase64EncodedInitialDefaults(icebergTable.schema());
        }
        IcebergTableQueryInfo selectedSnapshot = getSpecifiedSnapshot();
        Optional<MvccSnapshot> mvccSnapshot = MvccUtil.getSnapshotFromContext(source.getTargetTable());
        Schema scanSchema = null;
        if (mvccSnapshot.isPresent() && mvccSnapshot.get() instanceof IcebergMvccSnapshot) {
            long schemaId = ((IcebergMvccSnapshot) mvccSnapshot.get())
                    .getSnapshotCacheValue().getSnapshot().getSchemaId();
            scanSchema = icebergTable.schemas().get(Math.toIntExact(schemaId));
        } else {
            scanSchema = selectedSnapshot == null
                    ? icebergTable.schema()
                    : icebergTable.schemas().get(selectedSnapshot.getSchemaId());
        }
        // A branch can expose a schema newer than its data snapshot. The statement-pinned schema
        // produced the target columns, so default markers must not be recomputed from that snapshot.
        return IcebergUtils.getBase64EncodedInitialDefaults(
                Preconditions.checkNotNull(scanSchema, "Schema for Iceberg scan is null"));
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {

        try {
            return preExecutionAuthenticator.execute(() -> doGetSplits(numBackends));
        } catch (Exception e) {
            Optional<NotSupportedException> opt = checkNotSupportedException(e);
            if (opt.isPresent()) {
                throw opt.get();
            } else {
                throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
            }
        }
    }

    /**
     * Get FileScanTasks from StatementContext for rewrite operations.
     * This allows setting file scan tasks before the plan is generated.
     */
    private List<FileScanTask> getFileScanTasksFromContext() {
        ConnectContext ctx = ConnectContext.get();
        Preconditions.checkNotNull(ctx);
        Preconditions.checkNotNull(ctx.getStatementContext());

        // Get the rewrite file scan tasks from statement context
        List<FileScanTask> tasks = ctx.getStatementContext().getAndClearIcebergRewriteFileScanTasks();
        if (tasks != null && !tasks.isEmpty()) {
            LOG.info("Retrieved {} file scan tasks from context for table {}",
                    tasks.size(), icebergTable.name());
            return new ArrayList<>(tasks);
        }
        return null;
    }

    @Override
    public void startSplit(int numBackends) throws UserException {
        try {
            preExecutionAuthenticator.execute(() -> {
                doStartSplit();
                return null;
            });
        } catch (Exception e) {
            throw new UserException(e.getMessage(), e);
        }
    }

    public void doStartSplit() throws UserException {
        TableScan scan = createTableScan();
        CompletableFuture.runAsync(() -> {
            AtomicReference<CloseableIterable<FileScanTask>> taskRef = new AtomicReference<>();
            try {
                preExecutionAuthenticator.execute(
                        () -> {
                            long startTime = System.currentTimeMillis();
                            try {
                                CloseableIterable<FileScanTask> fileScanTasks = planFileScanTask(scan);
                                taskRef.set(fileScanTasks);
                                CloseableIterator<FileScanTask> iterator = fileScanTasks.iterator();
                                while (splitAssignment.needMoreSplit() && iterator.hasNext()) {
                                    try {
                                        splitAssignment.addToQueue(
                                                Lists.newArrayList(createIcebergSplit(iterator.next())));
                                    } catch (UserException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            } finally {
                                if (getSummaryProfile() != null) {
                                    getSummaryProfile().addExternalTableGetFileScanTasksTime(
                                            System.currentTimeMillis() - startTime);
                                }
                            }
                        }
                );
                splitAssignment.finishSchedule();
                recordManifestCacheProfile();
            } catch (Exception e) {
                Optional<NotSupportedException> opt = checkNotSupportedException(e);
                if (opt.isPresent()) {
                    splitAssignment.setException(new UserException(opt.get().getMessage(), opt.get()));
                } else {
                    splitAssignment.setException(new UserException(e.getMessage(), e));
                }
            } finally {
                if (taskRef.get() != null) {
                    try {
                        taskRef.get().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }, Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor());
    }

    @VisibleForTesting
    public TableScan createTableScan() throws UserException {
        if (icebergTableScan != null) {
            return icebergTableScan;
        }

        TableScan scan = icebergTable.newScan().metricsReporter(new IcebergMetricsReporter());

        // set snapshot
        IcebergTableQueryInfo info = getSpecifiedSnapshot();
        if (info != null) {
            if (info.getRef() != null) {
                scan = scan.useRef(info.getRef());
            } else {
                scan = scan.useSnapshot(info.getSnapshotId());
            }
        }

        // set filter
        List<Expression> expressions = new ArrayList<>();
        for (Expr conjunct : conjuncts) {
            Expression expression = IcebergUtils.convertToIcebergExpr(conjunct, icebergTable.schema());
            if (expression != null) {
                expressions.add(expression);
            }
        }
        for (Expression predicate : expressions) {
            scan = scan.filter(predicate);
            this.pushdownIcebergPredicates.add(predicate.toString());
        }

        // Doris reads normal Iceberg table files in BE and applies column pruning through scan range params.
        // System tables are different: Iceberg SDK DataTask materializes rows using the projected scan
        // schema. Keep Doris file slots in the same order as the JNI reader's required fields.
        if (isSystemTable) {
            Schema projectedSchema = getSystemTableProjectedSchema(expressions, scan.isCaseSensitive());
            Preconditions.checkState(!projectedSchema.columns().isEmpty(),
                    "Iceberg system table scan must materialize at least one file slot");
            scan = scan.project(projectedSchema);
        }

        icebergTableScan = scan.planWith(source.getCatalog().getThreadPoolWithPreAuth());

        return icebergTableScan;
    }

    @VisibleForTesting
    Schema getSystemTableProjectedSchema(List<Expression> expressions, boolean caseSensitive)
            throws UserException {
        List<NestedField> projectedFields = new ArrayList<>();
        Set<Integer> projectedFieldIds = new HashSet<>();
        List<String> partitionKeys = getPathPartitionKeys();
        for (SlotDescriptor slot : desc.getSlots()) {
            Column column = slot.getColumn();
            String columnName = column.getName();
            if (!isFileSlot(classifyColumn(slot, partitionKeys))) {
                continue;
            }

            NestedField field = caseSensitive
                    ? icebergTable.schema().findField(columnName)
                    : icebergTable.schema().caseInsensitiveFindField(columnName);
            if (field == null) {
                throw new UserException("Column " + columnName + " not found in Iceberg system table schema");
            }
            if (projectedFieldIds.add(field.fieldId())) {
                projectedFields.add(field);
            }
        }

        Set<Integer> filterFieldIds = Binder.boundReferences(
                icebergTable.schema().asStruct(), expressions, caseSensitive);
        for (Integer fieldId : filterFieldIds) {
            NestedField field = getTopLevelSystemTableField(fieldId);
            if (field == null) {
                throw new UserException(
                        "Column with field id " + fieldId + " not found in Iceberg system table schema");
            }
            if (!projectedFieldIds.contains(field.fieldId())) {
                throw new UserException("Iceberg system table filter column " + field.name()
                        + " is not materialized by the planner");
            }
        }
        return new Schema(projectedFields);
    }

    private NestedField getTopLevelSystemTableField(int fieldId) {
        for (NestedField field : icebergTable.schema().columns()) {
            if (field.fieldId() == fieldId || TypeUtil.getProjectedIds(field.type()).contains(fieldId)) {
                return field;
            }
        }
        return null;
    }

    private CloseableIterable<FileScanTask> planFileScanTask(TableScan scan) {
        if (!IcebergUtils.isManifestCacheEnabled(source.getCatalog())) {
            return splitFiles(scan);
        }
        try {
            return planFileScanTaskWithManifestCache(scan);
        } catch (Exception e) {
            manifestCacheFailures++;
            LOG.warn("Plan with manifest cache failed, fallback to original scan: " + e.getMessage(), e);
            return splitFiles(scan);
        }
    }

    private CloseableIterable<FileScanTask> splitFiles(TableScan scan) {
        if (sessionVariable.getFileSplitSize() > 0) {
            return TableScanUtil.splitFiles(scan.planFiles(),
                    sessionVariable.getFileSplitSize());
        }
        if (isBatchMode()) {
            // Currently iceberg batch split mode will use max split size.
            // TODO: dynamic split size in batch split mode need to customize iceberg splitter.
            return TableScanUtil.splitFiles(scan.planFiles(), sessionVariable.getMaxSplitSize());
        }

        // Non Batch Mode
        // Materialize planFiles() into a list to avoid iterating the CloseableIterable twice.
        // RISK: It will cost memory if the table is large.
        List<FileScanTask> fileScanTaskList = new ArrayList<>();
        try (CloseableIterable<FileScanTask> scanTasksIter = scan.planFiles()) {
            for (FileScanTask task : scanTasksIter) {
                fileScanTaskList.add(task);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to materialize file scan tasks", e);
        }

        targetSplitSize = determineTargetFileSplitSize(fileScanTaskList);
        return TableScanUtil.splitFiles(CloseableIterable.withNoopClose(fileScanTaskList), targetSplitSize);
    }

    private long determineTargetFileSplitSize(Iterable<? extends ContentScanTask<?>> tasks) {
        long result = sessionVariable.getMaxInitialSplitSize();
        long accumulatedTotalFileSize = 0;
        boolean exceedInitialThreshold = false;
        for (ContentScanTask<?> task : tasks) {
            accumulatedTotalFileSize += ScanTaskUtil.contentSizeInBytes(task.file());
            if (!exceedInitialThreshold && accumulatedTotalFileSize
                    >= sessionVariable.getMaxSplitSize() * sessionVariable.getMaxInitialSplitNum()) {
                exceedInitialThreshold = true;
            }
        }
        result = exceedInitialThreshold ? sessionVariable.getMaxSplitSize() : result;
        if (!isBatchMode()) {
            result = applyMaxFileSplitNumLimit(result, accumulatedTotalFileSize);
        }
        return result;
    }

    private long determinePositionDeleteTargetSplitSize(Iterable<PositionDeletesScanTask> tasks) {
        if (sessionVariable.getFileSplitSize() > 0) {
            return sessionVariable.getFileSplitSize();
        }
        return determineTargetFileSplitSize(tasks);
    }

    private CloseableIterable<FileScanTask> planFileScanTaskWithManifestCache(TableScan scan) throws IOException {
        // Get the snapshot from the scan; return empty if no snapshot exists
        Snapshot snapshot = scan.snapshot();
        if (snapshot == null) {
            return CloseableIterable.withNoopClose(Collections.emptyList());
        }

        // Initialize manifest cache for efficient manifest file access
        IcebergExternalMetaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().iceberg(source.getCatalog().getId());
        if (!(source.getTargetTable() instanceof ExternalTable)) {
            throw new RuntimeException("Iceberg scan target table is not an external table");
        }
        ExternalTable targetExternalTable = (ExternalTable) source.getTargetTable();

        // Convert query conjuncts to Iceberg filter expression
        // This combines all predicates with AND logic for partition/file pruning
        Expression filterExpr = conjuncts.stream()
                .map(conjunct -> IcebergUtils.convertToIcebergExpr(conjunct, icebergTable.schema()))
                .filter(Objects::nonNull)
                .reduce(Expressions.alwaysTrue(), Expressions::and);

        // Get all partition specs by their IDs for later use
        Map<Integer, PartitionSpec> specsById = icebergTable.specs();
        boolean caseSensitive = true;

        // Create residual evaluators for each partition spec
        // Residual evaluators compute the remaining filter expression after partition pruning
        Map<Integer, ResidualEvaluator> residualEvaluators = new HashMap<>();
        specsById.forEach((id, spec) -> residualEvaluators.put(id,
                ResidualEvaluator.of(spec, filterExpr, caseSensitive)));

        // Create metrics evaluator for file-level pruning based on column statistics
        InclusiveMetricsEvaluator metricsEvaluator =
                new InclusiveMetricsEvaluator(icebergTable.schema(), filterExpr, caseSensitive);

        // ========== Phase 1: Load delete files from delete manifests ==========
        List<DeleteFile> deleteFiles = new ArrayList<>();
        List<ManifestFile> deleteManifests = snapshot.deleteManifests(icebergTable.io());
        for (ManifestFile manifest : deleteManifests) {
            // Skip non-delete manifests
            if (manifest.content() != ManifestContent.DELETES) {
                continue;
            }
            // Get the partition spec for this manifest
            PartitionSpec spec = specsById.get(manifest.partitionSpecId());
            if (spec == null) {
                continue;
            }
            // Create manifest evaluator for partition-level pruning
            ManifestEvaluator evaluator =
                    ManifestEvaluator.forPartitionFilter(filterExpr, spec, caseSensitive);
            // Skip manifest if it doesn't match the filter expression (partition pruning)
            if (!evaluator.eval(manifest)) {
                continue;
            }
            // Load delete files from cache (or from storage if not cached)
            ManifestCacheValue value = IcebergManifestCacheLoader.loadDeleteFilesWithCache(cache,
                    targetExternalTable, manifest, icebergTable, this::recordManifestCacheAccess);
            deleteFiles.addAll(value.getDeleteFiles());
        }

        // Build delete file index for efficient lookup of deletes applicable to each data file
        DeleteFileIndex deleteIndex = DeleteFileIndex.builderFor(deleteFiles)
                .specsById(specsById)
                .caseSensitive(caseSensitive)
                .build();

        // ========== Phase 2: Load data files and create scan tasks ==========
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<ManifestFile> dataManifests =
                     IcebergUtils.getMatchingManifest(snapshot.dataManifests(icebergTable.io()),
                             specsById, filterExpr)) {
            for (ManifestFile manifest : dataManifests) {
                // Skip non-data manifests
                if (manifest.content() != ManifestContent.DATA) {
                    continue;
                }
                // Get the partition spec for this manifest
                PartitionSpec spec = specsById.get(manifest.partitionSpecId());
                if (spec == null) {
                    continue;
                }
                // Get the residual evaluator for this partition spec
                ResidualEvaluator residualEvaluator = residualEvaluators.get(manifest.partitionSpecId());
                if (residualEvaluator == null) {
                    continue;
                }

                // Load data files from cache (or from storage if not cached)
                ManifestCacheValue value = IcebergManifestCacheLoader.loadDataFilesWithCache(cache,
                        targetExternalTable, manifest, icebergTable, this::recordManifestCacheAccess);

                // Process each data file in the manifest
                for (org.apache.iceberg.DataFile dataFile : value.getDataFiles()) {
                    // Skip file if column statistics indicate no matching rows (metrics-based pruning)
                    if (metricsEvaluator != null && !metricsEvaluator.eval(dataFile)) {
                        continue;
                    }
                    // Skip file if partition values don't match the residual filter
                    if (residualEvaluator.residualFor(dataFile.partition()).equals(Expressions.alwaysFalse())) {
                        continue;
                    }
                    // Find all delete files that apply to this data file based on sequence number
                    List<DeleteFile> deletes = Arrays.asList(
                            deleteIndex.forDataFile(dataFile.dataSequenceNumber(), dataFile));

                    // Create a FileScanTask containing the data file, associated deletes, and metadata
                    tasks.add(new BaseFileScanTask(
                            dataFile,
                            deletes.toArray(new DeleteFile[0]),
                            SchemaParser.toJson(icebergTable.schema()),
                            PartitionSpecParser.toJson(spec),
                            residualEvaluator));
                }
            }
        }

        // Split tasks into smaller chunks based on target split size for parallel processing
        targetSplitSize = determineTargetFileSplitSize(tasks);
        return TableScanUtil.splitFiles(CloseableIterable.withNoopClose(tasks), targetSplitSize);
    }

    /**
     * Initialize cached values for LocationPath creation on first use.
     * This avoids repeated StorageProperties lookup, scheme parsing, and S3URI regex parsing for each file.
     */
    private void initLocationPathCache(String samplePath) {
        try {
            // Create a LocationPath using the full method to get all cached values
            LocationPath sampleLocationPath = LocationPath.ofAdapters(samplePath, storagePropertiesMap);
            cachedStorageProperties = sampleLocationPath.getStorageAdapter();
            cachedSchema = sampleLocationPath.getSchema();
            cachedFsIdentifier = sampleLocationPath.getFsIdentifier();

            // Extract fsIdPrefix like "s3://" from fsIdentifier like "s3://bucket"
            int schemeEnd = cachedFsIdentifier.indexOf("://");
            if (schemeEnd > 0) {
                cachedFsIdPrefix = cachedFsIdentifier.substring(0, schemeEnd + 3);
            }

            // Cache path prefix mapping for fast transformation
            // This allows subsequent files to skip S3URI regex parsing entirely
            String normalizedPath = sampleLocationPath.getNormalizedLocation();

            // Find the common prefix by looking for the last '/' before the filename
            int lastSlashInOriginal = samplePath.lastIndexOf('/');
            int lastSlashInNormalized = normalizedPath.lastIndexOf('/');

            if (lastSlashInOriginal > 0 && lastSlashInNormalized > 0) {
                cachedOriginalPathPrefix = samplePath.substring(0, lastSlashInOriginal + 1);
                cachedNormalizedPathPrefix = normalizedPath.substring(0, lastSlashInNormalized + 1);
            }

            locationPathCacheInitialized = true;
        } catch (Exception e) {
            // If caching fails, try to initialize again on next use
            LOG.warn("Failed to initialize LocationPath cache, will use full parsing", e);
        }
    }

    /**
     * Create a LocationPath with cached values for better performance.
     * Uses cached path prefix mapping to completely bypass S3URI regex parsing for most files.
     * Falls back to full parsing if cache is not available or path doesn't match cached prefix.
     */
    private LocationPath createLocationPathWithCache(String path) {
        // Initialize cache on first call
        if (!locationPathCacheInitialized) {
            initLocationPathCache(path);
        }

        // Fast path: if path starts with cached original prefix, directly transform without any parsing
        if (cachedOriginalPathPrefix != null && path.startsWith(cachedOriginalPathPrefix)) {
            // Transform: replace original prefix with normalized prefix
            String normalizedPath = cachedNormalizedPathPrefix + path.substring(cachedOriginalPathPrefix.length());
            return LocationPath.ofDirect(normalizedPath, cachedSchema, cachedFsIdentifier, cachedStorageProperties);
        }

        // Medium path: use cached StorageAdapter but still need validateAndNormalizeUri
        if (cachedStorageProperties != null) {
            return LocationPath.ofWithCache(path, cachedStorageProperties, cachedSchema, cachedFsIdPrefix);
        }

        // Fallback to full parsing
        return LocationPath.ofAdapters(path, storagePropertiesMap);
    }

    private Split createIcebergSplit(FileScanTask fileScanTask) {
        DataFile dataFile = fileScanTask.file();
        String originalPath = dataFile.path().toString();
        LocationPath locationPath = createLocationPathWithCache(originalPath);
        IcebergSplit split = new IcebergSplit(
                locationPath,
                fileScanTask.start(),
                fileScanTask.length(),
                dataFile.fileSizeInBytes(),
                new String[0],
                formatVersion,
                storagePropertiesMap,
                new ArrayList<>(),
                originalPath);
        split.setSplitFileFormat(dataFile.format());
        if (formatVersion >= 3) {
            // -1 means that this table was just upgraded from v2 to v3.
            // _row_id and _last_updated_sequence_number column is NULL.
            split.setFirstRowId(dataFile.firstRowId() != null ? dataFile.firstRowId() : -1);
            split.setLastUpdatedSequenceNumber(
                    dataFile.fileSequenceNumber() != null && dataFile.firstRowId() != null
                            ? dataFile.fileSequenceNumber() : -1);
        }
        if (!fileScanTask.deletes().isEmpty()) {
            split.setDeleteFileFilters(fileScanTask.deletes(), getDeleteFileFilters(fileScanTask));
        }
        split.setTableFormatType(TableFormatType.ICEBERG);
        split.setTargetSplitSize(targetSplitSize);
        if (isPartitionedTable) {
            int specId = fileScanTask.file().specId();
            PartitionSpec partitionSpec = icebergTable.specs().get(specId);
            Preconditions.checkNotNull(partitionSpec, "Partition spec with specId %s not found for table %s",
                    specId, icebergTable.name());
            PartitionData partitionData = (PartitionData) fileScanTask.file().partition();
            if (partitionData != null) {
                split.setPartitionSpecId(specId);
                split.setPartitionDataJson(IcebergUtils.getPartitionDataJson(
                        partitionData, partitionSpec, sessionVariable.getTimeZone()));
                Map<String, String> partitionInfoMap = partitionMapInfos.computeIfAbsent(
                        partitionData, k -> IcebergUtils.getIdentityPartitionInfoMap(
                                partitionData, partitionSpec, icebergTable, sessionVariable.getTimeZone()));
                if (!partitionInfoMap.isEmpty()) {
                    split.setIcebergPartitionValues(partitionInfoMap);
                }
            } else {
                partitionMapInfos.put(null, Collections.emptyMap());
            }
        }
        return split;
    }

    private Split createIcebergSysSplit(ScanTask scanTask) {
        long rowCount = Math.max(scanTask.estimatedRowsCount(), 1L);
        if (scanTask.isFileScanTask() && scanTask.asFileScanTask().file() != null) {
            rowCount = Math.max(scanTask.asFileScanTask().file().recordCount(), 1L);
        }
        IcebergSplit split = IcebergSplit.newSysTableSplit(
                SerializationUtil.serializeToBase64(scanTask), rowCount);
        split.setTableFormatType(TableFormatType.ICEBERG);
        return split;
    }

    private Split createIcebergPositionDeleteSysSplit(PositionDeletesScanTask task) throws UserException {
        DeleteFile deleteFile = task.file();
        String originalPath = deleteFile.path().toString();
        LocationPath locationPath = createLocationPathWithCache(originalPath);
        IcebergSplit split = IcebergSplit.newPositionDeleteSysTableSplit(
                locationPath, task.start(), task.length(), deleteFile.fileSizeInBytes(),
                storagePropertiesMap, originalPath);
        split.setTableFormatType(TableFormatType.ICEBERG);
        split.setPositionDeleteFileFormat(getNativePositionDeleteFileFormat(deleteFile.format()));
        split.setPositionDeleteOriginalPath(originalPath);
        if (deleteFile.format() == FileFormat.PUFFIN) {
            Long contentOffset = deleteFile.contentOffset();
            Long contentLength = deleteFile.contentSizeInBytes();
            IcebergDeleteFileFilter.validateDeletionVectorMetadata(
                    originalPath, deleteFile.fileSizeInBytes(), contentOffset, contentLength);
            split.setPositionDeleteContent(IcebergDeleteFileFilter.DeletionVector.type());
            split.setPositionDeleteReferencedDataFilePath(deleteFile.referencedDataFile());
            split.setPositionDeleteContentOffset(contentOffset);
            split.setPositionDeleteContentSizeInBytes(contentLength);
        } else {
            split.setPositionDeleteContent(IcebergDeleteFileFilter.PositionDelete.type());
        }

        split.setPartitionSpecId(deleteFile.specId());
        PartitionSpec partitionSpec = icebergTable.specs().get(deleteFile.specId());
        Preconditions.checkNotNull(partitionSpec, "Partition spec with specId %s not found for table %s",
                deleteFile.specId(), icebergTable.name());
        if (partitionSpec.isPartitioned() && deleteFile.partition() != null
                && isPositionDeletesPartitionColumnRequested()) {
            split.setPartitionDataJson(getPartitionDataObjectJson(
                    (PartitionData) deleteFile.partition(), partitionSpec,
                    getPositionDeletesOutputPartitionFields()));
        }
        return split;
    }

    @SuppressWarnings("unchecked")
    private Iterable<PositionDeletesScanTask> splitPositionDeleteScanTask(PositionDeletesScanTask task) {
        return ((SplittableScanTask<PositionDeletesScanTask>) task).split(targetSplitSize);
    }

    private TFileFormatType getNativePositionDeleteFileFormat(FileFormat fileFormat) {
        if (fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.PUFFIN) {
            return TFileFormatType.FORMAT_PARQUET;
        } else if (fileFormat == FileFormat.ORC) {
            return TFileFormatType.FORMAT_ORC;
        }
        throw new UnsupportedOperationException(
                "Unsupported Iceberg position delete file format: " + fileFormat);
    }

    private List<NestedField> getPositionDeletesOutputPartitionFields() {
        NestedField partitionField = icebergTable.schema().findField("partition");
        Preconditions.checkNotNull(partitionField,
                "Partition field not found in Iceberg position_deletes metadata table schema");
        return partitionField.type().asNestedType().fields();
    }

    private boolean isPositionDeletesPartitionColumnRequested() {
        return desc.getSlots().stream()
                .anyMatch(slot -> "partition".equalsIgnoreCase(slot.getColumn().getName()));
    }

    private String getPartitionDataObjectJson(PartitionData partitionData, PartitionSpec partitionSpec,
            List<NestedField> outputPartitionFields) throws UserException {
        List<NestedField> partitionTypes = partitionData.getPartitionType().asNestedType().fields();
        boolean enableMappingVarbinary = getEnableMappingVarbinary();
        for (int i = 0; i < partitionTypes.size(); i++) {
            Type type = partitionTypes.get(i).type();
            if (partitionData.get(i) != null && (type.typeId() == Type.TypeID.BINARY
                    || type.typeId() == Type.TypeID.FIXED
                    || (type.typeId() == Type.TypeID.UUID && enableMappingVarbinary))) {
                throw new UserException("Iceberg position_deletes cannot materialize non-null partition field '"
                        + partitionTypes.get(i).name() + "' of type " + type
                        + " without a binary-safe partition transport");
            }
        }
        List<String> partitionValues = IcebergUtils.getPartitionValues(
                partitionData, partitionSpec, sessionVariable.getTimeZone());
        Map<Integer, Object> partitionValueByFieldId = new HashMap<>();
        List<PartitionField> fields = partitionSpec.fields();
        for (int i = 0; i < fields.size(); i++) {
            partitionValueByFieldId.put(fields.get(i).fieldId(),
                    getPartitionJsonValue(partitionTypes.get(i).type(), partitionValues.get(i)));
        }
        JsonObject partitionJson = new JsonObject();
        for (NestedField outputPartitionField : outputPartitionFields) {
            partitionJson.add(outputPartitionField.name(),
                    GsonUtils.GSON.toJsonTree(partitionValueByFieldId.get(outputPartitionField.fieldId())));
        }
        return GsonUtils.GSON.toJson(partitionJson);
    }

    private static Object getPartitionJsonValue(Type type, String partitionValue) {
        if (partitionValue == null) {
            return null;
        }
        switch (type.typeId()) {
            case BOOLEAN:
                return Boolean.parseBoolean(partitionValue);
            case INTEGER:
                return Integer.parseInt(partitionValue);
            case LONG:
                return Long.parseLong(partitionValue);
            case FLOAT:
                return Float.parseFloat(partitionValue);
            case DOUBLE:
                return Double.parseDouble(partitionValue);
            case DECIMAL:
                return new BigDecimal(partitionValue);
            case STRING:
            case UUID:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return partitionValue;
            default:
                return partitionValue;
        }
    }

    @Override
    protected TColumnCategory classifyColumn(SlotDescriptor slot, List<String> partitionKeys) {
        if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(slot.getColumn().getName())) {
            return TColumnCategory.SYNTHESIZED;
        }
        if (slot.getColumn().getName().startsWith(Column.GLOBAL_ROWID_COL)) {
            return TColumnCategory.SYNTHESIZED;
        }
        if (IcebergUtils.isIcebergRowLineageColumn(slot.getColumn())) {
            return TColumnCategory.GENERATED;
        }
        return super.classifyColumn(slot, partitionKeys);
    }

    private List<Split> doGetSplits(int numBackends) throws UserException {
        if (isSystemTable) {
            return doGetSystemTableSplits();
        }

        List<Split> splits = new ArrayList<>();

        // Use custom file scan tasks if available (for rewrite operations)
        List<FileScanTask> customFileScanTasks = getFileScanTasksFromContext();
        if (customFileScanTasks != null) {
            for (FileScanTask task : customFileScanTasks) {
                splits.add(createIcebergSplit(task));
            }
            selectedPartitionNum = partitionMapInfos.size();
            recordManifestCacheProfile();
            return splits;
        }

        // Normal table scan planning
        TableScan scan = createTableScan();

        long startTime = System.currentTimeMillis();
        try (CloseableIterable<FileScanTask> fileScanTasks = planFileScanTask(scan)) {
            if (tableLevelPushDownCount) {
                int needSplitCnt = countFromSnapshot < COUNT_WITH_PARALLEL_SPLITS
                        ? 1 : sessionVariable.getParallelExecInstanceNum(scanContext.getClusterName())
                                * numBackends;
                for (FileScanTask next : fileScanTasks) {
                    splits.add(createIcebergSplit(next));
                    if (splits.size() >= needSplitCnt) {
                        break;
                    }
                }
                setPushDownCount(countFromSnapshot);
                assignCountToSplits(splits, countFromSnapshot);
                recordManifestCacheProfile();
                return splits;
            } else {
                fileScanTasks.forEach(taskGrp -> splits.add(createIcebergSplit(taskGrp)));
            }
        } catch (IOException e) {
            throw new UserException(e.getMessage(), e.getCause());
        } finally {
            if (getSummaryProfile() != null) {
                getSummaryProfile().addExternalTableGetFileScanTasksTime(System.currentTimeMillis() - startTime);
            }
        }

        selectedPartitionNum = partitionMapInfos.size();
        recordManifestCacheProfile();
        return splits;
    }

    private List<Split> doGetSystemTableSplits() throws UserException {
        if (isPositionDeletesSystemTable()) {
            return doGetPositionDeletesSystemTableSplits();
        }
        List<Split> splits = new ArrayList<>();
        TableScan scan = createTableScan();
        long startTime = System.currentTimeMillis();
        try (CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles()) {
            fileScanTasks.forEach(task -> splits.add(createIcebergSysSplit(task)));
        } catch (IOException e) {
            throw new UserException(e.getMessage(), e);
        } finally {
            if (getSummaryProfile() != null) {
                getSummaryProfile().addExternalTableGetFileScanTasksTime(System.currentTimeMillis() - startTime);
            }
        }
        selectedPartitionNum = 0;
        return splits;
    }

    private boolean isPositionDeletesSystemTable() {
        TableIf targetTable = source.getTargetTable();
        return targetTable instanceof IcebergSysExternalTable
                && ((IcebergSysExternalTable) targetTable).isPositionDeletesTable();
    }

    private List<Split> doGetPositionDeletesSystemTableSplits() throws UserException {
        checkPositionDeletesBackendCompatibility(backendPolicy.getBackends());
        List<Split> splits = new ArrayList<>();
        List<PositionDeletesScanTask> positionDeleteTasks = new ArrayList<>();
        BatchScan scan = icebergTable.newBatchScan().metricsReporter(new IcebergMetricsReporter());

        IcebergTableQueryInfo info = getSpecifiedSnapshot();
        if (info != null) {
            if (info.getRef() != null) {
                scan = scan.useRef(info.getRef());
            } else {
                scan = scan.useSnapshot(info.getSnapshotId());
            }
        }

        List<Expression> expressions = new ArrayList<>();
        for (Expr conjunct : conjuncts) {
            Expression expression = IcebergUtils.convertToIcebergExpr(conjunct, icebergTable.schema());
            if (expression != null) {
                expressions.add(expression);
            }
        }
        for (Expression predicate : expressions) {
            scan = scan.filter(predicate);
            this.pushdownIcebergPredicates.add(predicate.toString());
        }

        long startTime = System.currentTimeMillis();
        scan = scan.planWith(source.getCatalog().getThreadPoolWithPreAuth());
        try (CloseableIterable<ScanTask> scanTasks = scan.planFiles()) {
            for (ScanTask task : scanTasks) {
                if (!(task instanceof PositionDeletesScanTask)) {
                    throw new UserException("Unexpected Iceberg position_deletes scan task: " + task);
                }
                positionDeleteTasks.add((PositionDeletesScanTask) task);
            }
        } catch (IOException e) {
            throw new UserException(e.getMessage(), e);
        } finally {
            if (getSummaryProfile() != null) {
                getSummaryProfile().addExternalTableGetFileScanTasksTime(System.currentTimeMillis() - startTime);
            }
        }
        targetSplitSize = determinePositionDeleteTargetSplitSize(positionDeleteTasks);
        for (PositionDeletesScanTask task : positionDeleteTasks) {
            for (PositionDeletesScanTask splitTask : splitPositionDeleteScanTask(task)) {
                splits.add(createIcebergPositionDeleteSysSplit(splitTask));
            }
        }
        selectedPartitionNum = 0;
        return splits;
    }

    @VisibleForTesting
    static void checkPositionDeletesBackendCompatibility(Iterable<Backend> backends) throws UserException {
        for (Backend backend : backends) {
            if (backend.isSmoothUpgradeSrc()) {
                throw new UserException("Iceberg position_deletes system table is unavailable while backend "
                        + backend.getId() + " is a smooth upgrade source");
            }
        }
    }

    @Override
    public boolean isBatchMode() {
        if (isSystemTable) {
            isBatchMode = false;
            return false;
        }
        Boolean cached = isBatchMode;
        if (cached != null) {
            return cached;
        }
        if (isTableLevelCountStarPushdown()) {
            try {
                countFromSnapshot = getCountFromSnapshot();
            } catch (UserException e) {
                throw new RuntimeException(e);
            }
            if (countFromSnapshot >= 0) {
                tableLevelPushDownCount = true;
                isBatchMode = false;
                return false;
            }
        }

        try {
            if (createTableScan().snapshot() == null) {
                isBatchMode = false;
                return false;
            }
        } catch (UserException e) {
            throw new RuntimeException(e);
        }

        if (!sessionVariable.getEnableExternalTableBatchMode()) {
            isBatchMode = false;
            return false;
        }

        try {
            return preExecutionAuthenticator.execute(() -> {
                try (CloseableIterator<ManifestFile> matchingManifest =
                        IcebergUtils.getMatchingManifest(
                                createTableScan().snapshot().dataManifests(icebergTable.io()),
                                icebergTable.specs(),
                                createTableScan().filter()).iterator()) {
                    int cnt = 0;
                    while (matchingManifest.hasNext()) {
                        ManifestFile next = matchingManifest.next();
                        cnt += next.addedFilesCount() + next.existingFilesCount();
                        if (cnt >= sessionVariable.getNumFilesInBatchMode()) {
                            isBatchMode = true;
                            return true;
                        }
                    }
                }
                isBatchMode = false;
                return false;
            });
        } catch (Exception e) {
            Optional<NotSupportedException> opt = checkNotSupportedException(e);
            if (opt.isPresent()) {
                throw opt.get();
            } else {
                throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
            }
        }
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

    private List<IcebergDeleteFileFilter> getDeleteFileFilters(FileScanTask spitTask) {
        List<IcebergDeleteFileFilter> filters = new ArrayList<>();
        for (DeleteFile delete : spitTask.deletes()) {
            if (delete.content() == FileContent.POSITION_DELETES) {
                filters.add(IcebergDeleteFileFilter.createPositionDelete(delete));
            } else if (delete.content() == FileContent.EQUALITY_DELETES) {
                filters.add(IcebergDeleteFileFilter.createEqualityDelete(
                        delete.path().toString(), delete.equalityFieldIds(),
                        delete.fileSizeInBytes(), delete.format()));
            } else {
                throw new IllegalStateException("Unknown delete content: " + delete.content());
            }
        }
        return filters;
    }

    @Override
    public TFileFormatType getFileFormatType() throws UserException {
        if (isSystemTable) {
            return TFileFormatType.FORMAT_JNI;
        }
        // for table level file format
        return toTFileFormatType(IcebergUtils.getFileFormat(icebergTable));
    }

    @Override
    public List<String> getPathPartitionKeys() throws UserException {
        return getOrderedPathPartitionKeys();
    }

    private void recordManifestCacheAccess(boolean cacheHit) {
        if (cacheHit) {
            manifestCacheHits++;
        } else {
            manifestCacheMisses++;
        }
    }

    private void recordManifestCacheProfile() {
        if (!IcebergUtils.isManifestCacheEnabled(source.getCatalog())) {
            return;
        }
        SummaryProfile summaryProfile = SummaryProfile.getSummaryProfile(ConnectContext.get());
        if (summaryProfile == null || summaryProfile.getExecutionSummary() == null) {
            return;
        }
        summaryProfile.getExecutionSummary().addInfoString("Manifest Cache",
                String.format("hits=%d, misses=%d, failures=%d",
                        manifestCacheHits, manifestCacheMisses, manifestCacheFailures));
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws UserException {
        return backendStorageProperties;
    }

    @VisibleForTesting
    public long getCountFromSnapshot() throws UserException {
        IcebergTableQueryInfo info = getSpecifiedSnapshot();

        Snapshot snapshot = info == null
                ? icebergTable.currentSnapshot() : icebergTable.snapshot(info.getSnapshotId());

        // empty table
        if (snapshot == null) {
            return 0;
        }

        return IcebergUtils.getCountFromSummary(snapshot.summary(), sessionVariable.ignoreIcebergDanglingDelete);
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        super.toThrift(planNode);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        String base = super.getNodeExplainString(prefix, detailLevel);
        StringBuilder builder = new StringBuilder(base);

        if (detailLevel == TExplainLevel.VERBOSE && IcebergUtils.isManifestCacheEnabled(source.getCatalog())) {
            builder.append(prefix).append("manifest cache: hits=").append(manifestCacheHits)
                    .append(", misses=").append(manifestCacheMisses)
                    .append(", failures=").append(manifestCacheFailures).append("\n");
        }

        if (!pushdownIcebergPredicates.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String predicate : pushdownIcebergPredicates) {
                sb.append(prefix).append(prefix).append(predicate).append("\n");
            }
            builder.append(String.format("%sicebergPredicatePushdown=\n%s\n", prefix, sb));
        }
        return builder.toString();
    }

    private void assignCountToSplits(List<Split> splits, long totalCount) {
        if (splits.isEmpty()) {
            return;
        }
        int size = splits.size();
        long countPerSplit = totalCount / size;
        for (int i = 0; i < size - 1; i++) {
            ((IcebergSplit) splits.get(i)).setTableLevelRowCount(countPerSplit);
        }
        ((IcebergSplit) splits.get(size - 1)).setTableLevelRowCount(countPerSplit + totalCount % size);
    }

    @Override
    public int numApproximateSplits() {
        return NUM_SPLITS_PER_PARTITION * partitionMapInfos.size() > 0 ? partitionMapInfos.size() : 1;
    }

    private Optional<NotSupportedException> checkNotSupportedException(Exception e) {
        if (e instanceof NullPointerException) {
            /*
        Caused by: java.lang.NullPointerException: Type cannot be null
            at org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkNotNull
                (Preconditions.java:921) ~[iceberg-bundled-guava-1.4.3.jar:?]
            at org.apache.iceberg.types.Types$NestedField.<init>(Types.java:447) ~[iceberg-api-1.4.3.jar:?]
            at org.apache.iceberg.types.Types$NestedField.optional(Types.java:416) ~[iceberg-api-1.4.3.jar:?]
            at org.apache.iceberg.PartitionSpec.partitionType(PartitionSpec.java:132) ~[iceberg-api-1.4.3.jar:?]
            at org.apache.iceberg.DeleteFileIndex.lambda$new$0(DeleteFileIndex.java:97) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.relocated.com.google.common.collect.RegularImmutableMap.forEach
                (RegularImmutableMap.java:297) ~[iceberg-bundled-guava-1.4.3.jar:?]
            at org.apache.iceberg.DeleteFileIndex.<init>(DeleteFileIndex.java:97) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.DeleteFileIndex.<init>(DeleteFileIndex.java:71) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.DeleteFileIndex$Builder.build(DeleteFileIndex.java:578) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.ManifestGroup.plan(ManifestGroup.java:183) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.ManifestGroup.planFiles(ManifestGroup.java:170) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.DataTableScan.doPlanFiles(DataTableScan.java:89) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.SnapshotScan.planFiles(SnapshotScan.java:139) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.doris.datasource.iceberg.source.IcebergScanNode.doGetSplits
                (IcebergScanNode.java:209) ~[doris-fe.jar:1.2-SNAPSHOT]
        EXAMPLE:
             CREATE TABLE iceberg_tb(col1 INT,col2 STRING) USING ICEBERG PARTITIONED BY (bucket(10,col2));
             INSERT INTO iceberg_tb VALUES( ... );
             ALTER TABLE iceberg_tb DROP PARTITION FIELD bucket(10,col2);
             ALTER TABLE iceberg_tb DROP COLUMNS col2;
        Link: https://github.com/apache/iceberg/pull/10755
            */
            LOG.warn("Unable to plan for iceberg table {}", this.desc.getTable().getName(), e);
            return Optional.of(
                    new NotSupportedException("Unable to plan for this table. "
                            + "Maybe read Iceberg table with dropped old partition column. Cause: "
                            + Util.getRootCauseMessage(e)));
        }
        return Optional.empty();
    }
}
