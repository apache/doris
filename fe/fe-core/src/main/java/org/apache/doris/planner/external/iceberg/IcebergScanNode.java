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

package org.apache.doris.planner.external.iceberg;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.catalog.external.IcebergExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.external.iceberg.util.IcebergUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.FileQueryScanNode;
import org.apache.doris.planner.external.TableFormatType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TTableFormatFileDesc;

import avro.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.util.TableScanUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public class IcebergScanNode extends FileQueryScanNode {

    public static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;
    public static final String DEFAULT_DATA_PATH = "/data/";
    private static final String TOTAL_RECORDS = "total-records";
    private static final String TOTAL_POSITION_DELETES = "total-position-deletes";
    private static final String TOTAL_EQUALITY_DELETES = "total-equality-deletes";

    private IcebergSource source;
    private Table icebergTable;

    /**
     * External file scan node for Query iceberg table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "ICEBERG_SCAN_NODE", StatisticalType.ICEBERG_SCAN_NODE, needCheckColumnPriv);

        ExternalTable table = (ExternalTable) desc.getTable();
        if (table instanceof HMSExternalTable) {
            source = new IcebergHMSSource((HMSExternalTable) table, desc, columnNameToRange);
        } else if (table instanceof IcebergExternalTable) {
            String catalogType = ((IcebergExternalTable) table).getIcebergCatalogType();
            switch (catalogType) {
                case IcebergExternalCatalog.ICEBERG_HMS:
                case IcebergExternalCatalog.ICEBERG_REST:
                case IcebergExternalCatalog.ICEBERG_DLF:
                case IcebergExternalCatalog.ICEBERG_GLUE:
                case IcebergExternalCatalog.ICEBERG_HADOOP:
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
        icebergTable = Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache().getIcebergTable(source);
        super.doInitialize();
    }

    public static void setIcebergParams(TFileRangeDesc rangeDesc, IcebergSplit icebergSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(icebergSplit.getTableFormatType().value());
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        int formatVersion = icebergSplit.getFormatVersion();
        fileDesc.setFormatVersion(formatVersion);
        if (formatVersion < MIN_DELETE_FILE_SUPPORT_VERSION) {
            fileDesc.setContent(FileContent.DATA.id());
        } else {
            for (IcebergDeleteFileFilter filter : icebergSplit.getDeleteFileFilters()) {
                TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
                String deleteFilePath = filter.getDeleteFilePath();
                deleteFileDesc.setPath(S3Util.toScanRangeLocation(deleteFilePath, icebergSplit.getConfig()).toString());
                if (filter instanceof IcebergDeleteFileFilter.PositionDelete) {
                    fileDesc.setContent(FileContent.POSITION_DELETES.id());
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
                } else {
                    fileDesc.setContent(FileContent.EQUALITY_DELETES.id());
                    IcebergDeleteFileFilter.EqualityDelete equalityDelete =
                            (IcebergDeleteFileFilter.EqualityDelete) filter;
                    deleteFileDesc.setFieldIds(equalityDelete.getFieldIds());
                }
                fileDesc.addToDeleteFiles(deleteFileDesc);
            }
        }
        tableFormatFileDesc.setIcebergParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits() throws UserException {
        return HiveMetaStoreClientHelper.ugiDoAs(source.getCatalog().getConfiguration(), this::doGetSplits);
    }

    private List<Split> doGetSplits() throws UserException {

        TableScan scan = icebergTable.newScan();

        // set snapshot
        Long snapshotId = getSpecifiedSnapshot();
        if (snapshotId != null) {
            scan = scan.useSnapshot(snapshotId);
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
        }

        // get splits
        List<Split> splits = new ArrayList<>();
        int formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
        // Min split size is DEFAULT_SPLIT_SIZE(128MB).
        long splitSize = Math.max(ConnectContext.get().getSessionVariable().getFileSplitSize(), DEFAULT_SPLIT_SIZE);
        HashSet<String> partitionPathSet = new HashSet<>();
        String dataPath = normalizeLocation(icebergTable.location()) + icebergTable.properties()
                .getOrDefault(TableProperties.WRITE_DATA_LOCATION, DEFAULT_DATA_PATH);
        boolean isPartitionedTable = icebergTable.spec().isPartitioned();

        CloseableIterable<FileScanTask> fileScanTasks = TableScanUtil.splitFiles(scan.planFiles(), splitSize);
        try (CloseableIterable<CombinedScanTask> combinedScanTasks =
                TableScanUtil.planTasks(fileScanTasks, splitSize, 1, 0)) {
            combinedScanTasks.forEach(taskGrp -> taskGrp.files().forEach(splitTask -> {
                String dataFilePath = normalizeLocation(splitTask.file().path().toString());

                // Counts the number of partitions read
                if (isPartitionedTable) {
                    int last = dataFilePath.lastIndexOf("/");
                    if (last > 0) {
                        partitionPathSet.add(dataFilePath.substring(dataPath.length(), last));
                    }
                }

                Path finalDataFilePath = S3Util.toScanRangeLocation(dataFilePath, source.getCatalog().getProperties());
                IcebergSplit split = new IcebergSplit(
                        finalDataFilePath,
                        splitTask.start(),
                        splitTask.length(),
                        splitTask.file().fileSizeInBytes(),
                        new String[0],
                        formatVersion,
                        source.getCatalog().getProperties());
                if (formatVersion >= MIN_DELETE_FILE_SUPPORT_VERSION) {
                    split.setDeleteFileFilters(getDeleteFileFilters(splitTask));
                }
                split.setTableFormatType(TableFormatType.ICEBERG);
                splits.add(split);
            }));
        } catch (IOException e) {
            throw new UserException(e.getMessage(), e.getCause());
        }

        TPushAggOp aggOp = getPushDownAggNoGroupingOp();
        if (aggOp.equals(TPushAggOp.COUNT) && getCountFromSnapshot() > 0) {
            // we can create a special empty split and skip the plan process
            return Collections.singletonList(splits.get(0));
        }

        readPartitionNum = partitionPathSet.size();

        return splits;
    }

    public Long getSpecifiedSnapshot() throws UserException {
        TableSnapshot tableSnapshot = source.getDesc().getRef().getTableSnapshot();
        if (tableSnapshot != null) {
            TableSnapshot.VersionType type = tableSnapshot.getType();
            try {
                if (type == TableSnapshot.VersionType.VERSION) {
                    return tableSnapshot.getVersion();
                } else {
                    long snapshotId = TimeUtils.timeStringToLong(tableSnapshot.getTime(), TimeUtils.getTimeZone());
                    return getSnapshotIdAsOfTime(icebergTable.history(), snapshotId);
                }
            } catch (IllegalArgumentException e) {
                throw new UserException(e);
            }
        }
        return null;
    }

    private long getSnapshotIdAsOfTime(List<HistoryEntry> historyEntries, long asOfTimestamp) {
        // find history at or before asOfTimestamp
        HistoryEntry latestHistory = null;
        for (HistoryEntry entry : historyEntries) {
            if (entry.timestampMillis() <= asOfTimestamp) {
                if (latestHistory == null) {
                    latestHistory = entry;
                    continue;
                }
                if (entry.timestampMillis() > latestHistory.timestampMillis()) {
                    latestHistory = entry;
                }
            }
        }
        if (latestHistory == null) {
            throw new NotFoundException("No version history at or before "
                    + Instant.ofEpochMilli(asOfTimestamp));
        }
        return latestHistory.snapshotId();
    }

    private List<IcebergDeleteFileFilter> getDeleteFileFilters(FileScanTask spitTask) {
        List<IcebergDeleteFileFilter> filters = new ArrayList<>();
        for (DeleteFile delete : spitTask.deletes()) {
            if (delete.content() == FileContent.POSITION_DELETES) {
                ByteBuffer lowerBoundBytes = delete.lowerBounds().get(MetadataColumns.DELETE_FILE_POS.fieldId());
                Optional<Long> positionLowerBound = Optional.ofNullable(lowerBoundBytes)
                        .map(bytes -> Conversions.fromByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), bytes));
                ByteBuffer upperBoundBytes = delete.upperBounds().get(MetadataColumns.DELETE_FILE_POS.fieldId());
                Optional<Long> positionUpperBound = Optional.ofNullable(upperBoundBytes)
                        .map(bytes -> Conversions.fromByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), bytes));
                filters.add(IcebergDeleteFileFilter.createPositionDelete(delete.path().toString(),
                        positionLowerBound.orElse(-1L), positionUpperBound.orElse(-1L)));
            } else if (delete.content() == FileContent.EQUALITY_DELETES) {
                // todo: filters.add(IcebergDeleteFileFilter.createEqualityDelete(delete.path().toString(),
                throw new IllegalStateException("Don't support equality delete file");
            } else {
                throw new IllegalStateException("Unknown delete content: " + delete.content());
            }
        }
        return filters;
    }

    @Override
    public TFileType getLocationType() throws UserException {
        String location = icebergTable.location();
        return getLocationType(location);
    }

    @Override
    public TFileType getLocationType(String location) throws UserException {
        final String fLocation = normalizeLocation(location);
        return getTFileType(fLocation).orElseThrow(() ->
                new DdlException("Unknown file location " + fLocation + " for iceberg table " + icebergTable.name()));
    }

    private String normalizeLocation(String location) {
        Map<String, String> props = source.getCatalog().getProperties();
        String icebergCatalogType = props.get(IcebergExternalCatalog.ICEBERG_CATALOG_TYPE);
        if ("hadoop".equalsIgnoreCase(icebergCatalogType)) {
            if (!location.startsWith(HdfsResource.HDFS_PREFIX)) {
                String fsName = props.get(HdfsResource.HADOOP_FS_NAME);
                location = fsName + location;
            }
        }
        return location;
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
    public TFileAttributes getFileAttributes() throws UserException {
        return source.getFileAttributes();
    }

    @Override
    public List<String> getPathPartitionKeys() throws UserException {
        return icebergTable.spec().fields().stream().map(PartitionField::name).map(String::toLowerCase)
                .collect(Collectors.toList());
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws UserException {
        return source.getCatalog().getCatalogProperty().getHadoopProperties();
    }

    @Override
    public boolean pushDownAggNoGrouping(FunctionCallExpr aggExpr) {
        String aggFunctionName = aggExpr.getFnName().getFunction().toUpperCase();
        return "COUNT".equals(aggFunctionName);
    }

    @Override
    public boolean pushDownAggNoGroupingCheckCol(FunctionCallExpr aggExpr, Column col) {
        return !col.isAllowNull();
    }

    private long getCountFromSnapshot() {
        Long specifiedSnapshot;
        try {
            specifiedSnapshot = getSpecifiedSnapshot();
        } catch (UserException e) {
            return -1;
        }

        Snapshot snapshot = specifiedSnapshot == null
                ? icebergTable.currentSnapshot() : icebergTable.snapshot(specifiedSnapshot);

        // empty table
        if (snapshot == null) {
            return -1;
        }

        Map<String, String> summary = snapshot.summary();
        if (summary.get(TOTAL_EQUALITY_DELETES).equals("0")) {
            return Long.parseLong(summary.get(TOTAL_RECORDS)) - Long.parseLong(summary.get(TOTAL_POSITION_DELETES));
        } else {
            return -1;
        }
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        super.toThrift(planNode);
        if (getPushDownAggNoGroupingOp().equals(TPushAggOp.COUNT)) {
            long countFromSnapshot = getCountFromSnapshot();
            if (countFromSnapshot > 0) {
                planNode.setPushDownCount(countFromSnapshot);
            }
        }
    }
}
