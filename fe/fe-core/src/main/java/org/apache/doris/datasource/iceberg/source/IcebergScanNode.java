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
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public class IcebergScanNode extends FileQueryScanNode {

    public static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;

    private IcebergSource source;
    private Table icebergTable;
    private List<String> pushdownIcebergPredicates = Lists.newArrayList();

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
        icebergTable = source.getIcebergTable();
        super.doInitialize();
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof IcebergSplit) {
            setIcebergParams(rangeDesc, (IcebergSplit) split);
        }
    }

    public void setIcebergParams(TFileRangeDesc rangeDesc, IcebergSplit icebergSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(icebergSplit.getTableFormatType().value());
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        int formatVersion = icebergSplit.getFormatVersion();
        fileDesc.setFormatVersion(formatVersion);
        fileDesc.setOriginalFilePath(icebergSplit.getOriginalPath());
        if (formatVersion < MIN_DELETE_FILE_SUPPORT_VERSION) {
            fileDesc.setContent(FileContent.DATA.id());
        } else {
            for (IcebergDeleteFileFilter filter : icebergSplit.getDeleteFileFilters()) {
                TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
                String deleteFilePath = filter.getDeleteFilePath();
                LocationPath locationPath = new LocationPath(deleteFilePath, icebergSplit.getConfig());
                Path splitDeletePath = locationPath.toStorageLocation();
                deleteFileDesc.setPath(splitDeletePath.toString());
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
                    deleteFileDesc.setContent(FileContent.POSITION_DELETES.id());
                } else {
                    IcebergDeleteFileFilter.EqualityDelete equalityDelete =
                            (IcebergDeleteFileFilter.EqualityDelete) filter;
                    deleteFileDesc.setFieldIds(equalityDelete.getFieldIds());
                    deleteFileDesc.setContent(FileContent.EQUALITY_DELETES.id());
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
            this.pushdownIcebergPredicates.add(predicate.toString());
        }

        // get splits
        List<Split> splits = new ArrayList<>();
        int formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
        // Min split size is DEFAULT_SPLIT_SIZE(128MB).
        long splitSize = Math.max(ConnectContext.get().getSessionVariable().getFileSplitSize(), DEFAULT_SPLIT_SIZE);
        HashSet<String> partitionPathSet = new HashSet<>();
        boolean isPartitionedTable = icebergTable.spec().isPartitioned();

        long rowCount = getCountFromSnapshot();
        if (getPushDownAggNoGroupingOp().equals(TPushAggOp.COUNT) && rowCount >= 0) {
            this.rowCount = rowCount;
            return new ArrayList<>();
        }

        CloseableIterable<FileScanTask> fileScanTasks = TableScanUtil.splitFiles(scan.planFiles(), splitSize);
        try (CloseableIterable<CombinedScanTask> combinedScanTasks =
                TableScanUtil.planTasks(fileScanTasks, splitSize, 1, 0)) {
            combinedScanTasks.forEach(taskGrp -> taskGrp.files().forEach(splitTask -> {
                String dataFilePath = normalizeLocation(splitTask.file().path().toString());

                List<String> partitionValues = new ArrayList<>();
                if (isPartitionedTable) {
                    StructLike structLike = splitTask.file().partition();
                    List<PartitionField> fields = splitTask.spec().fields();
                    Types.StructType structType = icebergTable.schema().asStruct();

                    // set partitionValue for this IcebergSplit
                    for (int i = 0; i < structLike.size(); i++) {
                        Object obj = structLike.get(i, Object.class);
                        String value = String.valueOf(obj);
                        PartitionField partitionField = fields.get(i);
                        if (partitionField.transform().isIdentity()) {
                            Type type = structType.fieldType(partitionField.name());
                            if (type != null && type.typeId().equals(Type.TypeID.DATE)) {
                                // iceberg use integer to store date,
                                // we need transform it to string
                                value = DateTimeUtil.daysToIsoDate((Integer) obj);
                            }
                        }
                        partitionValues.add(value);
                    }

                    // Counts the number of partitions read
                    partitionPathSet.add(structLike.toString());
                }
                LocationPath locationPath = new LocationPath(dataFilePath, source.getCatalog().getProperties());
                Path finalDataFilePath = locationPath.toStorageLocation();
                IcebergSplit split = new IcebergSplit(
                        finalDataFilePath,
                        splitTask.start(),
                        splitTask.length(),
                        splitTask.file().fileSizeInBytes(),
                        new String[0],
                        formatVersion,
                        source.getCatalog().getProperties(),
                        partitionValues,
                        splitTask.file().path().toString());
                if (formatVersion >= MIN_DELETE_FILE_SUPPORT_VERSION) {
                    split.setDeleteFileFilters(getDeleteFileFilters(splitTask));
                }
                split.setTableFormatType(TableFormatType.ICEBERG);
                splits.add(split);
            }));
        } catch (IOException e) {
            throw new UserException(e.getMessage(), e.getCause());
        }

        selectedPartitionNum = partitionPathSet.size();

        return splits;
    }

    public Long getSpecifiedSnapshot() throws UserException {
        TableSnapshot tableSnapshot = source.getDesc().getRef().getTableSnapshot();
        if (tableSnapshot == null) {
            tableSnapshot = this.tableSnapshot;
        }
        if (tableSnapshot != null) {
            TableSnapshot.VersionType type = tableSnapshot.getType();
            try {
                if (type == TableSnapshot.VersionType.VERSION) {
                    return tableSnapshot.getVersion();
                } else {
                    long timestamp = TimeUtils.timeStringToLong(tableSnapshot.getTime(), TimeUtils.getTimeZone());
                    return SnapshotUtil.snapshotIdAsOfTime(icebergTable, timestamp);
                }
            } catch (IllegalArgumentException e) {
                throw new UserException(e);
            }
        }
        return null;
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
                filters.add(IcebergDeleteFileFilter.createEqualityDelete(
                        delete.path().toString(), delete.equalityFieldIds()));
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
        return Optional.ofNullable(LocationPath.getTFileTypeForBE(location)).orElseThrow(() ->
                new DdlException("Unknown file location " + fLocation + " for iceberg table " + icebergTable.name()));
    }

    private String normalizeLocation(String location) {
        Map<String, String> props = source.getCatalog().getProperties();
        LocationPath locationPath = new LocationPath(location, props);
        String icebergCatalogType = props.get(IcebergExternalCatalog.ICEBERG_CATALOG_TYPE);
        if ("hadoop".equalsIgnoreCase(icebergCatalogType)) {
            // if no scheme info, fill will HADOOP_FS_NAME
            // if no HADOOP_FS_NAME, then should be local file system
            if (locationPath.getLocationType() == LocationPath.LocationType.NOSCHEME) {
                String fsName = props.get(HdfsResource.HADOOP_FS_NAME);
                if (fsName != null) {
                    location = fsName + location;
                }
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
            return 0;
        }

        Map<String, String> summary = snapshot.summary();
        if (summary.get(IcebergUtils.TOTAL_EQUALITY_DELETES).equals("0")) {
            return Long.parseLong(summary.get(IcebergUtils.TOTAL_RECORDS))
                - Long.parseLong(summary.get(IcebergUtils.TOTAL_POSITION_DELETES));
        } else {
            return -1;
        }
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        super.toThrift(planNode);
        if (getPushDownAggNoGroupingOp().equals(TPushAggOp.COUNT)) {
            long countFromSnapshot = getCountFromSnapshot();
            if (countFromSnapshot >= 0) {
                planNode.setPushDownCount(countFromSnapshot);
            }
        }
    }

    @Override
    public long getPushDownCount() {
        return getCountFromSnapshot();
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (pushdownIcebergPredicates.isEmpty()) {
            return super.getNodeExplainString(prefix, detailLevel);
        }
        StringBuilder sb = new StringBuilder();
        for (String predicate : pushdownIcebergPredicates) {
            sb.append(prefix).append(prefix).append(predicate).append("\n");
        }
        return super.getNodeExplainString(prefix, detailLevel)
                + String.format("%sicebergPredicatePushdown=\n%s\n", prefix, sb);
    }

    public void setTableSnapshot(TableSnapshot tableSnapshot) {
        this.tableSnapshot = tableSnapshot;
    }
}
