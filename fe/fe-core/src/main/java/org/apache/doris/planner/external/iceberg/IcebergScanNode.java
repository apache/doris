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
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
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
import org.apache.doris.thrift.TTableFormatFileDesc;

import avro.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Conversions;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public class IcebergScanNode extends FileQueryScanNode {

    public static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;

    private IcebergSource source;

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
                deleteFileDesc.setPath(S3Util.toScanRangeLocation(deleteFilePath).toString());
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
        List<Expression> expressions = new ArrayList<>();
        org.apache.iceberg.Table table = source.getIcebergTable();
        for (Expr conjunct : conjuncts) {
            Expression expression = IcebergUtils.convertToIcebergExpr(conjunct, table.schema());
            if (expression != null) {
                expressions.add(expression);
            }
        }
        TableScan scan = table.newScan();
        TableSnapshot tableSnapshot = source.getDesc().getRef().getTableSnapshot();
        if (tableSnapshot != null) {
            TableSnapshot.VersionType type = tableSnapshot.getType();
            try {
                if (type == TableSnapshot.VersionType.VERSION) {
                    scan = scan.useSnapshot(tableSnapshot.getVersion());
                } else {
                    long snapshotId = TimeUtils.timeStringToLong(tableSnapshot.getTime(), TimeUtils.getTimeZone());
                    scan = scan.useSnapshot(getSnapshotIdAsOfTime(table.history(), snapshotId));
                }
            } catch (IllegalArgumentException e) {
                throw new UserException(e);
            }
        }
        for (Expression predicate : expressions) {
            scan = scan.filter(predicate);
        }
        List<Split> splits = new ArrayList<>();
        int formatVersion = ((BaseTable) table).operations().current().formatVersion();
        // Min split size is DEFAULT_SPLIT_SIZE(128MB).
        long splitSize = Math.max(ConnectContext.get().getSessionVariable().getFileSplitSize(), DEFAULT_SPLIT_SIZE);
        for (FileScanTask task : scan.planFiles()) {
            long fileSize = task.file().fileSizeInBytes();
            for (FileScanTask splitTask : task.split(splitSize)) {
                String dataFilePath = splitTask.file().path().toString();
                Path finalDataFilePath = S3Util.toScanRangeLocation(dataFilePath);
                IcebergSplit split = new IcebergSplit(finalDataFilePath, splitTask.start(),
                        splitTask.length(), fileSize, new String[0]);
                split.setFormatVersion(formatVersion);
                if (formatVersion >= MIN_DELETE_FILE_SUPPORT_VERSION) {
                    split.setDeleteFileFilters(getDeleteFileFilters(splitTask));
                }
                split.setTableFormatType(TableFormatType.ICEBERG);
                splits.add(split);
            }
        }
        return splits;
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
        Table icebergTable = source.getIcebergTable();
        String location = icebergTable.location();
        return getLocationType(location);
    }

    @Override
    public TFileType getLocationType(String location) throws UserException {
        Table icebergTable = source.getIcebergTable();
        return getTFileType(location).orElseThrow(() ->
            new DdlException("Unknown file location " + location + " for iceberg table " + icebergTable.name()));
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
        return source.getIcebergTable().spec().fields().stream().map(PartitionField::name).map(String::toLowerCase)
            .collect(Collectors.toList());
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws UserException {
        return source.getCatalog().getProperties();
    }
}
