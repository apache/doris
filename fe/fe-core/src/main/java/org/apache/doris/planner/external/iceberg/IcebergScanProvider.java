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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.external.iceberg.util.IcebergUtils;
import org.apache.doris.planner.external.ExternalFileScanNode;
import org.apache.doris.planner.external.QueryScanProvider;
import org.apache.doris.planner.external.TableFormatType;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
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

/**
 * A file scan provider for iceberg.
 */
public class IcebergScanProvider extends QueryScanProvider {

    private static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;
    private final Analyzer analyzer;
    private final IcebergSource icebergSource;

    public IcebergScanProvider(IcebergSource icebergSource, Analyzer analyzer) {
        this.icebergSource = icebergSource;
        this.analyzer = analyzer;
    }

    public static void setIcebergParams(TFileRangeDesc rangeDesc, IcebergSplit icebergSplit)
            throws UserException {
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
                deleteFileDesc.setPath(filter.getDeleteFilePath());
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
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        String location = icebergSource.getIcebergTable().location();
        if (location != null && !location.isEmpty()) {
            if (location.startsWith(FeConstants.FS_PREFIX_HDFS)
                || location.startsWith(FeConstants.FS_PREFIX_COSN)) {
                return TFileType.FILE_HDFS;
            } else if (location.startsWith(FeConstants.FS_PREFIX_S3)
                || location.startsWith(FeConstants.FS_PREFIX_S3A)
                || location.startsWith(FeConstants.FS_PREFIX_S3N)
                || location.startsWith(FeConstants.FS_PREFIX_BOS)
                || location.startsWith(FeConstants.FS_PREFIX_COS)
                || location.startsWith(FeConstants.FS_PREFIX_OSS)
                || location.startsWith(FeConstants.FS_PREFIX_OBS)) {
                return TFileType.FILE_S3;
            } else if (location.startsWith(FeConstants.FS_PREFIX_FILE)) {
                return TFileType.FILE_LOCAL;
            } else if (location.startsWith(FeConstants.FS_PREFIX_OFS)) {
                return TFileType.FILE_BROKER;
            } else if (location.startsWith(FeConstants.FS_PREFIX_GFS)) {
                return TFileType.FILE_HDFS;
            } else if (location.startsWith(FeConstants.FS_PREFIX_JFS)) {
                return TFileType.FILE_BROKER;
            }
        }
        throw new DdlException("Unknown file location " + location
            + " for hms table " + icebergSource.getIcebergTable().name());
    }

    @Override
    public List<InputSplit> getSplits(List<Expr> exprs) throws UserException {
        List<Expression> expressions = new ArrayList<>();
        org.apache.iceberg.Table table = icebergSource.getIcebergTable();
        for (Expr conjunct : exprs) {
            Expression expression = IcebergUtils.convertToIcebergExpr(conjunct, table.schema());
            if (expression != null) {
                expressions.add(expression);
            }
        }
        TableScan scan = table.newScan();
        TableSnapshot tableSnapshot = icebergSource.getDesc().getRef().getTableSnapshot();
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
        List<InputSplit> splits = new ArrayList<>();
        int formatVersion = ((BaseTable) table).operations().current().formatVersion();
        for (FileScanTask task : scan.planFiles()) {
            for (FileScanTask splitTask : task.split(128 * 1024 * 1024)) {
                String dataFilePath = splitTask.file().path().toString();
                IcebergSplit split = new IcebergSplit(new Path(dataFilePath), splitTask.start(),
                        splitTask.length(), task.file().fileSizeInBytes(), new String[0]);
                split.setFormatVersion(formatVersion);
                if (formatVersion >= MIN_DELETE_FILE_SUPPORT_VERSION) {
                    split.setDeleteFileFilters(getDeleteFileFilters(splitTask));
                }
                split.setTableFormatType(TableFormatType.ICEBERG);
                split.setAnalyzer(analyzer);
                splits.add(split);
            }
        }
        return splits;
    }

    public static long getSnapshotIdAsOfTime(List<HistoryEntry> historyEntries, long asOfTimestamp) {
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
                // delete.equalityFieldIds()));
                throw new IllegalStateException("Don't support equality delete file");
            } else {
                throw new IllegalStateException("Unknown delete content: " + delete.content());
            }
        }
        return filters;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return icebergSource.getIcebergTable().spec().fields().stream().map(PartitionField::name)
                .collect(Collectors.toList());
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        TFileFormatType type;
        String icebergFormat = icebergSource.getFileFormat();
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
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return icebergSource.getCatalog().getProperties();
    }

    @Override
    public ExternalFileScanNode.ParamCreateContext createContext(Analyzer analyzer) throws UserException {
        return icebergSource.createContext();
    }

    public void updateRequiredSlots(ExternalFileScanNode.ParamCreateContext context) throws UserException {
        icebergSource.updateRequiredSlots(context);
    }

    @Override
    public TableIf getTargetTable() {
        return icebergSource.getTargetTable();
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return icebergSource.getFileAttributes();
    }
}
