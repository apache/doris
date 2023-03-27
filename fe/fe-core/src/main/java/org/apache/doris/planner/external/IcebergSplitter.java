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

package org.apache.doris.planner.external;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.external.iceberg.util.IcebergUtils;
import org.apache.doris.planner.Split;
import org.apache.doris.planner.Splitter;
import org.apache.doris.planner.external.iceberg.IcebergDeleteFileFilter;
import org.apache.doris.planner.external.iceberg.IcebergScanProvider;
import org.apache.doris.planner.external.iceberg.IcebergSource;
import org.apache.doris.planner.external.iceberg.IcebergSplit;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Conversions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class IcebergSplitter implements Splitter {
    private static final Logger LOG = LogManager.getLogger(IcebergSplitter.class);

    private final IcebergSource icebergSource;
    private final Analyzer analyzer;

    public IcebergSplitter(IcebergSource icebergSource, Analyzer analyzer) {
        this.icebergSource = icebergSource;
        this.analyzer = analyzer;
    }

    @Override
    public List<Split> getSplits(List<Expr> exprs) throws UserException {
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
        List<Split> splits = new ArrayList<>();
        int formatVersion = ((BaseTable) table).operations().current().formatVersion();
        for (FileScanTask task : scan.planFiles()) {
            for (FileScanTask splitTask : task.split(128 * 1024 * 1024)) {
                String dataFilePath = splitTask.file().path().toString();
                IcebergSplit split = new IcebergSplit(new Path(dataFilePath), splitTask.start(),
                        splitTask.length(), new String[0]);
                split.setFormatVersion(formatVersion);
                if (formatVersion >= IcebergScanProvider.MIN_DELETE_FILE_SUPPORT_VERSION) {
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
}
