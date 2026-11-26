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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.spi.ConnectorTableStatistics;
import org.apache.doris.thrift.TDeltaLakeDeletionVectorDesc;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.IntervalParserUtils;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** The only production class coupled to Kernel internal APIs; pinned to Kernel 4.4.1. */
final class DeltaKernelCompat {
    private static final Pattern CALENDAR_TERM = Pattern.compile(
            "(?<=\\p{javaWhitespace})((?:[+-]\\p{javaWhitespace}*)?)([0-9]+)(\\p{javaWhitespace}+)(year|month)s?\\b");

    private DeltaKernelCompat() {
    }

    static String tableId(Snapshot snapshot) {
        return snapshot.getMetadata().getId();
    }

    static long deletedFileRetentionMillis(Map<String, String> properties) {
        TableConfig<Long> retention = TableConfig.TOMBSTONE_RETENTION;
        String value = properties.get(retention.getKey());
        if (value == null) {
            return retention.fromMetadata(properties);
        }
        String interval = value.trim().toLowerCase(Locale.ROOT);
        if (!interval.startsWith("interval ")) {
            interval = "interval " + interval;
        }
        long micros = IntervalParserUtils.parseIntervalAsMicros(withoutCalendarMonths(interval));
        if (micros < 0) {
            throw new IllegalArgumentException(retention.getKey() + " must be non-negative: " + value);
        }
        return TimeUnit.MICROSECONDS.toMillis(micros);
    }

    // Spark accepts a zero total calendar-month component. Kernel 4.4.1 rejects month and year tokens outright.
    private static String withoutCalendarMonths(String interval) {
        Matcher term = CALENDAR_TERM.matcher(interval);
        if (!term.find()) {
            return interval;
        }
        int months = 0;
        StringBuilder fixed = new StringBuilder(interval.length());
        do {
            String sign = term.group(1);
            try {
                long quantity = Long.parseLong(term.group(2));
                if (sign.startsWith("-")) {
                    quantity = -quantity;
                }
                if ("year".equals(term.group(4))) {
                    months = Math.toIntExact(Math.addExact(months, Math.multiplyExact(quantity, 12L)));
                } else {
                    months = Math.addExact(months, Math.toIntExact(quantity));
                }
            } catch (ArithmeticException | NumberFormatException e) {
                throw new IllegalArgumentException("Calendar months out of range in Delta retention: " + interval, e);
            }
            // Keep the sign and separators so malformed tokens still fail Kernel's syntax checks.
            term.appendReplacement(fixed, sign + "0" + term.group(3) + "days");
        } while (term.find());
        if (months != 0) {
            throw new IllegalArgumentException("Delta retention requires zero total calendar months: " + interval);
        }
        return term.appendTail(fixed).toString();
    }

    static OptionalLong earliestHistoryVersion(Engine engine, Snapshot snapshot) throws IOException {
        OptionalLong staged = snapshot.getLogSegment().getAllCatalogCommits().stream()
                .mapToLong(commit -> commit.getVersion()).min();
        if (staged.orElse(-1) == 0) {
            // UC can ratify v0 before any numbered JSON exists in _delta_log.
            return staged;
        }
        Path logPath = new Path(snapshot.getPath(), "_delta_log");
        try (CloseableIterator<FileStatus> files = engine.getFileSystemClient()
                .listFrom(FileNames.listingPrefix(logPath, 0))) {
            while (files.hasNext()) {
                String path = files.next().getPath();
                if (FileNames.isPublishedDeltaFile(path)) {
                    long published = FileNames.deltaVersion(path);
                    return OptionalLong.of(Math.min(published, staged.orElse(published)));
                }
            }
        }
        return staged;
    }

    static OptionalLong historyIctStart(Snapshot snapshot) {
        var metadata = snapshot.getMetadata();
        if (!TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
            return OptionalLong.empty();
        }
        Optional<Long> version = TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetadata(metadata);
        Optional<Long> timestamp = TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetadata(metadata);
        if (version.isPresent() != timestamp.isPresent()) {
            throw new IllegalStateException("Incomplete Delta in-commit timestamp enablement metadata");
        }
        return OptionalLong.of(version.orElse(0L));
    }

    static CloseableIterator<FileStatus> singleLogFile(FileStatus file) {
        return Utils.toCloseableIterator(List.of(file).iterator());
    }

    record FileTotals(long files, long bytes) {
    }

    static Optional<FileTotals> fileTotals(Snapshot snapshot) {
        return snapshot.getCurrentCrcInfo().map(crc -> new FileTotals(crc.getNumFiles(), crc.getTableSizeBytes()));
    }

    static String tableDescription(Snapshot snapshot) {
        return snapshot.getMetadata().getDescription().orElse(null);
    }

    static Optional<Long> createdTime(Snapshot snapshot) {
        return snapshot.getMetadata().getCreatedTime();
    }

    static int minReaderVersion(Snapshot snapshot) {
        return snapshot.getProtocol().getMinReaderVersion();
    }

    static int minWriterVersion(Snapshot snapshot) {
        return snapshot.getProtocol().getMinWriterVersion();
    }

    static List<String> tableFeatures(Snapshot snapshot) {
        return snapshot.getProtocol().getReaderAndWriterFeatures().stream().sorted().toList();
    }

    static OptionalLong fileCount(Snapshot snapshot) {
        return snapshot.getCurrentCrcInfo().map(crc -> OptionalLong.of(crc.getNumFiles()))
                .orElseGet(OptionalLong::empty);
    }

    static Optional<ConnectorTableStatistics> tableStatistics(Snapshot snapshot) {
        // A version checksum has no total row count. Only an empty active-file set proves zero rows.
        return snapshot.getCurrentCrcInfo().map(crc -> new ConnectorTableStatistics(
                crc.getNumFiles() == 0 ? 0 : -1, crc.getTableSizeBytes()));
    }

    static void prepareForConcurrentReads(Snapshot snapshot) {
        // Kernel's Lazy is not synchronized. Materialize read-side metadata before sharing a snapshot.
        snapshot.getSchema();
        snapshot.getTableProperties();
        snapshot.getMetadata().getDataSchema();
        snapshot.getScanBuilder();
        snapshot.getCurrentCrcInfo();
        snapshot.getLogSegment().allFilesWithCompactionsReversed();
    }

    static long snapshotRetainedBytes(Snapshot snapshot) {
        prepareForConcurrentReads(snapshot);
        LogSegment segment = snapshot.getLogSegment();
        // Kernel 4.4.1 retains metadata/schema copies, lazy lookup maps and log FileStatus lists.
        // Charge a conservative expansion of schema JSON and CRC bytes, including optional allFiles
        // embedded in a CRC. The catalog's shared Engine/FileIO service graph is not entry-owned.
        long bytes = 16384L + 32L * snapshot.getMetadata().getSchemaString().length();
        for (Map.Entry<String, String> property : snapshot.getTableProperties().entrySet()) {
            bytes = JvmSizeUtils.saturatedAdd(bytes,
                    128L + 4L * (property.getKey().length() + property.getValue().length()));
        }
        for (FileStatus file : segment.allLogFilesReversed()) {
            bytes = JvmSizeUtils.saturatedAdd(bytes, 256L + 4L * file.getPath().length());
        }
        for (FileStatus file : segment.getCompactions()) {
            bytes = JvmSizeUtils.saturatedAdd(bytes, 256L + 4L * file.getPath().length());
        }
        if (segment.getLastSeenChecksum().isPresent()) {
            long size = segment.getLastSeenChecksum().get().getSize();
            bytes = JvmSizeUtils.saturatedAdd(bytes, size > Long.MAX_VALUE / 32 ? Long.MAX_VALUE : size * 32);
        }
        return bytes;
    }

    static FileStatus fileStatus(Row file) {
        return InternalScanFileUtils.getAddFileStatus(file);
    }

    static Map<String, String> partitionValues(Row file) {
        return InternalScanFileUtils.getPartitionValues(file);
    }

    static Object partitionLiteral(DataType type, String value) {
        return PartitionUtils.literalForPartitionValue(type, value).getValue();
    }

    static java.util.function.Predicate<Map<String, String>> normalizedPartitionFilter(
            Engine engine, StructType schema, Predicate predicate) {
        PredicateEvaluator evaluator = engine.getExpressionHandler().getPredicateEvaluator(schema, predicate);
        // A single split producer owns this one-row batch. Consume the selection before updating the next row.
        Object[][] values = new Object[schema.length()][1];
        ColumnVector[] columns = new ColumnVector[schema.length()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = DefaultGenericVector.fromArray(schema.at(i).getDataType(), values[i]);
        }
        DefaultColumnarBatch batch = new DefaultColumnarBatch(1, schema, columns);
        return partitions -> {
            for (int i = 0; i < columns.length; i++) {
                StructField field = schema.at(i);
                values[i][0] = normalizedPartitionLiteral(field.getDataType(), partitions.get(field.getName()));
            }
            try (ColumnVector keep = evaluator.eval(batch, Optional.empty())) {
                return !keep.isNullAt(0) && keep.getBoolean(0);
            }
        };
    }

    private static Object normalizedPartitionLiteral(DataType type, String value) {
        if (value == null) {
            return null;
        }
        if (type instanceof DateType) {
            return Math.toIntExact(LocalDate.parse(value).toEpochDay());
        }
        if (type instanceof TimestampNTZType) {
            // Normalized NTZ values are ISO local times. Kernel represents them as wall-clock microseconds.
            return PartitionUtils.tryParseTimestamp(value + "Z");
        }
        if (type instanceof BinaryType) {
            return value.getBytes(StandardCharsets.UTF_8);
        }
        return partitionLiteral(type, value);
    }

    static OptionalLong numRecords(Row file) {
        Row add = file.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL);
        int ordinal = InternalScanFileUtils.ADD_FILE_STATS_ORDINAL;
        if (add.isNullAt(ordinal)) {
            return OptionalLong.empty();
        }
        Optional<Long> count = DataFileStatistics.getNumRecords(add.getString(ordinal));
        return count.isPresent() ? OptionalLong.of(count.get()) : OptionalLong.empty();
    }

    static long deletionCardinality(Row file) {
        DeletionVectorDescriptor dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(file);
        return dv == null ? 0 : dv.getCardinality();
    }

    record FileDeletionVector(String storageType, String path, long cardinality) {
    }

    static FileDeletionVector fileDeletionVector(Row row, String tablePath, boolean needPath) {
        DeletionVectorDescriptor dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(row);
        if (dv == null) {
            return null;
        }
        String path = needPath && !dv.isInline() ? dv.getAbsolutePath(tablePath) : null;
        return new FileDeletionVector(dv.getStorageType(), path, dv.getCardinality());
    }

    static Optional<TDeltaLakeDeletionVectorDesc> deletionVector(Row file, String tablePath) {
        DeletionVectorDescriptor dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(file);
        if (dv == null || dv.getCardinality() == 0) {
            return Optional.empty();
        }
        TDeltaLakeDeletionVectorDesc result = new TDeltaLakeDeletionVectorDesc();
        result.setSizeInBytes(dv.getSizeInBytes());
        result.setCardinality(dv.getCardinality());
        if (dv.isInline()) {
            result.setInlineData(dv.inlineData());
        } else {
            result.setPath(dv.getAbsolutePath(tablePath));
            result.setOffset(dv.getOffset().orElse(0));
        }
        return Optional.of(result);
    }

    static Set<String> readerFeatures(Snapshot snapshot) {
        return readerFeatures(snapshot.getProtocol());
    }

    static Set<String> readerFeatures(Protocol protocol) {
        // Kernel 4.4.1's legacy-protocol branch also selects writer-only features (reader version 0).
        // Those do not require reader support and must not reject an otherwise readable table.
        return protocol.getImplicitlyAndExplicitlySupportedReaderWriterFeatures().stream()
                .filter(feature -> feature.minReaderVersion() > 0)
                .map(TableFeature::featureName).collect(Collectors.toSet());
    }
}
