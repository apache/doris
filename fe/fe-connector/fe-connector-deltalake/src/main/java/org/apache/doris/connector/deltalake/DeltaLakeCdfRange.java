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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScopes;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/** Inclusive, statement-bound change interval. The schema snapshot is a separate coordinate. */
record DeltaLakeCdfRange(long start, long end, long schemaVersion) implements Serializable {
    static final String START = "deltalake.cdf.start";
    private static final Set<String> KEYS = Set.of("start_version", "start_timestamp", "end_version", "end_timestamp");
    private static final StructType CONTROL = new StructType()
            .add("commitInfo", new StructType().add("inCommitTimestamp", LongType.LONG))
            .add("metaData", new StructType().add("schemaString", StringType.STRING))
            .add("protocol", new StructType().add("minReaderVersion", IntegerType.INTEGER))
            .add("cdc", new StructType().add("path", StringType.STRING));

    record Commit(long version, long timestamp, FileStatus file, boolean definitionChanged, boolean hasCdc) {
    }

    ConnectorMvccSnapshot pin(String tableId) {
        return ConnectorMvccSnapshot.builder().snapshotId(end).schemaId(schemaVersion).retainSchema(true)
                .property(DeltaLakeConnectorMetadata.TABLE_ID, tableId).property(START, Long.toString(start)).build();
    }

    static DeltaLakeCdfRange fromPin(ConnectorMvccSnapshot pin) {
        return new DeltaLakeCdfRange(Long.parseLong(pin.getProperties().get(START)), pin.getSnapshotId(),
                pin.getSchemaId());
    }

    static DeltaLakeCdfRange resolve(ConnectorSession session, DeltaLakeRuntime runtime, DeltaLakeTableHandle handle,
            Map<String, String> arguments) throws IOException {
        Map<String, String> bounds = new TreeMap<>();
        arguments.forEach((key, value) -> {
            String lower = key.toLowerCase(Locale.ROOT);
            if (!KEYS.contains(lower) || bounds.putIfAbsent(lower, value) != null) {
                throw new DorisConnectorException("Unknown or duplicate Delta @incr boundary: " + key);
            }
        });
        if (bounds.containsKey("start_version") == bounds.containsKey("start_timestamp")
                || (bounds.containsKey("end_version") && bounds.containsKey("end_timestamp"))) {
            throw new DorisConnectorException(
                    "Delta $changes requires one start boundary and at most one end boundary");
        }
        // All CDF aliases (including open-ended ranges) share one latest generation per statement.
        Snapshot latest = ConnectorStatementScopes.resolveInStatement(session, "deltalake.cdf.latest",
                handle.getDatabase(), handle.getPath() + ":" + handle.getTableId(),
                () -> handle.loadLatest(runtime, runtime.engine(handle.reference())));
        long last = latest.getVersion();
        long first = DeltaKernelCompat.earliestHistoryVersion(runtime.engine(handle.reference()), latest)
                .orElseThrow(() -> new DorisConnectorException("No retained Delta change log for " + handle.getPath()));
        Long startTime = timestamp(session, bounds.get("start_timestamp"));
        Long endTime = timestamp(session, bounds.get("end_timestamp"));
        List<Commit> clock = startTime == null && endTime == null ? List.of()
                : commits(session, runtime, handle, latest, first, last);
        long start = startTime == null ? version(bounds.get("start_version"))
                : clock.stream().filter(c -> c.timestamp >= startTime).mapToLong(Commit::version).findFirst()
                        .orElseThrow(() -> new DorisConnectorException(
                                "CDF start timestamp is after the latest commit"));
        if (endTime != null && endTime > clock.get(clock.size() - 1).timestamp) {
            throw new DorisConnectorException("CDF end timestamp is after the latest commit");
        }
        long end = endTime == null
                ? (bounds.containsKey("end_version") ? Math.min(last, version(bounds.get("end_version"))) : last)
                : clock.stream().filter(c -> c.timestamp <= endTime).mapToLong(Commit::version).max()
                        .orElseThrow(() -> new DorisConnectorException(
                                "CDF end timestamp is before the earliest commit"));
        if (start < first || start > last) {
            throw new DorisConnectorException("CDF start version " + start + " outside retained log [" + first
                    + ", " + last + "]");
        }
        if ((startTime != null && endTime != null && startTime > endTime)
                || (start > end && (startTime == null || endTime == null))) {
            throw new DorisConnectorException("Invalid CDF range: end cannot be before start");
        }
        // Use the end schema for all mapping modes, including an unmapped table that later adds columns.
        long schemaVersion = end;
        Snapshot output = snapshot(runtime, handle, schemaVersion, latest);
        DeltaLakeCdfRange range = new DeltaLakeCdfRange(start, end, schemaVersion);
        if (start > end) {
            // For a gap between commits, validate the snapshot that supplies the output schema.
            validateCdfSnapshot(output, handle.reference().managed);
            return range;
        }
        long expected = start;
        for (Commit commit : commits(session, runtime, handle, latest, start, end)) {
            if (commit.version != expected) {
                throw new DorisConnectorException("Missing Delta change log at version " + expected);
            }
            if (expected == start || commit.definitionChanged) {
                Snapshot state = snapshot(runtime, handle, expected, latest);
                validateCdfSnapshot(state, handle.reference().managed);
                if (!compatible(state.getSchema(), output.getSchema(),
                        DeltaLakeSchemaBuilder.hasColumnMapping(output))) {
                    throw new DorisConnectorException("Incompatible change data feed schema at version " + expected);
                }
            }
            expected++;
        }
        if (expected <= end) {
            throw new DorisConnectorException("Missing Delta change log at version " + expected);
        }
        return range;
    }

    private static void validateCdfSnapshot(Snapshot snapshot, boolean managed) {
        DeltaLakeFeatureCheck.check(snapshot, managed);
        if (!DeltaKernelCompat.changeDataFeedEnabled(snapshot)) {
            throw new DorisConnectorException("Change data feed was not enabled at version " + snapshot.getVersion());
        }
    }

    private static Snapshot snapshot(DeltaLakeRuntime runtime, DeltaLakeTableHandle handle,
            long version, Snapshot latest) {
        return latest.getVersion() == version ? latest : handle.atVersion(runtime, version).snapshot(runtime);
    }

    private static long version(String raw) {
        try {
            long value = Long.parseLong(raw);
            if (value >= 0) {
                return value;
            }
        } catch (NumberFormatException ignored) {
            // Report a single diagnostic for both malformed and negative coordinates.
        }
        throw new DorisConnectorException("CDF version must be a non-negative 64-bit integer: " + raw);
    }

    private static Long timestamp(ConnectorSession session, String value) {
        return value == null ? null : DeltaLakeTimeTravel.parseTimestampMillis(session,
                ConnectorTimeTravelSpec.timestamp(value, false));
    }

    static List<Commit> commits(ConnectorSession session, DeltaLakeRuntime runtime, DeltaLakeTableHandle handle,
            Snapshot snapshot, long start, long end) {
        String key = String.join("\0", handle.getPath(), handle.getTableId(), Long.toString(start), Long.toString(end));
        return ConnectorStatementScopes.resolveInStatement(session, "deltalake.cdf.commits", handle.getDatabase(),
                key, () -> runtime.execute(() -> readCommits(runtime, handle, snapshot, start, end)));
    }

    private static List<Commit> readCommits(DeltaLakeRuntime runtime, DeltaLakeTableHandle handle, Snapshot snapshot,
            long start, long end) throws IOException {
        Engine engine = runtime.engine(handle.reference());
        OptionalLong ict = DeltaKernelCompat.historyIctStart(snapshot);
        // Pre-ICT timestamps depend on earlier file mtimes. Only list that prefix, never open its JSON.
        long earliest = ict.isPresent() && start >= ict.getAsLong() ? start
                : DeltaKernelCompat.earliestHistoryVersion(engine, snapshot).orElseThrow();
        List<Commit> commits = new ArrayList<>();
        long previous = Long.MIN_VALUE;
        for (FileStatus file : handle.commitRange(runtime, engine, earliest, end).getDeltaFiles()) {
            long version = DeltaKernelCompat.commitVersion(file);
            previous = DeltaKernelCompat.monotonicMtime(previous, file.getModificationTime());
            if (version < start) {
                continue;
            }
            boolean usesIct = ict.isPresent() && version >= ict.getAsLong();
            long[] stamp = {previous};
            boolean[] changed = {false};
            boolean[] hasCdc = {false};
            readActions(engine, file, CONTROL, row -> {
                hasCdc[0] |= !row.isNullAt(3);
                if (!row.isNullAt(1) || !row.isNullAt(2)) {
                    changed[0] = true;
                }
                if (usesIct && !row.isNullAt(0) && !row.getStruct(0).isNullAt(0)) {
                    stamp[0] = row.getStruct(0).getLong(0);
                }
            });
            commits.add(new Commit(version, stamp[0], file, changed[0], hasCdc[0]));
            previous = stamp[0];
        }
        if (commits.isEmpty() || commits.get(commits.size() - 1).version != end) {
            throw new DorisConnectorException("Missing Delta change log at version " + end);
        }
        return List.copyOf(commits);
    }

    static void readActions(Engine engine, FileStatus file, StructType schema, Consumer<Row> consumer)
            throws IOException {
        try (CloseableIterator<ColumnarBatch> batches = engine.getJsonHandler()
                .readJsonFiles(DeltaKernelCompat.singleLogFile(file), schema, Optional.empty())) {
            while (batches.hasNext()) {
                try (CloseableIterator<Row> rows = batches.next().getRows()) {
                    while (rows.hasNext()) {
                        consumer.accept(rows.next());
                    }
                }
            }
        }
    }

    // Spark accepts additive changes. Crossing a rename, drop, physical re-add, or retype is incompatible.
    static boolean compatible(DataType oldType, DataType target, boolean mapped) {
        if (oldType instanceof StructType oldStruct && target instanceof StructType newStruct) {
            for (StructField oldField : oldStruct.fields()) {
                Optional<StructField> match = newStruct.fields().stream()
                        .filter(f -> f.getName().equals(oldField.getName())).findFirst();
                if (match.isEmpty() || (oldField.isNullable() && !match.get().isNullable())
                        || (mapped && oldField.getMetadata().contains("delta.columnMapping.id")
                        && !oldField.getMetadata().getLong("delta.columnMapping.id")
                                .equals(match.get().getMetadata().getLong("delta.columnMapping.id")))
                        || !compatible(oldField.getDataType(), match.get().getDataType(), mapped)) {
                    return false;
                }
            }
            return true;
        }
        if (oldType instanceof ArrayType a && target instanceof ArrayType b) {
            return (!a.containsNull() || b.containsNull())
                    && compatible(a.getElementType(), b.getElementType(), mapped);
        }
        if (oldType instanceof MapType a && target instanceof MapType b) {
            return (!a.isValueContainsNull() || b.isValueContainsNull())
                    && compatible(a.getKeyType(), b.getKeyType(), mapped)
                    && compatible(a.getValueType(), b.getValueType(), mapped);
        }
        return oldType.equals(target);
    }
}
