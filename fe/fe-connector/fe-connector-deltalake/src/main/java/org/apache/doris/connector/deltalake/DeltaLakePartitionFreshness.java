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

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.deltalake.DeltaLakeSnapshotCache.TableKey;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CommitRangeNotFoundException;
import io.delta.kernel.exceptions.StartVersionNotFoundException;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Last logical change to each active LIST partition at a fixed Delta generation. */
final class DeltaLakePartitionFreshness {
    static final String CACHE_NAME = "partition-freshness";
    private static final StructType FILE = new StructType()
            .add("partitionValues", new MapType(StringType.STRING, StringType.STRING, true))
            .add("dataChange", BooleanType.BOOLEAN);
    private static final StructType ACTIONS = new StructType().add("add", FILE).add("remove", FILE)
            .add("commitInfo", new StructType().add("inCommitTimestamp", LongType.LONG))
            .add("metaData", new StructType().add("schemaString", StringType.STRING));
    private final MetaCache<Key, State> cache;

    DeltaLakePartitionFreshness(CatalogMetaCache owner, Map<String, String> properties) {
        cache = owner.create(MetaCacheDefinition.<Key, State>builder("deltalake-" + CACHE_NAME,
                        DeltaLakeSnapshotCache.spec(properties, CACHE_NAME), key -> key.table.scope())
                .sizeEstimator((key, state) -> MetaCacheSizeEstimate.complete(key.table.estimatedBytes()
                        + JvmSizeUtils.stringSize(key.tableId) + state.estimatedBytes()))
                .build());
    }

    private record Key(TableKey table, String tableId, ZoneId zone) {
    }

    record Partitions(Map<String, Map<String, String>> values, Set<String> normalizedColumns, long fileCount) {
        static Partitions collect(Snapshot snapshot, Engine engine, ZoneId zone) throws IOException {
            StructType schema = snapshot.getSchema();
            List<String> columns = snapshot.getPartitionColumnNames();
            boolean mapped = DeltaLakeSchemaBuilder.hasColumnMapping(snapshot);
            Set<String> normalized = new HashSet<>();
            Map<String, Map<String, String>> values = new LinkedHashMap<>();
            long[] count = {0};
            DeltaLakeScanFiles.forEach(snapshot, engine, false, row -> {
                count[0]++;
                Map<String, String> partition = DeltaLakePartitionValues.normalize(schema, columns, mapped,
                        DeltaKernelCompat.partitionValues(row), zone, normalized::add);
                values.putIfAbsent(DeltaLakePartitionValues.partitionName(partition), partition);
            });
            return new Partitions(Collections.unmodifiableMap(values), Set.copyOf(normalized), count[0]);
        }
    }

    @FunctionalInterface
    interface PartitionLoader {
        Partitions load() throws IOException;
    }

    record State(long version, long lastMtime, String schema, long baseline, Map<String, Long> changes,
            Partitions partitions) {
        long lastChanged(String name) {
            return changes.getOrDefault(name, baseline);
        }

        long estimatedBytes() {
            long bytes = 160 + JvmSizeUtils.stringSize(schema);
            for (var entry : partitions.values.entrySet()) {
                bytes += 256 + JvmSizeUtils.stringSize(entry.getKey());
                for (String value : entry.getValue().values()) {
                    bytes += 96 + JvmSizeUtils.stringSize(value);
                }
            }
            return bytes;
        }
    }

    State atSnapshot(ConnectorSession session, DeltaLakeTableHandle handle, DeltaLakeRuntime runtime,
            Snapshot snapshot, ZoneId zone, PartitionLoader partitions) {
        Function<State, State> replay = previous -> {
            try {
                return replay(handle, runtime, snapshot, zone, partitions.load(), previous);
            } catch (IOException e) {
                throw new DorisConnectorException("Cannot read Delta partition changes for " + handle.getPath(), e);
            }
        };
        if (session == null || !cache.isEnabled()) {
            return replay.apply(null);
        }
        Key key = new Key(new TableKey(session.getUser(), handle.getDatabase(), handle.getTable(), handle.reference()),
                handle.getTableId(), zone);
        return atVersion(key, handle.getVersion(), replay);
    }

    private State atVersion(Key key, long target, Function<State, State> replay) {
        State previous = cache.get(key, ignored -> replay.apply(null));
        while (previous.version < target) {
            // Serialize suffix reads only for this predecessor. REFRESH can evict it during I/O, in which
            // case the caller still owns its pinned result, but cannot publish into the replacement entry.
            synchronized (previous) {
                State current = cache.getIfPresent(key);
                if (current == previous || current == null) {
                    State advanced = replay.apply(previous);
                    cache.compareAndSet(key, previous, advanced);
                    return advanced;
                }
                previous = current;
            }
        }
        return previous.version == target ? previous : replay.apply(null);
    }

    private static State replay(DeltaLakeTableHandle handle, DeltaLakeRuntime runtime, Snapshot snapshot,
            ZoneId zone, Partitions partitions, State previous) throws IOException {
        Engine engine = runtime.engine(handle.reference());
        long target = handle.getVersion();
        String schema = snapshot.getMetadata().getSchemaString();
        long next = previous == null ? 0 : previous.version + 1;
        List<FileStatus> files = retainedFiles(handle, runtime, engine, snapshot, next);
        Replay state = new Replay(snapshot, zone, partitions.values.keySet(), previous);
        OptionalLong ictStart = DeltaKernelCompat.historyIctStart(snapshot);
        for (FileStatus file : files) {
            long version = DeltaKernelCompat.commitVersion(file);
            state.apply(engine, file, version != next || (previous == null && version == 0),
                    ictStart.isPresent() && version >= ictStart.getAsLong());
            next = version + 1;
        }
        if (next <= target) {
            long timestamp = snapshot.getTimestamp(engine);
            return new State(target, timestamp, schema, timestamp, Map.of(), partitions);
        }
        return new State(target, state.lastMtime, schema, state.baseline, Map.copyOf(state.changes), partitions);
    }

    private static List<FileStatus> retainedFiles(DeltaLakeTableHandle handle, DeltaLakeRuntime runtime,
            Engine engine, Snapshot snapshot, long start) throws IOException {
        long target = handle.getVersion();
        if (start == 0) {
            start = DeltaKernelCompat.earliestHistoryVersion(engine, snapshot).orElse(Long.MAX_VALUE);
        }
        while (start <= target) {
            try {
                return handle.commitRange(runtime, engine, start, target).getDeltaFiles();
            } catch (StartVersionNotFoundException e) {
                // The cache outlived log retention, or cleanup advanced while we were listing. Kernel
                // reports the first available commit, including catalog-ratified commits for managed tables.
                long first = e.getEarliestAvailableVersion().orElse(Long.MAX_VALUE);
                if (first <= start) {
                    throw e;
                }
                start = first;
            } catch (CommitRangeNotFoundException e) {
                break;
            }
        }
        return List.of();
    }

    private static final class Replay {
        private final StructType schema;
        private final List<String> columns;
        private final boolean mapped;
        private final Set<String> logKeys;
        private final ZoneId zone;
        private final Set<String> active;
        private final Map<String, Long> changes;
        private long lastMtime;
        private long baseline;
        private String previousSchema;

        Replay(Snapshot snapshot, ZoneId zone, Set<String> active, State previous) {
            this.zone = zone;
            this.active = active;
            schema = snapshot.getSchema();
            columns = snapshot.getPartitionColumnNames();
            mapped = DeltaLakeSchemaBuilder.hasColumnMapping(snapshot);
            logKeys = columns.stream().map(name -> DeltaLakePartitionValues.logKey(schema.get(name), mapped))
                    .collect(Collectors.toSet());
            changes = previous == null ? new HashMap<>() : new HashMap<>(previous.changes);
            changes.keySet().retainAll(active);
            lastMtime = previous == null ? Long.MIN_VALUE : previous.lastMtime;
            baseline = previous == null ? 0 : previous.baseline;
            previousSchema = previous == null ? null : previous.schema;
        }

        void apply(Engine engine, FileStatus file, boolean newBaseline, boolean usesIct) throws IOException {
            if (newBaseline) {
                // Missing commits may have changed the schema before the first retained metadata action.
                previousSchema = null;
            }
            lastMtime = DeltaKernelCompat.monotonicMtime(lastMtime, file.getModificationTime());
            long timestamp = lastMtime;
            boolean all = newBaseline;
            Set<String> touched = new HashSet<>();
            try (CloseableIterator<ColumnarBatch> batches = engine.getJsonHandler()
                    .readJsonFiles(DeltaKernelCompat.singleLogFile(file), ACTIONS, Optional.empty())) {
                while (batches.hasNext()) {
                    try (CloseableIterator<Row> rows = batches.next().getRows()) {
                        while (rows.hasNext()) {
                            Row action = rows.next();
                            if (!action.isNullAt(2)) {
                                Row info = action.getStruct(2);
                                if (usesIct && !info.isNullAt(0)) {
                                    timestamp = info.getLong(0);
                                }
                            } else if (!action.isNullAt(3)) {
                                String currentSchema = action.getStruct(3).getString(0);
                                all |= !currentSchema.equals(previousSchema);
                                previousSchema = currentSchema;
                            } else if (!all && (!action.isNullAt(0) || !action.isNullAt(1))) {
                                Row fileAction = action.getStruct(action.isNullAt(0) ? 1 : 0);
                                if (fileAction.getBoolean(1)) {
                                    all = !touch(fileAction, touched);
                                }
                            }
                        }
                    }
                }
            }
            if (all) {
                changes.clear();
                baseline = timestamp;
            } else {
                for (String name : touched) {
                    changes.put(name, timestamp);
                }
            }
        }

        // Missing extended remove metadata and older physical partition layouts have no unambiguous owner.
        private boolean touch(Row fileAction, Set<String> touched) {
            if (fileAction.isNullAt(0)) {
                return false;
            }
            Map<String, String> raw = DeltaKernelCompat.partitionValues(fileAction.getMap(0));
            if (!raw.keySet().containsAll(logKeys)) {
                return false;
            }
            String name;
            try {
                name = DeltaLakePartitionValues.partitionName(
                        DeltaLakePartitionValues.normalize(schema, columns, mapped, raw, zone, ignored -> { }));
            } catch (DorisConnectorException e) {
                // An old value can be valid for its historical type but unrepresentable in the target schema.
                return false;
            }
            if (active.contains(name)) {
                touched.add(name);
            }
            return true;
        }
    }
}
