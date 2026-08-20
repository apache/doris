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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.DorisConnectorException;

import com.google.common.collect.ImmutableSet;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.FallbackKey;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.snapshot.FullCompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validation and application rules for relation-scoped Paimon scan parameters
 * ({@code @options('scan.snapshot-id'='1', ...)}).
 *
 * <p>Ported from upstream {@code org.apache.doris.datasource.paimon.PaimonScanParams} (#65984). It lands
 * in the connector rather than fe-core because every rule here is expressed in the paimon SDK's own
 * vocabulary ({@link CoreOptions} keys, {@link TimeTravelUtil}, {@link FileStoreTable}) — fe-core is
 * paimon-SDK-free since P5-T29 and never inspects an option key. The engine hands the raw
 * {@code @options} map over as a {@code ConnectorTimeTravelSpec.Kind#OPTIONS} spec, exactly as it does
 * for the {@code @incr} window (see {@link PaimonIncrementalScanParams}).
 *
 * <p>The upstream {@code validateSystemTable(String, TableScanParams)} entry point does NOT appear here:
 * it took an fe-core type, and its per-system-table capability matrix is expressed instead as
 * {@link #supportsIncrementalRead} / {@link #supportsOptions}, which
 * {@code PaimonScanPlanProvider} publishes through the generic SPI hooks
 * {@code ConnectorScanPlanProvider.supportsSystemTableIncrementalRead/Options}. Its one residual check
 * survives as {@link #validateSystemTableOptions}.
 */
public final class PaimonScanParams {
    /** Prefix of every marker this class puts in a resolved map; never reaches the paimon SDK. */
    private static final String INTERNAL_PREFIX = "doris.internal.paimon.";
    private static final String PINNED_FILE_CREATION_TIME =
            "doris.internal.paimon.file-creation-time-millis";
    private static final String PINNED_EMPTY_SCAN = "doris.internal.paimon.empty-scan";
    private static final String PRESERVE_BOUND_SCHEMA =
            "doris.internal.paimon.preserve-bound-schema";
    /**
     * Marks a pin as originating from {@code @options} rather than from a time-travel / {@code @incr}
     * selector. The three families share one carrier (the handle's scan-option map), but only this one
     * needs {@link #applyOptions}' read-state isolation, and only its keys are drawn from the
     * {@code @options} vocabulary — so the family must be recoverable at the {@code Table.copy}
     * chokepoint. Stripped there along with every other marker.
     */
    private static final String PINNED_OPTIONS_MARKER = "doris.internal.paimon.options";

    private static final Set<String> QUERY_OPTION_KEYS = ImmutableSet.<String>builder()
            .add(CoreOptions.SCAN_MODE.key())
            .add(CoreOptions.SCAN_TIMESTAMP.key())
            .add(CoreOptions.SCAN_TIMESTAMP_MILLIS.key())
            .add(CoreOptions.SCAN_WATERMARK.key())
            .add(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key())
            .add(CoreOptions.SCAN_CREATION_TIME_MILLIS.key())
            .add(CoreOptions.SCAN_SNAPSHOT_ID.key())
            .add(CoreOptions.SCAN_TAG_NAME.key())
            .add(CoreOptions.SCAN_VERSION.key())
            .add(CoreOptions.SCAN_MANIFEST_PARALLELISM.key())
            .add(CoreOptions.SCAN_PLAN_SORT_PARTITION.key())
            .addAll(PaimonReaderOptions.supportedOptions())
            .build();

    private static final Set<String> STARTUP_POSITION_KEYS = ImmutableSet.of(
            CoreOptions.SCAN_TIMESTAMP.key(),
            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
            CoreOptions.SCAN_WATERMARK.key(),
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(),
            CoreOptions.SCAN_CREATION_TIME_MILLIS.key(),
            CoreOptions.SCAN_SNAPSHOT_ID.key(),
            CoreOptions.SCAN_TAG_NAME.key(),
            CoreOptions.SCAN_VERSION.key());

    private static final Set<String> INHERITED_READ_STATE_KEYS = buildInheritedReadStateKeys();

    // FilesScan enumerates the latest partitions before applying its range-aware per-partition scan,
    // so it cannot safely read a range when a partition in that range has since been dropped.
    private static final Set<String> INCREMENTAL_SYSTEM_TABLES = ImmutableSet.of(
            "audit_log", "binlog", "partitions", "ro", "row_tracking");

    private static final Set<String> PAIMON_READER_SYSTEM_TABLES = ImmutableSet.of(
            "audit_log", "binlog", "row_tracking");

    // Not a capability the user selects but a property of where the snapshot is resolved: these five
    // pick it inside the BE reader rather than reading what the FE planned. See
    // resolvesSnapshotOnBackend for the per-table call chain.
    private static final Set<String> BACKEND_SNAPSHOT_SYSTEM_TABLES = ImmutableSet.of(
            "files", "partitions", "manifests", "statistics", "table_indexes");

    private static final Set<String> OPTIONS_SYSTEM_TABLES = ImmutableSet.of(
            // A system table may advertise OPTIONS only when every row-producing stage observes
            // the selected snapshot; files and buckets still consult latest metadata internally.
            "audit_log", "binlog", "manifests", "partitions", "ro",
            "row_tracking", "table_indexes");

    private PaimonScanParams() {
    }

    public static void validateOptions(Map<String, String> options) {
        if (options.containsKey(CoreOptions.SCAN_FALLBACK_BRANCH.key())) {
            throw new DorisConnectorException("Paimon query option '"
                    + CoreOptions.SCAN_FALLBACK_BRANCH.key()
                    + "' is not supported because it requires rebuilding the table through the catalog factory.");
        }

        Set<String> unsupported = options.keySet().stream()
                .filter(key -> !QUERY_OPTION_KEYS.contains(key))
                .collect(Collectors.toSet());
        if (!unsupported.isEmpty()) {
            throw new DorisConnectorException("Unsupported Paimon query option(s): " + unsupported);
        }

        PaimonReaderOptions.validateEffectiveTableOptions(options);

        String scanMode = options.get(CoreOptions.SCAN_MODE.key());
        if ("from-creation-timestamp".equalsIgnoreCase(scanMode)
                && options.get(CoreOptions.SCAN_CREATION_TIME_MILLIS.key()) == null) {
            // Paimon 1.3.1 does not validate this newer mode, but its starting scanner
            // requires the creation timestamp and otherwise fails after analysis.
            throw new DorisConnectorException("Paimon scan mode 'from-creation-timestamp' requires query option '"
                    + CoreOptions.SCAN_CREATION_TIME_MILLIS.key() + "'.");
        }

        long positionCount = options.keySet().stream().filter(STARTUP_POSITION_KEYS::contains).count();
        if (positionCount > 1) {
            throw new DorisConnectorException(
                    "Only one Paimon startup position can be specified: " + STARTUP_POSITION_KEYS);
        }

        if (options.containsKey(CoreOptions.SCAN_MODE.key()) && positionCount == 1) {
            String position = options.keySet().stream()
                    .filter(STARTUP_POSITION_KEYS::contains)
                    .findFirst()
                    .get();
            String mode = scanMode.toLowerCase(Locale.ROOT);
            if (!isCompatibleStartupMode(position, mode)) {
                throw new DorisConnectorException("Paimon scan mode '" + mode
                        + "' is incompatible with startup position '" + position + "'.");
            }
        }
    }

    public static Table applyOptions(Table table, Map<String, String> options) {
        Map<String, String> tableOptions = userOptions(options);
        validateOptions(tableOptions);
        Map<String, String> isolatedOptions = new HashMap<>(tableOptions);
        if (hasStartupOptions(tableOptions)) {
            // Startup mode, position, and range form one inherited state family in Paimon. Clear
            // absent members so one relation cannot accidentally reuse another relation's read state.
            INHERITED_READ_STATE_KEYS.stream()
                    .filter(key -> !tableOptions.containsKey(key))
                    .forEach(key -> isolatedOptions.put(key, null));
        }
        // Apply the relation override first, then normalize each physical planning branch. A
        // single combined copy would flatten independent fallback values into one sibling's cap.
        Table effectiveTable = PaimonReaderOptions.runtimeSafeTable(table.copy(isolatedOptions));
        // Validate after every copy so relation options participate in the documented
        // relation > catalog > physical precedence before the effective value is judged.
        PaimonReaderOptions.validateEffectiveTable(effectiveTable);
        return effectiveTable;
    }

    public static FileStoreTable applyOptionsWithoutTimeTravel(
            FileStoreTable table, Map<String, String> options) {
        Map<String, String> tableOptions = userOptions(options);
        validateOptions(tableOptions);
        Map<String, String> isolatedOptions = new HashMap<>(tableOptions);
        if (hasStartupOptions(tableOptions)) {
            INHERITED_READ_STATE_KEYS.stream()
                    .filter(key -> !tableOptions.containsKey(key))
                    .forEach(key -> isolatedOptions.put(key, null));
        }
        // The statement fence already selected the schema generation. Preserve that generation
        // while carrying only the resolved read selector and execution options into this copy.
        FileStoreTable effectiveTable = (FileStoreTable) PaimonReaderOptions.runtimeSafeTable(
                table.copyWithoutTimeTravel(isolatedOptions));
        PaimonReaderOptions.validateEffectiveTable(effectiveTable);
        return effectiveTable;
    }

    /**
     * Paimon's inherited read-state family: startup mode, startup position and incremental range, plus
     * every fallback key. Shared with {@link PaimonIncrementalScanParams#applyResetsIfIncremental}, which
     * nulls the whole family so a value persisted on the base table cannot leak into a relation-scoped read.
     */
    public static Set<String> inheritedReadStateKeys() {
        return INHERITED_READ_STATE_KEYS;
    }

    private static Set<String> buildInheritedReadStateKeys() {
        ImmutableSet.Builder<String> keys = ImmutableSet.builder();
        for (ConfigOption<?> option : Arrays.asList(
                CoreOptions.SCAN_TIMESTAMP,
                CoreOptions.SCAN_TIMESTAMP_MILLIS,
                CoreOptions.SCAN_WATERMARK,
                CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
                CoreOptions.SCAN_CREATION_TIME_MILLIS,
                CoreOptions.SCAN_SNAPSHOT_ID,
                CoreOptions.SCAN_TAG_NAME,
                CoreOptions.SCAN_VERSION,
                CoreOptions.SCAN_MODE,
                CoreOptions.SCAN_BOUNDED_WATERMARK,
                CoreOptions.INCREMENTAL_BETWEEN,
                CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP,
                CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE,
                CoreOptions.INCREMENTAL_TO_AUTO_TAG)) {
            keys.add(option.key());
            for (FallbackKey fallbackKey : option.fallbackKeys()) {
                keys.add(fallbackKey.getKey());
            }
        }
        return keys.build();
    }

    public static boolean hasStartupOptions(Map<String, String> options) {
        return options.containsKey(CoreOptions.SCAN_MODE.key())
                || options.keySet().stream().anyMatch(STARTUP_POSITION_KEYS::contains);
    }

    public static boolean selectsSchema(Map<String, String> options) {
        return hasStartupOptions(options);
    }

    public static boolean hasOnlyReaderOptions(Map<String, String> options) {
        Map<String, String> tableOptions = userOptions(options);
        return !tableOptions.isEmpty()
                && PaimonReaderOptions.metadataNeutralOptions().containsAll(tableOptions.keySet());
    }

    public static boolean usesStatementSnapshot(Map<String, String> options) {
        if (options.keySet().stream().anyMatch(STARTUP_POSITION_KEYS::contains)) {
            return false;
        }
        String mode = options.get(CoreOptions.SCAN_MODE.key());
        return mode == null
                || "default".equalsIgnoreCase(mode)
                || "latest".equalsIgnoreCase(mode)
                || "latest-full".equalsIgnoreCase(mode)
                || "full".equalsIgnoreCase(mode);
    }

    public static Map<String, String> resolveOptions(Table table, Map<String, String> options) {
        validateOptions(options);
        if (!hasStartupOptions(options)) {
            return options;
        }
        if (usesStatementSnapshot(options)) {
            String pinnedSnapshotId = table.options().get(CoreOptions.SCAN_SNAPSHOT_ID.key());
            if (pinnedSnapshotId != null) {
                return resolvedSnapshotOptions(options, pinnedSnapshotId);
            }
        }
        if (!(table instanceof FileStoreTable)) {
            throw new DorisConnectorException("Paimon startup options require a file-store data table.");
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        if (options.containsKey(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key())) {
            return resolveFileCreationTime(
                    options,
                    fileStoreTable,
                    Long.parseLong(options.get(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key())));
        }
        if (options.containsKey(CoreOptions.SCAN_CREATION_TIME_MILLIS.key())) {
            long creationTime = Long.parseLong(options.get(CoreOptions.SCAN_CREATION_TIME_MILLIS.key()));
            Long previousSnapshotId = TimeTravelUtil.earlierThanTimeMills(
                    fileStoreTable.snapshotManager(),
                    fileStoreTable.changelogManager(),
                    creationTime,
                    fileStoreTable.coreOptions().changelogLifecycleDecoupled(),
                    true);
            if (previousSnapshotId != null
                    && fileStoreTable.snapshotManager().snapshotExists(previousSnapshotId + 1)) {
                return resolvedSnapshotOptions(options, String.valueOf(previousSnapshotId + 1));
            }
            return resolveFileCreationTime(options, fileStoreTable, creationTime);
        }

        Table selectedTable = applyOptions(table, options);
        if ("compacted-full".equalsIgnoreCase(options.get(CoreOptions.SCAN_MODE.key()))) {
            Long snapshotId = compactedFullSnapshotId((FileStoreTable) selectedTable);
            return snapshotId == null
                    ? resolvedEmptyOptions(options)
                    : resolvedSnapshotOptions(options, String.valueOf(snapshotId));
        }
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest((FileStoreTable) selectedTable);
        if (snapshot == null) {
            return resolvedEmptyOptions(options);
        }
        String tagName = selectedTagName(options, (FileStoreTable) selectedTable);
        if (tagName != null) {
            // A tag owns a retained Snapshot copy even after the ordinary snapshot file expires.
            // Keep the canonical tag selector so planning reads that retained metadata path.
            return resolvedTagOptions(options, tagName);
        }
        return resolvedSnapshotOptions(options, String.valueOf(snapshot.id()));
    }

    private static String selectedTagName(Map<String, String> options, FileStoreTable selectedTable) {
        String tagName = options.get(CoreOptions.SCAN_TAG_NAME.key());
        if (tagName != null) {
            return tagName;
        }
        // Paimon normalizes a tag-valued scan.version while copying the selected table.
        return selectedTable.options().get(CoreOptions.SCAN_TAG_NAME.key());
    }

    private static Long compactedFullSnapshotId(FileStoreTable table) {
        CoreOptions coreOptions = table.coreOptions();
        int deltaCommits = coreOptions.toConfiguration()
                .getOptional(CoreOptions.FULL_COMPACTION_DELTA_COMMITS)
                .orElse(1);
        if (coreOptions.changelogProducer() == CoreOptions.ChangelogProducer.FULL_COMPACTION
                || coreOptions.toConfiguration().contains(CoreOptions.FULL_COMPACTION_DELTA_COMMITS)) {
            return table.snapshotManager().pickOrLatest(snapshot ->
                    snapshot.commitKind() == Snapshot.CommitKind.COMPACT
                            && FullCompactedStartingScanner.isFullCompactedIdentifier(
                                    snapshot.commitIdentifier(), deltaCommits));
        }
        // COMPACTED_FULL means the newest compact snapshot, which can be older than latest.
        return table.snapshotManager().pickOrLatest(
                snapshot -> snapshot.commitKind() == Snapshot.CommitKind.COMPACT);
    }

    private static Map<String, String> resolveFileCreationTime(
            Map<String, String> options, FileStoreTable table, long creationTime) {
        Map<String, String> resolved = new HashMap<>(options);
        INHERITED_READ_STATE_KEYS.forEach(resolved::remove);
        Optional<Snapshot> latestSnapshot = table.latestSnapshot();
        if (latestSnapshot.isPresent()) {
            resolved.put(CoreOptions.SCAN_SNAPSHOT_ID.key(),
                    String.valueOf(latestSnapshot.get().id()));
        } else {
            resolved.put(PINNED_EMPTY_SCAN, Boolean.TRUE.toString());
        }
        // Paimon's file-creation scanner consults latest lazily. Keep its filter as internal
        // metadata while replacing the live latest lookup with the snapshot fixed above.
        resolved.put(PINNED_FILE_CREATION_TIME, String.valueOf(creationTime));
        return resolved;
    }

    private static Map<String, String> resolvedSnapshotOptions(
            Map<String, String> options, String snapshotId) {
        Map<String, String> resolved = new HashMap<>(options);
        INHERITED_READ_STATE_KEYS.forEach(resolved::remove);
        resolved.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), snapshotId);
        return resolved;
    }

    public static Map<String, String> pinOptionsToSnapshot(
            Map<String, String> options, long snapshotId) {
        // Statement-fence pinning removes inherited selectors, so validate the raw map first;
        // otherwise an unsupported inherited key can disappear before the common validation path.
        validateOptions(options);
        Map<String, String> pinned = snapshotId < 0
                ? resolvedEmptyOptions(options)
                : resolvedSnapshotOptions(options, String.valueOf(snapshotId));
        // This snapshot selector comes from the statement fence, whose table already owns the
        // bound schema generation. Explicit user selectors must not carry this provenance marker.
        pinned.put(PRESERVE_BOUND_SCHEMA, Boolean.TRUE.toString());
        return pinned;
    }

    public static boolean preservesBoundSchema(Map<String, String> options) {
        return options != null && Boolean.parseBoolean(options.get(PRESERVE_BOUND_SCHEMA));
    }

    private static Map<String, String> resolvedTagOptions(Map<String, String> options, String tagName) {
        Map<String, String> resolved = new HashMap<>(options);
        INHERITED_READ_STATE_KEYS.forEach(resolved::remove);
        resolved.put(CoreOptions.SCAN_TAG_NAME.key(), tagName);
        return resolved;
    }

    private static Map<String, String> resolvedEmptyOptions(Map<String, String> options) {
        Map<String, String> resolved = new HashMap<>(options);
        INHERITED_READ_STATE_KEYS.forEach(resolved::remove);
        // An empty table is also a statement state. Remember it explicitly so a commit between
        // binding and split planning cannot turn the relation into a non-empty scan.
        resolved.put(PINNED_EMPTY_SCAN, Boolean.TRUE.toString());
        return resolved;
    }

    private static Map<String, String> userOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith(INTERNAL_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Stamps a resolved {@code @options} map so the {@code Table.copy} chokepoint can tell it apart from a
     * time-travel or {@code @incr} pin, which share the same carrier but must not get this family's
     * read-state isolation. Applied once, right after resolution.
     */
    public static Map<String, String> markAsOptions(Map<String, String> resolved) {
        Map<String, String> marked = new HashMap<>(resolved);
        marked.put(PINNED_OPTIONS_MARKER, Boolean.TRUE.toString());
        return marked;
    }

    /** Whether {@code scanOptions} is a resolved {@code @options} pin (see {@link #markAsOptions}). */
    public static boolean isOptionsPin(Map<String, String> scanOptions) {
        return scanOptions != null && scanOptions.containsKey(PINNED_OPTIONS_MARKER);
    }

    public static Optional<Long> getPinnedFileCreationTime(Map<String, String> options) {
        return Optional.ofNullable(options.get(PINNED_FILE_CREATION_TIME)).map(Long::parseLong);
    }

    public static boolean isPinnedEmptyScan(Map<String, String> options) {
        return Boolean.parseBoolean(options.get(PINNED_EMPTY_SCAN));
    }

    public static Map<String, String> isolateIncrementalRead(Map<String, String> incrementalOptions) {
        Map<String, String> isolatedOptions = new HashMap<>();
        INHERITED_READ_STATE_KEYS.forEach(key -> isolatedOptions.put(key, null));
        isolatedOptions.putAll(incrementalOptions);
        return isolatedOptions;
    }

    private static boolean isCompatibleStartupMode(String position, String mode) {
        if ("default".equals(mode)) {
            return true;
        }
        if (CoreOptions.SCAN_TIMESTAMP.key().equals(position)
                || CoreOptions.SCAN_TIMESTAMP_MILLIS.key().equals(position)) {
            return "from-timestamp".equals(mode);
        }
        if (CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key().equals(position)) {
            return "from-file-creation-time".equals(mode);
        }
        if (CoreOptions.SCAN_CREATION_TIME_MILLIS.key().equals(position)) {
            return "from-creation-timestamp".equals(mode);
        }
        return "from-snapshot".equals(mode)
                || (CoreOptions.SCAN_SNAPSHOT_ID.key().equals(position)
                && "from-snapshot-full".equals(mode));
    }

    public static boolean supportsIncrementalRead(String systemTableType) {
        return INCREMENTAL_SYSTEM_TABLES.contains(systemTableType.toLowerCase());
    }

    public static boolean supportsOptions(String systemTableType) {
        return OPTIONS_SYSTEM_TABLES.contains(systemTableType.toLowerCase());
    }

    public static boolean requiresPaimonReader(String systemTableType) {
        // Range support and reader requirements are independent capabilities. File-backed system
        // tables can honor INCR while still using Doris' native reader.
        return PAIMON_READER_SYSTEM_TABLES.contains(systemTableType.toLowerCase());
    }

    /**
     * The system tables whose rows are not fixed by what the FE planned: the FE only sends marker
     * splits and the snapshot is picked inside the BE reader. Two shapes, both honoring
     * {@code scan.snapshot-id}:
     * <ul>
     *   <li>{@code $files} / {@code $partitions} re-plan the base table on the BE
     *       ({@code FilesTable.FilesRead#createReader} -&gt; {@code DataTableScan#plan()},
     *       {@code PartitionsTable.PartitionsRead#createReader} -&gt;
     *       {@code newScan().listPartitionEntries()});</li>
     *   <li>{@code $manifests} / {@code $table_indexes} / {@code $statistics} resolve it directly
     *       ({@code TimeTravelUtil#tryTravelOrLatest}, reached from {@code ManifestsTable},
     *       {@code TableIndexesTable} and {@code AbstractFileStoreTable#statistics}).</li>
     * </ul>
     * All five have a fixed row type, so pinning the catalog's snapshot for them cannot rewind the
     * BE schema. Consumed by {@code PaimonScanPlanProvider}, which hands the BE a catalog-less table
     * and therefore has to do on the FE what the catalog would have done inside the BE reader.
     */
    public static boolean resolvesSnapshotOnBackend(String systemTableType) {
        return BACKEND_SNAPSHOT_SYSTEM_TABLES.contains(systemTableType.toLowerCase());
    }

    /**
     * Rejects the ONE option a system table can never honor. Which system table accepts {@code @options}
     * at all is answered earlier and generically by
     * {@code PaimonScanPlanProvider.supportsSystemTableOptions}; this covers the residual case where an
     * accepted system table is handed an option its generic wrapper cannot carry: Paimon's creation-time
     * file filter is a manifest-entry predicate, and a system-table wrapper has nowhere to put it.
     * Rejecting is the only safe answer — silently dropping it would widen the read to the whole pinned
     * snapshot and return rows the user excluded.
     */
    public static void validateSystemTableOptions(Map<String, String> options) {
        if (options.containsKey(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key())) {
            throw new DorisConnectorException(
                    "Paimon system tables do not support scan.file-creation-time-millis OPTIONS.");
        }
    }
}
