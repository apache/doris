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

import com.google.common.collect.ImmutableSet;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.StringUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

/** Validation shared by catalog-scoped and relation-scoped Paimon reader tuning. */
public final class PaimonReaderOptions {
    public static final String TABLE_OPTION_PREFIX = "paimon.table-option.";
    public static final int MIN_READ_BATCH_SIZE = 1;
    public static final int MAX_READ_BATCH_SIZE = 65536;
    // Keep catalog replay deterministic while bounding a single option's JVM-wide thread impact.
    public static final int MAX_MANIFEST_PARALLELISM = 256;
    public static final long MIN_ASYNC_THRESHOLD_BYTES = 1024L * 1024L;
    public static final long MAX_ASYNC_THRESHOLD_BYTES = 1024L * 1024L * 1024L;

    // Keep this list to batch-read controls consumed by Doris' Paimon scan path. Context selectors,
    // streaming-source settings, storage layout, and write options are unsafe after schema binding.
    private static final Set<String> SUPPORTED_OPTIONS = ImmutableSet.of(
            CoreOptions.READ_BATCH_SIZE.key(),
            CoreOptions.FILE_READER_ASYNC_THRESHOLD.key(),
            CoreOptions.FILE_INDEX_READ_ENABLED.key(),
            CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(),
            CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(),
            CoreOptions.SCAN_MANIFEST_PARALLELISM.key(),
            CoreOptions.SCAN_PLAN_SORT_PARTITION.key());

    // These settings do not alter the selected snapshot or manifest projection, so relation-local
    // copies can reuse the memoized partition projection while still planning splits from the copy.
    private static final Set<String> METADATA_NEUTRAL_OPTIONS = ImmutableSet.of(
            CoreOptions.READ_BATCH_SIZE.key(),
            CoreOptions.FILE_READER_ASYNC_THRESHOLD.key(),
            CoreOptions.FILE_INDEX_READ_ENABLED.key(),
            CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(),
            CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key());

    private PaimonReaderOptions() {
    }

    public static Set<String> supportedOptions() {
        return SUPPORTED_OPTIONS;
    }

    public static Set<String> metadataNeutralOptions() {
        return METADATA_NEUTRAL_OPTIONS;
    }

    public static void validate(String key, String value) {
        if (!SUPPORTED_OPTIONS.contains(key)) {
            throw new IllegalArgumentException("Unsupported Paimon dynamic reader option '" + key
                    + "'. Supported options are " + SUPPORTED_OPTIONS);
        }

        if (CoreOptions.READ_BATCH_SIZE.key().equals(key)) {
            int batchSize = parse(key, value, CoreOptions.READ_BATCH_SIZE);
            // A zero batch can make Paimon's vectorized reader report success without
            // advancing input; the upper bound also prevents one relation from over-allocating.
            requireRange(key, batchSize, MIN_READ_BATCH_SIZE, MAX_READ_BATCH_SIZE);
        } else if (CoreOptions.FILE_READER_ASYNC_THRESHOLD.key().equals(key)) {
            MemorySize threshold = parse(key, value, CoreOptions.FILE_READER_ASYNC_THRESHOLD);
            // Bound the trigger on both sides so a query cannot fan out tiny async reads or
            // silently disable asynchronous reading with an effectively infinite threshold.
            requireRange(key, threshold.getBytes(),
                    MIN_ASYNC_THRESHOLD_BYTES, MAX_ASYNC_THRESHOLD_BYTES);
        } else if (CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key().equals(key)) {
            MemorySize targetSize = parse(key, value, CoreOptions.SOURCE_SPLIT_TARGET_SIZE);
            // A split-size option represents byte capacity; non-positive values silently defeat
            // Paimon's bin packing and turn every data file into a separate Doris scan range.
            requireRange(key, targetSize.getBytes(), 1, Long.MAX_VALUE);
        } else if (CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key().equals(key)) {
            parse(key, value, CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST);
        } else if (CoreOptions.SCAN_MANIFEST_PARALLELISM.key().equals(key)) {
            validateManifestParallelism(value);
        } else if (CoreOptions.FILE_INDEX_READ_ENABLED.key().equals(key)) {
            parse(key, value, CoreOptions.FILE_INDEX_READ_ENABLED);
        } else {
            parse(key, value, CoreOptions.SCAN_PLAN_SORT_PARTITION);
        }
    }

    public static void validateCatalogProperties(Map<String, String> properties) {
        properties.forEach((key, value) -> {
            if (!key.toLowerCase(Locale.ROOT).startsWith(TABLE_OPTION_PREFIX)) {
                return;
            }
            String optionKey = key.substring(TABLE_OPTION_PREFIX.length());
            if (optionKey.isEmpty()) {
                throw new IllegalArgumentException(
                        "Paimon table option name must not be empty after prefix " + TABLE_OPTION_PREFIX);
            }
            validate(optionKey, value);
        });
    }

    public static Map<String, String> compatibleCatalogOptions(Map<String, String> properties) {
        Map<String, String> compatibleOptions = new LinkedHashMap<>();
        properties.forEach((key, value) -> {
            if (!key.toLowerCase(Locale.ROOT).startsWith(TABLE_OPTION_PREFIX)) {
                return;
            }
            String optionKey = key.substring(TABLE_OPTION_PREFIX.length());
            try {
                validate(optionKey, value);
                compatibleOptions.put(optionKey, value);
            } catch (IllegalArgumentException ignored) {
                // Images written before the reader-only allowlist may contain arbitrary Paimon
                // options. Keep the catalog loadable, but never apply an unsafe legacy option.
            }
        });
        return Collections.unmodifiableMap(compatibleOptions);
    }

    public static void validateReaderOptions(Map<String, String> options) {
        SUPPORTED_OPTIONS.stream()
                .filter(options::containsKey)
                .forEach(key -> validate(key, options.get(key)));
    }

    public static void validateEffectiveTableOptions(Map<String, String> options) {
        validateReaderOptions(options);
        validateIfPresentForRuntime(options, CoreOptions.SCAN_MANIFEST_PARALLELISM.key());
    }

    public static OptionalInt backendManifestParallelismCap(Table table) {
        // The transport value is the execution ceiling, not the smallest branch preference;
        // the BE preserves each branch's lower value while independently capping larger siblings.
        return OptionalInt.of(Math.min(
                Runtime.getRuntime().availableProcessors(), MAX_MANIFEST_PARALLELISM));
    }

    public static Table runtimeSafeTable(Table table) {
        return runtimeSafeTable(table, Runtime.getRuntime().availableProcessors());
    }

    static Table runtimeSafeTable(Table table, int localCapacity) {
        if (localCapacity < 1) {
            throw new IllegalArgumentException("Paimon planning capacity must be positive.");
        }
        int safeBound = Math.min(localCapacity, MAX_MANIFEST_PARALLELISM);
        return normalizeManifestParallelism(table, safeBound, localCapacity > safeBound);
    }

    public static Table runtimeSafeSystemTable(
            String systemTableType, Table systemTable, Table sourceTable, Map<String, String> scanOptions) {
        Table effectiveSource = runtimeSafeSystemSource(sourceTable, scanOptions);
        validateEffectiveTable(effectiveSource);
        if (effectiveSource instanceof FileStoreTable) {
            // Paimon dispatches fallback reads only when the fallback pair is the system wrapper's
            // immediate child; a privilege decorator must not hide that pair during this rebuild.
            FileStoreTable systemSource = PaimonTableDecorators.unwrapToFallbackOrBase(
                    (FileStoreTable) effectiveSource);
            Table rebuilt = SystemTableLoader.load(systemTableType, systemSource);
            if (rebuilt == null) {
                throw new IllegalArgumentException("Unsupported Paimon system table '"
                        + systemTableType + "'");
            }
            return rebuilt;
        }
        return runtimeSafeTable(systemTable);
    }

    public static Table runtimeSafeSystemSource(Table sourceTable, Map<String, String> scanOptions) {
        if (PaimonScanParams.isOptionsPin(scanOptions)) {
            if (sourceTable instanceof FileStoreTable
                    && PaimonScanParams.preservesBoundSchema(scanOptions)) {
                // Only an internal statement fence already owns its schema generation. A user tag
                // or snapshot must still time-travel here so the rebuilt wrapper matches binding.
                return PaimonScanParams.applyOptionsWithoutTimeTravel(
                        (FileStoreTable) sourceTable, scanOptions);
            }
            return PaimonScanParams.applyOptions(sourceTable, scanOptions);
        }
        Table effectiveSource = sourceTable;
        if (scanOptions != null && !scanOptions.isEmpty()) {
            // Incremental ranges are relation state too; rebuilding a capped system wrapper from the
            // undecorated source must not silently turn @incr back into a latest scan.
            effectiveSource = sourceTable.copy(
                    PaimonIncrementalScanParams.applyResetsIfIncremental(scanOptions));
        }
        return runtimeSafeTable(effectiveSource);
    }

    private static Table normalizeManifestParallelism(
            Table table, int safeBound, boolean materializeAbsent) {
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) table;
            FileStoreTable main = normalizeManifestParallelism(
                    pair.wrapped(), safeBound, materializeAbsent);
            FileStoreTable other = normalizeManifestParallelism(
                    pair.other(), safeBound, materializeAbsent);
            return main == pair.wrapped() && other == pair.other()
                    ? table : new FallbackReadFileStoreTable(
                            main, other, isWrappedFirst(pair));
        }

        if (table instanceof DelegatedFileStoreTable) {
            if (!(table instanceof PrivilegedFileStoreTable)) {
                throw new IllegalArgumentException("Unsupported Paimon planning table delegate: "
                        + table.getClass().getName());
            }
            FileStoreTable wrapped = ((DelegatedFileStoreTable) table).wrapped();
            FileStoreTable normalized = normalizeManifestParallelism(
                    wrapped, safeBound, materializeAbsent);
            if (normalized == wrapped) {
                return table;
            }
            // A delegate copy broadcasts one value to every fallback branch. Check authorization
            // before peeling the privilege-only layer so branch-local limits remain independent.
            ((FileStoreTable) table).newScan();
            return normalized;
        }

        if (!(table instanceof FileStoreTable)) {
            // System-table wrappers hide the already-normalized source. Treating the wrapper as a
            // planning leaf would broadcast one cap through its private fallback tree on copy().
            return table;
        }

        String key = CoreOptions.SCAN_MANIFEST_PARALLELISM.key();
        String configured = table.options().get(key);
        if (configured == null && !materializeAbsent) {
            return table;
        }
        if (configured != null) {
            validateManifestParallelism(configured);
            if (Integer.parseInt(configured) <= safeBound) {
                return table;
            }
        }
        // Paimon's absent value inherits the processor count. Materialize the bound on every leaf;
        // otherwise one missing fallback child can escape a safe explicit sibling on a large FE.
        Map<String, String> cap = Collections.singletonMap(key, String.valueOf(safeBound));
        return ((FileStoreTable) table).copyWithoutTimeTravel(cap);
    }

    private static FileStoreTable normalizeManifestParallelism(
            FileStoreTable table, int safeBound, boolean materializeAbsent) {
        return (FileStoreTable) normalizeManifestParallelism(
                (Table) table, safeBound, materializeAbsent);
    }

    static boolean isWrappedFirst(FallbackReadFileStoreTable table) {
        Map<String, String> options = table.options();
        // Match FileStoreTableFactory's construction order. Paimon does not expose wrappedFirst.
        if (ChainTableUtils.isChainTable(options)) {
            return true;
        }
        if (!StringUtils.isNullOrWhitespaceOnly(
                options.get(CoreOptions.SCAN_FALLBACK_BRANCH.key()))) {
            return true;
        }
        return StringUtils.isNullOrWhitespaceOnly(
                options.get(CoreOptions.SCAN_PRIMARY_BRANCH.key()));
    }

    public static void validateEffectiveTable(Table table) {
        validateEffectiveTableOptions(table.options());
        if (table instanceof FallbackReadFileStoreTable) {
            // The fallback scan plans its private child independently, so the visible main options
            // cannot prove that every manifest executor input is safe.
            validateEffectiveTable(((FallbackReadFileStoreTable) table).other());
        }
        if (table instanceof DelegatedFileStoreTable) {
            // Privilege and other supported delegates can hide a fallback planner behind their
            // own visible main options, so traverse the complete planning-handle chain.
            validateEffectiveTable(((DelegatedFileStoreTable) table).wrapped());
        }
    }

    public static void validateEffectivePlanningTable(Table table) {
        // Partition projection never opens a data reader. Revalidating batch/async values here
        // would reject a raw handle even when the final relation copy safely overrides them.
        validateIfPresentForRuntime(table.options(), CoreOptions.SCAN_MANIFEST_PARALLELISM.key());
        validateIfPresent(table.options(), CoreOptions.SCAN_PLAN_SORT_PARTITION.key());
        if (table instanceof FallbackReadFileStoreTable) {
            validateEffectivePlanningTable(((FallbackReadFileStoreTable) table).other());
        }
        if (table instanceof DelegatedFileStoreTable) {
            validateEffectivePlanningTable(((DelegatedFileStoreTable) table).wrapped());
        }
    }

    private static void validateIfPresent(Map<String, String> options, String key) {
        if (options.containsKey(key)) {
            validate(key, options.get(key));
        }
    }

    private static void validateIfPresentForRuntime(Map<String, String> options, String key) {
        validateIfPresent(options, key);
        if (options.containsKey(key)) {
            validateRuntimeManifestParallelism(options.get(key));
        }
    }

    private static void validateManifestParallelism(String value) {
        if (value == null) {
            return;
        }
        int parallelism;
        try {
            parallelism = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for Paimon option '"
                    + CoreOptions.SCAN_MANIFEST_PARALLELISM.key() + "': " + value, e);
        }
        // Catalog properties are replayed on every FE, so validity must not depend on the CPU count
        // of whichever node happens to deserialize the image or become leader.
        requireRange(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), parallelism, 1, MAX_MANIFEST_PARALLELISM);
    }

    private static void validateRuntimeManifestParallelism(String value) {
        int parallelism = Integer.parseInt(value);
        requireRange(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), parallelism, 1,
                Runtime.getRuntime().availableProcessors());
    }

    private static <T> T parse(String key, String value, ConfigOption<T> option) {
        try {
            return new Options(Collections.singletonMap(key, value)).get(option);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value for Paimon option '" + key + "': "
                    + e.getMessage(), e);
        }
    }

    private static void requireRange(String key, long value, long minimum, long maximum) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException("Paimon option '" + key + "' must be between "
                    + minimum + " and " + maximum + ", but was " + value);
        }
    }
}
