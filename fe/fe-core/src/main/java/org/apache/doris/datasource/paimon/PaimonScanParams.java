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

package org.apache.doris.datasource.paimon;

import org.apache.doris.analysis.TableScanParams;

import com.google.common.collect.ImmutableSet;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.Table;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validation and application rules for relation-scoped Paimon scan parameters.
 */
public final class PaimonScanParams {
    private static final Set<String> QUERY_OPTION_KEYS = CoreOptions.getOptions().stream()
            .map(option -> option.key())
            .filter(key -> key.startsWith("scan."))
            .collect(Collectors.toSet());

    private static final Set<String> STARTUP_POSITION_KEYS = ImmutableSet.of(
            CoreOptions.SCAN_TIMESTAMP.key(),
            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
            CoreOptions.SCAN_WATERMARK.key(),
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(),
            CoreOptions.SCAN_CREATION_TIME_MILLIS.key(),
            CoreOptions.SCAN_SNAPSHOT_ID.key(),
            CoreOptions.SCAN_TAG_NAME.key(),
            CoreOptions.SCAN_VERSION.key());

    private static final Set<String> INHERITED_READ_STATE_KEYS = ImmutableSet.<String>builder()
            .addAll(STARTUP_POSITION_KEYS)
            .add(CoreOptions.SCAN_MODE.key())
            .add(CoreOptions.SCAN_BOUNDED_WATERMARK.key())
            .add(CoreOptions.INCREMENTAL_BETWEEN.key())
            .add(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key())
            .add(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key())
            .add(CoreOptions.INCREMENTAL_TO_AUTO_TAG.key())
            .build();

    private static final Set<String> INCREMENTAL_SYSTEM_TABLES = ImmutableSet.of(
            "audit_log", "binlog", "row_tracking");

    private static final Set<String> OPTIONS_SYSTEM_TABLES = ImmutableSet.of(
            "audit_log", "binlog", "files", "manifests", "partitions", "ro", "row_tracking");

    private PaimonScanParams() {
    }

    public static void validateOptions(Map<String, String> options) {
        if (options.containsKey(CoreOptions.SCAN_FALLBACK_BRANCH.key())) {
            throw new IllegalArgumentException("Paimon query option '"
                    + CoreOptions.SCAN_FALLBACK_BRANCH.key()
                    + "' is not supported because it requires rebuilding the table through the catalog factory.");
        }

        Set<String> unsupported = options.keySet().stream()
                .filter(key -> !QUERY_OPTION_KEYS.contains(key))
                .collect(Collectors.toSet());
        if (!unsupported.isEmpty()) {
            throw new IllegalArgumentException("Unsupported Paimon query option(s): " + unsupported);
        }

        long positionCount = options.keySet().stream().filter(STARTUP_POSITION_KEYS::contains).count();
        if (positionCount > 1) {
            throw new IllegalArgumentException(
                    "Only one Paimon startup position can be specified: " + STARTUP_POSITION_KEYS);
        }

        if (options.containsKey(CoreOptions.SCAN_MODE.key()) && positionCount == 1) {
            String position = options.keySet().stream()
                    .filter(STARTUP_POSITION_KEYS::contains)
                    .findFirst()
                    .get();
            String mode = options.get(CoreOptions.SCAN_MODE.key()).toLowerCase(Locale.ROOT);
            if (!isCompatibleStartupMode(position, mode)) {
                throw new IllegalArgumentException("Paimon scan mode '" + mode
                        + "' is incompatible with startup position '" + position + "'.");
            }
        }
    }

    public static Table applyOptions(Table table, Map<String, String> options) {
        validateOptions(options);
        Map<String, String> isolatedOptions = new HashMap<>(options);
        if (hasStartupOptions(options)) {
            // Startup mode, position, and range form one inherited state family in Paimon. Clear
            // absent members so one relation cannot accidentally reuse another relation's read state.
            INHERITED_READ_STATE_KEYS.stream()
                    .filter(key -> !options.containsKey(key))
                    .forEach(key -> isolatedOptions.put(key, null));
        }
        return table.copy(isolatedOptions);
    }

    public static boolean hasStartupOptions(Map<String, String> options) {
        return options.containsKey(CoreOptions.SCAN_MODE.key())
                || options.keySet().stream().anyMatch(STARTUP_POSITION_KEYS::contains);
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
        return INCREMENTAL_SYSTEM_TABLES.contains(systemTableType.toLowerCase());
    }

    public static void validateSystemTable(String systemTableType, TableScanParams scanParams) {
        if (scanParams == null) {
            return;
        }
        if (scanParams.incrementalRead() && !supportsIncrementalRead(systemTableType)) {
            throw new IllegalArgumentException(
                    "Paimon system table '" + systemTableType + "' does not support INCR scan params.");
        }
        if (scanParams.isOptions() && !supportsOptions(systemTableType)) {
            throw new IllegalArgumentException(
                    "Paimon system table '" + systemTableType + "' does not support OPTIONS scan params.");
        }
        if (!scanParams.incrementalRead() && !scanParams.isOptions()) {
            throw new IllegalArgumentException("Paimon system tables only support INCR or OPTIONS scan params.");
        }
    }
}
