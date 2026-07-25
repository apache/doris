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

    private static final Set<String> SNAPSHOT_SELECTOR_KEYS = ImmutableSet.of(
            CoreOptions.SCAN_SNAPSHOT_ID.key(),
            CoreOptions.SCAN_TAG_NAME.key(),
            CoreOptions.SCAN_TIMESTAMP.key(),
            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
            CoreOptions.SCAN_WATERMARK.key(),
            CoreOptions.SCAN_VERSION.key());

    private static final Set<String> INCREMENTAL_SYSTEM_TABLES = ImmutableSet.of(
            "audit_log", "binlog", "row_tracking");

    private static final Set<String> OPTIONS_SYSTEM_TABLES = ImmutableSet.of(
            "audit_log", "binlog", "files", "manifests", "partitions", "ro", "row_tracking");

    private PaimonScanParams() {
    }

    public static void validateOptions(Map<String, String> options) {
        Set<String> unsupported = options.keySet().stream()
                .filter(key -> !QUERY_OPTION_KEYS.contains(key))
                .collect(Collectors.toSet());
        if (!unsupported.isEmpty()) {
            throw new IllegalArgumentException("Unsupported Paimon query option(s): " + unsupported);
        }

        long selectorCount = options.keySet().stream().filter(SNAPSHOT_SELECTOR_KEYS::contains).count();
        if (selectorCount > 1) {
            throw new IllegalArgumentException(
                    "Only one Paimon snapshot selector can be specified: " + SNAPSHOT_SELECTOR_KEYS);
        }
    }

    public static Table applyOptions(Table table, Map<String, String> options) {
        validateOptions(options);
        Map<String, String> isolatedOptions = new HashMap<>(options);
        if (options.keySet().stream().anyMatch(SNAPSHOT_SELECTOR_KEYS::contains)) {
            // Cached MVCC handles are pinned with scan.snapshot-id. Clear every competing selector
            // so a relation-local tag/timestamp selection cannot inherit another relation's snapshot.
            SNAPSHOT_SELECTOR_KEYS.stream()
                    .filter(key -> !options.containsKey(key))
                    .forEach(key -> isolatedOptions.put(key, null));
        }
        return table.copy(isolatedOptions);
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
