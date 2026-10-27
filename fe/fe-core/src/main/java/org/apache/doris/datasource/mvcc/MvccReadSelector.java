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

package org.apache.doris.datasource.mvcc;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Immutable read selector shared by statement snapshot lookup and SQL result-cache dependencies.
 * Stores the original selector, never a prior execution's resolved dynamic parameters. Every resolution
 * receives fresh copies, so rebinding or PREPARE/EXECUTE cannot mutate a cached dependency.
 */
public final class MvccReadSelector {
    public static final MvccReadSelector LATEST = new MvccReadSelector(Optional.empty(), Optional.empty());

    private final Optional<TableSnapshot> tableSnapshot;
    private final Optional<TableScanParams> scanParams;
    private final String key;

    private MvccReadSelector(Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams) {
        this.tableSnapshot = tableSnapshot.map(TableSnapshot::new);
        this.scanParams = scanParams.map(MvccReadSelector::copyScanParams);
        this.key = versionKeyOf(this.tableSnapshot, this.scanParams);
    }

    public static MvccReadSelector of(Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams) {
        return tableSnapshot.isEmpty() && scanParams.isEmpty()
                ? LATEST : new MvccReadSelector(tableSnapshot, scanParams);
    }

    public Optional<TableSnapshot> getTableSnapshot() {
        return tableSnapshot.map(TableSnapshot::new);
    }

    public Optional<TableScanParams> getScanParams() {
        return scanParams.map(MvccReadSelector::copyScanParams);
    }

    public boolean isLatest() {
        return key.isEmpty();
    }

    public String getKey() {
        return key;
    }

    private static TableScanParams copyScanParams(TableScanParams params) {
        return new TableScanParams(params.getParamType(), params.getMapParams(), params.getListParams());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof MvccReadSelector && key.equals(((MvccReadSelector) other).key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    private static String versionKeyOf(Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams) {
        // Concatenate both selectors (rather than returning on the first) so the key stays injective even
        // if a reference ever carried BOTH a snapshot and scan params. Today the two are mutually exclusive
        // (e.g. IcebergUtils.getQuerySpecSnapshot rejects FOR-TIME together with @branch/@tag), so in every
        // reachable case exactly one part is non-empty and the key is "v:...", "p:...", or "".
        StringBuilder key = new StringBuilder();
        if (tableSnapshot.isPresent()) {
            TableSnapshot ts = tableSnapshot.get();
            key.append('v');
            appendVersionKeyPart(key, ts.getType());
            appendVersionKeyPart(key, ts.getValue());
        }
        if (scanParams.isPresent()) {
            TableScanParams sp = scanParams.get();
            key.append('p');
            appendVersionKeyPart(key, sp.getParamType());
            Map<String, String> sortedParams = new TreeMap<>(sp.getMapParams());
            key.append('m').append(sortedParams.size()).append(':');
            for (Map.Entry<String, String> entry : sortedParams.entrySet()) {
                // Length prefixes preserve entry boundaries even when user values contain Map.toString()
                // delimiters; sorting separately keeps semantically identical maps order-independent.
                appendVersionKeyPart(key, entry.getKey());
                appendVersionKeyPart(key, entry.getValue());
            }
            key.append('l').append(sp.getListParams().size()).append(':');
            for (String value : sp.getListParams()) {
                appendVersionKeyPart(key, value);
            }
        }
        return key.toString();
    }

    private static void appendVersionKeyPart(StringBuilder key, Object value) {
        String text = String.valueOf(value);
        key.append(text.length()).append(':').append(text);
    }
}
