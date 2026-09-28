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

import com.google.common.hash.Hashing;
import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.SnapshotManager;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/** Immutable file coordinates carried by the MVCC pin, including across INSERT planning attempts. */
final class PaimonSchemaPin {
    private static final String PREFIX = "doris.internal.paimon.schema-pin.";

    private PaimonSchemaPin() {}

    static Map<String, String> capture(Table table, long schemaId, long snapshotId) {
        Map<String, String> pin = new HashMap<>();
        if (schemaId >= 0) {
            capture(table, schemaId, snapshotId, "", pin);
            if (!pin.isEmpty()) {
                pin.put(PREFIX + "shape", shape(table));
            }
        }
        return pin;
    }

    private static void capture(Table table, long schemaId, long snapshotId, String path, Map<String, String> pin) {
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) table;
            capture(pair.wrapped(), schemaId, snapshotId, path, pin);
            TableSchema fallback = pair.fallback().schemaManager().latest().orElseThrow(IllegalStateException::new);
            String fallbackPath = path + "fallback.";
            long fallbackSnapshotId = -1L;
            if (snapshotId >= 0) {
                SnapshotManager manager = pair.fallback().snapshotManager();
                long time = snapshot(pair.wrapped(), snapshotId).timeMillis();
                Snapshot eligible = manager.earlierOrEqualTimeMills(time);
                // Match the SDK's FIRST_SNAPSHOT_ID fallback once, while pinning, rather than
                // allowing a later handle reload to select replacement branch data.
                fallbackSnapshotId = eligible == null ? Snapshot.FIRST_SNAPSHOT_ID : eligible.id();
                if (!manager.snapshotExists(fallbackSnapshotId)) {
                    fallbackSnapshotId = -1L;
                    pin.put(PREFIX + fallbackPath + "snapshot-absent", "true");
                }
            }
            capture(pair.fallback(), fallback.id(), fallbackSnapshotId, fallbackPath, pin);
        } else if (table instanceof DelegatedFileStoreTable) {
            capture(((DelegatedFileStoreTable) table).wrapped(), schemaId, snapshotId, path, pin);
        } else if (table instanceof DataTable) {
            DataTable data = (DataTable) table;
            pin.put(PREFIX + path + "schema-id", Long.toString(schemaId));
            pin.put(PREFIX + path + "schema", schemaDigest(data.schemaManager().schema(schemaId)));
            if (snapshotId >= 0) {
                pin.put(PREFIX + path + "snapshot-id", Long.toString(snapshotId));
                pin.put(PREFIX + path + "snapshot", snapshotDigest(data, snapshotId));
            }
        }
    }

    static void validate(Table table, Map<String, String> pin) {
        String capturedShape = pin.get(PREFIX + "shape");
        if (capturedShape != null && !capturedShape.equals(shape(table))) {
            throw changed();
        }
        validate(table, pin, "");
    }

    private static void validate(Table table, Map<String, String> pin, String path) {
        if (!pin.containsKey(PREFIX + path + "schema-id")) {
            return;
        }
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) table;
            validate(pair.wrapped(), pin, path);
            validate(pair.fallback(), pin, path + "fallback.");
        } else if (table instanceof DelegatedFileStoreTable) {
            validate(((DelegatedFileStoreTable) table).wrapped(), pin, path);
        } else if (table instanceof DataTable) {
            DataTable data = (DataTable) table;
            long schemaId = Long.parseLong(pin.get(PREFIX + path + "schema-id"));
            // IDs are reusable after DROP/CREATE; compare the immutable schema and snapshot contents
            // before either a retained schema or a newly resolved scan table can consume those IDs.
            if (!pin.get(PREFIX + path + "schema").equals(schemaDigest(data.schemaManager().schema(schemaId)))) {
                throw changed();
            }
            if (pin.containsKey(PREFIX + path + "snapshot-absent")
                    && data.snapshotManager().latestSnapshotId() != null) {
                throw changed();
            }
            String snapshotId = pin.get(PREFIX + path + "snapshot-id");
            if (snapshotId != null) {
                long id = Long.parseLong(snapshotId);
                if (!data.snapshotManager().snapshotExists(id)
                        || !pin.get(PREFIX + path + "snapshot").equals(snapshotDigest(data, id))) {
                    throw changed();
                }
            }
        }
    }

    static void validateRetainedSchema(TableSchema retained, TableSchema persisted) {
        // Dynamic copies legitimately overlay options, but preserve the schema's structural metadata
        // and creation timestamp. Compare those against the same ID's physical history, not latest.
        if (!schemaDigest(retained.copy(Collections.emptyMap()))
                .equals(schemaDigest(persisted.copy(Collections.emptyMap())))) {
            throw changed();
        }
    }

    static String schemaDigest(TableSchema schema) {
        return digest(schema.toString());
    }

    static void validateSchema(PaimonCatalogOps.PaimonSchemaSnapshot schema, Map<String, String> pin) {
        String captured = pin.get(PREFIX + "schema");
        if (captured != null && schema.fileDigest() != null && !captured.equals(schema.fileDigest())) {
            throw changed();
        }
    }

    private static String snapshotDigest(DataTable table, long snapshotId) {
        return digest(snapshot(table, snapshotId).toJson());
    }

    private static Snapshot snapshot(DataTable table, long snapshotId) {
        SnapshotManager manager = table.snapshotManager();
        // SDK snapshot caches are keyed by reusable paths, so generation checks must bypass them.
        return SnapshotManager.fromPath(manager.fileIO(), manager.snapshotPath(snapshotId));
    }

    private static String shape(Table table) {
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) table;
            return "fallback(" + shape(pair.wrapped()) + "," + shape(pair.fallback()) + ")";
        }
        if (table instanceof DelegatedFileStoreTable) {
            return shape(((DelegatedFileStoreTable) table).wrapped());
        }
        return table instanceof DataTable ? "data" : "other";
    }

    static Map<String, String> coordinates(Map<String, String> options) {
        Map<String, String> result = new HashMap<>();
        options.forEach((key, value) -> {
            if (key.startsWith(PREFIX)) {
                result.put(key, value);
            }
        });
        return result;
    }

    static String fallbackSnapshotId(Map<String, String> options, String path) {
        return options.get(PREFIX + path + "snapshot-id");
    }

    static long fallbackSchemaId(Map<String, String> options, String path, LongSupplier defaultId) {
        String pinned = options.get(PREFIX + path + "schema-id");
        return pinned == null ? defaultId.getAsLong() : Long.parseLong(pinned);
    }

    private static DorisConnectorException changed() {
        return new DorisConnectorException(
                "Paimon table generation changed after the statement was pinned; retry the statement");
    }

    private static String digest(String value) {
        return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();
    }
}
