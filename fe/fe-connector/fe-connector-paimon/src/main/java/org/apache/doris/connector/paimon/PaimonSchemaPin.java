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
import org.apache.doris.connector.spi.mvcc.ConnectorSqlCacheVersion;

import com.google.common.hash.Hashing;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.SnapshotManager;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
            TableSchema fallback = pair.other().schemaManager().latest().orElseThrow(IllegalStateException::new);
            String fallbackPath = path + "fallback.";
            long fallbackSnapshotId = -1L;
            if (snapshotId >= 0) {
                SnapshotManager manager = pair.other().snapshotManager();
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
            capture(pair.other(), fallback.id(), fallbackSnapshotId, fallbackPath, pin);
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
            validate(pair.other(), pin, path + "fallback.");
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

    static Map<String, String> captureRestSchemas(FileStoreTable table) {
        Map<String, String> schemas = new HashMap<>();
        captureRestSchemas(table, "", schemas);
        schemas.put("shape", shape(table));
        return schemas;
    }

    private static void captureRestSchemas(FileStoreTable table, String path, Map<String, String> schemas) {
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) table;
            captureRestSchemas(pair.wrapped(), path, schemas);
            captureRestSchemas(pair.other(), path + "fallback.", schemas);
        } else if (table instanceof DelegatedFileStoreTable) {
            captureRestSchemas(((DelegatedFileStoreTable) table).wrapped(), path, schemas);
        } else {
            SchemaManager manager = table.schemaManager();
            schemas.put(path, restSchemaIdentity(manager, manager.schema(table.schema().id())));
        }
    }

    static FileStoreTable bindRestSchema(Table refreshed, Map<String, String> schemas) {
        if (!(refreshed instanceof FileStoreTable) || !shape(refreshed).equals(schemas.get("shape"))) {
            throw changed();
        }
        return bindRestSchema((FileStoreTable) refreshed, "", schemas);
    }

    private static FileStoreTable bindRestSchema(FileStoreTable table, String path, Map<String, String> schemas) {
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) table;
            return PaimonTableDecorators.rebuildFallbackPair(pair,
                    bindRestSchema(pair.wrapped(), path, schemas),
                    bindRestSchema(pair.other(), path + "fallback.", schemas));
        }
        TableSchema retained = table.schema();
        SchemaManager manager = table.schemaManager();
        TableSchema persisted = manager.schema(retained.id());
        // Bracket the fresh REST lookup with physical reads. The first REST handle only locates files;
        // reusing its options could combine an old bucket key with a replacement's schema identity.
        if (!restSchemaIdentity(manager, persisted).equals(schemas.get(path))) {
            throw changed();
        }
        // REST synthesizes timeMillis, version and highestFieldId, so compare its logical schema only
        // after confirming that the physical generation stayed unchanged across the REST lookup.
        if (!retained.copy(Collections.emptyMap()).toSchema()
                .equals(persisted.copy(Collections.emptyMap()).toSchema())) {
            throw new DorisConnectorException("Paimon REST schema does not match stored schema "
                    + retained.id() + "; retry the statement");
        }
        // Preserve the freshly loaded REST FileIO, catalog callbacks and options. Cache hits never rebind.
        return table.copy(persisted.copy(retained.options()));
    }

    private static String restSchemaIdentity(SchemaManager manager, TableSchema schema) {
        // Include the branch/path and all persisted options, even if a replacement reuses a timestamp.
        return manager.toSchemaPath(schema.id()) + "\n" + schemaDigest(schema);
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
            return "fallback(" + shape(pair.wrapped()) + "," + shape(pair.other()) + ")";
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

    static Optional<ConnectorSqlCacheVersion> resolveSqlCacheVersion(
            Table table, String branchName, Map<String, String> scanOptions) {
        if (!PaimonScanParams.hasPinnedVersion(scanOptions)) {
            return Optional.empty();
        }
        if (hasQueryAuth(table)) {
            // Dynamic server-side row/column authorization runs in scan.plan(); do not serve cached results.
            return Optional.empty();
        }
        if (!coordinates(scanOptions).isEmpty()) {
            validate(table, scanOptions);
        }
        Optional<ResolvedVersion> resolved = resolveTableVersion(table, scanOptions, "");
        if (!resolved.isPresent()) {
            return Optional.empty();
        }
        String branch = branchName == null ? "main" : branchName;
        String payload = "branch=" + branch
                + "|options=" + PaimonScanParams.canonicalCacheOptions(scanOptions)
                + "|table=" + resolved.get().token;
        return Optional.of(new ConnectorSqlCacheVersion(digest(payload), resolved.get().updateTimeMillis));
    }

    private static boolean hasQueryAuth(Table table) {
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) table;
            return hasQueryAuth(pair.wrapped()) || hasQueryAuth(pair.other());
        }
        if (table instanceof DelegatedFileStoreTable) {
            return hasQueryAuth(((DelegatedFileStoreTable) table).wrapped());
        }
        return table instanceof FileStoreTable && ((FileStoreTable) table).coreOptions().queryAuthEnabled();
    }

    private static Optional<ResolvedVersion> resolveTableVersion(
            Table table, Map<String, String> scanOptions, String path) {
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) table;
            Optional<ResolvedVersion> main = resolveTableVersion(pair.wrapped(), scanOptions, path);
            if (!main.isPresent()) {
                return Optional.empty();
            }
            Optional<ResolvedVersion> fallback = resolveFallbackBranchVersion(
                    pair.other(), scanOptions, path + "fallback.", main.get().mainSnapshotMillis);
            if (!fallback.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(new ResolvedVersion(
                    "fallback(" + main.get().token + "," + fallback.get().token + ")",
                    Math.max(main.get().updateTimeMillis, fallback.get().updateTimeMillis),
                    main.get().mainSnapshotMillis));
        }
        if (table instanceof DelegatedFileStoreTable) {
            return resolveTableVersion(((DelegatedFileStoreTable) table).wrapped(), scanOptions, path);
        }
        if (!(table instanceof DataTable)) {
            return Optional.empty();
        }
        return resolveDataTableVersion((DataTable) table, scanOptions);
    }

    private static Optional<ResolvedVersion> resolveDataTableVersion(
            DataTable data, Map<String, String> scanOptions) {
        OptionalLong boundSchema = PaimonScanParams.boundSchemaId(scanOptions);
        String tagName = scanOptions.get(CoreOptions.SCAN_TAG_NAME.key());
        if (tagName != null) {
            Optional<Tag> tagOpt = data.tagManager().get(tagName);
            if (!tagOpt.isPresent()) {
                return Optional.empty();
            }
            Tag tag = tagOpt.get();
            long schemaId = boundSchema.orElse(tag.schemaId());
            Optional<TableSchema> schema = readSchema(data.schemaManager(), schemaId);
            if (!schema.isPresent()) {
                return Optional.empty();
            }
            String token = data.location() + "|schema:" + schemaId + ":" + schemaDigest(schema.get())
                    + "|tag:" + tagName + ":" + tag.id() + ":" + digest(tag.toJson());
            return Optional.of(new ResolvedVersion(token, tag.timeMillis(), OptionalLong.of(tag.timeMillis())));
        }
        if (PaimonScanParams.isPinnedEmptyScan(scanOptions)) {
            Optional<TableSchema> schema = boundSchema.isPresent()
                    ? readSchema(data.schemaManager(), boundSchema.getAsLong())
                    : data.schemaManager().latest();
            if (!schema.isPresent()) {
                return Optional.empty();
            }
            String token = data.location() + "|schema:" + schema.get().id() + ":"
                    + schemaDigest(schema.get()) + "|empty";
            return Optional.of(new ResolvedVersion(token, 0L, OptionalLong.empty()));
        }
        String snapshotIdValue = scanOptions.get(CoreOptions.SCAN_SNAPSHOT_ID.key());
        if (snapshotIdValue == null) {
            return Optional.empty();
        }
        long snapshotId = Long.parseLong(snapshotIdValue);
        if (!data.snapshotManager().snapshotExists(snapshotId)) {
            return Optional.empty();
        }
        Snapshot snap = snapshot(data, snapshotId);
        long schemaId = boundSchema.orElse(snap.schemaId());
        Optional<TableSchema> schema = readSchema(data.schemaManager(), schemaId);
        if (!schema.isPresent()) {
            return Optional.empty();
        }
        String token = data.location() + "|schema:" + schemaId + ":" + schemaDigest(schema.get())
                + "|snap:" + snapshotId + ":" + digest(snap.toJson());
        return Optional.of(new ResolvedVersion(token, snap.timeMillis(), OptionalLong.of(snap.timeMillis())));
    }

    private static Optional<ResolvedVersion> resolveFallbackBranchVersion(
            FileStoreTable other, Map<String, String> scanOptions, String fallbackPath,
            OptionalLong mainSnapshotMillis) {
        if (other instanceof FallbackReadFileStoreTable) {
            // Chain tables nest a ChainGroupReadTable (snapshotBranch, deltaBranch) as pair.other();
            // mirror capture() by walking both branches so delta-branch commits invalidate SQL cache.
            FallbackReadFileStoreTable nested = (FallbackReadFileStoreTable) other;
            Optional<ResolvedVersion> first = resolveFallbackBranchVersion(
                    nested.wrapped(), scanOptions, fallbackPath, mainSnapshotMillis);
            if (!first.isPresent()) {
                return Optional.empty();
            }
            Optional<ResolvedVersion> second = resolveFallbackBranchVersion(
                    nested.other(), scanOptions, fallbackPath + "fallback.", first.get().mainSnapshotMillis);
            if (!second.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(new ResolvedVersion(
                    "fallback(" + first.get().token + "," + second.get().token + ")",
                    Math.max(first.get().updateTimeMillis, second.get().updateTimeMillis),
                    first.get().mainSnapshotMillis));
        }
        if (other instanceof DelegatedFileStoreTable) {
            return resolveFallbackBranchVersion(
                    ((DelegatedFileStoreTable) other).wrapped(), scanOptions, fallbackPath, mainSnapshotMillis);
        }
        String pinnedFallbackSchema = scanOptions.get(PREFIX + fallbackPath + "schema-id");
        Optional<TableSchema> schema = pinnedFallbackSchema != null
                ? readSchema(other.schemaManager(), Long.parseLong(pinnedFallbackSchema))
                : other.schemaManager().latest();
        if (!schema.isPresent()) {
            return Optional.empty();
        }
        SnapshotManager manager = other.snapshotManager();
        String pinnedFallbackSnapshot = fallbackSnapshotId(scanOptions, fallbackPath);
        Long selectedSnapshotId;
        if (pinnedFallbackSnapshot != null) {
            selectedSnapshotId = Long.parseLong(pinnedFallbackSnapshot);
        } else if (Boolean.parseBoolean(scanOptions.get(PREFIX + fallbackPath + "snapshot-absent"))) {
            selectedSnapshotId = null;
        } else if (mainSnapshotMillis.isPresent()) {
            Snapshot eligible = manager.earlierOrEqualTimeMills(mainSnapshotMillis.getAsLong());
            long candidate = eligible == null ? Snapshot.FIRST_SNAPSHOT_ID : eligible.id();
            selectedSnapshotId = manager.snapshotExists(candidate) ? candidate : null;
        } else {
            selectedSnapshotId = manager.latestSnapshotId();
        }
        if (selectedSnapshotId == null) {
            String token = other.location() + "|schema:" + schema.get().id() + ":"
                    + schemaDigest(schema.get()) + "|empty";
            return Optional.of(new ResolvedVersion(token, 0L, OptionalLong.empty()));
        }
        if (!manager.snapshotExists(selectedSnapshotId)) {
            return Optional.empty();
        }
        Snapshot snap = snapshot(other, selectedSnapshotId);
        String token = other.location() + "|schema:" + schema.get().id() + ":" + schemaDigest(schema.get())
                + "|snap:" + selectedSnapshotId + ":" + digest(snap.toJson());
        return Optional.of(new ResolvedVersion(token, snap.timeMillis(), OptionalLong.of(snap.timeMillis())));
    }

    private static Optional<TableSchema> readSchema(SchemaManager manager, long schemaId) {
        try {
            return Optional.of(manager.schema(schemaId));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static final class ResolvedVersion {
        private final String token;
        private final long updateTimeMillis;
        private final OptionalLong mainSnapshotMillis;

        private ResolvedVersion(String token, long updateTimeMillis, OptionalLong mainSnapshotMillis) {
            this.token = token;
            this.updateTimeMillis = updateTimeMillis;
            this.mainSnapshotMillis = mainSnapshotMillis;
        }
    }

    private static DorisConnectorException changed() {
        return new DorisConnectorException(
                "Paimon table generation changed after the statement was pinned; retry the statement");
    }

    private static String digest(String value) {
        return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();
    }
}
