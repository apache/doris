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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.DataField;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Injection seam over the remote Paimon {@link Catalog} calls.
 *
 * <p>The default {@link CatalogBackedPaimonCatalogOps} simply delegates to a real
 * {@code Catalog}, which requires a live remote catalog (filesystem / HMS / DLF / REST /
 * JDBC). By depending on this interface instead of {@code Catalog} directly,
 * {@link PaimonConnectorMetadata} becomes unit-testable offline with a hand-written
 * recording fake (no Mockito) — mirroring the maxcompute connector's
 * {@link org.apache.doris.connector.maxcompute.McStructureHelper McStructureHelper} pattern.
 *
 * <p>The read methods landed in B0. B3 added the four DDL methods
 * ({@link #createDatabase}, {@link #dropDatabase}, {@link #createTable}, {@link #dropTable}),
 * whose signatures (and checked exceptions) mirror the real Paimon {@code Catalog} exactly.
 * Existence is probed via the existing {@link #getTable} / {@link #getDatabase} read methods
 * (plus the caught not-exist exceptions); the seam intentionally has no separate probe methods.
 */
public interface PaimonCatalogOps {

    List<String> listDatabases();

    Database getDatabase(String name) throws Catalog.DatabaseNotExistException;

    List<String> listTables(String databaseName) throws Catalog.DatabaseNotExistException;

    Table getTable(Identifier identifier) throws Catalog.TableNotExistException;

    List<Partition> listPartitions(Identifier identifier) throws Catalog.TableNotExistException;

    void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws Catalog.DatabaseAlreadyExistException;

    void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws Catalog.DatabaseNotExistException, Catalog.DatabaseNotEmptyException;

    void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException;

    void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws Catalog.TableNotExistException;

    // ---- E5: MVCC snapshot lookups (T20) ----
    // These return plain {@code long}s (not paimon {@code Snapshot} objects) so the metadata
    // layer's MVCC logic (sys-guard, empty->-1, found/empty mapping) is unit-testable offline with
    // {@code RecordingPaimonCatalogOps} — faking a concrete paimon {@code Snapshot}/
    // {@code SnapshotManager} directly is impractical. The production impl uses the paimon SDK.

    /**
     * Returns the latest snapshot id of {@code table} ({@code table.latestSnapshot().get().id()}),
     * or empty when the table has no snapshot (empty table). The caller maps empty to the legacy
     * {@code INVALID_SNAPSHOT_ID} (-1).
     */
    OptionalLong latestSnapshotId(Table table);

    /**
     * Returns the id of the latest snapshot committed at or before {@code timestampMillis}
     * ({@code snapshotManager().earlierOrEqualTimeMills(ts)}), or empty when no such snapshot
     * exists (the SDK returns null).
     */
    OptionalLong snapshotIdAtOrBefore(Table table, long timestampMillis);

    /**
     * Returns {@code true} iff a snapshot with {@code snapshotId} exists
     * ({@code snapshotManager().tryGetSnapshot(id)} succeeds; a {@code FileNotFoundException} from
     * the SDK means it does not exist).
     */
    boolean snapshotExists(Table table, long snapshotId);

    // ---- B5b-2a: explicit time-travel resolution (SNAPSHOT_ID / TIMESTAMP / TAG) ----
    // Like the T20 lookups above, these return plain {@code long}s / small immutable structs (never
    // a raw paimon {@code Snapshot} / {@code Tag} / {@code TableSchema}) so the metadata layer's
    // resolution logic is unit-testable offline with {@code RecordingPaimonCatalogOps}.

    /**
     * Returns the schema version (schemaId) of the snapshot with {@code snapshotId}
     * ({@code snapshotManager().snapshot(id).schemaId()}), or empty when it cannot be resolved.
     * Used to stamp {@link org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot#getSchemaId()}
     * for snapshot-id / timestamp time-travel so schema-at-snapshot reads pick the historical schema.
     */
    OptionalLong snapshotSchemaId(Table table, long snapshotId);

    /**
     * Resolves the named tag to its snapshot id + schema id ({@code tagManager().get(tagName)};
     * a paimon {@code Tag} IS-A {@code Snapshot}, so {@code tag.id()} / {@code tag.schemaId()}), or
     * empty when no tag with {@code tagName} exists. Legacy
     * {@code PaimonUtil.getPaimonSnapshotByTag} threw on absent; this returns empty (the metadata
     * layer maps empty → {@link java.util.Optional#empty()} per the SPI empty-if-none contract).
     */
    Optional<TagSnapshot> getSnapshotByTag(Table table, String tagName);

    /**
     * Returns the schema AS OF {@code schemaId} ({@code schemaManager().schema(schemaId)}), reduced
     * to the fields + partition keys + primary keys the metadata layer needs to build Doris columns.
     * Mirrors legacy {@code PaimonExternalTable.initSchema(schemaId)}, which read the same
     * {@code tableSchema.fields()} / {@code tableSchema.partitionKeys()} of the pinned version.
     */
    PaimonSchemaSnapshot schemaAt(Table table, long schemaId);

    // ---- B5b-2c: branch time-travel resolution ----

    /**
     * Returns true iff {@code branchName} exists on {@code table}. The base table must be a
     * {@code FileStoreTable} (cast + {@code branchManager().branchExists(name)}, mirroring legacy
     * PaimonUtil.resolvePaimonBranch); a non-FileStoreTable backend (e.g. jdbc-only) cannot have
     * branches, so this returns {@code false} gracefully (the metadata layer maps that to
     * Optional.empty(), which the fe-core consumer later translates to "can't find branch").
     */
    boolean branchExists(Table table, String branchName);

    /**
     * Returns the total row count of {@code table} = sum of {@code split.rowCount()} over
     * {@code table.newReadBuilder().newScan().plan().splits()} (legacy
     * {@code PaimonExternalTable.fetchRowCount} / {@code PaimonSysExternalTable.fetchRowCount}).
     * Returns a plain {@code long} (never a paimon {@code Split} list) so the metadata layer's
     * &gt;0-else-UNKNOWN logic is unit-testable offline with {@code RecordingPaimonCatalogOps}
     * ({@code FakePaimonTable.newReadBuilder()} throws).
     */
    long rowCount(Table table);

    void close() throws Exception;

    /**
     * Immutable carrier for a resolved tag: the tag's snapshot id and schema id. Lets the metadata
     * layer pin without depending on a concrete paimon {@code Tag} (impractical to fake offline).
     */
    final class TagSnapshot {
        private final long snapshotId;
        private final long schemaId;

        public TagSnapshot(long snapshotId, long schemaId) {
            this.snapshotId = snapshotId;
            this.schemaId = schemaId;
        }

        /** The tag's snapshot id ({@code tag.id()}). */
        public long snapshotId() {
            return snapshotId;
        }

        /** The tag's schema id ({@code tag.schemaId()}). */
        public long schemaId() {
            return schemaId;
        }
    }

    /**
     * Immutable carrier for a schema AS OF a schemaId: the paimon fields plus the partition-key and
     * primary-key name lists. Returned by {@link #schemaAt} so the metadata layer can map columns
     * offline without faking a concrete paimon {@code TableSchema}.
     */
    final class PaimonSchemaSnapshot {
        private final List<DataField> fields;
        private final List<String> partitionKeys;
        private final List<String> primaryKeys;

        public PaimonSchemaSnapshot(List<DataField> fields, List<String> partitionKeys,
                List<String> primaryKeys) {
            this.fields = fields;
            this.partitionKeys = partitionKeys;
            this.primaryKeys = primaryKeys;
        }

        /** The schema's fields ({@code tableSchema.fields()}). */
        public List<DataField> fields() {
            return fields;
        }

        /** The schema's partition key names ({@code tableSchema.partitionKeys()}). */
        public List<String> partitionKeys() {
            return partitionKeys;
        }

        /** The schema's primary key names ({@code tableSchema.primaryKeys()}). */
        public List<String> primaryKeys() {
            return primaryKeys;
        }
    }

    /**
     * Default implementation backing the seam with a real Paimon {@link Catalog}.
     * Each method is a thin delegation; the {@code Catalog} is the only state.
     */
    class CatalogBackedPaimonCatalogOps implements PaimonCatalogOps {
        private final Catalog catalog;

        public CatalogBackedPaimonCatalogOps(Catalog catalog) {
            this.catalog = catalog;
        }

        @Override
        public List<String> listDatabases() {
            return catalog.listDatabases();
        }

        @Override
        public Database getDatabase(String name) throws Catalog.DatabaseNotExistException {
            return catalog.getDatabase(name);
        }

        @Override
        public List<String> listTables(String databaseName) throws Catalog.DatabaseNotExistException {
            return catalog.listTables(databaseName);
        }

        @Override
        public Table getTable(Identifier identifier) throws Catalog.TableNotExistException {
            return catalog.getTable(identifier);
        }

        @Override
        public List<Partition> listPartitions(Identifier identifier) throws Catalog.TableNotExistException {
            return catalog.listPartitions(identifier);
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
                throws Catalog.DatabaseAlreadyExistException {
            catalog.createDatabase(name, ignoreIfExists, properties);
        }

        @Override
        public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
                throws Catalog.DatabaseNotExistException, Catalog.DatabaseNotEmptyException {
            catalog.dropDatabase(name, ignoreIfNotExists, cascade);
        }

        @Override
        public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException {
            catalog.createTable(identifier, schema, ignoreIfExists);
        }

        @Override
        public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
                throws Catalog.TableNotExistException {
            catalog.dropTable(identifier, ignoreIfNotExists);
        }

        @Override
        public OptionalLong latestSnapshotId(Table table) {
            return table.latestSnapshot()
                    .map(snapshot -> OptionalLong.of(snapshot.id()))
                    .orElseGet(OptionalLong::empty);
        }

        @Override
        public OptionalLong snapshotIdAtOrBefore(Table table, long timestampMillis) {
            // Time-travel by wall-clock requires the snapshotManager(), which only DataTable exposes
            // (legacy PaimonUtil.getPaimonSnapshotByTimestamp casts to DataTable too).
            Snapshot snapshot = ((DataTable) table).snapshotManager().earlierOrEqualTimeMills(timestampMillis);
            return snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot.id());
        }

        @Override
        public boolean snapshotExists(Table table, long snapshotId) {
            try {
                // tryGetSnapshot throws FileNotFoundException when the id does not exist (legacy
                // PaimonUtil.getPaimonSnapshotBySnapshotId catches the same exception).
                ((DataTable) table).snapshotManager().tryGetSnapshot(snapshotId);
                return true;
            } catch (FileNotFoundException e) {
                return false;
            }
        }

        @Override
        public OptionalLong snapshotSchemaId(Table table, long snapshotId) {
            // snapshotManager() is only on DataTable (same cast legacy PaimonUtil uses). snapshot(id)
            // returns the Snapshot whose schemaId is the version pinned for schema-at-snapshot.
            Snapshot snapshot = ((DataTable) table).snapshotManager().snapshot(snapshotId);
            return snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot.schemaId());
        }

        @Override
        public Optional<TagSnapshot> getSnapshotByTag(Table table, String tagName) {
            // tagManager() is only on DataTable. A paimon Tag IS-A Snapshot, so id()/schemaId() are
            // inherited (legacy PaimonUtil.getPaimonSnapshotByTag read the Tag the same way).
            Optional<Tag> tag = ((DataTable) table).tagManager().get(tagName);
            return tag.map(t -> new TagSnapshot(t.id(), t.schemaId()));
        }

        @Override
        public PaimonSchemaSnapshot schemaAt(Table table, long schemaId) {
            // schemaManager() is only on DataTable. schema(schemaId) is the historical TableSchema
            // (legacy PaimonExternalTable.initSchema(schemaId) reads the same accessors).
            TableSchema tableSchema = ((DataTable) table).schemaManager().schema(schemaId);
            return new PaimonSchemaSnapshot(
                    tableSchema.fields(), tableSchema.partitionKeys(), tableSchema.primaryKeys());
        }

        @Override
        public boolean branchExists(Table table, String branchName) {
            // Mirrors legacy PaimonUtil.resolvePaimonBranch: only a FileStoreTable has a
            // branchManager(); a non-FileStoreTable backend (e.g. jdbc-only) cannot have branches.
            if (!(table instanceof FileStoreTable)) {
                return false;
            }
            return ((FileStoreTable) table).branchManager().branchExists(branchName);
        }

        @Override
        public long rowCount(Table table) {
            // Legacy PaimonExternalTable.fetchRowCount / PaimonSysExternalTable.fetchRowCount: sum
            // the planned-split record counts.
            long rowCount = 0;
            for (Split split : table.newReadBuilder().newScan().plan().splits()) {
                rowCount += split.rowCount();
            }
            return rowCount;
        }

        @Override
        public void close() throws Exception {
            catalog.close();
        }
    }
}
