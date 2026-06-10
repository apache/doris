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
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
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

    void close() throws Exception;

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
        public void close() throws Exception {
            catalog.close();
        }
    }
}
