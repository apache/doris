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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Hand-written recording fake for {@link PaimonCatalogOps} (no Mockito), mirroring the
 * maxcompute connector's recording {@code McStructureHelper}.
 *
 * <p>Records an ordered call log, returns configurable fixed data, and can be told to throw
 * the paimon {@code DatabaseNotExistException} / {@code TableNotExistException} (and the B3
 * DDL exceptions) that the production code catches/wraps. Because the seam fully covers every
 * remote call {@link PaimonConnectorMetadata} makes, the metadata under test is built with a
 * {@code null} real Catalog — the test stays entirely offline.
 */
final class RecordingPaimonCatalogOps implements PaimonCatalogOps {

    final List<String> log = new ArrayList<>();

    List<String> databases = new ArrayList<>();
    List<String> tables = new ArrayList<>();
    Table table;
    List<Partition> partitions = new ArrayList<>();

    /** The Identifier the metadata layer passed to the most recent {@link #getTable} call. */
    Identifier lastGetTableId;
    /**
     * Optional override returned by {@link #getTable} when the requested Identifier carries a
     * system-table name (4-arg sys Identifier). When unset, {@link #table} is returned for both
     * base and sys lookups.
     */
    Table sysTable;
    /**
     * Optional override returned by {@link #getTable} when the requested Identifier denotes a real
     * (non-main, non-sys) branch (3-arg branch Identifier). When set, a branch load returns a
     * DIFFERENT table double than the base {@link #table}, so a branch read can be proven to operate
     * on the branch's own schema/snapshots. When unset, {@link #table} is returned.
     */
    Table branchTable;

    boolean throwDatabaseNotExist;
    boolean throwTableNotExist;

    // ---- B3 DDL capture fields (inputs the metadata layer passed to the seam) ----
    Schema lastCreatedSchema;
    Identifier lastCreatedTableId;
    boolean lastCreateTableIgnoreIfExists;
    Identifier lastDroppedTableId;
    boolean lastDropTableIgnoreIfNotExists;
    String lastCreatedDb;
    Map<String, String> lastCreatedDbProps;
    boolean lastCreateDbIgnoreIfExists;
    String lastDroppedDb;
    boolean lastDropCascade;
    boolean lastDropDbIgnoreIfNotExists;

    // ---- B3 DDL throw flags (mirror the read-path throwDatabaseNotExist/throwTableNotExist) ----
    boolean throwTableAlreadyExist;
    boolean throwTableNotExistOnDrop;
    boolean throwDatabaseAlreadyExist;
    boolean throwDatabaseNotEmpty;
    boolean throwDatabaseNotExistOnDrop;

    // ---- T20 E5 MVCC seam: configurable lookup results (no real Snapshot/SnapshotManager) ----
    OptionalLong latestSnapshotId = OptionalLong.empty();
    OptionalLong snapshotIdAtOrBefore = OptionalLong.empty();
    boolean snapshotExists;
    /** The table the metadata layer passed to the most recent MVCC seam call. */
    Table lastMvccTable;
    /** The timestamp (millis) the metadata layer passed to the most recent snapshotIdAtOrBefore. */
    long snapshotIdAtOrBeforeArg;

    // ---- B5b-2a explicit time-travel seam: configurable results + call capture ----
    /** schemaId returned by snapshotSchemaId (default empty => stamps -1). */
    OptionalLong snapshotSchemaId = OptionalLong.empty();
    /** tag resolution returned by getSnapshotByTag (default null => empty => not found). */
    PaimonCatalogOps.TagSnapshot tagSnapshot;
    /** schema returned by schemaAt (set per-test to drive the at-schemaId column mapping). */
    PaimonCatalogOps.PaimonSchemaSnapshot schemaAt;
    /** The arguments the metadata layer passed to the most recent time-travel seam call. */
    long lastSnapshotSchemaIdArg;
    String lastTagNameArg;
    long lastSchemaAtArg;

    // ---- B5b-2c branch time-travel seam: configurable result + call capture ----
    /** Whether the configured branch is reported to exist by {@link #branchExists}. */
    boolean branchExists;
    /** The branch name the metadata layer passed to the most recent {@link #branchExists} call. */
    String lastBranchExistsArg;
    /** The base table the metadata layer passed to the most recent {@link #branchExists} call. */
    Table lastBranchExistsTable;

    // ---- FIX-TABLE-STATS: row-count seam ----
    /** Configurable row count returned by {@link #rowCount}. */
    long rowCount;
    /** The table the metadata layer passed to the most recent {@link #rowCount} call. */
    Table lastRowCountTable;
    /** When set, {@link #rowCount} throws (drives the best-effort planning-failure path). */
    boolean throwOnRowCount;

    @Override
    public List<String> listDatabases() {
        log.add("listDatabases");
        return databases;
    }

    @Override
    public Database getDatabase(String name) throws Catalog.DatabaseNotExistException {
        log.add("getDatabase:" + name);
        if (throwDatabaseNotExist) {
            throw new Catalog.DatabaseNotExistException(name);
        }
        // databaseExists ignores the returned Database (only the throw/no-throw matters),
        // so a null is sufficient and keeps the fake free of a Database double.
        return null;
    }

    @Override
    public List<String> listTables(String databaseName) throws Catalog.DatabaseNotExistException {
        log.add("listTables:" + databaseName);
        if (throwDatabaseNotExist) {
            throw new Catalog.DatabaseNotExistException(databaseName);
        }
        return tables;
    }

    @Override
    public Table getTable(Identifier identifier) throws Catalog.TableNotExistException {
        log.add("getTable:" + identifier.getFullName());
        lastGetTableId = identifier;
        if (throwTableNotExist) {
            throw new Catalog.TableNotExistException(identifier);
        }
        // A 4-arg sys Identifier carries a non-null system-table name; serve sysTable when set so
        // sys-handle schema/columns can be built from a DIFFERENT rowType than the base table.
        if (sysTable != null && identifier.getSystemTableName() != null) {
            return sysTable;
        }
        // A 3-arg branch Identifier carries a non-"main" branch and no system-table name; serve
        // branchTable when set so a branch load returns a DIFFERENT table double than the base.
        // getBranchNameOrDefault() returns "main" for a base/sys identifier and the real branch name
        // for a 3-arg branch identifier — robustly distinguishing the branch load.
        if (branchTable != null
                && identifier.getSystemTableName() == null
                && !"main".equals(identifier.getBranchNameOrDefault())) {
            return branchTable;
        }
        return table;
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws Catalog.TableNotExistException {
        log.add("listPartitions:" + identifier.getFullName());
        if (throwTableNotExist) {
            throw new Catalog.TableNotExistException(identifier);
        }
        return partitions;
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws Catalog.DatabaseAlreadyExistException {
        log.add("createDatabase:" + name);
        lastCreatedDb = name;
        lastCreateDbIgnoreIfExists = ignoreIfExists;
        lastCreatedDbProps = properties;
        if (throwDatabaseAlreadyExist) {
            throw new Catalog.DatabaseAlreadyExistException(name);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws Catalog.DatabaseNotExistException, Catalog.DatabaseNotEmptyException {
        log.add("dropDatabase:" + name + ",cascade=" + cascade);
        lastDroppedDb = name;
        lastDropDbIgnoreIfNotExists = ignoreIfNotExists;
        lastDropCascade = cascade;
        if (throwDatabaseNotExistOnDrop) {
            throw new Catalog.DatabaseNotExistException(name);
        }
        if (throwDatabaseNotEmpty) {
            throw new Catalog.DatabaseNotEmptyException(name);
        }
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException {
        log.add("createTable:" + identifier.getFullName());
        lastCreatedTableId = identifier;
        lastCreatedSchema = schema;
        lastCreateTableIgnoreIfExists = ignoreIfExists;
        if (throwTableAlreadyExist) {
            throw new Catalog.TableAlreadyExistException(identifier);
        }
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws Catalog.TableNotExistException {
        log.add("dropTable:" + identifier.getFullName());
        lastDroppedTableId = identifier;
        lastDropTableIgnoreIfNotExists = ignoreIfNotExists;
        if (throwTableNotExistOnDrop || throwTableNotExist) {
            throw new Catalog.TableNotExistException(identifier);
        }
    }

    @Override
    public OptionalLong latestSnapshotId(Table table) {
        log.add("latestSnapshotId");
        lastMvccTable = table;
        return latestSnapshotId;
    }

    @Override
    public OptionalLong snapshotIdAtOrBefore(Table table, long timestampMillis) {
        log.add("snapshotIdAtOrBefore:" + timestampMillis);
        lastMvccTable = table;
        snapshotIdAtOrBeforeArg = timestampMillis;
        return snapshotIdAtOrBefore;
    }

    @Override
    public boolean snapshotExists(Table table, long snapshotId) {
        log.add("snapshotExists:" + snapshotId);
        lastMvccTable = table;
        return snapshotExists;
    }

    @Override
    public OptionalLong snapshotSchemaId(Table table, long snapshotId) {
        log.add("snapshotSchemaId:" + snapshotId);
        lastMvccTable = table;
        lastSnapshotSchemaIdArg = snapshotId;
        return snapshotSchemaId;
    }

    @Override
    public java.util.Optional<PaimonCatalogOps.TagSnapshot> getSnapshotByTag(Table table, String tagName) {
        log.add("getSnapshotByTag:" + tagName);
        lastMvccTable = table;
        lastTagNameArg = tagName;
        return java.util.Optional.ofNullable(tagSnapshot);
    }

    @Override
    public PaimonCatalogOps.PaimonSchemaSnapshot schemaAt(Table table, long schemaId) {
        log.add("schemaAt:" + schemaId);
        lastMvccTable = table;
        lastSchemaAtArg = schemaId;
        return schemaAt;
    }

    @Override
    public boolean branchExists(Table table, String branchName) {
        log.add("branchExists:" + branchName);
        // Capture which table validation ran against (must be the BASE table, mirroring legacy
        // resolvePaimonBranch which validates the branch on the base table's branchManager).
        // Kept in a DEDICATED field so lastMvccTable stays a pure MVCC-seam artifact.
        lastBranchExistsTable = table;
        lastBranchExistsArg = branchName;
        return branchExists;
    }

    @Override
    public long rowCount(Table table) {
        log.add("rowCount");
        lastRowCountTable = table;
        if (throwOnRowCount) {
            throw new RuntimeException("simulated planning failure");
        }
        return rowCount;
    }

    @Override
    public void close() {
        log.add("close");
    }
}
