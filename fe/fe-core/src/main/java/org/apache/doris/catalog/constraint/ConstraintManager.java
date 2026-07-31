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

package org.apache.doris.catalog.constraint;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.persist.AlterConstraintLog;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Centralized manager for all table constraints (PK, FK, UNIQUE).
 * Constraints are indexed by fully qualified table name (catalog.db.table).
 */
public class ConstraintManager implements Writable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(ConstraintManager.class);

    @SerializedName("cm")
    private final ConcurrentHashMap<String, Map<String, Constraint>> constraintsMap
            = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ConstraintManager() {
    }

    private static String toKey(TableNameInfo tni) {
        return tni.getCtl() + "." + tni.getDb() + "." + tni.getTbl();
    }

    /** Returns true if no constraints are stored. */
    public boolean isEmpty() {
        return constraintsMap.isEmpty();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    /**
     * Add a constraint to the specified table.
     * For FK constraints, validates that the referenced PK exists
     * and registers bidirectional reference via foreignTableInfos.
     *
     * <p>Note: validation is performed inside the write lock to prevent TOCTOU races
     * (e.g., table dropped between validation and registration). For external catalogs,
     * this could cause longer lock hold times if catalog initialization is slow.
     * This tradeoff is intentional — correctness over performance.</p>
     */
    public void addConstraint(TableNameInfo tableNameInfo, String constraintName,
            Constraint constraint, boolean replay) {
        String key = toKey(tableNameInfo);
        writeLock();
        try {
            if (!replay) {
                validateTableAndColumns(tableNameInfo, constraint);
            }
            Map<String, Constraint> tableConstraints = constraintsMap.computeIfAbsent(
                    key, k -> new HashMap<>());
            checkConstraintNotExistence(constraintName, constraint, tableConstraints);
            if (constraint instanceof ForeignKeyConstraint) {
                registerForeignKeyReference(
                        tableNameInfo, (ForeignKeyConstraint) constraint);
            }
            tableConstraints.put(constraintName, constraint);
            if (!replay) {
                logAddConstraint(tableNameInfo, constraint);
            }
            LOG.info("Added constraint {} on table {}", constraintName, key);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Drop a constraint from the specified table.
     * For PK constraints, cascade-drops all referencing FKs.
     * For FK constraints, updates the referenced PK's foreign table set.
     */
    public void dropConstraint(TableNameInfo tableNameInfo, String constraintName,
            boolean replay) {
        String key = toKey(tableNameInfo);
        writeLock();
        try {
            Map<String, Constraint> tableConstraints = constraintsMap.get(key);
            if (tableConstraints == null || !tableConstraints.containsKey(constraintName)) {
                if (replay) {
                    LOG.warn("Constraint {} not found on table {} during replay, skipping",
                            constraintName, key);
                    return;
                }
                throw new AnalysisException(String.format(
                        "Unknown constraint %s on table %s.",
                        constraintName, key));
            }
            Constraint constraint = tableConstraints.remove(constraintName);
            cleanupConstraintReferences(tableNameInfo, constraint);
            if (tableConstraints.isEmpty()) {
                constraintsMap.remove(key);
            }
            if (!replay) {
                logDropConstraint(tableNameInfo, constraint);
            }
            LOG.info("Dropped constraint {} from table {}",
                    constraintName, key);
        } finally {
            writeUnlock();
        }
    }

    /** Returns an immutable copy of all constraints for the given table. */
    public Map<String, Constraint> getConstraints(TableNameInfo tableNameInfo) {
        String key = toKey(tableNameInfo);
        readLock();
        try {
            Map<String, Constraint> tableConstraints
                    = constraintsMap.get(key);
            if (tableConstraints == null) {
                return ImmutableMap.of();
            }
            return ImmutableMap.copyOf(tableConstraints);
        } finally {
            readUnlock();
        }
    }

    /** Get a single constraint by name, or null if not found. */
    public Constraint getConstraint(TableNameInfo tableNameInfo,
            String constraintName) {
        String key = toKey(tableNameInfo);
        readLock();
        try {
            Map<String, Constraint> tableConstraints
                    = constraintsMap.get(key);
            if (tableConstraints == null) {
                return null;
            }
            return tableConstraints.get(constraintName);
        } finally {
            readUnlock();
        }
    }

    /** Returns all PrimaryKeyConstraints for the given table. */
    public ImmutableList<PrimaryKeyConstraint> getPrimaryKeyConstraints(
            TableNameInfo tableNameInfo) {
        return getConstraintsByType(toKey(tableNameInfo),
                PrimaryKeyConstraint.class);
    }

    /** Returns all ForeignKeyConstraints for the given table. */
    public ImmutableList<ForeignKeyConstraint> getForeignKeyConstraints(
            TableNameInfo tableNameInfo) {
        return getConstraintsByType(toKey(tableNameInfo),
                ForeignKeyConstraint.class);
    }

    /** Returns all UniqueConstraints for the given table. */
    public ImmutableList<UniqueConstraint> getUniqueConstraints(
            TableNameInfo tableNameInfo) {
        return getConstraintsByType(toKey(tableNameInfo),
                UniqueConstraint.class);
    }

    /**
     * Remove all constraints for a table and clean up bidirectional references.
     * Called when a table is dropped.
     */
    /**
     * Atomically check for referencing foreign keys and then drop all constraints
     * for the given table. Holds the write lock for both operations to prevent
     * TOCTOU races where a new FK could be added between the check and the drop.
     *
     * @param tableNameInfo the table whose constraints are to be dropped
     * @param checkForeignKeys if true, throw DdlException if any PK is FK-referenced
     */
    public void checkAndDropTableConstraints(TableNameInfo tableNameInfo,
            boolean checkForeignKeys) throws DdlException {
        String key = toKey(tableNameInfo);
        writeLock();
        try {
            Map<String, Constraint> tableConstraints = constraintsMap.get(key);
            if (tableConstraints == null) {
                return;
            }
            if (checkForeignKeys) {
                for (Constraint c : tableConstraints.values()) {
                    if (c instanceof PrimaryKeyConstraint) {
                        PrimaryKeyConstraint pk = (PrimaryKeyConstraint) c;
                        List<TableNameInfo> fkTables = pk.getForeignTableInfos();
                        if (fkTables != null && !fkTables.isEmpty()) {
                            String fkTableNames = fkTables.stream()
                                    .map(t -> toKey(t))
                                    .collect(Collectors.joining(", "));
                            throw new DdlException(String.format(
                                    "Cannot drop table %s because its primary"
                                            + " key is referenced by foreign key"
                                            + " constraints from table(s): %s."
                                            + " Drop the foreign key constraints"
                                            + " first.",
                                    key, fkTableNames));
                        }
                    }
                }
            }
            constraintsMap.remove(key);
            for (Constraint constraint : tableConstraints.values()) {
                cleanupConstraintReferences(tableNameInfo, constraint);
            }
            LOG.info("Dropped all constraints for table {}", key);
        } finally {
            writeUnlock();
        }
    }

    public void dropTableConstraints(TableNameInfo tableNameInfo) {
        String key = toKey(tableNameInfo);
        writeLock();
        try {
            Map<String, Constraint> tableConstraints
                    = constraintsMap.remove(key);
            if (tableConstraints == null) {
                return;
            }
            for (Constraint constraint : tableConstraints.values()) {
                cleanupConstraintReferences(tableNameInfo, constraint);
            }
            LOG.info("Dropped all constraints for table {}", key);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Remove all constraints whose qualified table name starts with
     * the given catalog prefix. Called when a catalog is dropped.
     */
    public void dropCatalogConstraints(String catalogName) {
        writeLock();
        try {
            String prefix = catalogName + ".";
            dropConstraintsByPrefix(prefix);
            LOG.info("Dropped all constraints for catalog {}", catalogName);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Remove all constraints for tables in the given database.
     * Called during DROP DATABASE to pre-clear all intra-database FK references
     * before individual table drops, avoiding ordering-dependent FK check failures.
     */
    public void dropDatabaseConstraints(String catalogName, String dbName) {
        writeLock();
        try {
            String prefix = catalogName + "." + dbName + ".";
            dropConstraintsByPrefix(prefix);
            LOG.info("Dropped all constraints for database {}.{}",
                    catalogName, dbName);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Remove all constraints whose qualified table name starts with
     * the given prefix, cleaning up cross-references outside the prefix.
     */
    private void dropConstraintsByPrefix(String prefix) {
        List<String> tablesToRemove = constraintsMap.keySet().stream()
                .filter(k -> k.startsWith(prefix))
                .collect(Collectors.toList());
        for (String tableName : tablesToRemove) {
            Map<String, Constraint> tableConstraints
                    = constraintsMap.remove(tableName);
            if (tableConstraints != null) {
                for (Constraint constraint : tableConstraints.values()) {
                    cleanupConstraintReferencesOutsideCatalog(
                            tableName, constraint, prefix);
                }
            }
        }
    }

    /**
     * Move constraints from oldTableInfo to newTableInfo
     * and update all FK/PK references. Called when a table is renamed.
     */
    public void renameTable(TableNameInfo oldTableInfo,
            TableNameInfo newTableInfo) {
        String oldKey = toKey(oldTableInfo);
        String newKey = toKey(newTableInfo);
        writeLock();
        try {
            // Move this table's own constraints
            Map<String, Constraint> tableConstraints
                    = constraintsMap.remove(oldKey);
            if (tableConstraints != null) {
                constraintsMap.put(newKey, tableConstraints);
            }
            // Update FK/PK references in OTHER tables
            for (Map.Entry<String, Map<String, Constraint>> entry
                    : constraintsMap.entrySet()) {
                if (entry.getKey().equals(newKey)) {
                    continue;
                }
                for (Constraint c : entry.getValue().values()) {
                    if (c instanceof ForeignKeyConstraint) {
                        ForeignKeyConstraint fk = (ForeignKeyConstraint) c;
                        TableNameInfo refInfo = fk.getReferencedTableName();
                        if (refInfo != null && oldTableInfo.equals(refInfo)) {
                            fk.setReferencedTableInfo(newTableInfo);
                        }
                    } else if (c instanceof PrimaryKeyConstraint) {
                        ((PrimaryKeyConstraint) c).renameForeignTable(
                                oldTableInfo, newTableInfo);
                    }
                }
            }
            LOG.info("Renamed table constraints from {} to {}",
                    oldKey, newKey);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Migrate constraints from old table-based storage into this manager.
     */
    public void migrateFromTable(TableNameInfo tableNameInfo,
            Map<String, Constraint> existingConstraints) {
        if (existingConstraints == null || existingConstraints.isEmpty()) {
            return;
        }
        String key = toKey(tableNameInfo);
        writeLock();
        try {
            Map<String, Constraint> tableConstraints
                    = constraintsMap.computeIfAbsent(
                            key, k -> new HashMap<>());
            tableConstraints.putAll(existingConstraints);
            LOG.info("Migrated {} constraints for table {}",
                    existingConstraints.size(), key);
        } finally {
            writeUnlock();
        }
    }

    /**
     * After all tables have been migrated, wire up FK→PK bidirectional
     * references that could not be established during per-table migration
     * (because the referenced PK table may not have been migrated yet).
     */
    public void rebuildForeignKeyReferences() {
        writeLock();
        try {
            for (Map.Entry<String, Map<String, Constraint>> entry
                    : constraintsMap.entrySet()) {
                String fkTableKey = entry.getKey();
                TableNameInfo fkTableInfo = new TableNameInfo(fkTableKey);
                for (Constraint c : entry.getValue().values()) {
                    if (!(c instanceof ForeignKeyConstraint)) {
                        continue;
                    }
                    ForeignKeyConstraint fk = (ForeignKeyConstraint) c;
                    TableNameInfo refTableInfo = fk.getReferencedTableName();
                    if (refTableInfo == null) {
                        continue;
                    }
                    String refTableKey = toKey(refTableInfo);
                    Map<String, Constraint> refTableConstraints
                            = constraintsMap.get(refTableKey);
                    if (refTableConstraints == null) {
                        continue;
                    }
                    for (Constraint rc : refTableConstraints.values()) {
                        if (rc instanceof PrimaryKeyConstraint) {
                            PrimaryKeyConstraint pk = (PrimaryKeyConstraint) rc;
                            if (pk.getPrimaryKeyNames().equals(
                                    fk.getReferencedColumnNames())) {
                                pk.addForeignTable(fkTableInfo);
                            }
                        }
                    }
                }
            }
            LOG.info("Rebuilt FK->PK bidirectional references");
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    /** Deserialize ConstraintManager from DataInput. */
    public static ConstraintManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ConstraintManager.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        LOG.info("ConstraintManager deserialized with {} table entries",
                constraintsMap.size());
    }

    // ==================== DDL-support methods ====================

    /**
     * Check if any PK constraint on this table is referenced by FK constraints
     * from other tables. Throws DdlException if references exist.
     * Used before drop table to prevent orphaned FK references.
     */
    public void checkNoReferencingForeignKeys(TableNameInfo tableNameInfo)
            throws DdlException {
        readLock();
        try {
            String key = toKey(tableNameInfo);
            Map<String, Constraint> tableConstraints
                    = constraintsMap.get(key);
            if (tableConstraints == null) {
                return;
            }
            for (Constraint c : tableConstraints.values()) {
                if (c instanceof PrimaryKeyConstraint) {
                    PrimaryKeyConstraint pk = (PrimaryKeyConstraint) c;
                    List<TableNameInfo> fkTables
                            = pk.getForeignTableInfos();
                    if (fkTables != null && !fkTables.isEmpty()) {
                        String fkTableNames = fkTables.stream()
                                .map(t -> toKey(t))
                                .collect(Collectors.joining(", "));
                        throw new DdlException(String.format(
                                "Cannot drop table %s because its primary"
                                        + " key is referenced by foreign key"
                                        + " constraints from table(s): %s."
                                        + " Drop the foreign key constraints"
                                        + " first.",
                                key, fkTableNames));
                    }
                }
            }
        } finally {
            readUnlock();
        }
    }

    /**
     * Check if the given column is part of any constraint on the table.
     * Returns the constraint name if found, or null if not.
     */
    public String findConstraintWithColumn(
            TableNameInfo tableNameInfo, String columnName) {
        readLock();
        try {
            String key = toKey(tableNameInfo);
            Map<String, Constraint> tableConstraints
                    = constraintsMap.get(key);
            if (tableConstraints == null) {
                return null;
            }
            for (Entry<String, Constraint> entry
                    : tableConstraints.entrySet()) {
                Constraint c = entry.getValue();
                if (c instanceof PrimaryKeyConstraint) {
                    if (((PrimaryKeyConstraint) c)
                            .getPrimaryKeyNames()
                            .contains(columnName)) {
                        return entry.getKey();
                    }
                } else if (c instanceof UniqueConstraint) {
                    if (((UniqueConstraint) c)
                            .getUniqueColumnNames()
                            .contains(columnName)) {
                        return entry.getKey();
                    }
                } else if (c instanceof ForeignKeyConstraint) {
                    if (((ForeignKeyConstraint) c)
                            .getForeignKeyNames()
                            .contains(columnName)) {
                        return entry.getKey();
                    }
                }
            }
            return null;
        } finally {
            readUnlock();
        }
    }

    /**
     * Atomically swap constraint mappings between two tables.
     * Used during REPLACE TABLE with SWAP.
     * Also updates all FK/PK cross-references.
     */
    public void swapTableConstraints(TableNameInfo tableA,
            TableNameInfo tableB) {
        String keyA = toKey(tableA);
        String keyB = toKey(tableB);
        writeLock();
        try {
            Map<String, Constraint> constraintsA
                    = constraintsMap.remove(keyA);
            Map<String, Constraint> constraintsB
                    = constraintsMap.remove(keyB);
            if (constraintsA != null) {
                constraintsMap.put(keyB, constraintsA);
            }
            if (constraintsB != null) {
                constraintsMap.put(keyA, constraintsB);
            }
            // Update FK/PK references in ALL tables
            for (Entry<String, Map<String, Constraint>> entry
                    : constraintsMap.entrySet()) {
                for (Constraint c : entry.getValue().values()) {
                    if (c instanceof ForeignKeyConstraint) {
                        swapForeignKeyReference(
                                (ForeignKeyConstraint) c,
                                tableA, tableB);
                    } else if (c instanceof PrimaryKeyConstraint) {
                        swapPrimaryKeyForeignTables(
                                (PrimaryKeyConstraint) c,
                                tableA, tableB);
                    }
                }
            }
            LOG.info("Swapped constraints between {} and {}",
                    keyA, keyB);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Drop constraints for oldTable and rename newTable's constraints
     * to oldTable's name. Used during REPLACE TABLE without SWAP.
     */
    public void dropAndRenameConstraints(TableNameInfo oldTable,
            TableNameInfo newTable) {
        writeLock();
        try {
            // Drop old table constraints (with cleanup)
            String oldKey = toKey(oldTable);
            Map<String, Constraint> oldConstraints
                    = constraintsMap.remove(oldKey);
            if (oldConstraints != null) {
                for (Constraint c : oldConstraints.values()) {
                    cleanupConstraintReferences(oldTable, c);
                }
            }
            // Rename new table constraints to old table name
            String newKey = toKey(newTable);
            Map<String, Constraint> newConstraints
                    = constraintsMap.remove(newKey);
            if (newConstraints != null) {
                constraintsMap.put(oldKey, newConstraints);
            }
            // Update FK/PK references pointing to newTable → oldTable
            for (Entry<String, Map<String, Constraint>> entry
                    : constraintsMap.entrySet()) {
                for (Constraint c : entry.getValue().values()) {
                    if (c instanceof ForeignKeyConstraint) {
                        ForeignKeyConstraint fk
                                = (ForeignKeyConstraint) c;
                        if (newTable.equals(
                                fk.getReferencedTableName())) {
                            fk.setReferencedTableInfo(oldTable);
                        }
                    } else if (c instanceof PrimaryKeyConstraint) {
                        ((PrimaryKeyConstraint) c)
                                .renameForeignTable(
                                        newTable, oldTable);
                    }
                }
            }
            LOG.info("Dropped constraints for {} and renamed {}"
                    + " constraints to {}",
                    oldKey, newKey, oldKey);
        } finally {
            writeUnlock();
        }
    }

    // ==================== Private helpers ====================

    private void checkConstraintNotExistence(String name,
            Constraint constraint, Map<String, Constraint> constraintMap) {
        if (constraintMap.containsKey(name)) {
            throw new AnalysisException(
                    String.format("Constraint name %s has existed", name));
        }
        for (Entry<String, Constraint> entry : constraintMap.entrySet()) {
            if (entry.getValue().equals(constraint)) {
                throw new AnalysisException(String.format(
                        "Constraint %s has existed, named %s",
                        constraint, entry.getKey()));
            }
        }
    }

    /**
     * For FK constraints: find the matching PK on the referenced table
     * (using FK's referencedTableInfo) and register the FK table in PK's
     * foreignTableInfos list.
     */
    private void registerForeignKeyReference(TableNameInfo fkTableInfo,
            ForeignKeyConstraint fkConstraint) {
        TableNameInfo refTableInfo = fkConstraint.getReferencedTableName();
        if (refTableInfo == null) {
            throw new AnalysisException(
                    "Foreign key constraint has no referenced table name");
        }
        String refTableKey = toKey(refTableInfo);
        Map<String, Constraint> refTableConstraints
                = constraintsMap.get(refTableKey);
        if (refTableConstraints == null) {
            throw new AnalysisException(String.format(
                    "Foreign key constraint requires a primary key constraint "
                            + "%s in %s",
                    fkConstraint.getReferencedColumnNames(), refTableKey));
        }
        boolean found = false;
        for (Constraint c : refTableConstraints.values()) {
            if (c instanceof PrimaryKeyConstraint) {
                PrimaryKeyConstraint pk = (PrimaryKeyConstraint) c;
                if (pk.getPrimaryKeyNames().equals(
                        fkConstraint.getReferencedColumnNames())) {
                    pk.addForeignTable(fkTableInfo);
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            throw new AnalysisException(String.format(
                    "Foreign key constraint requires a primary key constraint "
                            + "%s in %s",
                    fkConstraint.getReferencedColumnNames(), refTableKey));
        }
    }

    /**
     * Clean up bidirectional references when a constraint is removed.
     * PK: cascade-drop all FKs in foreign tables that reference this PK.
     * FK: remove the FK table from the referenced PK's foreignTableInfos.
     */
    private void cleanupConstraintReferences(TableNameInfo tableNameInfo,
            Constraint constraint) {
        if (constraint instanceof PrimaryKeyConstraint) {
            cascadeDropForeignKeys(tableNameInfo,
                    (PrimaryKeyConstraint) constraint);
        } else if (constraint instanceof ForeignKeyConstraint) {
            removeForeignKeyFromPK(tableNameInfo,
                    (ForeignKeyConstraint) constraint);
        }
    }

    /**
     * Similar to cleanupConstraintReferences but only cleans references
     * to tables outside the given catalog prefix (used during catalog drop).
     */
    private void cleanupConstraintReferencesOutsideCatalog(
            String qualifiedTableName, Constraint constraint,
            String catalogPrefix) {
        if (constraint instanceof PrimaryKeyConstraint) {
            PrimaryKeyConstraint pk = (PrimaryKeyConstraint) constraint;
            for (TableNameInfo fkTableInfo : pk.getForeignTableInfos()) {
                String fkTableKey = toKey(fkTableInfo);
                if (fkTableKey.startsWith(catalogPrefix)) {
                    // intra-catalog; will be removed together
                    continue;
                }
                Map<String, Constraint> fkTableConstraints
                        = constraintsMap.get(fkTableKey);
                if (fkTableConstraints != null) {
                    TableNameInfo pkTableInfo = new TableNameInfo(qualifiedTableName);
                    removeFKsReferencingTable(fkTableConstraints,
                            pkTableInfo, pk);
                    if (fkTableConstraints.isEmpty()) {
                        constraintsMap.remove(fkTableKey);
                    }
                }
            }
        } else if (constraint instanceof ForeignKeyConstraint) {
            ForeignKeyConstraint fk = (ForeignKeyConstraint) constraint;
            TableNameInfo refTableInfo = fk.getReferencedTableName();
            if (refTableInfo != null) {
                String refTableKey = toKey(refTableInfo);
                if (!refTableKey.startsWith(catalogPrefix)) {
                    TableNameInfo fkTableInfo = new TableNameInfo(qualifiedTableName);
                    removeForeignKeyFromPK(fkTableInfo, fk);
                }
            }
        }
    }

    /**
     * When a PK is dropped, cascade-drop all FK constraints in the PK's
     * registered foreign tables that reference this PK.
     */
    private void cascadeDropForeignKeys(TableNameInfo pkTableInfo,
            PrimaryKeyConstraint pkConstraint) {
        for (TableNameInfo fkTableInfo : pkConstraint.getForeignTableInfos()) {
            String fkTableKey = toKey(fkTableInfo);
            Map<String, Constraint> fkTableConstraints
                    = constraintsMap.get(fkTableKey);
            if (fkTableConstraints == null) {
                continue;
            }
            removeFKsReferencingTable(fkTableConstraints,
                    pkTableInfo, pkConstraint);
            if (fkTableConstraints.isEmpty()) {
                constraintsMap.remove(fkTableKey);
            }
        }
    }

    private void removeFKsReferencingTable(
            Map<String, Constraint> fkTableConstraints,
            TableNameInfo pkTableInfo, PrimaryKeyConstraint pkConstraint) {
        Iterator<Entry<String, Constraint>> it
                = fkTableConstraints.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Constraint> entry = it.next();
            if (entry.getValue() instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint fk
                        = (ForeignKeyConstraint) entry.getValue();
                if (pkTableInfo.equals(fk.getReferencedTableName())
                        && fk.getReferencedColumnNames().equals(
                                pkConstraint.getPrimaryKeyNames())) {
                    it.remove();
                }
            }
        }
    }

    /**
     * When an FK is dropped, remove the FK table from the referenced PK's
     * foreignTableInfos list.
     */
    private void removeForeignKeyFromPK(TableNameInfo fkTableInfo,
            ForeignKeyConstraint fkConstraint) {
        TableNameInfo refTableInfo = fkConstraint.getReferencedTableName();
        if (refTableInfo == null) {
            return;
        }
        String refTableKey = toKey(refTableInfo);
        Map<String, Constraint> refTableConstraints
                = constraintsMap.get(refTableKey);
        if (refTableConstraints == null) {
            return;
        }
        for (Constraint c : refTableConstraints.values()) {
            if (c instanceof PrimaryKeyConstraint) {
                PrimaryKeyConstraint pk = (PrimaryKeyConstraint) c;
                if (pk.getPrimaryKeyNames().equals(
                        fkConstraint.getReferencedColumnNames())) {
                    pk.removeForeignTable(fkTableInfo);
                    break;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends Constraint> ImmutableList<T> getConstraintsByType(
            String qualifiedTableName, Class<T> type) {
        readLock();
        try {
            Map<String, Constraint> tableConstraints
                    = constraintsMap.get(qualifiedTableName);
            if (tableConstraints == null) {
                return ImmutableList.of();
            }
            ImmutableList.Builder<T> builder = ImmutableList.builder();
            for (Constraint constraint : tableConstraints.values()) {
                if (type.isInstance(constraint)) {
                    builder.add(type.cast(constraint));
                }
            }
            return builder.build();
        } finally {
            readUnlock();
        }
    }

    // ==================== Validation helpers ====================

    /**
     * Validate that the table and columns referenced by the constraint
     * actually exist. Only called for non-replay operations.
     */
    private void validateTableAndColumns(TableNameInfo tableNameInfo,
            Constraint constraint) {
        TableIf table = resolveTableForValidation(tableNameInfo);
        if (constraint instanceof PrimaryKeyConstraint) {
            validateColumnsExist(table,
                    ((PrimaryKeyConstraint) constraint)
                            .getPrimaryKeyNames(),
                    toKey(tableNameInfo));
        } else if (constraint instanceof UniqueConstraint) {
            validateColumnsExist(table,
                    ((UniqueConstraint) constraint)
                            .getUniqueColumnNames(),
                    toKey(tableNameInfo));
        } else if (constraint instanceof ForeignKeyConstraint) {
            ForeignKeyConstraint fk = (ForeignKeyConstraint) constraint;
            validateColumnsExist(table,
                    fk.getForeignKeyNames(),
                    toKey(tableNameInfo));
            TableNameInfo refTableInfo = fk.getReferencedTableName();
            if (refTableInfo != null) {
                TableIf refTable
                        = resolveTableForValidation(refTableInfo);
                validateColumnsExist(refTable,
                        fk.getReferencedColumnNames(),
                        toKey(refTableInfo));
            }
        }
    }

    private TableIf resolveTableForValidation(
            TableNameInfo tableNameInfo) {
        try {
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalog(tableNameInfo.getCtl());
            if (catalog == null) {
                throw new AnalysisException(
                        "Catalog not found: "
                                + tableNameInfo.getCtl());
            }
            DatabaseIf db = catalog.getDbNullable(
                    tableNameInfo.getDb());
            if (db == null) {
                throw new AnalysisException(
                        "Database not found: "
                                + tableNameInfo.getDb()
                                + " in catalog "
                                + tableNameInfo.getCtl());
            }
            TableIf table = db.getTableNullable(
                    tableNameInfo.getTbl());
            if (table == null) {
                throw new AnalysisException(
                        "Table not found: "
                                + toKey(tableNameInfo));
            }
            return table;
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new AnalysisException(
                    "Failed to resolve table "
                            + toKey(tableNameInfo)
                            + ": " + e.getMessage());
        }
    }

    private void validateColumnsExist(TableIf table,
            Collection<String> columnNames,
            String qualifiedTableName) {
        for (String columnName : columnNames) {
            if (table.getColumn(columnName) == null) {
                throw new AnalysisException(String.format(
                        "Column %s does not exist in table %s",
                        columnName, qualifiedTableName));
            }
        }
    }

    // ==================== Swap helpers ====================

    private void swapForeignKeyReference(ForeignKeyConstraint fk,
            TableNameInfo tableA, TableNameInfo tableB) {
        TableNameInfo ref = fk.getReferencedTableName();
        if (ref == null) {
            return;
        }
        if (tableA.equals(ref)) {
            fk.setReferencedTableInfo(tableB);
        } else if (tableB.equals(ref)) {
            fk.setReferencedTableInfo(tableA);
        }
    }

    /**
     * Swap references to tableA and tableB in a PK's foreign table list.
     * Handles correctly the case where only one, both, or neither is
     * present.
     */
    private void swapPrimaryKeyForeignTables(PrimaryKeyConstraint pk,
            TableNameInfo tableA, TableNameInfo tableB) {
        List<TableNameInfo> fkInfos = pk.getForeignTableInfos();
        boolean hasA = fkInfos.stream().anyMatch(tableA::equals);
        boolean hasB = fkInfos.stream().anyMatch(tableB::equals);
        if (hasA && !hasB) {
            pk.renameForeignTable(tableA, tableB);
        } else if (!hasA && hasB) {
            pk.renameForeignTable(tableB, tableA);
        }
        // If both or neither present, no change needed
    }

    // ==================== EditLog integration ====================

    private void logAddConstraint(TableNameInfo tableNameInfo,
            Constraint constraint) {
        AlterConstraintLog log = new AlterConstraintLog(
                constraint, tableNameInfo);
        Env.getCurrentEnv().getEditLog().logAddConstraint(log);
    }

    private void logDropConstraint(TableNameInfo tableNameInfo,
            Constraint constraint) {
        AlterConstraintLog log = new AlterConstraintLog(
                constraint, tableNameInfo);
        Env.getCurrentEnv().getEditLog().logDropConstraint(log);
    }
}
