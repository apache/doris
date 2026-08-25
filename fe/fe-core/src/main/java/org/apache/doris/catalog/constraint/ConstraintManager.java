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
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Version;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.persist.AlterConstraintLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Frontend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Centralized manager for all table constraints.
 * Constraints are indexed by fully qualified table name (catalog.db.table).
 */
public class ConstraintManager implements Writable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(ConstraintManager.class);

    @SerializedName("cm")
    private final ConcurrentHashMap<String, Map<String, Constraint>> constraintsMap
            = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // A candidate FE cannot report its version before loading the image and replaying journals.
    private final ReentrantLock frontendAdmissionLock = new ReentrantLock();

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
     */
    public void addConstraint(TableNameInfo tableNameInfo, String constraintName,
            Constraint constraint, boolean replay) {
        EditLog.EditLogItem logItem = addConstraint(
                tableNameInfo, constraintName, constraint, null, null, replay);
        awaitEditLog(logItem);
    }

    private EditLog.EditLogItem addConstraint(TableNameInfo tableNameInfo, String constraintName,
            Constraint constraint, TableIf resolvedTable, TableIf resolvedReferencedTable,
            boolean replay) {
        String key = toKey(tableNameInfo);
        EditLog.EditLogItem logItem = null;
        boolean acquireFrontendAdmission = !replay
                && constraint instanceof DistributionMappingConstraint
                && !frontendAdmissionLock.isHeldByCurrentThread();
        if (acquireFrontendAdmission) {
            acquireFrontendAdmissionForMapping();
        }
        writeLock();
        try {
            TableIf table = null;
            if (!replay) {
                if (resolvedTable == null) {
                    table = validateTableAndColumns(tableNameInfo, constraint);
                } else {
                    table = resolvedTable;
                    validateResolvedConstraint(
                            tableNameInfo, table, resolvedReferencedTable, constraint);
                }
            } else if (constraint instanceof DistributionMappingConstraint) {
                table = resolveTableIfPresent(tableNameInfo);
            }
            Map<String, Constraint> tableConstraints = constraintsMap.computeIfAbsent(
                    key, k -> new HashMap<>());
            checkConstraintNotExistence(constraintName, constraint, tableConstraints);
            if (constraint instanceof ForeignKeyConstraint) {
                registerForeignKeyReference(
                        tableNameInfo, (ForeignKeyConstraint) constraint);
            }
            tableConstraints.put(constraintName, constraint);
            if (constraint instanceof DistributionMappingConstraint) {
                putTableLocalConstraint(table, constraintName, constraint);
            }
            if (!replay) {
                logItem = submitAddConstraint(tableNameInfo, constraint);
            }
            LOG.info("Added constraint {} on table {}", constraintName, key);
        } finally {
            writeUnlock();
            if (acquireFrontendAdmission) {
                frontendAdmissionLock.unlock();
            }
        }
        return logItem;
    }

    public void acquireFrontendAdmission() throws DdlException {
        try {
            if (!frontendAdmissionLock.tryLock(
                    Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                throw new DdlException("Failed to acquire frontend admission lock. Try again");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DdlException("Interrupted while acquiring frontend admission lock", e);
        }
        boolean admitted = false;
        try {
            readLock();
            try {
                for (Map<String, Constraint> tableConstraints : constraintsMap.values()) {
                    if (tableConstraints.values().stream()
                            .anyMatch(DistributionMappingConstraint.class::isInstance)) {
                        throw new DdlException("Cannot add frontend while distribution mapping constraints exist."
                                + " Drop all distribution mapping constraints before adding a frontend");
                    }
                }
            } finally {
                readUnlock();
            }
            if (Env.getCurrentRecycleBin().containsDistributionMappingConstraint()) {
                throw new DdlException("Cannot add frontend while distribution mapping constraints exist"
                        + " in the recycle bin. Permanently erase the affected tables or databases"
                        + " before adding a frontend");
            }
            if (Env.getCurrentEnv().getBackupHandler().containsDistributionMappingConstraint()) {
                throw new DdlException("Cannot add frontend while distribution mapping constraints exist"
                        + " in retained backup or restore jobs. Wait for the active job to release its metadata"
                        + " before adding a frontend");
            }
            admitted = true;
        } finally {
            if (!admitted) {
                frontendAdmissionLock.unlock();
            }
        }
    }

    public void releaseFrontendAdmissionFence() {
        frontendAdmissionLock.unlock();
    }

    public boolean acquireFrontendAdmissionFence() throws DdlException {
        if (frontendAdmissionLock.isHeldByCurrentThread()) {
            return false;
        }
        try {
            if (!frontendAdmissionLock.tryLock(
                    Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                throw new DdlException("Failed to acquire frontend admission lock. Try again");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DdlException("Interrupted while acquiring frontend admission lock", e);
        }
        return true;
    }

    public void acquireFrontendAdmissionForMapping() {
        try {
            if (!frontendAdmissionLock.tryLock(
                    Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                throw new AnalysisException(
                        "Failed to acquire frontend admission lock for distribution mapping constraint. Try again");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AnalysisException(
                    "Interrupted while acquiring frontend admission lock for distribution mapping constraint", e);
        }
        boolean validated = false;
        try {
            validateFrontendVersionsForDistributionMappingConstraint();
            validated = true;
        } finally {
            if (!validated) {
                frontendAdmissionLock.unlock();
            }
        }
    }

    /**
     * Add a non-replay constraint using tables already analyzed and resolved by the command.
     *
     * <p>This avoids connector table/schema load-through while database locks are held. Column
     * existence was established by Nereids analysis before locking; internal table identities are
     * then revalidated before this method is called.</p>
     */
    public EditLog.EditLogItem addConstraintWithResolvedTables(TableNameInfo tableNameInfo, String constraintName,
            Constraint constraint, TableIf table, TableIf referencedTable) {
        return addConstraint(
                tableNameInfo, constraintName, constraint, table, referencedTable, false);
    }

    /**
     * Snapshot the tables whose foreign keys would be cascade-dropped along with the given primary
     * key constraint. Taken under the read lock: {@link PrimaryKeyConstraint#getForeignTableInfos()}
     * is only a view over a list mutated by manager operations under the write lock, so callers outside
     * the lock must not iterate it directly. Returns an empty list for other constraint types.
     */
    public List<TableNameInfo> getCascadeDropTables(Constraint constraint) {
        if (!(constraint instanceof PrimaryKeyConstraint)) {
            return ImmutableList.of();
        }
        readLock();
        try {
            return ImmutableList.copyOf(((PrimaryKeyConstraint) constraint).getForeignTableInfos());
        } finally {
            readUnlock();
        }
    }

    /**
     * Drop a constraint from the specified table.
     * For PK constraints, cascade-drops all referencing FKs.
     * For FK constraints, updates the referenced PK's foreign table set.
     */
    public void dropConstraint(TableNameInfo tableNameInfo, String constraintName,
            boolean replay) {
        EditLog.EditLogItem logItem =
                dropConstraintInternal(tableNameInfo, constraintName, null, replay);
        awaitEditLog(logItem);
    }

    public EditLog.EditLogItem dropConstraintAndSubmit(TableNameInfo tableNameInfo,
            String constraintName, List<TableNameInfo> expectedCascadeDropTables) {
        return dropConstraintInternal(
                tableNameInfo, constraintName, expectedCascadeDropTables, false);
    }

    private EditLog.EditLogItem dropConstraintInternal(TableNameInfo tableNameInfo,
            String constraintName, List<TableNameInfo> expectedCascadeDropTables,
            boolean replay) {
        String key = toKey(tableNameInfo);
        EditLog.EditLogItem logItem = null;
        writeLock();
        try {
            Map<String, Constraint> tableConstraints = constraintsMap.get(key);
            if (tableConstraints == null || !tableConstraints.containsKey(constraintName)) {
                if (replay) {
                    LOG.warn("Constraint {} not found on table {} during replay, skipping",
                            constraintName, key);
                    return null;
                }
                throw new AnalysisException(String.format(
                        "Unknown constraint %s on table %s.",
                        constraintName, key));
            }
            Constraint existingConstraint = tableConstraints.get(constraintName);
            if (expectedCascadeDropTables != null
                    && !sameTables(expectedCascadeDropTables,
                            getCascadeDropTablesWithoutLock(existingConstraint))) {
                throw new AnalysisException(
                        "Foreign key references changed while dropping constraint "
                                + constraintName + " on " + tableNameInfo
                                + ", retry the statement");
            }
            Constraint constraint = tableConstraints.remove(constraintName);
            if (constraint instanceof DistributionMappingConstraint) {
                removeTableLocalConstraint(
                        resolveTableIfPresent(tableNameInfo), constraintName);
            }
            cleanupConstraintReferences(tableNameInfo, constraint);
            if (tableConstraints.isEmpty()) {
                constraintsMap.remove(key);
            }
            if (!replay) {
                logItem = submitDropConstraint(tableNameInfo, constraint);
            }
            LOG.info("Dropped constraint {} from table {}",
                    constraintName, key);
        } finally {
            writeUnlock();
        }
        return logItem;
    }

    private List<TableNameInfo> getCascadeDropTablesWithoutLock(Constraint constraint) {
        return constraint instanceof PrimaryKeyConstraint
                ? ImmutableList.copyOf(
                        ((PrimaryKeyConstraint) constraint).getForeignTableInfos())
                : ImmutableList.of();
    }

    private boolean sameTables(List<TableNameInfo> first, List<TableNameInfo> second) {
        Set<String> firstKeys = first.stream()
                .map(ConstraintManager::toKey)
                .collect(Collectors.toSet());
        Set<String> secondKeys = second.stream()
                .map(ConstraintManager::toKey)
                .collect(Collectors.toSet());
        return firstKeys.equals(secondKeys);
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

    /** Resolve the persisted spelling used by a case-insensitive external catalog. */
    public TableNameInfo canonicalizeExternalTableName(TableNameInfo tableNameInfo,
            String constraintName, boolean databaseCaseInsensitive, boolean tableCaseInsensitive) {
        String requestedKey = toKey(tableNameInfo);
        readLock();
        try {
            Map<String, Constraint> exactConstraints = constraintsMap.get(requestedKey);
            if (exactConstraints != null && exactConstraints.containsKey(constraintName)) {
                return tableNameInfo;
            }
            TableNameInfo matchedName = null;
            for (Entry<String, Map<String, Constraint>> entry : constraintsMap.entrySet()) {
                if (!entry.getValue().containsKey(constraintName)) {
                    continue;
                }
                TableNameInfo storedName = new TableNameInfo(entry.getKey());
                if (storedName.getCtl().equals(tableNameInfo.getCtl())
                        && namesEqual(storedName.getDb(), tableNameInfo.getDb(), databaseCaseInsensitive)
                        && namesEqual(storedName.getTbl(), tableNameInfo.getTbl(), tableCaseInsensitive)) {
                    if (matchedName != null) {
                        throw new AnalysisException(
                                "Ambiguous constraint table name " + tableNameInfo);
                    }
                    matchedName = storedName;
                }
            }
            return matchedName == null ? tableNameInfo : matchedName;
        } finally {
            readUnlock();
        }
    }

    private boolean namesEqual(String left, String right, boolean caseInsensitive) {
        return caseInsensitive ? left.equalsIgnoreCase(right) : left.equals(right);
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
     * Returns mappings owned by the concrete table object.
     *
     * <p>The table-local copy follows the table through recycle, backup, restore, and rename lifecycles.
     * Optimizer code must use this overload so a stale qualified-name entry can never bind to another table.</p>
     */
    @SuppressWarnings("deprecation")
    public ImmutableList<DistributionMappingConstraint> getDistributionMappingConstraints(TableIf table) {
        if (!(table instanceof Table)) {
            return ImmutableList.of();
        }
        readLock();
        try {
            return ((Table) table).getTableAttributes().getConstraintsMap().values().stream()
                    .filter(DistributionMappingConstraint.class::isInstance)
                    .map(DistributionMappingConstraint.class::cast)
                    .collect(ImmutableList.toImmutableList());
        } finally {
            readUnlock();
        }
    }

    /** Return the table-owned mapping that uses the given column, if any. */
    @SuppressWarnings("deprecation")
    public String findDistributionMappingConstraintWithColumn(TableIf table, String columnName) {
        if (!(table instanceof Table)) {
            return null;
        }
        readLock();
        try {
            return ((Table) table).getTableAttributes().getConstraintsMap().entrySet().stream()
                    .filter(entry -> entry.getValue() instanceof DistributionMappingConstraint)
                    .filter(entry -> {
                        DistributionMappingConstraint mapping =
                                (DistributionMappingConstraint) entry.getValue();
                        return containsIgnoreCase(mapping.getDeterminantColumnNames(), columnName)
                                || containsIgnoreCase(mapping.getDistributionColumnNames(), columnName);
                    })
                    .map(Entry::getKey)
                    .findFirst()
                    .orElse(null);
        } finally {
            readUnlock();
        }
    }

    /** Rebuild the qualified-name index from constraints persisted with a recovered or restored table. */
    @SuppressWarnings("deprecation")
    public void restoreTableConstraints(TableNameInfo tableNameInfo, TableIf table) {
        if (!(table instanceof Table)) {
            return;
        }
        Map<String, Constraint> tableLocalConstraints =
                ((Table) table).getTableAttributes().getConstraintsMap();
        String key = toKey(tableNameInfo);
        writeLock();
        try {
            Map<String, Constraint> indexedConstraints = constraintsMap.get(key);
            if (indexedConstraints != null) {
                indexedConstraints.entrySet().removeIf(
                        entry -> entry.getValue() instanceof DistributionMappingConstraint);
            }
            for (Entry<String, Constraint> entry : tableLocalConstraints.entrySet()) {
                Constraint constraint = entry.getValue();
                if (constraint instanceof DistributionMappingConstraint) {
                    if (indexedConstraints == null) {
                        indexedConstraints = new HashMap<>();
                    }
                    indexedConstraints.put(entry.getKey(), constraint);
                }
            }
            if (indexedConstraints == null || indexedConstraints.isEmpty()) {
                constraintsMap.remove(key);
            } else {
                constraintsMap.put(key, indexedConstraints);
            }
        } finally {
            writeUnlock();
        }
    }

    /** Populate table-local mapping metadata after loading an image created before table-local ownership. */
    public void syncDistributionMappingsToTables() {
        Map<String, Map<String, Constraint>> snapshot;
        readLock();
        try {
            snapshot = constraintsMap.entrySet().stream()
                    .collect(Collectors.toMap(
                            Entry::getKey,
                            entry -> ImmutableMap.copyOf(entry.getValue())));
        } finally {
            readUnlock();
        }
        snapshot.forEach((tableKey, constraints) -> {
            Map<String, Constraint> mappings = constraints.entrySet().stream()
                    .filter(entry -> entry.getValue() instanceof DistributionMappingConstraint)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
            if (mappings.isEmpty()) {
                return;
            }
            TableIf table = resolveTableIfPresent(new TableNameInfo(tableKey));
            mappings.forEach((name, constraint) ->
                    putTableLocalConstraint(table, name, constraint));
        });
    }

    /**
     * Atomically check for referencing foreign keys and then drop all constraints
     * for the given table. Holds the write lock for both operations to prevent
     * TOCTOU races where a new FK could be added between the check and the drop.
     *
     * @param tableNameInfo the table whose constraints are to be dropped
     * @param checkForeignKeys if true, throw DdlException if any PK is FK-referenced
     */
    public List<TableNameInfo> checkAndDropTableConstraints(TableNameInfo tableNameInfo,
            boolean checkForeignKeys) throws DdlException {
        return checkAndDropTableConstraints(ImmutableList.of(tableNameInfo), checkForeignKeys);
    }

    /**
     * Atomically validate and drop constraints for a set of tables.
     * Foreign keys owned by another table in the same set do not block the operation.
     */
    public List<TableNameInfo> checkAndDropTableConstraints(List<TableNameInfo> tableNameInfos,
            boolean checkForeignKeys) throws DdlException {
        writeLock();
        try {
            Map<String, TableNameInfo> tablesByKey = new LinkedHashMap<>();
            for (TableNameInfo tableNameInfo : tableNameInfos) {
                tablesByKey.putIfAbsent(toKey(tableNameInfo), tableNameInfo);
            }
            if (checkForeignKeys) {
                for (String tableKey : tablesByKey.keySet()) {
                    checkForeignKeyReferences(
                            tableKey, constraintsMap.get(tableKey), tablesByKey.keySet());
                }
            }
            Map<String, TableNameInfo> affectedTables = new LinkedHashMap<>();
            for (Entry<String, TableNameInfo> table : tablesByKey.entrySet()) {
                collectConstraintRelatedTables(
                        affectedTables, table.getValue(), constraintsMap.get(table.getKey()));
            }
            for (Entry<String, TableNameInfo> table : tablesByKey.entrySet()) {
                dropTableConstraintsWithoutLock(table.getKey(), table.getValue());
            }
            return ImmutableList.copyOf(affectedTables.values());
        } finally {
            writeUnlock();
        }
    }

    private void checkForeignKeyReferences(String tableKey,
            Map<String, Constraint> tableConstraints, Set<String> tablesBeingDropped)
            throws DdlException {
        if (tableConstraints == null) {
            return;
        }
        for (Constraint constraint : tableConstraints.values()) {
            if (constraint instanceof PrimaryKeyConstraint) {
                PrimaryKeyConstraint primaryKey = (PrimaryKeyConstraint) constraint;
                List<TableNameInfo> foreignKeyTables = primaryKey.getForeignTableInfos();
                List<TableNameInfo> externalForeignKeyTables = foreignKeyTables == null
                        ? Collections.emptyList()
                        : foreignKeyTables.stream()
                                .filter(table -> !tablesBeingDropped.contains(toKey(table)))
                                .collect(Collectors.toList());
                if (!externalForeignKeyTables.isEmpty()) {
                    String foreignKeyTableNames = externalForeignKeyTables.stream()
                            .map(ConstraintManager::toKey)
                            .collect(Collectors.joining(", "));
                    throw new DdlException(String.format(
                            "Cannot drop table %s because its primary"
                                    + " key is referenced by foreign key"
                                    + " constraints from table(s): %s."
                                    + " Drop the foreign key constraints"
                                    + " first.",
                            tableKey, foreignKeyTableNames));
                }
            }
        }
    }

    public List<TableNameInfo> dropTableConstraints(TableNameInfo tableNameInfo) {
        String key = toKey(tableNameInfo);
        writeLock();
        try {
            Map<String, TableNameInfo> affectedTables = new LinkedHashMap<>();
            collectConstraintRelatedTables(affectedTables, tableNameInfo, constraintsMap.get(key));
            dropTableConstraintsWithoutLock(key, tableNameInfo);
            return ImmutableList.copyOf(affectedTables.values());
        } finally {
            writeUnlock();
        }
    }

    /**
     * Drop constraints that reference columns removed by an out-of-band external schema change.
     * The mutation is local on every FE, like the surrounding metastore-event cleanup paths.
     */
    public List<TableNameInfo> dropConstraintsReferencingColumns(
            TableNameInfo tableNameInfo, Collection<String> columnNames) {
        String key = toKey(tableNameInfo);
        writeLock();
        try {
            Map<String, Constraint> tableConstraints = constraintsMap.get(key);
            if (tableConstraints == null) {
                return ImmutableList.of();
            }
            Map<String, Constraint> constraintsToDrop = new LinkedHashMap<>();
            for (Entry<String, Constraint> entry : tableConstraints.entrySet()) {
                if (columnNames.stream().anyMatch(
                        columnName -> constraintReferencesColumn(entry.getValue(), columnName))) {
                    constraintsToDrop.put(entry.getKey(), entry.getValue());
                }
            }
            if (constraintsToDrop.isEmpty()) {
                return ImmutableList.of();
            }

            Map<String, TableNameInfo> affectedTables = new LinkedHashMap<>();
            collectConstraintRelatedTables(affectedTables, tableNameInfo, constraintsToDrop);
            constraintsToDrop.forEach((constraintName, constraint) -> {
                tableConstraints.remove(constraintName);
                if (constraint instanceof DistributionMappingConstraint) {
                    removeTableLocalConstraint(
                            resolveTableIfPresent(tableNameInfo), constraintName);
                }
            });
            constraintsToDrop.values().forEach(
                    constraint -> cleanupConstraintReferences(tableNameInfo, constraint));
            if (tableConstraints.isEmpty()) {
                constraintsMap.remove(key);
            }
            LOG.info("Dropped constraints {} from table {} after columns {} were removed",
                    constraintsToDrop.keySet(), key, columnNames);
            return ImmutableList.copyOf(affectedTables.values());
        } finally {
            writeUnlock();
        }
    }

    private void dropTableConstraintsWithoutLock(String key, TableNameInfo tableNameInfo) {
        Map<String, Constraint> tableConstraints = constraintsMap.remove(key);
        if (tableConstraints == null) {
            return;
        }
        for (Constraint constraint : tableConstraints.values()) {
            cleanupConstraintReferences(tableNameInfo, constraint);
        }
        LOG.info("Dropped all constraints for table {}", key);
    }

    /**
     * Remove all constraints whose qualified table name starts with
     * the given catalog prefix. Called when a catalog is dropped.
     */
    public List<TableNameInfo> dropCatalogConstraints(String catalogName) {
        writeLock();
        try {
            String prefix = catalogName + ".";
            List<TableNameInfo> affectedTables = dropConstraintsByPrefix(prefix);
            LOG.info("Dropped all constraints for catalog {}", catalogName);
            return affectedTables;
        } finally {
            writeUnlock();
        }
    }

    /**
     * Remove all constraints for tables in the given database.
     * Called during DROP DATABASE to pre-clear all intra-database FK references
     * before individual table drops, avoiding ordering-dependent FK check failures.
     */
    public List<TableNameInfo> dropDatabaseConstraints(String catalogName, String dbName) {
        writeLock();
        try {
            String prefix = catalogName + "." + dbName + ".";
            List<TableNameInfo> affectedTables = dropConstraintsByPrefix(prefix);
            LOG.info("Dropped all constraints for database {}.{}",
                    catalogName, dbName);
            return affectedTables;
        } finally {
            writeUnlock();
        }
    }

    /**
     * Remove all constraints whose qualified table name starts with
     * the given prefix, cleaning up cross-references outside the prefix.
     */
    private List<TableNameInfo> dropConstraintsByPrefix(String prefix) {
        List<String> tablesToRemove = constraintsMap.keySet().stream()
                .filter(k -> k.startsWith(prefix))
                .collect(Collectors.toList());
        Map<String, TableNameInfo> affectedTables = new LinkedHashMap<>();
        for (String tableName : tablesToRemove) {
            TableNameInfo tableNameInfo = new TableNameInfo(tableName);
            Map<String, Constraint> tableConstraints
                    = constraintsMap.remove(tableName);
            if (tableConstraints != null) {
                collectConstraintRelatedTables(affectedTables, tableNameInfo, tableConstraints);
                for (Constraint constraint : tableConstraints.values()) {
                    cleanupConstraintReferencesOutsideCatalog(
                            tableName, constraint, prefix);
                }
            }
        }
        return ImmutableList.copyOf(affectedTables.values());
    }

    private void collectConstraintRelatedTables(Map<String, TableNameInfo> affectedTables,
            TableNameInfo tableNameInfo, Map<String, Constraint> constraints) {
        if (constraints == null || constraints.isEmpty()) {
            return;
        }
        affectedTables.putIfAbsent(toKey(tableNameInfo), tableNameInfo);
        for (Constraint constraint : constraints.values()) {
            if (constraint instanceof ForeignKeyConstraint) {
                TableNameInfo referencedTable = ((ForeignKeyConstraint) constraint).getReferencedTableName();
                if (referencedTable != null) {
                    affectedTables.putIfAbsent(toKey(referencedTable), referencedTable);
                }
            } else if (constraint instanceof PrimaryKeyConstraint) {
                for (TableNameInfo foreignTable : ((PrimaryKeyConstraint) constraint).getForeignTableInfos()) {
                    affectedTables.putIfAbsent(toKey(foreignTable), foreignTable);
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
        writeLock();
        try {
            renameTableWithoutLock(oldTableInfo, newTableInfo);
        } finally {
            writeUnlock();
        }
    }

    /** Move every qualified table key when a database is renamed. */
    public void renameDatabase(String catalogName, String oldDbName, String newDbName) {
        String oldPrefix = catalogName + "." + oldDbName + ".";
        writeLock();
        try {
            Map<TableNameInfo, TableNameInfo> renamedTables = constraintsMap.keySet().stream()
                    .filter(key -> key.startsWith(oldPrefix))
                    .map(TableNameInfo::new)
                    .collect(Collectors.toMap(
                            tableInfo -> tableInfo,
                            tableInfo -> new TableNameInfo(
                                    catalogName, newDbName, tableInfo.getTbl())));
            for (Entry<TableNameInfo, TableNameInfo> renamedTable : renamedTables.entrySet()) {
                Map<String, Constraint> tableConstraints =
                        constraintsMap.remove(toKey(renamedTable.getKey()));
                constraintsMap.put(toKey(renamedTable.getValue()), tableConstraints);
            }
            for (Map<String, Constraint> tableConstraints : constraintsMap.values()) {
                for (Constraint constraint : tableConstraints.values()) {
                    if (constraint instanceof ForeignKeyConstraint) {
                        ForeignKeyConstraint foreignKey = (ForeignKeyConstraint) constraint;
                        TableNameInfo referencedTable = foreignKey.getReferencedTableName();
                        if (referencedTable != null) {
                            TableNameInfo renamedTable = renamedTables.get(referencedTable);
                            if (renamedTable != null) {
                                foreignKey.setReferencedTableInfo(renamedTable);
                            }
                        }
                    } else if (constraint instanceof PrimaryKeyConstraint) {
                        ((PrimaryKeyConstraint) constraint).renameForeignTables(renamedTables);
                    }
                }
            }
            LOG.info("Renamed database constraints from {}.{} to {}.{}",
                    catalogName, oldDbName, catalogName, newDbName);
        } finally {
            writeUnlock();
        }
    }

    private void renameTableWithoutLock(TableNameInfo oldTableInfo, TableNameInfo newTableInfo) {
        String oldKey = toKey(oldTableInfo);
        String newKey = toKey(newTableInfo);
        Map<String, Constraint> tableConstraints = constraintsMap.remove(oldKey);
        if (tableConstraints != null) {
            constraintsMap.put(newKey, tableConstraints);
        }
        for (Map.Entry<String, Map<String, Constraint>> entry : constraintsMap.entrySet()) {
            for (Constraint constraint : entry.getValue().values()) {
                if (constraint instanceof ForeignKeyConstraint) {
                    ForeignKeyConstraint foreignKey = (ForeignKeyConstraint) constraint;
                    TableNameInfo referencedTable = foreignKey.getReferencedTableName();
                    if (referencedTable != null && oldTableInfo.equals(referencedTable)) {
                        foreignKey.setReferencedTableInfo(newTableInfo);
                    }
                } else if (constraint instanceof PrimaryKeyConstraint) {
                    ((PrimaryKeyConstraint) constraint).renameForeignTable(oldTableInfo, newTableInfo);
                }
            }
        }
        LOG.info("Renamed table constraints from {} to {}", oldKey, newKey);
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
                if (constraintReferencesColumn(entry.getValue(), columnName)) {
                    return entry.getKey();
                }
            }
            return null;
        } finally {
            readUnlock();
        }
    }

    private boolean constraintReferencesColumn(Constraint constraint, String columnName) {
        if (constraint instanceof PrimaryKeyConstraint) {
            return containsIgnoreCase(
                    ((PrimaryKeyConstraint) constraint).getPrimaryKeyNames(), columnName);
        } else if (constraint instanceof UniqueConstraint) {
            return containsIgnoreCase(
                    ((UniqueConstraint) constraint).getUniqueColumnNames(), columnName);
        } else if (constraint instanceof ForeignKeyConstraint) {
            return containsIgnoreCase(
                    ((ForeignKeyConstraint) constraint).getForeignKeyNames(), columnName);
        } else if (constraint instanceof DistributionMappingConstraint) {
            DistributionMappingConstraint mapping = (DistributionMappingConstraint) constraint;
            return containsIgnoreCase(mapping.getDeterminantColumnNames(), columnName)
                    || containsIgnoreCase(mapping.getDistributionColumnNames(), columnName);
        }
        return false;
    }

    private boolean containsIgnoreCase(Collection<String> columnNames, String columnName) {
        return columnNames.stream().anyMatch(name -> name.equalsIgnoreCase(columnName));
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
    private TableIf validateTableAndColumns(TableNameInfo tableNameInfo,
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
        } else if (constraint instanceof DistributionMappingConstraint) {
            validateDistributionMappingConstraint(
                    tableNameInfo, table, (DistributionMappingConstraint) constraint);
        }
        return table;
    }

    private void validateResolvedConstraint(TableNameInfo tableNameInfo, TableIf table,
            TableIf referencedTable, Constraint constraint) {
        if (constraint instanceof PrimaryKeyConstraint) {
            validateColumnsExist(table,
                    ((PrimaryKeyConstraint) constraint).getPrimaryKeyNames(),
                    toKey(tableNameInfo));
        } else if (constraint instanceof UniqueConstraint) {
            validateColumnsExist(table,
                    ((UniqueConstraint) constraint).getUniqueColumnNames(),
                    toKey(tableNameInfo));
        } else if (constraint instanceof ForeignKeyConstraint) {
            if (referencedTable == null) {
                throw new AnalysisException("Referenced table changed while adding constraint on "
                        + tableNameInfo);
            }
            ForeignKeyConstraint foreignKey = (ForeignKeyConstraint) constraint;
            validateColumnsExist(table, foreignKey.getForeignKeyNames(), toKey(tableNameInfo));
            validateColumnsExist(referencedTable, foreignKey.getReferencedColumnNames(),
                    toKey(foreignKey.getReferencedTableName()));
        } else if (constraint instanceof DistributionMappingConstraint) {
            validateDistributionMappingConstraint(
                    tableNameInfo, table, (DistributionMappingConstraint) constraint);
        }
    }

    private TableIf resolveTableIfPresent(TableNameInfo tableNameInfo) {
        try {
            return resolveTableForValidation(tableNameInfo);
        } catch (AnalysisException e) {
            LOG.debug("Table {} is unavailable while synchronizing table-local constraints",
                    tableNameInfo, e);
            return null;
        }
    }

    @SuppressWarnings("deprecation")
    private void putTableLocalConstraint(TableIf table, String constraintName, Constraint constraint) {
        if (table instanceof Table) {
            ((Table) table).getTableAttributes().getConstraintsMap().put(constraintName, constraint);
        }
    }

    @SuppressWarnings("deprecation")
    private void removeTableLocalConstraint(TableIf table, String constraintName) {
        if (table instanceof Table) {
            ((Table) table).getTableAttributes().getConstraintsMap().remove(constraintName);
        }
    }

    private void validateDistributionMappingConstraint(TableNameInfo tableNameInfo, TableIf table,
            DistributionMappingConstraint constraint) {
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("Distribution mapping constraint only supports OLAP tables");
        }
        validateColumnsExist(table, constraint.getDeterminantColumnNames(), toKey(tableNameInfo));
        validateColumnsExist(table, constraint.getDistributionColumnNames(), toKey(tableNameInfo));
        TreeSet<String> determinantColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        determinantColumns.addAll(constraint.getDeterminantColumnNames());
        if (determinantColumns.size() != constraint.getDeterminantColumnNames().size()) {
            throw new AnalysisException("Determinant columns in distribution mapping constraint must be unique");
        }
        TreeSet<String> distributionColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        distributionColumns.addAll(constraint.getDistributionColumnNames());
        if (distributionColumns.size() != constraint.getDistributionColumnNames().size()) {
            throw new AnalysisException("Distribution columns in distribution mapping constraint must be unique");
        }

        OlapTable olapTable = (OlapTable) table;
        if (!(olapTable.getDefaultDistributionInfo() instanceof HashDistributionInfo)) {
            throw new AnalysisException("Distribution mapping constraint requires hash distribution");
        }
        List<String> tableDistributionColumns = ((HashDistributionInfo) olapTable.getDefaultDistributionInfo())
                .getDistributionColumns().stream()
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toList());
        List<String> constraintDistributionColumns = constraint.getDistributionColumnNames().stream()
                .map(column -> column.toLowerCase(Locale.ROOT))
                .collect(Collectors.toList());
        int previousIndex = -1;
        for (String column : constraintDistributionColumns) {
            int index = tableDistributionColumns.indexOf(column);
            if (index <= previousIndex) {
                throw new AnalysisException("Distribution columns in distribution mapping constraint"
                        + " must be an ordered subset of table distribution columns");
            }
            previousIndex = index;
        }
    }

    private void validateFrontendVersionsForDistributionMappingConstraint() {
        String currentVersion = Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH;
        List<String> incompatibleFrontends = new ArrayList<>();
        for (Frontend frontend : Env.getCurrentEnv().getFrontends(null)) {
            String frontendVersion = frontend.getVersion();
            if (!currentVersion.equals(frontendVersion)) {
                incompatibleFrontends.add(frontend.getNodeName() + "(" + frontendVersion + ")");
            }
        }
        Collections.sort(incompatibleFrontends);
        if (!incompatibleFrontends.isEmpty()) {
            throw new AnalysisException("Cannot add distribution mapping constraint while frontend versions"
                    + " are mixed or unknown. Current version: " + currentVersion
                    + ", incompatible frontends: " + incompatibleFrontends);
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

    private EditLog.EditLogItem submitAddConstraint(TableNameInfo tableNameInfo,
            Constraint constraint) {
        AlterConstraintLog log = new AlterConstraintLog(
                constraint, tableNameInfo);
        return Env.getCurrentEnv().getEditLog()
                .submitEdit(OperationType.OP_ADD_CONSTRAINT, log);
    }

    private EditLog.EditLogItem submitDropConstraint(TableNameInfo tableNameInfo,
            Constraint constraint) {
        AlterConstraintLog log = new AlterConstraintLog(
                constraint, tableNameInfo);
        return Env.getCurrentEnv().getEditLog()
                .submitEdit(OperationType.OP_DROP_CONSTRAINT, log);
    }

    private void awaitEditLog(EditLog.EditLogItem logItem) {
        if (logItem != null) {
            logItem.await();
        }
    }
}
