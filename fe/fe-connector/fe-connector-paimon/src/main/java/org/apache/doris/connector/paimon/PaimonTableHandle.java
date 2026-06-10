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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.paimon.table.Table;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Opaque table handle for Paimon tables.
 * Carries database name, table name, partition key names, and the Paimon Table reference.
 *
 * <p>A handle may also represent a <b>system table</b> (e.g. {@code mytable$snapshots}). For a
 * system handle {@link #sysTableName} is the bare sys-table name (no {@code "$"}) and
 * {@link #isSystemTable()} returns true; {@link #forceJni} carries the name-forced JNI hint
 * computed by {@link PaimonConnectorMetadata#getSysTableHandle}. This class is Java
 * {@link java.io.Serializable} only (there is no GSON registration for it): {@link #sysTableName},
 * {@link #forceJni}, {@link #scanOptions} and {@link #branchName} are non-transient so they survive
 * a Java serialization round-trip, while the resolved {@link Table} stays {@code transient} and is
 * re-loaded (a sys handle via the 4-arg sys {@code Identifier}, a branch handle via the 3-arg branch
 * {@code Identifier}) when null. Normal handles keep {@code sysTableName == null},
 * {@code forceJni == false} and {@code branchName == null}.
 *
 * <p>{@link #scanOptions} carries paimon scan options (e.g. {@code {"scan.snapshot-id": "5"}}) for
 * a time-travel / MVCC-pinned read. It is empty for a normal/sys handle; a pinned handle is built
 * via {@link #withScanOptions(Map)} and the scan path applies it with {@code Table.copy(options)}.
 */
public class PaimonTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String tableName;
    private final List<String> partitionKeys;
    private final List<String> primaryKeys;

    /**
     * Bare system-table name (no {@code "$"}), or {@code null} for a normal table handle.
     * Serializable: a deserialized sys handle must still reload via the 4-arg sys Identifier.
     */
    private final String sysTableName;

    /**
     * Branch name for a branch time-travel read ({@code @branch('name')}), or {@code null} for a
     * normal/base handle. A branch is a DIFFERENT table identity than its base (independent schema +
     * snapshots), so it is part of {@link #equals}/{@link #hashCode} (exactly like {@link
     * #sysTableName}) and a non-null branch reloads via the 3-arg branch Identifier (see
     * {@link PaimonTableResolver#resolve}). Serializable: a deserialized branch handle must still
     * reload the branch table. Branch and sys are mutually exclusive in practice.
     */
    private final String branchName;

    /**
     * Name-forced JNI hint for system tables (legacy parity: true only for {@code binlog} /
     * {@code audit_log}). Always {@code false} for a normal handle. Serializable.
     */
    private final boolean forceJni;

    /**
     * Paimon scan options for a time-travel / MVCC-pinned read (e.g. {@code scan.snapshot-id=5}).
     * Empty for a normal/sys handle; populated only via {@link #withScanOptions(Map)} when the
     * engine threads a pinned snapshot in. Serializable (survives the FE/BE round-trip) so the JNI
     * serialized-table read pins to the same version as the planned splits.
     */
    private final Map<String, String> scanOptions;

    /** Transient Paimon Table reference; not serialized. Set by PaimonConnectorMetadata. */
    private transient Table paimonTable;

    public PaimonTableHandle(String databaseName, String tableName,
            List<String> partitionKeys, List<String> primaryKeys) {
        this(databaseName, tableName, partitionKeys, primaryKeys, null, false);
    }

    /**
     * Full constructor including the system-table fields. Use
     * {@link #forSystemTable(String, String, String, boolean)} to build a sys handle. scanOptions
     * defaults to empty (a normal/sys handle is not snapshot-pinned).
     */
    public PaimonTableHandle(String databaseName, String tableName,
            List<String> partitionKeys, List<String> primaryKeys,
            String sysTableName, boolean forceJni) {
        this(databaseName, tableName, partitionKeys, primaryKeys, sysTableName, forceJni,
                Collections.emptyMap(), null);
    }

    private PaimonTableHandle(String databaseName, String tableName,
            List<String> partitionKeys, List<String> primaryKeys,
            String sysTableName, boolean forceJni, Map<String, String> scanOptions,
            String branchName) {
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.sysTableName = sysTableName;
        this.forceJni = forceJni;
        this.branchName = branchName;
        // Defensive immutable copy (codebase convention, cf. ConnectorPartitionInfo /
        // ConnectorMvccSnapshot): the HashMap-backed unmodifiable map stays Serializable so the
        // Java-serialization round-trip is preserved.
        this.scanOptions = scanOptions == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(scanOptions));
    }

    /**
     * Builds a system-table handle for {@code db.table$sysTableName}. Partition/primary keys are
     * empty: system tables are scanned as their own (synthetic) tables, not as partitions of the
     * base table.
     */
    public static PaimonTableHandle forSystemTable(String databaseName, String tableName,
            String sysTableName, boolean forceJni) {
        return new PaimonTableHandle(databaseName, tableName,
                Collections.emptyList(), Collections.emptyList(), sysTableName, forceJni);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    /** Bare system-table name (no {@code "$"}), or {@code null} for a normal handle. */
    public String getSysTableName() {
        return sysTableName;
    }

    /** True when this handle represents a Paimon system table. */
    public boolean isSystemTable() {
        return sysTableName != null;
    }

    /** Branch name for a branch time-travel read, or {@code null} for a normal/base handle. */
    public String getBranchName() {
        return branchName;
    }

    /** Name-forced JNI hint (true only for {@code binlog} / {@code audit_log} sys tables). */
    public boolean isForceJni() {
        return forceJni;
    }

    /** Paimon scan options for a pinned read (empty for a normal/sys handle). */
    public Map<String, String> getScanOptions() {
        return scanOptions;
    }

    /**
     * Returns a NEW handle identical to this one (db/table/partitionKeys/primaryKeys/sysTableName/
     * forceJni/branchName and the transient {@link Table} are preserved) but carrying the given
     * scanOptions — the snapshot-pinned read variant. The transient Table is copied over as-is; the
     * scan path applies {@code Table.copy(scanOptions)} at resolution time. branchName is preserved
     * because it is part of the handle identity.
     */
    public PaimonTableHandle withScanOptions(Map<String, String> options) {
        PaimonTableHandle copy = new PaimonTableHandle(databaseName, tableName,
                partitionKeys, primaryKeys, sysTableName, forceJni, options, branchName);
        copy.paimonTable = this.paimonTable;
        return copy;
    }

    /**
     * Returns a NEW handle identical to this one (db/table/partitionKeys/primaryKeys/sysTableName/
     * forceJni/scanOptions preserved) but carrying the given {@code branchName} — the branch
     * time-travel read variant.
     *
     * <p>CRITICAL: unlike {@link #withScanOptions(Map)}, this does NOT copy the transient
     * {@link Table} over. A branch is a DIFFERENT table (independent schema + snapshots), so the
     * transient reference is left {@code null} and {@link PaimonTableResolver#resolve} reloads the
     * BRANCH table via the 3-arg branch Identifier. Copying the base Table over would make the
     * branch read return the BASE table's rows — a silent data error.
     */
    public PaimonTableHandle withBranch(String branchName) {
        return new PaimonTableHandle(databaseName, tableName,
                partitionKeys, primaryKeys, sysTableName, forceJni, scanOptions, branchName);
    }

    /** Returns the transient Paimon Table reference, or null if not set. */
    public Table getPaimonTable() {
        return paimonTable;
    }

    /** Sets the transient Paimon Table reference. */
    public void setPaimonTable(Table paimonTable) {
        this.paimonTable = paimonTable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PaimonTableHandle)) {
            return false;
        }
        PaimonTableHandle that = (PaimonTableHandle) o;
        // sysTableName AND branchName are part of identity: a sys handle (db.table$snapshots) never
        // equals its base handle (db.table), and a branch handle (db.table@branch) never equals its
        // base — all are distinct tables to the engine (independent schema/snapshots). scanOptions is
        // intentionally NOT part of identity: a snapshot-pinned handle is the SAME table, just read
        // at a version, so it must equal/hash identically to its base handle.
        return databaseName.equals(that.databaseName) && tableName.equals(that.tableName)
                && Objects.equals(sysTableName, that.sysTableName)
                && Objects.equals(branchName, that.branchName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, sysTableName, branchName);
    }

    @Override
    public String toString() {
        String base = sysTableName == null
                ? databaseName + "." + tableName
                : databaseName + "." + tableName + "$" + sysTableName;
        return branchName == null ? base : base + "@" + branchName;
    }
}
