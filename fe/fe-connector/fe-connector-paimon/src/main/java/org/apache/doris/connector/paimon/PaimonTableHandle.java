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
import java.util.List;
import java.util.Objects;

/**
 * Opaque table handle for Paimon tables.
 * Carries database name, table name, partition key names, and the Paimon Table reference.
 *
 * <p>A handle may also represent a <b>system table</b> (e.g. {@code mytable$snapshots}). For a
 * system handle {@link #sysTableName} is the bare sys-table name (no {@code "$"}) and
 * {@link #isSystemTable()} returns true; {@link #forceJni} carries the name-forced JNI hint
 * computed by {@link PaimonConnectorMetadata#getSysTableHandle}. This class is Java
 * {@link java.io.Serializable} only (there is no GSON registration for it): {@link #sysTableName}
 * and {@link #forceJni} are non-transient so they survive a Java serialization round-trip, while
 * the resolved {@link Table} stays {@code transient} and is re-loaded (a sys handle via the 4-arg
 * sys {@code Identifier}) when null. Normal handles keep {@code sysTableName == null} and
 * {@code forceJni == false}.
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
     * Name-forced JNI hint for system tables (legacy parity: true only for {@code binlog} /
     * {@code audit_log}). Always {@code false} for a normal handle. Serializable.
     */
    private final boolean forceJni;

    /** Transient Paimon Table reference; not serialized. Set by PaimonConnectorMetadata. */
    private transient Table paimonTable;

    public PaimonTableHandle(String databaseName, String tableName,
            List<String> partitionKeys, List<String> primaryKeys) {
        this(databaseName, tableName, partitionKeys, primaryKeys, null, false);
    }

    /**
     * Full constructor including the system-table fields. Use
     * {@link #forSystemTable(String, String, String, boolean)} to build a sys handle.
     */
    public PaimonTableHandle(String databaseName, String tableName,
            List<String> partitionKeys, List<String> primaryKeys,
            String sysTableName, boolean forceJni) {
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.sysTableName = sysTableName;
        this.forceJni = forceJni;
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

    /** Name-forced JNI hint (true only for {@code binlog} / {@code audit_log} sys tables). */
    public boolean isForceJni() {
        return forceJni;
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
        // sysTableName is part of identity so a sys handle (db.table$snapshots) never equals its
        // base handle (db.table) — they are distinct tables to the engine.
        return databaseName.equals(that.databaseName) && tableName.equals(that.tableName)
                && Objects.equals(sysTableName, that.sysTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, sysTableName);
    }

    @Override
    public String toString() {
        return sysTableName == null
                ? databaseName + "." + tableName
                : databaseName + "." + tableName + "$" + sysTableName;
    }
}
