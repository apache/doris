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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import java.util.Objects;

/**
 * Opaque table handle for an Iceberg table, carrying the database (namespace)
 * and table name coordinates plus an optional MVCC / time-travel pin (T07).
 *
 * <p>The pin is threaded in by {@code IcebergConnectorMetadata.applySnapshot} (called by the generic
 * {@code PluginDrivenScanNode} before {@code planScan} / {@code getScanNodeProperties}). It mirrors the
 * paimon connector's {@code PaimonTableHandle} scan options, but iceberg pins via typed carriers — the
 * iceberg SDK applies time-travel through {@code TableScan.useSnapshot(id)} / {@code useRef(name)} rather
 * than a {@code Table.copy(properties)} option map:
 * <ul>
 *   <li>{@code snapshotId} ({@code -1} = none) — {@code FOR VERSION AS OF <id>} / {@code FOR TIME AS OF}.</li>
 *   <li>{@code ref} ({@code null} = none) — a tag/branch name; the scan pins by REF ({@code useRef}) so a
 *       later commit to the tag/branch is honored (legacy parity).</li>
 *   <li>{@code schemaId} ({@code -1} = latest) — the schema version AS OF the pin, so the field-id dictionary
 *       and {@code getTableSchema(@snapshot)} read the historical schema.</li>
 * </ul>
 * The handle is immutable: {@link #withSnapshot} returns a NEW handle (the pin is part of the handle
 * identity, so {@link #equals}/{@link #hashCode}/{@link #toString} include it).
 *
 * <p>A handle may also represent a <b>system table</b> (e.g. {@code t$snapshots}); see
 * {@link #forSystemTable}. For a system handle {@link #sysTableName} is the bare sys-table name (no
 * {@code "$"}) and {@link #isSystemTable()} returns true. Unlike paimon's {@code forSystemTable},
 * the snapshot/ref/schema pin is RETAINED on a system handle because iceberg system tables legally
 * time-travel ({@code t$snapshots FOR VERSION AS OF ...}).
 */
public class IcebergTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    /** Sentinel for "no snapshot / latest schema" — mirrors legacy {@code IcebergUtils.UNKNOWN_SNAPSHOT_ID}. */
    private static final long NO_PIN = -1L;

    private final String dbName;
    private final String tableName;
    private final long snapshotId;
    private final String ref;
    private final long schemaId;

    /**
     * Bare system-table name (no {@code "$"}), lower-cased by the caller
     * ({@code IcebergConnectorMetadata.getSysTableHandle}), or {@code null} for a normal data-table
     * handle. Non-transient: the JNI sys-table read happens on a DESERIALIZED handle, so a deserialized
     * sys handle must still know it is a sys table (and at which snapshot) — otherwise it would silently
     * read the base data table at the latest version. It is part of the handle identity (a
     * {@code t$snapshots} read is a different table than {@code t}), so {@link #equals}/{@link #hashCode}/
     * {@link #toString} include it.
     */
    private final String sysTableName;

    public IcebergTableHandle(String dbName, String tableName) {
        this(dbName, tableName, NO_PIN, null, NO_PIN, null);
    }

    private IcebergTableHandle(String dbName, String tableName, long snapshotId, String ref, long schemaId,
            String sysTableName) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.snapshotId = snapshotId;
        this.ref = ref;
        this.schemaId = schemaId;
        this.sysTableName = sysTableName;
    }

    /**
     * Builds a system-table handle for {@code db.table$sysName} (e.g. {@code t$snapshots}). Unlike
     * paimon's {@code forSystemTable}, the snapshot/ref/schema pin is RETAINED and threaded straight
     * through: iceberg system tables legally time-travel ({@code FOR VERSION/TIME AS OF}), so a pinned
     * sys read must honor the pin (deviation 1). {@code sysName} is the bare lower-cased name (no
     * {@code "$"}); the caller normalizes it.
     */
    public static IcebergTableHandle forSystemTable(String dbName, String tableName, String sysName,
            long snapshotId, String ref, long schemaId) {
        return new IcebergTableHandle(dbName, tableName, snapshotId, ref, schemaId, sysName);
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    /** The pinned snapshot id, or {@code -1} when there is no snapshot-id pin. */
    public long getSnapshotId() {
        return snapshotId;
    }

    /** The pinned tag/branch ref name, or {@code null} when there is no ref pin. */
    public String getRef() {
        return ref;
    }

    /** The pinned schema id, or {@code -1} (latest) when there is no pin. */
    public long getSchemaId() {
        return schemaId;
    }

    /** Bare system-table name (no {@code "$"}), or {@code null} for a normal data-table handle. */
    public String getSysTableName() {
        return sysTableName;
    }

    /** Whether this handle represents an iceberg system table (e.g. {@code t$snapshots}). */
    public boolean isSystemTable() {
        return sysTableName != null;
    }

    /** Whether this handle carries an explicit MVCC / time-travel pin (a snapshot id or a tag/branch ref). */
    public boolean hasSnapshotPin() {
        return snapshotId >= 0 || ref != null;
    }

    /**
     * Returns a copy of this handle carrying the resolved time-travel pin. Mirrors paimon's
     * {@code PaimonTableHandle.withScanOptions}/{@code withBranch} but with iceberg's typed carriers.
     */
    public IcebergTableHandle withSnapshot(long snapshotId, String ref, long schemaId) {
        // sysTableName is preserved: threading a resolved time-travel pin in must not degrade a sys
        // handle (t$snapshots) into a normal data-table handle. Mirrors paimon withScanOptions/withBranch.
        return new IcebergTableHandle(dbName, tableName, snapshotId, ref, schemaId, sysTableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergTableHandle)) {
            return false;
        }
        IcebergTableHandle that = (IcebergTableHandle) o;
        return snapshotId == that.snapshotId
                && schemaId == that.schemaId
                && Objects.equals(dbName, that.dbName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(ref, that.ref)
                && Objects.equals(sysTableName, that.sysTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName, snapshotId, ref, schemaId, sysTableName);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("IcebergTableHandle{").append(dbName).append('.').append(tableName);
        if (sysTableName != null) {
            sb.append('$').append(sysTableName);
        }
        if (hasSnapshotPin()) {
            sb.append(", snapshotId=").append(snapshotId).append(", ref=").append(ref)
                    .append(", schemaId=").append(schemaId);
        }
        return sb.append('}').toString();
    }
}
