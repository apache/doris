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

    public IcebergTableHandle(String dbName, String tableName) {
        this(dbName, tableName, NO_PIN, null, NO_PIN);
    }

    private IcebergTableHandle(String dbName, String tableName, long snapshotId, String ref, long schemaId) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.snapshotId = snapshotId;
        this.ref = ref;
        this.schemaId = schemaId;
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

    /** Whether this handle carries an explicit MVCC / time-travel pin (a snapshot id or a tag/branch ref). */
    public boolean hasSnapshotPin() {
        return snapshotId >= 0 || ref != null;
    }

    /**
     * Returns a copy of this handle carrying the resolved time-travel pin. Mirrors paimon's
     * {@code PaimonTableHandle.withScanOptions}/{@code withBranch} but with iceberg's typed carriers.
     */
    public IcebergTableHandle withSnapshot(long snapshotId, String ref, long schemaId) {
        return new IcebergTableHandle(dbName, tableName, snapshotId, ref, schemaId);
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
                && Objects.equals(ref, that.ref);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName, snapshotId, ref, schemaId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("IcebergTableHandle{").append(dbName).append('.').append(tableName);
        if (hasSnapshotPin()) {
            sb.append(", snapshotId=").append(snapshotId).append(", ref=").append(ref)
                    .append(", schemaId=").append(schemaId);
        }
        return sb.append('}').toString();
    }
}
