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

package org.apache.doris.connector.cache;

import java.util.Objects;

/**
 * Immutable cache key for {@link ConnectorPartitionViewCache}: {@code (db, table, snapshotId, schemaId)}.
 *
 * <p>Engine-agnostic (external-partition-derived-cache design doc §5, "cache A"): a table's derived partition
 * view is a pure function of its identity plus the MVCC coordinate it was read at, so pinning that coordinate
 * into the key is what makes the cache "always correct" — a new snapshot/schema yields a new key, never a stale
 * hit. Non-MVCC engines (hive) or engines without a separate schema version pass {@code snapshotId = -1} /
 * {@code schemaId = -1}; the key still holds them, it just means "unversioned" for that axis.
 *
 * <p>{@link #matches} / {@link #matchesDb} back {@link ConnectorPartitionViewCache#invalidateTable} /
 * {@link ConnectorPartitionViewCache#invalidateDb}, which must drop every snapshot/schema of a (db, table) or
 * every table of a db — mirrors the {@code matches}/{@code matchesDb} helpers on the sibling connector caches
 * ({@code MaxComputePartitionCache.PartitionKey}, {@code HiveFileListingCache.FileListingKey}).
 */
public final class PartitionViewCacheKey {
    private final String db;
    private final String table;
    private final long snapshotId;
    private final long schemaId;

    public PartitionViewCacheKey(String db, String table, long snapshotId, long schemaId) {
        this.db = db;
        this.table = table;
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getSchemaId() {
        return schemaId;
    }

    /** Whether this key belongs to the given (db, table), regardless of snapshotId/schemaId. */
    public boolean matches(String db, String table) {
        return Objects.equals(this.db, db) && Objects.equals(this.table, table);
    }

    /** Whether this key belongs to the given db, regardless of table/snapshotId/schemaId. */
    public boolean matchesDb(String db) {
        return Objects.equals(this.db, db);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionViewCacheKey)) {
            return false;
        }
        PartitionViewCacheKey that = (PartitionViewCacheKey) o;
        return snapshotId == that.snapshotId
                && schemaId == that.schemaId
                && Objects.equals(db, that.db)
                && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, table, snapshotId, schemaId);
    }

    @Override
    public String toString() {
        return "PartitionViewCacheKey{db=" + db + ", table=" + table
                + ", snapshotId=" + snapshotId + ", schemaId=" + schemaId + '}';
    }
}
