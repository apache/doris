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
 * Hierarchical invalidation path within one catalog-owned scoped cache registry.
 *
 * <p>The catalog identity is implicit in the owning {@link ScopedMetaCacheRegistry}. Database and table
 * identities are strings because FE and connector callers already invalidate by their respective local or remote
 * names. A partition identity is deliberately opaque: Hive partition values, Iceberg transforms, and other
 * connector-specific partition identities do not share one representation. Like a cache key, it must keep stable
 * {@link Object#equals(Object)} and {@link Object#hashCode()} semantics while registered.
 */
public final class ScopePath {
    private enum SyntheticPartition {
        COLLECTION,
        METADATA
    }

    private enum NameCollection {
        DATABASES,
        TABLES
    }

    public enum Level {
        CATALOG,
        DATABASE,
        TABLE,
        PARTITION
    }

    private static final ScopePath CATALOG = new ScopePath(Level.CATALOG, null, null, null);

    private final Level level;
    private final String database;
    private final String table;
    private final Object partition;
    private final NameCollection nameCollection;

    private ScopePath(Level level, String database, String table, Object partition) {
        this(level, database, table, partition, null);
    }

    private ScopePath(Level level, String database, String table, Object partition, NameCollection nameCollection) {
        this.level = level;
        this.database = database;
        this.table = table;
        this.partition = partition;
        this.nameCollection = nameCollection;
    }

    public static ScopePath catalog() {
        return CATALOG;
    }

    public static ScopePath database(String database) {
        return new ScopePath(Level.DATABASE, Objects.requireNonNull(database, "database can not be null"),
                null, null);
    }

    public static ScopePath table(String database, String table) {
        return new ScopePath(
                Level.TABLE,
                Objects.requireNonNull(database, "database can not be null"),
                Objects.requireNonNull(table, "table can not be null"),
                null);
    }

    public static ScopePath partition(String database, String table, Object partition) {
        return new ScopePath(
                Level.PARTITION,
                Objects.requireNonNull(database, "database can not be null"),
                Objects.requireNonNull(table, "table can not be null"),
                Objects.requireNonNull(partition, "partition can not be null"));
    }

    /**
     * Scope for a value describing the table's partition collection as a whole, such as a partition-name list.
     * It is a child of the table but distinct from every connector partition identity, allowing a partition
     * change to invalidate membership views without also dropping unrelated table metadata.
     */
    public static ScopePath partitionCollection(String database, String table) {
        return partition(database, table, SyntheticPartition.COLLECTION);
    }

    /** The catalog's database-name snapshot, distinct from every user database. */
    public static ScopePath databaseCollection() {
        return new ScopePath(Level.DATABASE, null, null, null, NameCollection.DATABASES);
    }

    /** A database's table-name snapshot, distinct from every user table. */
    public static ScopePath tableCollection(String database) {
        return new ScopePath(Level.TABLE, Objects.requireNonNull(database, "database"),
                null, null, NameCollection.TABLES);
    }

    /** FE schema/content entries; invalidating them preserves the FE table object and name snapshot. */
    public static ScopePath tableMetadata(String database, String table) {
        return partition(database, table, SyntheticPartition.METADATA);
    }

    Object databaseKey() {
        return nameCollection == NameCollection.DATABASES ? NameCollection.DATABASES : database;
    }

    Object tableKey() {
        return nameCollection == NameCollection.TABLES ? NameCollection.TABLES : table;
    }

    public Level level() {
        return level;
    }

    public String database() {
        return database;
    }

    public String table() {
        return table;
    }

    public Object partition() {
        return partition;
    }

    public boolean contains(ScopePath other) {
        Objects.requireNonNull(other, "other can not be null");
        if (level.ordinal() > other.level.ordinal()) {
            return false;
        }
        if (level == Level.CATALOG) {
            return true;
        }
        if (!Objects.equals(databaseKey(), other.databaseKey())) {
            return false;
        }
        if (level == Level.DATABASE) {
            return true;
        }
        if (!Objects.equals(tableKey(), other.tableKey())) {
            return false;
        }
        return level == Level.TABLE || partition.equals(other.partition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ScopePath)) {
            return false;
        }
        ScopePath other = (ScopePath) obj;
        return level == other.level
                && Objects.equals(databaseKey(), other.databaseKey())
                && Objects.equals(tableKey(), other.tableKey())
                && Objects.equals(partition, other.partition);
    }

    @Override
    public int hashCode() {
        int pathHashCode = 1;
        pathHashCode = 31 * pathHashCode + Objects.hashCode(level);
        pathHashCode = 31 * pathHashCode + Objects.hashCode(databaseKey());
        pathHashCode = 31 * pathHashCode + Objects.hashCode(tableKey());
        return 31 * pathHashCode + Objects.hashCode(partition);
    }

    @Override
    public String toString() {
        if (nameCollection != null) {
            return nameCollection == NameCollection.DATABASES
                    ? "databaseCollection" : "tableCollection[" + database + "]";
        }
        switch (level) {
            case CATALOG:
                return "catalog";
            case DATABASE:
                return "database[" + database + "]";
            case TABLE:
                return "table[" + database + "." + table + "]";
            case PARTITION:
                return "partition[" + database + "." + table + ":" + partition + "]";
            default:
                throw new IllegalStateException("Unknown scope level: " + level);
        }
    }
}
