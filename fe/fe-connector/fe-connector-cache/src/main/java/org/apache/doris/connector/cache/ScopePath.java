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

    private ScopePath(Level level, String database, String table, Object partition) {
        this.level = level;
        this.database = database;
        this.table = table;
        this.partition = partition;
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
        if (!database.equals(other.database)) {
            return false;
        }
        if (level == Level.DATABASE) {
            return true;
        }
        if (!table.equals(other.table)) {
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
                && Objects.equals(database, other.database)
                && Objects.equals(table, other.table)
                && Objects.equals(partition, other.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, database, table, partition);
    }

    @Override
    public String toString() {
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
