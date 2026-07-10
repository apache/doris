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

package org.apache.doris.connector.api.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A single connector-neutral description of one metadata change the engine must apply after a
 * metastore poll. It is the vocabulary {@link ConnectorEventSource#pollOnce} returns: the plugin
 * fetches and parses the source's native notification events (Hive {@code NotificationEvent} +
 * JSON/GZIP messages, iceberg/hudi-on-HMS included) and maps each to one of these, so the engine can
 * update its catalog&#8594;db&#8594;table object graph and caches WITHOUT ever seeing a source-native type.
 *
 * <p><b>Type-neutrality is load-bearing.</b> Every field is a primitive, a {@code String}, or a
 * {@code List<String>} — no Hive/thrift class ever crosses this boundary. The engine calls the plugin
 * under a classloader pin; if a plugin-loaded object leaked into a field, a field access on the engine
 * side would fail across the loader split. Keep it neutral.</p>
 *
 * <p><b>Engine/connector split.</b> The connector owns fetch+parse and emits these descriptors
 * (already merged/deduplicated). The engine owns the structural application — register/unregister/
 * rename in {@code CatalogMgr}, cache invalidation via {@code Connector.invalidateTable/Db/Partition},
 * the master's edit-log write, and follower/master role handling. Trino-aligned: the plugin surfaces
 * what changed; the engine decides how its own metadata reacts.</p>
 *
 * <p>Partition granularity is by canonical partition NAME ({@code "col=val/.../colN=valN"}), never by
 * raw column values — the caches are name-keyed, so carrying names avoids the values&#8594;whole-table
 * degrade.</p>
 */
public final class MetastoreChangeDescriptor {

    /**
     * The kind of change. Maps 1:1 to what the engine does when applying the descriptor. A source
     * event that changes nothing the engine tracks (e.g. a properties-only ALTER DATABASE, or an
     * unsupported event type) produces NO descriptor rather than a no-op {@code Op}.
     */
    public enum Op {
        /** A new database exists; register it into the catalog. */
        REGISTER_DATABASE,
        /** A database was dropped; unregister it from the catalog. */
        UNREGISTER_DATABASE,
        /** A database was renamed; {@link #getDbName()} &#8594; {@link #getDbNameAfter()}. */
        RENAME_DATABASE,
        /** A new table exists; register it into its database. */
        REGISTER_TABLE,
        /** A table was dropped; unregister it from its database. */
        UNREGISTER_TABLE,
        /**
         * A table was renamed OR a view was recreated; {@link #getTableName()} &#8594;
         * {@link #getTableNameAfter()} (the after-name equals the before-name for a view recreate,
         * which rebuilds the table object so a changed view definition takes effect).
         */
        RENAME_TABLE,
        /** A table changed in place (insert / plain alter); drop its caches, keep it registered. */
        REFRESH_TABLE,
        /** Partitions were added; invalidate the table's partition caches so a re-list picks them up. */
        ADD_PARTITIONS,
        /** Partitions were dropped; invalidate the table's partition caches. */
        DROP_PARTITIONS,
        /** Partitions changed in place; invalidate the named partitions' caches. */
        REFRESH_PARTITIONS
    }

    private final Op op;
    private final String dbName;
    private final String tableName;
    private final String dbNameAfter;
    private final String tableNameAfter;
    private final List<String> partitionNames;
    private final long updateTime;
    private final long eventId;

    private MetastoreChangeDescriptor(Op op, String dbName, String tableName, String dbNameAfter,
            String tableNameAfter, List<String> partitionNames, long updateTime, long eventId) {
        this.op = Objects.requireNonNull(op, "op");
        this.dbName = dbName;
        this.tableName = tableName;
        this.dbNameAfter = dbNameAfter;
        this.tableNameAfter = tableNameAfter;
        this.partitionNames = partitionNames == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(partitionNames));
        this.updateTime = updateTime;
        this.eventId = eventId;
    }

    /** A database-level change ({@code REGISTER_/UNREGISTER_/RENAME_DATABASE}). */
    public static MetastoreChangeDescriptor forDatabase(Op op, String dbName, String dbNameAfter,
            long eventId, long updateTime) {
        return new MetastoreChangeDescriptor(op, dbName, null, dbNameAfter, null, null, updateTime, eventId);
    }

    /** A table-level change ({@code REGISTER_/UNREGISTER_/RENAME_TABLE}, {@code REFRESH_TABLE}). */
    public static MetastoreChangeDescriptor forTable(Op op, String dbName, String tableName,
            String tableNameAfter, long eventId, long updateTime) {
        return new MetastoreChangeDescriptor(op, dbName, tableName, null, tableNameAfter, null,
                updateTime, eventId);
    }

    /** A partition-level change ({@code ADD_/DROP_/REFRESH_PARTITIONS}). */
    public static MetastoreChangeDescriptor forPartitions(Op op, String dbName, String tableName,
            List<String> partitionNames, long eventId, long updateTime) {
        return new MetastoreChangeDescriptor(op, dbName, tableName, null, null, partitionNames,
                updateTime, eventId);
    }

    public Op getOp() {
        return op;
    }

    /** The remote database name (as the connector sees it). */
    public String getDbName() {
        return dbName;
    }

    /** The remote table name, or {@code null} for database-level ops. */
    public String getTableName() {
        return tableName;
    }

    /** The new database name for {@link Op#RENAME_DATABASE}, else {@code null}. */
    public String getDbNameAfter() {
        return dbNameAfter;
    }

    /** The new table name for {@link Op#RENAME_TABLE} (may equal {@link #getTableName()}), else {@code null}. */
    public String getTableNameAfter() {
        return tableNameAfter;
    }

    /** Canonical partition names for partition ops (empty for non-partition ops). */
    public List<String> getPartitionNames() {
        return partitionNames;
    }

    /** The source update time in epoch millis, used as the object's freshness signal. */
    public long getUpdateTime() {
        return updateTime;
    }

    /** The source notification event id this descriptor was derived from. */
    public long getEventId() {
        return eventId;
    }

    @Override
    public String toString() {
        return "MetastoreChangeDescriptor{op=" + op + ", db=" + dbName + ", table=" + tableName
                + ", dbAfter=" + dbNameAfter + ", tableAfter=" + tableNameAfter
                + ", partitions=" + partitionNames + ", updateTime=" + updateTime
                + ", eventId=" + eventId + '}';
    }
}
