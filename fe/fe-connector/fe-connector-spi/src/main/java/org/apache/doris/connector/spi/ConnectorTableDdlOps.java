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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import java.util.List;

/**
 * Table-level DDL: create, drop, rename, truncate.
 *
 * <p><b>The whole domain is optional</b> — a read-only connector implements nothing here and every default
 * fails loud with a "not supported" message, which is the correct answer for it.</p>
 *
 * <p>Minimum implementation set: to support {@code CREATE TABLE}, override
 * {@link #createTable(ConnectorSession, ConnectorCreateTableRequest)}. There is deliberately only ONE create
 * entry point: a narrower {@code (schema, properties)} overload used to exist and the request overload
 * degraded to it, silently dropping the partition spec, the bucket spec and {@code IF NOT EXISTS} — a
 * connector implementing only the narrow form reported success on a partitioned {@code CREATE TABLE} and
 * created an unpartitioned table. Nothing implemented it, so it is gone rather than documented.
 * {@link #dropTable}, {@link #renameTable} and {@link #truncateTable} are independent and optional.</p>
 */
public interface ConnectorTableDdlOps {

    /**
     * Creates a table with full DDL semantics (partition, bucket, {@code IF NOT EXISTS}).
     *
     * <p>The request carries everything the statement said; a connector that cannot honor part of it must
     * reject that part rather than ignore it, because a {@code CREATE TABLE} reporting success after dropping
     * the partition spec is indistinguishable from one that worked.</p>
     *
     * @throws DorisConnectorException if the connector cannot create tables, or cannot honor the request
     */
    @ConnectorMustImplement(when = "the connector supports CREATE TABLE")
    default void createTable(ConnectorSession session,
            ConnectorCreateTableRequest request) {
        throw new DorisConnectorException(
                "CREATE TABLE not supported");
    }

    /** Drops the specified table. */
    default void dropTable(ConnectorSession session,
            ConnectorTableHandle handle) {
        throw new DorisConnectorException(
                "DROP TABLE not supported");
    }

    /** Renames the table identified by {@code handle} to {@code newName} within the same database. */
    default void renameTable(ConnectorSession session,
            ConnectorTableHandle handle, String newName) {
        throw new DorisConnectorException(
                "RENAME TABLE not supported");
    }

    /**
     * Truncates the table identified by {@code handle}. When {@code partitions} is non-empty only those
     * partitions are truncated; {@code null} / empty truncates the whole table.
     *
     * <p>Connectors that support {@code TRUNCATE TABLE} override this. The default throws, matching the
     * pre-flip behavior of the generic bridge (which had no truncate route for the SPI path).</p>
     *
     * @throws DorisConnectorException if the connector does not support truncate
     */
    default void truncateTable(ConnectorSession session,
            ConnectorTableHandle handle, List<String> partitions) {
        throw new DorisConnectorException(
                "TRUNCATE TABLE not supported");
    }

    /**
     * Clears the DATA of the named partitions ({@code ALTER TABLE ... DROP PARTITION}). Each entry of
     * {@code partitionNames} is a partition DISPLAY name in the connector's own {@code listPartitions}
     * form (e.g. paimon's {@code k1=v1/k2=v2}); the connector resolves each back to its native partition
     * spec, so fe-core never re-parses values out of the name.
     *
     * <p>{@code ifExists} follows the Doris {@code DROP PARTITION IF EXISTS} contract: a name absent from
     * the table is a silent no-op when {@code true} and an error when {@code false}. Distinct from
     * {@link ConnectorSnapshotRefOps#dropPartitionField}, which drops a partition COLUMN from the SPEC — this
     * only removes rows and leaves the schema untouched.</p>
     *
     * <p>Connectors that support it override this; the default throws, so a connector that cannot drop
     * partitions rejects the clause with a clear message rather than silently ignoring it.</p>
     *
     * @throws DorisConnectorException if the connector does not support dropping partitions, or a named
     *     partition does not exist and {@code ifExists} is false
     */
    default void dropPartitions(ConnectorSession session,
            ConnectorTableHandle handle, List<String> partitionNames, boolean ifExists) {
        throw new DorisConnectorException(
                "DROP PARTITION not supported");
    }
}
