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

package org.apache.doris.connector.api;

/**
 * Enumerates the optional capabilities a connector may declare.
 * The planner and execution engine use these to decide which
 * pushdown and write paths are available.
 */
public enum ConnectorCapability {
    SUPPORTS_FILTER_PUSHDOWN,
    SUPPORTS_PROJECTION_PUSHDOWN,
    SUPPORTS_LIMIT_PUSHDOWN,
    SUPPORTS_PARTITION_PRUNING,
    SUPPORTS_INSERT,
    SUPPORTS_DELETE,
    SUPPORTS_UPDATE,
    SUPPORTS_MERGE,
    SUPPORTS_CREATE_TABLE,
    SUPPORTS_MVCC_SNAPSHOT,
    SUPPORTS_METASTORE_EVENTS,
    SUPPORTS_STATISTICS,
    SUPPORTS_VENDED_CREDENTIALS,
    SUPPORTS_ACID_TRANSACTIONS,
    SUPPORTS_TIME_TRAVEL,
    /**
     * Indicates the connector supports multiple concurrent writers (sink instances).
     *
     * <p>Connectors that do NOT declare this capability will use GATHER distribution
     * (single writer), which is the safe default for transactional sinks like JDBC
     * where each writer commits independently.</p>
     *
     * <p>File-based connectors (Hive, Iceberg, etc.) that can safely handle
     * parallel writers should declare this capability.</p>
     */
    SUPPORTS_PARALLEL_WRITE,
    /**
     * Indicates the connector requires dynamic-partition writes to be hash-distributed by
     * partition columns and locally sorted by them before reaching the sink.
     *
     * <p>Streaming partition writers (e.g. the MaxCompute Storage API) close the previous
     * partition writer as soon as a new partition value appears; un-grouped (unsorted)
     * multi-partition rows therefore cause "writer has been closed" errors. The planner uses
     * this capability to require a hash-by-partition distribution plus a mandatory local sort
     * on the partition columns for dynamic-partition writes.</p>
     *
     * <p>A connector declaring this is expected to also declare
     * {@link #SUPPORTS_PARALLEL_WRITE} (hash distribution is inherently parallel) and
     * {@link #SINK_REQUIRE_FULL_SCHEMA_ORDER}: the sink distribution locates partition columns by their
     * <b>full-schema</b> position in the child output, which only holds when the bind layer projects the
     * write to full-schema order (the projection gated by {@code SINK_REQUIRE_FULL_SCHEMA_ORDER}). A
     * connector declaring this without {@code SINK_REQUIRE_FULL_SCHEMA_ORDER} would shuffle/sort by the
     * wrong column whenever cols order diverges from the full schema.</p>
     */
    SINK_REQUIRE_PARTITION_LOCAL_SORT,
    /**
     * Indicates the connector's write path maps data columns <b>positionally</b> against the full
     * table schema (e.g. MaxCompute's columnar Storage API / JNI writer), rather than by column name.
     *
     * <p>For such connectors the sink's output rows must be projected to <b>full table schema order</b>
     * with any unmentioned columns filled (NULL / default) — exactly like the legacy MaxCompute bind
     * path — so that a reordered or partial explicit column list does not land values in the wrong
     * remote columns. Name-mapped connectors (e.g. JDBC, which builds an {@code INSERT INTO t (cols)}
     * statement) must NOT declare this capability: their data stays in user/cols order to match the
     * generated column list.</p>
     */
    SINK_REQUIRE_FULL_SCHEMA_ORDER,
    /**
     * Indicates the connector supports passthrough query via the {@code query()} TVF.
     *
     * <p>Connectors declaring this capability must implement
     * {@link ConnectorTableOps#getColumnsFromQuery} to provide column metadata
     * for arbitrary SQL queries passed through to the remote data source.</p>
     */
    SUPPORTS_PASSTHROUGH_QUERY
}
