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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorStatementScopes;

import java.util.List;
import java.util.function.Supplier;

/**
 * Connector-private helpers over the neutral {@link ConnectorStatementScope} (reached via
 * {@link ConnectorSession#getStatementScope()}), giving hudi one place to key its per-statement metaClient memos.
 *
 * <p>Two INDEPENDENT memos, so a statement builds each latest fact at most once and the two never couple: the
 * latest-schema read ({@code getTableSchema} with no time-travel pin) resolves through {@link #sharedLatestColumns},
 * and the query-snapshot pin ({@code beginQuerySnapshot}) through {@link #sharedLatestInstant}. Each loader is the
 * connector's existing single-fact method, so the memo only collapses REPEATED same-fact resolves within a statement
 * and leaves each fact's value / failure semantics byte-identical. Distinct namespaces
 * ({@link #LATEST_SCHEMA_NAMESPACE} vs {@link #LATEST_INSTANT_NAMESPACE}) keep the two value types (columns vs
 * instant) from colliding on the shared {@code (catalog, db, table, queryId)} coordinate.</p>
 *
 * <p>Under {@link ConnectorStatementScope#NONE} (offline planning / no live statement) or a {@code null} session each
 * loader runs every time — byte-identical to resolving the fact per call, as before the memo.</p>
 */
final class HudiStatementScope {

    /**
     * Namespace for hudi's per-statement latest-columns memo. Source-prefixed with the connector type ("hudi")
     * so it stays distinct across a heterogeneous gateway; see {@link ConnectorStatementScopes}.
     */
    static final String LATEST_SCHEMA_NAMESPACE = "hudi.latest_schema";

    /** Namespace for hudi's per-statement latest-completed-instant memo. Source-prefixed with the type ("hudi"). */
    static final String LATEST_INSTANT_NAMESPACE = "hudi.latest_instant";

    /** Namespace for hudi's per-statement unpinned metadata partition listing memo. */
    static final String METADATA_PARTITIONS_NAMESPACE = "hudi.metadata_partitions";

    /** Namespace for hudi's per-statement HMS partition path listing memo. */
    static final String HMS_PARTITIONS_NAMESPACE = "hudi.hms_partitions";

    private HudiStatementScope() {}

    /**
     * Resolves the latest columns for {@code db.table} once per statement, sharing the single result across every
     * latest-schema resolver of the statement. {@code loader} (the connector's existing latest-schema read) runs at
     * most once per statement; the key carries the catalog id and queryId (cross-catalog / prepared-execution
     * isolation).
     */
    static List<ConnectorColumn> sharedLatestColumns(ConnectorSession session, String dbName, String tableName,
            Supplier<List<ConnectorColumn>> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, LATEST_SCHEMA_NAMESPACE, dbName, tableName, loader);
    }

    /**
     * Resolves the latest completed instant for {@code db.table} once per statement, sharing the single value across
     * every snapshot-pin resolver of the statement. {@code loader} (the connector's existing instant read) runs at
     * most once per statement.
     */
    static long sharedLatestInstant(ConnectorSession session, String dbName, String tableName,
            Supplier<Long> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, LATEST_INSTANT_NAMESPACE, dbName, tableName, loader);
    }

    /**
     * Resolves the unpinned Hudi metadata-table partition listing once per statement for the physical table identity
     * ({@code tableName@basePath}), sharing it across {@code listPartitions}, {@code applyFilter}, and
     * {@code HudiScanPlanProvider.resolvePartitions}.
     */
    static HudiConnectorMetadata.Listing sharedMetadataPartitions(ConnectorSession session, HudiTableHandle handle,
            Supplier<HudiConnectorMetadata.Listing> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, METADATA_PARTITIONS_NAMESPACE, handle.getDbName(), physicalTableKey(handle), loader);
    }

    /**
     * Records an already-loaded unpinned metadata-table partition listing in the current statement scope (e.g. from a
     * pinned {@code listPartitions} visit that already enumerated all metadata paths on the same metaClient), so a
     * later {@code applyFilter} or {@code resolvePartitions} in the same statement reuses it without re-listing.
     */
    static void rememberMetadataPartitions(ConnectorSession session, HudiTableHandle handle,
            HudiConnectorMetadata.Listing unpinnedListing) {
        if (session != null && unpinnedListing != null) {
            ConnectorStatementScopes.resolveInStatement(session, METADATA_PARTITIONS_NAMESPACE,
                    handle.getDbName(), physicalTableKey(handle), () -> unpinnedListing);
        }
    }

    /**
     * Resolves the HMS-backed relative partition paths once per statement for the physical table identity
     * ({@code tableName@basePath}), sharing them between {@code listPartitions} and {@code applyFilter}.
     */
    static HudiConnectorMetadata.HmsPartitionListing sharedHmsPartitions(ConnectorSession session,
            HudiTableHandle handle, Supplier<HudiConnectorMetadata.HmsPartitionListing> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, HMS_PARTITIONS_NAMESPACE, handle.getDbName(), physicalTableKey(handle), loader);
    }

    private static String physicalTableKey(HudiTableHandle handle) {
        String basePath = handle.getBasePath();
        return basePath == null || basePath.isEmpty()
                ? handle.getTableName()
                : handle.getTableName() + "@" + basePath;
    }
}
