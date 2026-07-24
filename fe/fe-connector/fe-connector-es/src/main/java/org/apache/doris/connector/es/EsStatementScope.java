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

package org.apache.doris.connector.es;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScopes;

import java.util.function.Supplier;

/**
 * Per-statement scope helper for the ES connector: shares one index's raw mapping JSON across the
 * schema path ({@link EsConnectorMetadata#getTableSchema}) and the scan path
 * ({@link EsMetadataFetcher}) within a single statement. Both paths independently fetched the same
 * {@code getMapping} remotely and derived different products from it (columns vs field-context);
 * routing both through the shared statement scope collapses those to one remote fetch per index per
 * statement while each path keeps its own derivation.
 *
 * <p>Only the raw mapping — stable within a statement — is shared this way. Shard routing and node
 * topology are freshness-sensitive (ES rebalances) and must stay per-scan; they are NOT shared here.
 *
 * <p>Under a {@code null} session or {@link org.apache.doris.connector.api.ConnectorStatementScope#NONE}
 * (offline / no live statement) the loader runs on every call — byte-identical to fetching every time.
 */
final class EsStatementScope {

    private EsStatementScope() {
    }

    static String sharedIndexMapping(ConnectorSession session, String indexName,
            Supplier<String> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, ConnectorStatementScopes.ES_INDEX_MAPPING,
                EsConnectorMetadata.DEFAULT_DB, indexName, loader);
    }
}
