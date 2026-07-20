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

package org.apache.doris.datasource.plugin;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;

import java.util.Objects;

/**
 * The single engine-side funnel through which every metadata acquisition in a statement flows, so that a
 * statement uses exactly ONE {@link ConnectorMetadata} instance per catalog: it memoizes the result of
 * {@code connector.getMetadata(session)} on the session's per-statement {@link ConnectorStatementScope} and hands
 * every read / scan / DDL / MVCC resolver the same instance, closed deterministically at statement end.
 *
 * <p>Under {@link ConnectorStatementScope#NONE} (offline / no live statement) the scope memoizes nothing, so the
 * factory runs on every call — {@code connector.getMetadata(session)} per call, byte-identical to today's behavior.
 * The {@link ConnectorSession} is still built and passed to metadata operations as before; only the
 * {@code getMetadata} result is memoized here, never the session.</p>
 *
 * <p>This class is the ONLY place in fe-core allowed to call {@code Connector#getMetadata} directly (an arch gate
 * enforces that every other seam routes through here). A heterogeneous gateway connector that fans a foreign
 * handle out to a sibling connector keys the memo by the owning connector as well as the catalog id; that
 * sibling-aware entry is added when the gateway is wired.</p>
 */
public final class PluginDrivenMetadata {

    private PluginDrivenMetadata() {
    }

    /**
     * Returns the statement's single memoized {@link ConnectorMetadata} for {@code connector}, building it via
     * {@code connector.getMetadata(session)} on first use and reusing it for the rest of the statement. Keyed by
     * the session's catalog id, so a cross-catalog statement resolves each catalog independently.
     */
    public static ConnectorMetadata get(ConnectorSession session, Connector connector) {
        ConnectorStatementScope scope = session.getStatementScope();
        long catalogId = session.getCatalogId();
        // A statement resolves a catalog under exactly one identity (one statement = one user = one credential).
        // Now that the write arm reuses the read arm's memoized instance, and a session=user connector bakes the
        // querying user's delegated credential into that instance at getMetadata time, reusing it under a second
        // identity would execute one user's operation with another's credentials. Pin the building identity and
        // fail loud if a different one ever reuses the instance, turning a silent cross-user leak into a hard
        // error. Uses the Doris principal (getUser), never a credential token, so fe-core parses no credentials.
        // Under NONE this stores nothing, so the check is vacuously true and the factory runs on every call.
        String user = session.getUser();
        String builderUser = scope.computeIfAbsent("metadata-identity:" + catalogId, () -> user);
        if (!Objects.equals(builderUser, user)) {
            throw new IllegalStateException("Per-statement metadata identity mismatch for catalog " + catalogId
                    + ": the instance was built for user '" + builderUser + "' but is being reused for user '"
                    + user + "'. A statement must resolve a catalog under a single identity.");
        }
        return scope.getOrCreateMetadata("metadata:" + catalogId, () -> connector.getMetadata(session));
    }
}
