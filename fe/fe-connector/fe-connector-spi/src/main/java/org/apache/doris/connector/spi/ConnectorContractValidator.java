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

import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import java.util.Set;

/**
 * Fails loud ({@link IllegalStateException}) if a connector's declared write capabilities are internally
 * inconsistent. The invariants are purely structural (no table handle, no live catalog needed) and mirror
 * the doc contracts the removed {@code ConnectorCapability} javadoc stated only in prose.
 *
 * <p>Because the invariants are static properties of a connector's own capability declarations, they are
 * meant to be enforced by per-connector contract tests (which build the connector and call {@link #validate}),
 * not at catalog registration: reading a connector's write capabilities constructs its write plan provider,
 * which for some connectors (e.g. iceberg) eagerly builds the live remote catalog — too costly and
 * outage-fragile to run on the FE metadata-replay / CREATE CATALOG path. This class stays available to any
 * caller that already holds an eagerly-built connector and wants the same check.</p>
 *
 * <p><b>Actual coverage today</b> (do not read the paragraph above as a statement that every connector is
 * checked): five connectors' tests call {@link #validate} — iceberg, elasticsearch, maxcompute, jdbc and
 * hive. Every invariant below now has a positive sample on a real connector: maxcompute for the local-sort
 * arm, hive for the hash arm and for their mutual exclusion. A connector added without such a test is simply
 * unchecked.</p>
 *
 * <p>Note also that this validator reads the CONNECTOR-LEVEL provider, while the engine's write path resolves
 * the provider per table ({@code Connector.getWritePlanProvider(ConnectorTableHandle)}). A heterogeneous
 * gateway connector can therefore be self-consistent here and still answer differently per table, which is by
 * design and out of this validator's scope.</p>
 */
public final class ConnectorContractValidator {

    private ConnectorContractValidator() {}

    /** @throws IllegalStateException if any write-capability invariant is violated. */
    public static void validate(Connector connector, String catalogType) {
        // Fetch the provider ONCE. Several connectors build a fresh provider per call and one of them
        // (iceberg) reaches the live remote catalog while doing so, so asking the connector separately for
        // each trait would pay that cost eight times over.
        ConnectorWritePlanProvider provider = connector.getWritePlanProvider();
        if (provider == null) {
            // No write support at all: every trait is vacuously false and no invariant can be violated.
            return;
        }
        Set<WriteOperation> ops = provider.supportedOperations();
        // #2 branch-write implies plain INSERT is supported (branch is an INSERT modifier).
        if (provider.supportsWriteBranch() && !ops.contains(WriteOperation.INSERT)) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares supportsWriteBranch but its supportedOperations lacks INSERT");
        }
        // #3 partition-local-sort implies parallel write AND full-schema write order.
        if (provider.requiresPartitionLocalSort()
                && !(provider.requiresParallelWrite() && provider.requiresFullSchemaWriteOrder())) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares requiresPartitionLocalSort without requiresParallelWrite"
                    + " AND requiresFullSchemaWriteOrder");
        }
        // #4 partition-hash-write (hash without sort) likewise implies parallel write AND full-schema write
        // order (the sink indexes partition columns by full-schema position and distributes in parallel).
        if (provider.requiresPartitionHashWrite()
                && !(provider.requiresParallelWrite() && provider.requiresFullSchemaWriteOrder())) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares requiresPartitionHashWrite without requiresParallelWrite"
                    + " AND requiresFullSchemaWriteOrder");
        }
        // #5 the two hash arms are mutually exclusive: the engine checks local-sort first, so declaring both
        // would silently ignore the hash-without-sort request. Fail loud instead.
        if (provider.requiresPartitionLocalSort() && provider.requiresPartitionHashWrite()) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares both requiresPartitionLocalSort and requiresPartitionHashWrite;"
                    + " a connector must pick at most one partition-distribution arm");
        }
    }
}
