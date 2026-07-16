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

import org.apache.doris.connector.api.handle.WriteOperation;

import java.util.Set;

/**
 * Fails loud ({@link IllegalStateException}) if a connector's declared write capabilities are internally
 * inconsistent. The invariants are purely structural (no table handle, no live catalog needed) and mirror
 * the doc contracts the removed {@code ConnectorCapability} javadoc stated only in prose.
 *
 * <p>Because the invariants are static properties of a connector's own capability declarations, they are
 * enforced by the per-connector contract tests (which build each connector and call {@link #validate}),
 * not at catalog registration: reading a connector's write capabilities constructs its write plan provider,
 * which for some connectors (e.g. iceberg) eagerly builds the live remote catalog — too costly and
 * outage-fragile to run on the FE metadata-replay / CREATE CATALOG path. This class stays available to any
 * caller that already holds an eagerly-built connector and wants the same check.</p>
 */
public final class ConnectorContractValidator {

    private ConnectorContractValidator() {}

    /** @throws IllegalStateException if any write-capability invariant is violated. */
    public static void validate(Connector connector, String catalogType) {
        Set<WriteOperation> ops = connector.supportedWriteOperations();
        // #2 branch-write implies plain INSERT is supported (branch is an INSERT modifier).
        if (connector.supportsWriteBranch() && !ops.contains(WriteOperation.INSERT)) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares supportsWriteBranch but its supportedOperations lacks INSERT");
        }
        // #3 partition-local-sort implies parallel write AND full-schema write order.
        if (connector.requiresPartitionLocalSort()
                && !(connector.requiresParallelWrite() && connector.requiresFullSchemaWriteOrder())) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares requiresPartitionLocalSort without requiresParallelWrite"
                    + " AND requiresFullSchemaWriteOrder");
        }
        // #4 partition-hash-write (hash without sort) likewise implies parallel write AND full-schema write
        // order (the sink indexes partition columns by full-schema position and distributes in parallel).
        if (connector.requiresPartitionHashWrite()
                && !(connector.requiresParallelWrite() && connector.requiresFullSchemaWriteOrder())) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares requiresPartitionHashWrite without requiresParallelWrite"
                    + " AND requiresFullSchemaWriteOrder");
        }
        // #5 the two hash arms are mutually exclusive: the engine checks local-sort first, so declaring both
        // would silently ignore the hash-without-sort request. Fail loud instead.
        if (connector.requiresPartitionLocalSort() && connector.requiresPartitionHashWrite()) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares both requiresPartitionLocalSort and requiresPartitionHashWrite;"
                    + " a connector must pick at most one partition-distribution arm");
        }
    }
}
