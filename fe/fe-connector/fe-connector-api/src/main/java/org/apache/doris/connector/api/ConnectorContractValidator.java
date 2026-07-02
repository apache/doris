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
 * Fails loud at connector registration if a connector's declared write capabilities are internally
 * inconsistent. The invariants are structural (no table handle needed) and mirror the doc contracts the
 * removed {@code ConnectorCapability} javadoc stated only in prose.
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
    }
}
