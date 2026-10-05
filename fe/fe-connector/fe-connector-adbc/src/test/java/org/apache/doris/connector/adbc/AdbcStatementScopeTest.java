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

package org.apache.doris.connector.adbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards this connector's statement-scope namespace prefix.
 *
 * <p>Statement-scoped values are keyed by (catalog, db, table, queryId) plus a connector-owned namespace
 * string. Inside a heterogeneous gateway two connectors serve tables in one statement, so a namespace that
 * did not start with the connector's own type name could collide with a sibling's -- and the failure would
 * be one connector reading the other's memoized object, i.e. a ClassCastException or, worse, plausible
 * metadata from the wrong source. The type prefix makes the namespaces distinct by construction.
 */
class AdbcStatementScopeTest {

    @Test
    void namespaceIsPrefixedWithTheConnectorType() {
        String type = new AdbcConnectorProvider().getType();

        Assertions.assertTrue(AdbcStatementScope.TABLE_SCHEMA_NAMESPACE.startsWith(type + "."),
                "Namespace '" + AdbcStatementScope.TABLE_SCHEMA_NAMESPACE
                        + "' must start with the connector type '" + type + ".'");
    }

    @Test
    void nullSessionRunsTheLoaderEveryTime() {
        // Offline callers (no live statement) must still get an answer, and it must be freshly loaded:
        // silently memoizing outside a statement would leak one statement's view into the next.
        int[] loads = {0};
        for (int i = 0; i < 3; i++) {
            AdbcStatementScope.sharedTableSchema(null,
                    new AdbcTableHandle(new AdbcNamespace("main", ""), "t1"),
                    () -> {
                        loads[0]++;
                        return null;
                    });
        }
        Assertions.assertEquals(3, loads[0]);
    }
}
