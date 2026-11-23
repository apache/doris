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

package org.apache.doris.connector.lance;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class LanceStatementScopeTest {

    /**
     * Statement-scoped values are keyed by (catalog, db, table, queryId) plus a connector-owned namespace.
     * Inside a heterogeneous gateway two connectors serve tables in one statement, so a namespace not
     * starting with the connector's type name could hand one connector the other's memoized object.
     */
    @Test
    void namespaceIsPrefixedWithTheConnectorType() {
        String type = new LanceConnectorProvider().getType();
        for (String namespace : Arrays.asList(LanceStatementScope.TABLE_METADATA_NAMESPACE,
                LanceStatementScope.INDEX_METADATA_NAMESPACE)) {
            Assertions.assertTrue(namespace.startsWith(type + "."),
                    "Namespace '" + namespace + "' must start with the connector type '" + type + ".'");
        }
    }

    /** The query-begin pin reads the newest version; the pinned reads after it must not read it again. */
    @Test
    void newestVersionReadServesThePinnedReadsOfTheStatement() {
        LanceTestSession session = new LanceTestSession();
        LanceTableHandle handle = new LanceTableHandle("default", "t");
        LanceTableMetadata latest = metadata(5);
        int[] reads = {0};
        Assertions.assertSame(latest, LanceStatementScope.sharedMetadata(session, handle, () -> {
            reads[0]++;
            return latest;
        }));
        Assertions.assertSame(latest, LanceStatementScope.sharedMetadata(session, handle.withVersion(5),
                () -> Assertions.fail("version 5 was already read by this statement")));
        Assertions.assertSame(latest, LanceStatementScope.sharedMetadata(session, handle,
                () -> Assertions.fail("the newest version was already read by this statement")));
        Assertions.assertEquals(1, reads[0]);
        // Another version is another read.
        LanceTableMetadata older = metadata(4);
        Assertions.assertSame(older, LanceStatementScope.sharedMetadata(session, handle.withVersion(4), () -> {
            reads[0]++;
            return older;
        }));
        Assertions.assertEquals(2, reads[0]);
    }

    @Test
    void nullSessionRunsTheLoaderEveryTime() {
        int[] loads = {0};
        for (int i = 0; i < 3; i++) {
            LanceStatementScope.sharedMetadata(null, new LanceTableHandle("default", "t"), () -> {
                loads[0]++;
                return metadata(1);
            });
        }
        Assertions.assertEquals(3, loads[0]);
    }

    private static LanceTableMetadata metadata(long version) {
        return new LanceTableMetadata(new LanceTableAccess("/tmp/t.lance", Collections.emptyMap()), version,
                new Schema(Collections.emptyList()), Collections.emptyList());
    }
}
