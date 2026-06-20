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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

/**
 * Pins the default behavior of the two B5b time-travel SPI seams on a connector that does
 * NOT override them.
 *
 * <p><b>WHY this matters:</b> these defaults are the zero-behavior-change contract for the
 * other connectors. {@code resolveTimeTravel} must default to {@code empty()} (a connector
 * without time-travel resolves nothing, and the engine then surfaces a user error rather than
 * silently reading latest). The snapshot-aware {@code getTableSchema} overload must default to
 * delegating to the 2-arg latest variant &mdash; if it ignored the delegation a non-evolving
 * connector would return null/throw on time-travel reads.</p>
 */
public class ConnectorMetadataTimeTravelDefaultsTest {

    /** A no-method handle; the defaults under test never inspect it. */
    private static final ConnectorTableHandle HANDLE = new ConnectorTableHandle() {
    };

    /**
     * Minimal metadata that overrides ONLY the 2-arg latest {@code getTableSchema}, so the test
     * can prove the 3-arg snapshot-aware default routes back to it.
     */
    private static final class LatestOnlyMetadata implements ConnectorMetadata {
        static final ConnectorTableSchema LATEST =
                new ConnectorTableSchema("t", Collections.emptyList(), null, Collections.emptyMap());

        @Override
        public ConnectorTableSchema getTableSchema(ConnectorSession session,
                ConnectorTableHandle handle) {
            return LATEST;
        }
    }

    @Test
    public void resolveTimeTravelDefaultsToEmpty() {
        ConnectorMetadata metadata = new LatestOnlyMetadata();
        ConnectorTimeTravelSpec spec = ConnectorTimeTravelSpec.snapshotId("1");

        // MUTATION: a default that returned a fabricated snapshot would make a non-MVCC connector
        // silently honor FOR VERSION AS OF instead of erroring.
        Optional<ConnectorMvccSnapshot> resolved =
                metadata.resolveTimeTravel(null, HANDLE, spec);
        Assertions.assertFalse(resolved.isPresent(),
                "a connector without time-travel must resolve nothing by default");
    }

    @Test
    public void snapshotAwareGetTableSchemaDelegatesToLatest() {
        LatestOnlyMetadata metadata = new LatestOnlyMetadata();
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(9L)
                .schemaId(2L)
                .build();

        // MUTATION: a default that returned null (or threw) instead of delegating to the 2-arg
        // variant would break time-travel reads on any connector that does not override it.
        ConnectorTableSchema schema = metadata.getTableSchema(null, HANDLE, snapshot);
        Assertions.assertSame(LatestOnlyMetadata.LATEST, schema,
                "default snapshot-aware getTableSchema must return the latest schema unchanged");
    }
}
