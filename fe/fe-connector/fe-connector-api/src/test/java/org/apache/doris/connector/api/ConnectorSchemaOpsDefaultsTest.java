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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the default behavior of {@link ConnectorSchemaOps#getDatabase} on a connector that does NOT
 * override it.
 *
 * <p><b>WHY this matters:</b> the default was softened from throwing to returning empty metadata so
 * SHOW CREATE DATABASE renders a bare {@code CREATE DATABASE} (no LOCATION) for connectors without
 * database-level metadata (paimon/jdbc/es), matching their pre-flip generic-else behavior — rather than
 * failing the command. The single fe-core caller ({@code PluginDrivenExternalDatabase.getLocation})
 * tolerates the empty map via {@code getOrDefault}.</p>
 */
public class ConnectorSchemaOpsDefaultsTest {

    /** A bare metadata implementing only the one abstract SPI method; exercises the schema-ops defaults. */
    private static final class BareMetadata implements ConnectorMetadata {
        @Override
        public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
            return null; // not exercised by this test
        }
    }

    @Test
    public void getDatabaseDefaultsToEmptyMetadataInsteadOfThrowing() {
        ConnectorMetadata metadata = new BareMetadata();

        // MUTATION: reverting the default to `throw` -> SHOW CREATE DATABASE on every non-overriding plugin
        // connector (paimon/jdbc/es) fails instead of rendering a bare CREATE DATABASE -> red.
        ConnectorDatabaseMetadata db = metadata.getDatabase(null, "db1");
        Assertions.assertNotNull(db, "the default getDatabase must return metadata, not null and not throw");
        Assertions.assertEquals("db1", db.getName(), "the default echoes the requested db name");
        Assertions.assertTrue(db.getProperties().isEmpty(),
                "the default carries no properties -> SHOW CREATE DATABASE renders no LOCATION");
    }
}
