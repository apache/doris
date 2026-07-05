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

import java.util.Collections;

/**
 * Pins the default behavior of the two B0 view SPI seams on a connector that does NOT override them.
 *
 * <p><b>WHY this matters:</b> these defaults are the zero-behavior-change contract for the other
 * connectors. {@code viewExists} must default to {@code false} and {@code listViewNames} to an empty list,
 * which is precisely what guarantees a view-less connector (jdbc / es / paimon / maxcompute, none of which
 * declare {@code SUPPORTS_VIEW}) reports every object as a non-view and issues no view round-trips. A
 * default of {@code true} / a non-empty list would make those connectors mis-classify tables as views and
 * leak phantom view names into {@code SHOW TABLES}.</p>
 */
public class ConnectorViewDefaultsTest {

    /**
     * Minimal metadata that overrides ONLY the abstract {@code getTableSchema}, leaving viewExists /
     * listViewNames at their interface defaults — the seams under test.
     */
    private static final class NoViewMetadata implements ConnectorMetadata {
        @Override
        public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
            return new ConnectorTableSchema("t", Collections.emptyList(), null, Collections.emptyMap());
        }
    }

    @Test
    public void viewExistsDefaultsToFalse() {
        // MUTATION: a default returning true would make every connector report its tables as views
        // (isView()==true on the flipped plugin table) -> scanned as views / rejected for INSERT -> red.
        Assertions.assertFalse(new NoViewMetadata().viewExists(null, "db1", "v1"),
                "a connector without view support must report no view by default");
    }

    @Test
    public void listViewNamesDefaultsToEmpty() {
        // MUTATION: a non-empty default would leak phantom view names into the catalog's SHOW TABLES
        // merge for connectors that have no views -> red.
        Assertions.assertTrue(new NoViewMetadata().listViewNames(null, "db1").isEmpty(),
                "a connector without view support must list no views by default");
    }

    @Test
    public void getViewDefinitionDefaultsToFailLoud() {
        // WHY: callers gate on SUPPORTS_VIEW + isView() before asking for a view body; a view-less connector
        // must never silently return a definition. MUTATION: a default returning null / an empty definition
        // would let a non-view connector pretend to have a view body -> red.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> new NoViewMetadata().getViewDefinition(null, "db1", "v1"),
                "a connector without view support must fail loud when asked for a view definition");
    }

    @Test
    public void dropViewDefaultsToFailLoud() {
        // WHY: PluginDrivenExternalCatalog.dropTable routes a DROP to dropView only after viewExists() is
        // true, so for a view-less connector (viewExists defaults to false) this default is unreachable in
        // production; it is a fail-loud guard. MUTATION: a default that silently no-ops would let a refactor
        // that bypasses the viewExists gate drop nothing without surfacing the unsupported operation -> red.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> new NoViewMetadata().dropView(null, "db1", "v1"),
                "a connector without view support must fail loud when asked to drop a view");
    }
}
