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

import java.util.Collections;
import java.util.List;

/**
 * Views exposed by a connector.
 *
 * <p><b>The whole domain is optional</b> — a connector without views implements nothing here, and the engine
 * never enters this domain unless the connector declares {@link ConnectorCapability#SUPPORTS_VIEW} (it checks
 * the capability before merging view names into {@code SHOW TABLES}).</p>
 *
 * <p>Minimum implementation set, once {@code SUPPORTS_VIEW} is declared:</p>
 * <ul>
 * <li>{@link #viewExists} and {@link #getViewDefinition} — required; the latter's default throws.</li>
 * <li>{@link #listViewNames} — required only when {@link ConnectorTableMetadataOps#listTableNames} does NOT
 *     already include views. A metastore listing that returns views alongside tables needs nothing here; a
 *     catalog that keeps views in a separate namespace does.</li>
 * <li>{@link #dropView} — only for {@code DROP VIEW} support.</li>
 * </ul>
 */
public interface ConnectorViewOps {

    /**
     * Returns whether the named view exists in the given database. Connectors that expose views
     * (declaring {@link ConnectorCapability#SUPPORTS_VIEW}) override this; the default {@code false}
     * keeps view-less connectors reporting every object as a non-view.
     */
    @ConnectorMustImplement(when = "the connector declares SUPPORTS_VIEW")
    default boolean viewExists(ConnectorSession session, String dbName, String viewName) {
        return false;
    }

    /**
     * Lists all view names within the given database. Connectors that subtract views from
     * {@link ConnectorTableMetadataOps#listTableNames} (e.g. iceberg) expose them here so the catalog can
     * merge them back into {@code SHOW TABLES}; the default is empty (no view support).
     */
    @ConnectorMustImplement(when = "listTableNames does not already include views")
    default List<String> listViewNames(ConnectorSession session, String dbName) {
        return Collections.emptyList();
    }

    /**
     * Loads the {@link ConnectorViewDefinition stored SQL definition + dialect} of the named view. Connectors
     * that expose views (declaring {@link ConnectorCapability#SUPPORTS_VIEW}) override this; callers gate on
     * {@code SUPPORTS_VIEW} and {@code isView()} so the default — for view-less connectors — fails loud.
     *
     * @throws DorisConnectorException if the connector does not support views
     */
    @ConnectorMustImplement(when = "the connector declares SUPPORTS_VIEW")
    default ConnectorViewDefinition getViewDefinition(ConnectorSession session, String dbName, String viewName) {
        throw new DorisConnectorException("GET VIEW DEFINITION not supported");
    }

    /**
     * Drops the named view. Connectors that expose views (declaring {@link ConnectorCapability#SUPPORTS_VIEW})
     * override this; callers route a DROP through {@link #viewExists} so the default — for view-less
     * connectors — is unreachable and fails loud as a guard.
     *
     * @throws DorisConnectorException if the connector does not support views
     */
    @ConnectorMustImplement(when = "the connector supports DROP VIEW")
    default void dropView(ConnectorSession session, String dbName, String viewName) {
        throw new DorisConnectorException("DROP VIEW not supported");
    }
}
