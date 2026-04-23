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

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.api.pushdown.LimitApplicationResult;
import org.apache.doris.connector.api.pushdown.ProjectionApplicationResult;

import java.util.List;
import java.util.Optional;

/**
 * Pushdown operations that a connector may implement.
 * All methods return {@link Optional#empty()} by default,
 * indicating the pushdown is not supported.
 */
public interface ConnectorPushdownOps {

    /** Attempts to push a filter into the table scan. */
    default Optional<FilterApplicationResult<ConnectorTableHandle>>
            applyFilter(ConnectorSession session,
                    ConnectorTableHandle handle,
                    ConnectorFilterConstraint constraint) {
        return Optional.empty();
    }

    /** Attempts to push a projection into the table scan. */
    default Optional<ProjectionApplicationResult<ConnectorTableHandle>>
            applyProjection(ConnectorSession session,
                    ConnectorTableHandle handle,
                    List<ConnectorColumnHandle> projections) {
        return Optional.empty();
    }

    /** Attempts to push a limit into the table scan. */
    default Optional<LimitApplicationResult<ConnectorTableHandle>>
            applyLimit(ConnectorSession session,
                    ConnectorTableHandle handle, long limit) {
        return Optional.empty();
    }

    /**
     * Returns whether this connector supports pushing down predicates that contain
     * implicit CAST expressions.
     *
     * <p>When this returns {@code false}, the engine will strip any conjuncts
     * containing CAST expressions from the filter before passing it to the connector.
     * The default is {@code true} (CASTs are pushed down).</p>
     *
     * <p>Connectors that delegate filtering to remote systems with different type
     * coercion rules (e.g., JDBC databases) may override this to disable CAST
     * pushdown when the session configuration indicates it is unsafe.</p>
     */
    default boolean supportsCastPredicatePushdown(ConnectorSession session) {
        return true;
    }
}
