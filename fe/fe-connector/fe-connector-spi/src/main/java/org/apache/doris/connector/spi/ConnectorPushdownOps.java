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

import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.spi.pushdown.FilterApplicationResult;
import org.apache.doris.connector.spi.pushdown.ProjectionApplicationResult;

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

    /**
     * Returns whether this connector supports pushing down predicates that contain
     * implicit CAST expressions.
     *
     * <p><b>This switch governs ONE of the two pushdown paths.</b> Returning {@code false} makes the engine
     * drop CAST-containing conjuncts from the RESIDUAL predicate it builds for the scan node. The
     * {@link #applyFilter} path never consults it: there the engine converts every conjunct and hands the
     * result to the connector regardless of what this method answers.</p>
     *
     * <p><b>And on either path a CAST does not arrive as a CAST.</b> The forward converter unwraps it and
     * pushes the child expression, so {@code CAST(dt AS INT) = 20240101} reaches the connector as
     * {@code dt = 20240101}, with nothing marking it as coerced — a connector cannot detect the situation by
     * inspecting what it received. The risk is therefore carried by the connector: a connector that turns such
     * a comparison into remote filtering (e.g. metastore partition pruning from equality / IN predicates on
     * partition columns) will UNDER-return rows whenever the remote comparison semantics differ from Doris's
     * coercion, and BE cannot recover the difference because the data was never scanned.</p>
     *
     * <p>The default is {@code true} (CASTs are pushed down), which is an opt-OUT polarity and the one
     * exception to this module's "capabilities default to false" rule; it is kept for compatibility. Do not
     * copy the polarity into a new switch. A connector that delegates filtering to a remote system with
     * different type coercion rules (e.g. a JDBC database) overrides this to {@code false}, optionally driven
     * by session configuration.</p>
     *
     * <p><b>Every shipped connector answers this deliberately; none of them merely inherits.</b> jdbc, paimon
     * and maxcompute return {@code false}; iceberg, elasticsearch and the trino bridge state {@code true} at
     * their own metadata class, each recording what it does with the predicate and that {@code true} is an
     * accepted risk rather than a safety claim. hive and hudi declare nothing because for them the switch is
     * INERT — their scan planning ignores the residual filter entirely, and the predicate they do consume
     * arrives on the {@link #applyFilter} path, which is never gated on this method. A new connector should
     * make the same deliberate choice, starting from {@code false}.</p>
     */
    default boolean supportsCastPredicatePushdown(ConnectorSession session) {
        return true;
    }
}
