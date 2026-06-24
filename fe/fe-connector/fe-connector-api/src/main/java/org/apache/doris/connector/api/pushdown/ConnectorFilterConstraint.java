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

package org.apache.doris.connector.api.pushdown;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Constraint information passed to {@code applyFilter}.
 *
 * <p>Carries two complementary representations of the filter:</p>
 * <ul>
 *   <li>{@link #getExpression()} — the full expression tree
 *       (for connectors that can inspect arbitrary predicates)</li>
 *   <li>{@link #getColumnDomains()} — per-column domain summaries
 *       (for connectors that only need range/equality checks,
 *       e.g. partition pruning)</li>
 * </ul>
 */
public final class ConnectorFilterConstraint implements Serializable {

    private static final long serialVersionUID = 2L;

    private final ConnectorExpression expression;
    private final Map<String, ConnectorDomain> columnDomains;

    public ConnectorFilterConstraint(ConnectorExpression expression,
            Map<String, ConnectorDomain> columnDomains) {
        this.expression = Objects.requireNonNull(expression, "expression");
        this.columnDomains = columnDomains == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(columnDomains);
    }

    /** Creates a constraint from an expression only (no domain summary). */
    public ConnectorFilterConstraint(ConnectorExpression expression) {
        this(expression, Collections.emptyMap());
    }

    /** The full filter expression tree. */
    public ConnectorExpression getExpression() {
        return expression;
    }

    /**
     * Per-column domain summaries extracted from the expression.
     * Key is the column name. Useful for quick partition pruning
     * without walking the expression tree.
     */
    public Map<String, ConnectorDomain> getColumnDomains() {
        return columnDomains;
    }
}
