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

/**
 * A neutral, engine-extracted boolean predicate handed to a connector for write-time conflict detection
 * (O5-2). It wraps a {@link ConnectorExpression} — the same engine-neutral expression representation used
 * by scan pushdown — so the connector can convert it to its own predicate dialect without depending on
 * fe-core / nereids types.
 *
 * <p>The engine builds it from the analyzed DML plan by keeping only the conjuncts over the <i>target
 * table's own columns</i> (slot origin-table == target, excluding synthetic {@code $row_id} / metadata /
 * join columns) and passes it via
 * {@link org.apache.doris.connector.api.handle.ConnectorTransaction#applyWriteConstraint(ConnectorPredicate)}.
 * It carries no plan view — only the neutral expression — so it does not breach the connector import gate.</p>
 */
public final class ConnectorPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ConnectorExpression expression;

    public ConnectorPredicate(ConnectorExpression expression) {
        this.expression = expression;
    }

    /** The engine-neutral boolean expression over the target table's own columns. May be {@code null}. */
    public ConnectorExpression getExpression() {
        return expression;
    }
}
