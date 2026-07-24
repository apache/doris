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
import java.util.Objects;

/**
 * Constraint information passed to {@code applyFilter}.
 *
 * <p>Carries the full filter expression tree via {@link #getExpression()},
 * which connectors inspect to push down predicates.</p>
 */
public final class ConnectorFilterConstraint implements Serializable {

    private static final long serialVersionUID = 2L;

    private final ConnectorExpression expression;

    /** Creates a constraint from a filter expression. */
    public ConnectorFilterConstraint(ConnectorExpression expression) {
        this.expression = Objects.requireNonNull(expression, "expression");
    }

    /** The full filter expression tree. */
    public ConnectorExpression getExpression() {
        return expression;
    }
}
