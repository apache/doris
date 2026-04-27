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

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;

import java.util.Objects;

/**
 * Associates a column handle with an expression, used to describe
 * how a projected column is computed.
 */
public final class ConnectorColumnAssignment {

    private final ConnectorColumnHandle column;
    private final ConnectorExpression expression;

    public ConnectorColumnAssignment(ConnectorColumnHandle column,
            ConnectorExpression expression) {
        this.column = Objects.requireNonNull(column, "column");
        this.expression = Objects.requireNonNull(
                expression, "expression");
    }

    public ConnectorColumnHandle getColumn() {
        return column;
    }

    public ConnectorExpression getExpression() {
        return expression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorColumnAssignment)) {
            return false;
        }
        ConnectorColumnAssignment that = (ConnectorColumnAssignment) o;
        return column.equals(that.column)
                && expression.equals(that.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, expression);
    }

    @Override
    public String toString() {
        return "ConnectorColumnAssignment{column=" + column
                + ", expression=" + expression + "}";
    }
}
