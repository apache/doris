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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A LIKE/REGEXP predicate: {@code value LIKE pattern}.
 */
public final class ConnectorLike implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    /** LIKE vs REGEXP distinction. */
    public enum Operator {
        LIKE,
        REGEXP
    }

    private final Operator operator;
    private final ConnectorExpression value;
    private final ConnectorExpression pattern;

    public ConnectorLike(Operator operator,
            ConnectorExpression value, ConnectorExpression pattern) {
        this.operator = Objects.requireNonNull(operator, "operator");
        this.value = Objects.requireNonNull(value, "value");
        this.pattern = Objects.requireNonNull(pattern, "pattern");
    }

    public Operator getOperator() {
        return operator;
    }

    public ConnectorExpression getValue() {
        return value;
    }

    public ConnectorExpression getPattern() {
        return pattern;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        return Arrays.asList(value, pattern);
    }

    @Override
    public String toString() {
        return value + " " + operator + " " + pattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorLike)) {
            return false;
        }
        ConnectorLike that = (ConnectorLike) o;
        return operator == that.operator
                && value.equals(that.value) && pattern.equals(that.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, value, pattern);
    }
}
