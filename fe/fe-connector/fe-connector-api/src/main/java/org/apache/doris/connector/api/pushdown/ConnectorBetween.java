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
 * A BETWEEN predicate: {@code value BETWEEN lower AND upper}.
 */
public final class ConnectorBetween implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    private final ConnectorExpression value;
    private final ConnectorExpression lower;
    private final ConnectorExpression upper;

    public ConnectorBetween(ConnectorExpression value,
            ConnectorExpression lower, ConnectorExpression upper) {
        this.value = Objects.requireNonNull(value, "value");
        this.lower = Objects.requireNonNull(lower, "lower");
        this.upper = Objects.requireNonNull(upper, "upper");
    }

    public ConnectorExpression getValue() {
        return value;
    }

    public ConnectorExpression getLower() {
        return lower;
    }

    public ConnectorExpression getUpper() {
        return upper;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        return Arrays.asList(value, lower, upper);
    }

    @Override
    public String toString() {
        return value + " BETWEEN " + lower + " AND " + upper;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorBetween)) {
            return false;
        }
        ConnectorBetween that = (ConnectorBetween) o;
        return value.equals(that.value)
                && lower.equals(that.lower) && upper.equals(that.upper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, lower, upper);
    }
}
