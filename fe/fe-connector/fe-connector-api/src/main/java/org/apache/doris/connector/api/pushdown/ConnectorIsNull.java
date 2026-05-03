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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An IS NULL / IS NOT NULL predicate.
 */
public final class ConnectorIsNull implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    private final ConnectorExpression operand;
    private final boolean negated;

    public ConnectorIsNull(ConnectorExpression operand, boolean negated) {
        this.operand = Objects.requireNonNull(operand, "operand");
        this.negated = negated;
    }

    public ConnectorExpression getOperand() {
        return operand;
    }

    /** True for IS NOT NULL, false for IS NULL. */
    public boolean isNegated() {
        return negated;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        return Collections.singletonList(operand);
    }

    @Override
    public String toString() {
        return operand + (negated ? " IS NOT NULL" : " IS NULL");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorIsNull)) {
            return false;
        }
        ConnectorIsNull that = (ConnectorIsNull) o;
        return negated == that.negated && operand.equals(that.operand);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operand, negated);
    }
}
