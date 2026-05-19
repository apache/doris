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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An IN predicate: {@code value IN (list)}.
 * If {@code negated} is true, represents {@code value NOT IN (list)}.
 */
public final class ConnectorIn implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    private final ConnectorExpression value;
    private final List<ConnectorExpression> inList;
    private final boolean negated;

    public ConnectorIn(ConnectorExpression value,
            List<ConnectorExpression> inList, boolean negated) {
        this.value = Objects.requireNonNull(value, "value");
        this.inList = Collections.unmodifiableList(
                new ArrayList<>(Objects.requireNonNull(inList, "inList")));
        this.negated = negated;
    }

    public ConnectorExpression getValue() {
        return value;
    }

    public List<ConnectorExpression> getInList() {
        return inList;
    }

    public boolean isNegated() {
        return negated;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        List<ConnectorExpression> children = new ArrayList<>(inList.size() + 1);
        children.add(value);
        children.addAll(inList);
        return Collections.unmodifiableList(children);
    }

    @Override
    public String toString() {
        return value + (negated ? " NOT IN " : " IN ") + inList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorIn)) {
            return false;
        }
        ConnectorIn that = (ConnectorIn) o;
        return negated == that.negated
                && value.equals(that.value) && inList.equals(that.inList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, inList, negated);
    }
}
