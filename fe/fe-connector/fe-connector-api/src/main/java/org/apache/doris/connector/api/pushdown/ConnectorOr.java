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
import java.util.stream.Collectors;

/**
 * Logical OR of two or more disjuncts.
 */
public final class ConnectorOr implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    private final List<ConnectorExpression> disjuncts;

    public ConnectorOr(List<ConnectorExpression> disjuncts) {
        Objects.requireNonNull(disjuncts, "disjuncts");
        this.disjuncts = Collections.unmodifiableList(disjuncts);
    }

    public List<ConnectorExpression> getDisjuncts() {
        return disjuncts;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        return disjuncts;
    }

    @Override
    public String toString() {
        return "(" + disjuncts.stream()
                .map(Object::toString)
                .collect(Collectors.joining(" OR ")) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorOr)) {
            return false;
        }
        return disjuncts.equals(((ConnectorOr) o).disjuncts);
    }

    @Override
    public int hashCode() {
        return disjuncts.hashCode();
    }
}
