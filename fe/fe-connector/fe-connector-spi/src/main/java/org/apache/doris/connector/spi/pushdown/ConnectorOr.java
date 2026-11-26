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

package org.apache.doris.connector.spi.pushdown;

import java.util.ArrayList;
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

    /**
     * @param disjuncts two or more disjuncts; fewer is a caller bug, not a degenerate node to absorb
     *         silently. Consumers translate this node arm by arm, and an arm that never materializes
     *         narrows the pushed predicate - the failure mode is missing rows, not an error. Copied
     *         defensively so a caller mutating its own list afterwards cannot change this node,
     *         matching {@link ConnectorIn}.
     */
    public ConnectorOr(List<ConnectorExpression> disjuncts) {
        Objects.requireNonNull(disjuncts, "disjuncts");
        if (disjuncts.size() < 2) {
            throw new IllegalArgumentException(
                    "ConnectorOr requires at least two disjuncts, got " + disjuncts.size());
        }
        this.disjuncts = Collections.unmodifiableList(new ArrayList<>(disjuncts));
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
