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

package org.apache.doris.nereids.trees.plans;

import java.util.Objects;
import java.util.Optional;

/**
 * PlanNode with plan hash.
 */
public class PlanNodeAndHash {
    private final AbstractPlan planNode;

    private final Optional<String> hash;

    public PlanNodeAndHash(AbstractPlan planNode, Optional<String> hash) {
        this.planNode = Objects.requireNonNull(planNode, "planNode is null");
        this.hash = Objects.requireNonNull(hash, "hash is null");
    }

    public AbstractPlan getPlanNode() {
        return planNode;
    }

    public Optional<String> getHash() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanNodeAndHash other = (PlanNodeAndHash) o;
        return planNode == other.planNode && Objects.equals(hash, other.hash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(System.identityHashCode(planNode), hash);
    }

    @Override
    public String toString() {
        return String.format("plan: %s, hash: %s", planNode, hash);
    }
}
