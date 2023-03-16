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

import org.apache.doris.analysis.AggregateInfo;

/**
 * Represents different phase of agg and map it to the
 * enum of agg phase definition of stale optimizer.
 */
public enum AggPhase {
    LOCAL("LOCAL", AggregateInfo.AggPhase.FIRST),
    GLOBAL("GLOBAL", AggregateInfo.AggPhase.FIRST_MERGE),
    DISTINCT_LOCAL("DISTINCT_LOCAL", AggregateInfo.AggPhase.SECOND),
    DISTINCT_GLOBAL("DISTINCT_GLOBAL", AggregateInfo.AggPhase.SECOND_MERGE);

    private final String name;

    private final AggregateInfo.AggPhase execAggPhase;

    AggPhase(String name, AggregateInfo.AggPhase execAggPhase) {
        this.name = name;
        this.execAggPhase = execAggPhase;
    }

    public boolean isLocal() {
        return this == LOCAL;
    }

    public boolean isGlobal() {
        return this == GLOBAL || this == DISTINCT_GLOBAL;
    }

    public AggregateInfo.AggPhase toExec() {
        return this.execAggPhase;
    }

    @Override
    public String toString() {
        return name;
    }
}
