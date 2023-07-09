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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;

import java.util.Objects;

/** AggregateParam. */
public class AggregateParam {

    public static AggregateParam LOCAL_RESULT = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_RESULT);
    public static AggregateParam LOCAL_BUFFER = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);

    public final AggPhase aggPhase;
    public final AggMode aggMode;
    // TODO: this is a short-term plan to process count(distinct a, b) correctly
    public final boolean canBeBanned;

    /** AggregateParam */
    public AggregateParam(AggPhase aggPhase, AggMode aggMode) {
        this(aggPhase, aggMode, true);
    }

    public AggregateParam(AggPhase aggPhase, AggMode aggMode, boolean canBeBanned) {
        this.aggMode = Objects.requireNonNull(aggMode, "aggMode cannot be null");
        this.aggPhase = Objects.requireNonNull(aggPhase, "aggPhase cannot be null");
        this.canBeBanned = canBeBanned;
    }

    public AggregateParam withAggPhase(AggPhase aggPhase) {
        return new AggregateParam(aggPhase, aggMode, canBeBanned);
    }

    public AggregateParam withAggPhase(AggMode aggMode) {
        return new AggregateParam(aggPhase, aggMode, canBeBanned);
    }

    public AggregateParam withAppPhaseAndAppMode(AggPhase aggPhase, AggMode aggMode) {
        return new AggregateParam(aggPhase, aggMode, canBeBanned);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AggregateParam that = (AggregateParam) o;
        return Objects.equals(aggPhase, that.aggPhase)
                && Objects.equals(aggMode, that.aggMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggPhase, aggMode);
    }

    @Override
    public String toString() {
        return "AggregateParam{"
                + "aggPhase=" + aggPhase
                + ", aggMode=" + aggMode
                + '}';
    }
}
