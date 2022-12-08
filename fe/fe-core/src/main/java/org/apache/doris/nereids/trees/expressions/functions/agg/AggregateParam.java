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

import org.apache.doris.nereids.trees.plans.AggPhase;

import com.google.common.base.Preconditions;

import java.util.Objects;

/** AggregateParam. */
public class AggregateParam {
    public final boolean isFinalPhase;

    public final AggPhase aggPhase;

    public final boolean isDistinct;

    public final boolean isDisassembled;

    /** AggregateParam */
    public AggregateParam(boolean isDistinct, boolean isFinalPhase, AggPhase aggPhase, boolean isDisassembled) {
        this.isFinalPhase = isFinalPhase;
        this.isDistinct = isDistinct;
        this.aggPhase = aggPhase;
        this.isDisassembled = isDisassembled;
        if (!isFinalPhase) {
            Preconditions.checkArgument(isDisassembled,
                    "non-final phase aggregate should be disassembed");
        }
    }

    public static AggregateParam finalPhase() {
        return new AggregateParam(false, true, AggPhase.LOCAL, false);
    }

    public static AggregateParam distinctAndFinalPhase() {
        return new AggregateParam(true, true, AggPhase.LOCAL, false);
    }

    public AggregateParam withDistinct(boolean isDistinct) {
        return new AggregateParam(isDistinct, isFinalPhase, aggPhase, isDisassembled);
    }

    public AggregateParam withAggPhase(AggPhase aggPhase) {
        return new AggregateParam(isDistinct, isFinalPhase, aggPhase, isDisassembled);
    }

    public AggregateParam withDisassembled(boolean isDisassembled) {
        return new AggregateParam(isDistinct, isFinalPhase, aggPhase, isDisassembled);
    }

    public AggregateParam withPhaseAndDisassembled(boolean isFinalPhase, AggPhase aggPhase,
                                                      boolean isDisassembled) {
        return new AggregateParam(isDistinct, isFinalPhase, aggPhase, isDisassembled);
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
        return isDistinct == that.isDistinct
                && isFinalPhase == that.isFinalPhase
                && Objects.equals(aggPhase, that.aggPhase)
                && Objects.equals(isDisassembled, that.isDisassembled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isDistinct, isFinalPhase, aggPhase, isDisassembled);
    }
}
