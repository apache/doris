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

import com.google.common.base.Preconditions;

import java.util.Objects;

/** AggregateParam. */
public class AggregateParam {
    public final boolean isGlobal;

    public final boolean isDistinct;

    public final boolean isDisassembled;

    /** AggregateParam */
    public AggregateParam(boolean isDistinct, boolean isGlobal, boolean isDisassembled) {
        this.isDistinct = isDistinct;
        this.isGlobal = isGlobal;
        this.isDisassembled = isDisassembled;
        if (!isGlobal) {
            Preconditions.checkArgument(isDisassembled == true,
                    "local aggregate should be disassembed");
        }
    }

    public static AggregateParam global() {
        return new AggregateParam(false, true, false);
    }

    public static AggregateParam distinctAndGlobal() {
        return new AggregateParam(true, true, false);
    }

    public AggregateParam withDistinct(boolean isDistinct) {
        return new AggregateParam(isDistinct, isGlobal, isDisassembled);
    }

    public AggregateParam withGlobal(boolean isGlobal) {
        return new AggregateParam(isDistinct, isGlobal, isDisassembled);
    }

    public AggregateParam withDisassembled(boolean isDisassembled) {
        return new AggregateParam(isDistinct, isGlobal, isDisassembled);
    }

    public AggregateParam withGlobalAndDisassembled(boolean isGlobal, boolean isDisassembled) {
        return new AggregateParam(isDistinct, isGlobal, isDisassembled);
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
                && Objects.equals(isGlobal, that.isGlobal)
                && Objects.equals(isDisassembled, that.isDisassembled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isDistinct, isGlobal, isDisassembled);
    }
}
