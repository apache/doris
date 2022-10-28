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

import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** AggregateParam. */
public class AggregateParam {
    public final boolean isGlobal;

    public final boolean isDistinct;

    // When AggregateDisassemble rule disassemble the aggregate function, say double avg(int), the local
    // aggregate keep the origin signature, but the global aggregate change to double avg(double).
    // This behavior is difference from the legacy optimizer, because legacy optimizer keep the same signature
    // between local aggregate and global aggregate. If the signatures are different, the result would wrong.
    // So we use this field to record the originInputTypes, and find the catalog function by the origin input types.
    public final Optional<List<DataType>> inputTypesBeforeDissemble;

    public AggregateParam() {
        this(false, true, Optional.empty());
    }

    public AggregateParam(boolean distinct) {
        this(distinct, true, Optional.empty());
    }

    public AggregateParam(boolean isDistinct, boolean isGlobal) {
        this(isDistinct, isGlobal, Optional.empty());
    }

    public AggregateParam(boolean isDistinct, boolean isGlobal, Optional<List<DataType>> inputTypesBeforeDissemble) {
        this.isDistinct = isDistinct;
        this.isGlobal = isGlobal;
        this.inputTypesBeforeDissemble = Objects.requireNonNull(inputTypesBeforeDissemble,
                "inputTypesBeforeDissemble can not be null");
    }

    public static AggregateParam distinctAndGlobal() {
        return new AggregateParam(true, true, Optional.empty());
    }

    public static AggregateParam global() {
        return new AggregateParam(false, true, Optional.empty());
    }

    public AggregateParam withDistinct(boolean isDistinct) {
        return new AggregateParam(isDistinct, isGlobal, inputTypesBeforeDissemble);
    }

    public AggregateParam withGlobal(boolean isGlobal) {
        return new AggregateParam(isDistinct, isGlobal, inputTypesBeforeDissemble);
    }

    public AggregateParam withInputTypesBeforeDissemble(Optional<List<DataType>> inputTypesBeforeDissemble) {
        return new AggregateParam(isDistinct, isGlobal, inputTypesBeforeDissemble);
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
                && Objects.equals(inputTypesBeforeDissemble, that.inputTypesBeforeDissemble);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isDistinct, isGlobal, inputTypesBeforeDissemble);
    }
}
