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

package org.apache.doris.mtmv.ivm.agg;

import java.util.Objects;

/**
 * Stable key for looking up one aggregate target's delta slot during apply.
 *
 * <p>The delta top project is still a plan, so apply expressions should not depend on generated column names after
 * slot resolution. This key identifies slots by aggregate ordinal plus a logical processor-owned slot name.
 */
public class IvmAggDeltaSlotRef {
    private final int targetOrdinal;
    private final String slotName;

    public IvmAggDeltaSlotRef(int targetOrdinal, IvmAggFunctionKind slotKind) {
        this(targetOrdinal, slotKind.name());
    }

    public IvmAggDeltaSlotRef(int targetOrdinal, String slotName) {
        this.targetOrdinal = targetOrdinal;
        this.slotName = Objects.requireNonNull(slotName);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof IvmAggDeltaSlotRef)) {
            return false;
        }
        IvmAggDeltaSlotRef that = (IvmAggDeltaSlotRef) other;
        return targetOrdinal == that.targetOrdinal && slotName.equals(that.slotName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetOrdinal, slotName);
    }

    @Override
    public String toString() {
        return targetOrdinal + ":" + slotName;
    }
}
