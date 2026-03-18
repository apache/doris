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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.Objects;

/**
 * One delta write plan for a single changed base table.
 * Produced by a per-pattern IVM Nereids rule and consumed by IVMDeltaExecutor.
 */
public class DeltaPlanBundle {
    // the base table whose changes this bundle handles
    private final BaseTableInfo baseTableInfo;
    // the logical delta write plan (INSERT / DELETE / MERGE INTO)
    private final LogicalPlan deltaWritePlan;

    public DeltaPlanBundle(BaseTableInfo baseTableInfo, LogicalPlan deltaWritePlan) {
        this.baseTableInfo = Objects.requireNonNull(baseTableInfo, "baseTableInfo can not be null");
        this.deltaWritePlan = Objects.requireNonNull(deltaWritePlan, "deltaWritePlan can not be null");
    }

    public BaseTableInfo getBaseTableInfo() {
        return baseTableInfo;
    }

    public LogicalPlan getDeltaWritePlan() {
        return deltaWritePlan;
    }

    @Override
    public String toString() {
        return "DeltaPlanBundle{"
                + "baseTableInfo=" + baseTableInfo
                + ", deltaWritePlan=" + deltaWritePlan.getClass().getSimpleName()
                + '}';
    }
}
