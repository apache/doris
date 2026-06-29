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

import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Structured dry-run result for EXPLAIN REFRESH.
 */
public class IvmRefreshExplainResult {
    private final Plan normalizedPlan;
    private final Plan mergedDeltaPlan;

    public IvmRefreshExplainResult(Plan normalizedPlan, Plan mergedDeltaPlan) {
        this.normalizedPlan = Objects.requireNonNull(normalizedPlan, "normalizedPlan can not be null");
        this.mergedDeltaPlan = Objects.requireNonNull(mergedDeltaPlan, "mergedDeltaPlan can not be null");
    }

    public Plan getNormalizedPlan() {
        return normalizedPlan;
    }

    public Plan getMergedDeltaPlan() {
        return mergedDeltaPlan;
    }

    public List<List<String>> formatOverviewRows() {
        List<List<String>> rows = new ArrayList<>();
        rows.add(row("IVM_NORMALIZED_PLAN", "", normalizedPlan.treeString()));
        rows.add(row("IVM_DELTA_PLAN", "", mergedDeltaPlan.treeString()));
        return rows;
    }

    public static List<List<String>> formatDeltaPlanRows(Plan deltaPlan) {
        return ImmutableList.of(ImmutableList.of(deltaPlan.treeString()));
    }

    private static List<String> row(String item, String delta, String value) {
        return ImmutableList.of(item, delta, value);
    }
}
