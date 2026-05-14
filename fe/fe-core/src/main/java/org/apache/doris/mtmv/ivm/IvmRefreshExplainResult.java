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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Structured dry-run result for EXPLAIN REFRESH.
 */
public class IvmRefreshExplainResult {
    private final Plan normalizedPlan;
    private final List<IvmDeltaExplainBundle> deltaBundles;

    public IvmRefreshExplainResult(Plan normalizedPlan, List<IvmDeltaExplainBundle> deltaBundles) {
        this.normalizedPlan = Objects.requireNonNull(normalizedPlan, "normalizedPlan can not be null");
        this.deltaBundles = ImmutableList.copyOf(deltaBundles);
    }

    public Plan getNormalizedPlan() {
        return normalizedPlan;
    }

    public List<IvmDeltaExplainBundle> getDeltaBundles() {
        return deltaBundles;
    }

    public IvmDeltaExplainBundle getDeltaBundle(int deltaId) throws AnalysisException {
        for (IvmDeltaExplainBundle bundle : deltaBundles) {
            if (bundle.getDeltaId() == deltaId) {
                return bundle;
            }
        }
        if (deltaBundles.isEmpty()) {
            throw new AnalysisException("Unknown IVM delta id: " + deltaId
                    + ". No IVM delta plans are available.");
        }
        throw new AnalysisException("Unknown IVM delta id: " + deltaId
                + ". Valid delta id range is [1, " + deltaBundles.size() + "].");
    }

    public List<List<String>> formatOverviewRows() {
        List<List<String>> rows = new ArrayList<>();
        rows.add(row("IVM_NORMALIZED_PLAN", formatNormalizedDelta(), normalizedPlan.treeString()));
        if (deltaBundles.isEmpty()) {
            rows.add(row("IVM_DELTA_PLAN", "NO_DELTA", "No IVM delta plans."));
            return rows;
        }
        for (IvmDeltaExplainBundle bundle : deltaBundles) {
            rows.add(row(deltaPlanItem(bundle), formatDeltaBundle(bundle), bundle.getDeltaPlan().treeString()));
        }
        return rows;
    }

    public static List<List<String>> formatDeltaPlanRows(Plan deltaPlan) {
        return ImmutableList.of(ImmutableList.of(deltaPlan.treeString()));
    }

    private String formatNormalizedDelta() {
        Set<String> baseTables = new LinkedHashSet<>();
        List<String> tableDescriptions = new ArrayList<>();
        for (IvmDeltaExplainBundle bundle : deltaBundles) {
            if (baseTables.add(bundle.getBaseTable().toString())) {
                tableDescriptions.add(formatNormalizedDeltaBundle(bundle));
            }
        }
        return String.join(",\n", tableDescriptions);
    }

    private String formatNormalizedDeltaBundle(IvmDeltaExplainBundle bundle) {
        return new StringBuilder()
                .append("base_table=").append(bundle.getBaseTable())
                .append(", consumedTso=").append(bundle.getConsumedTso())
                .append(", latestTso=").append(bundle.getLatestTso())
                .append(", status=").append(bundle.isNoOp() ? "NO_OP" : "PENDING")
                .toString();
    }

    private String formatDeltaBundle(IvmDeltaExplainBundle bundle) {
        return new StringBuilder()
                .append("delta_id=").append(bundle.getDeltaId())
                .append(", base_table=").append(bundle.getBaseTable())
                .append(", occurrence=").append(bundle.getOccurrence())
                .append(", consumedTso=").append(bundle.getConsumedTso())
                .append(", latestTso=").append(bundle.getLatestTso())
                .append(", status=").append(bundle.isNoOp() ? "NO_OP" : "PENDING")
                .toString();
    }

    private String deltaPlanItem(IvmDeltaExplainBundle bundle) {
        return "IVM_DELTA_PLAN_" + bundle.getDeltaId();
    }

    private static List<String> row(String item, String delta, String value) {
        return ImmutableList.of(item, delta, value);
    }
}
