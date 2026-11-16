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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.Config;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPaloScanRange;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** Stable identity for reusable TopN exclusions, before BE adds the scan predicates. */
public final class TopnConditionCache {
    private TopnConditionCache() {
    }

    static long computeDigest(TopnFilter filter) {
        if (Config.isCloudMode() || filter.targets.size() != 1 || filter.legacyTargets.size() != 1
                || filter.topn.getOrderKeys().size() != 1) {
            return 0;
        }
        ConnectContext context = ConnectContext.get();
        // Debug reads can skip versions/tablets or expose rows removed later by compaction.
        // Their actual input need not match the finalized logical snapshot.
        if (context == null || !context.getSessionVariable().enableConditionCache
                || context.getSessionVariable().isInDebugMode()) {
            return 0;
        }
        PhysicalRelation relation = filter.targets.keySet().iterator().next();
        if (!(relation instanceof PhysicalOlapScan)) {
            return 0;
        }
        PhysicalOlapScan physicalScan = (PhysicalOlapScan) relation;
        if (physicalScan.getTableSample().isPresent() || !physicalScan.getAppliedRuntimeFilters().isEmpty()
                || physicalScan.getTable().getKeysType() != KeysType.DUP_KEYS
                || physicalScan.getTable().hasRowTtl()) {
            return 0;
        }
        ScanNode target = filter.legacyTargets.keySet().iterator().next();
        PlanNode input = filter.legacySortNode.getChild(0);
        // The local shuffle planner can insert this row-preserving fan-out below the sort.
        while (input instanceof LocalExchangeNode
                && ((LocalExchangeNode) input).getExchangeType() == LocalExchangeType.PASSTHROUGH) {
            input = input.getChild(0);
        }
        // Every condition contributing candidates must be included in the BE scan digest.
        // In particular, a Select/Join/Aggregate between the scan and the sort is not eligible.
        if (!(target instanceof OlapScanNode) || input != target
                || target.hasLimit() || target.getTopnFilterSortNodes().size() != 1) {
            return 0;
        }
        OlapScanNode scan = (OlapScanNode) target;
        if (scan.getSelectedIndexId() != scan.getOlapTable().getBaseIndexId()) {
            return 0;
        }
        Expr expression = filter.legacyTargets.get(target);
        if (!(expression instanceof SlotRef) || ((SlotRef) expression).getColumn() == null) {
            return 0;
        }
        SlotRef slot = (SlotRef) expression;
        // Read the finalized scan ranges, not catalog versions or per-worker assignments.
        List<TPaloScanRange> ranges = scan.getScanRangeLocations(0).stream()
                .map(location -> location.getScanRange().getPaloScanRange())
                .sorted(Comparator.comparingLong(TPaloScanRange::getTabletId))
                .collect(Collectors.toList());
        Hasher hasher = Hashing.murmur3_128().newHasher()
                .putString("TOPN_CONDITION_CACHE_V1", StandardCharsets.UTF_8)
                .putLong(scan.getOlapTable().getId()).putLong(scan.getSelectedIndexId())
                .putInt(slot.getColumn().getUniqueId())
                .putString(slot.getColumn().getName(), StandardCharsets.UTF_8).putByte((byte) 0)
                .putString(slot.getType().toSql(), StandardCharsets.UTF_8).putByte((byte) 0)
                .putBoolean(filter.topn.getOrderKeys().get(0).isAsc())
                .putBoolean(filter.topn.getOrderKeys().get(0).isNullFirst())
                .putLong(filter.legacySortNode.getLimit()).putLong(filter.legacySortNode.getOffset())
                .putInt(ranges.size());
        for (TPaloScanRange range : ranges) {
            hasher.putLong(range.getTabletId()).putLong(Long.parseLong(range.getVersion()))
                    .putInt(Integer.parseInt(range.getSchemaHash()));
        }
        return hasher.hash().asLong();
    }
}
