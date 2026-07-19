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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/PlanFragment.java
// and modified by Doris

package org.apache.doris.planner.normalize;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Pair;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TQueryCacheParam;
import org.apache.doris.thrift.TStringLiteral;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** QueryCacheNormalizer */
public class QueryCacheNormalizer implements Normalizer {
    private final PlanFragment fragment;
    private final DescriptorTable descriptorTable;
    private final NormalizedIdGenerator normalizedPlanIds = new NormalizedIdGenerator();
    private final NormalizedIdGenerator normalizedTupleIds = new NormalizedIdGenerator();
    private final NormalizedIdGenerator normalizedSlotIds = new NormalizedIdGenerator();

    // result
    private final TQueryCacheParam queryCacheParam = new TQueryCacheParam();

    public QueryCacheNormalizer(PlanFragment fragment, DescriptorTable descriptorTable) {
        this.fragment = Objects.requireNonNull(fragment, "fragment can not be null");
        this.descriptorTable = Objects.requireNonNull(descriptorTable, "descriptorTable can not be null");
    }

    public Optional<TQueryCacheParam> normalize(ConnectContext context) {
        try {
            Optional<CachePoint> cachePoint = computeCachePoint();
            if (!cachePoint.isPresent()) {
                return Optional.empty();
            }
            List<TNormalizedPlanNode> normalizedDigestPlans = normalizePlanTree(context, cachePoint.get());
            byte[] digest = computeDigest(context, normalizedDigestPlans);
            return setQueryCacheParam(cachePoint.get(), digest, context);
        } catch (Throwable t) {
            return Optional.empty();
        }
    }

    @VisibleForTesting
    public List<TNormalizedPlanNode> normalizePlans(ConnectContext context) {
        Optional<CachePoint> cachePoint = computeCachePoint();
        if (!cachePoint.isPresent()) {
            return ImmutableList.of();
        }
        return normalizePlanTree(context, cachePoint.get());
    }

    private Optional<TQueryCacheParam> setQueryCacheParam(
            CachePoint cachePoint, byte[] digest, ConnectContext context) {
        SessionVariable sessionVariable = context.getSessionVariable();
        queryCacheParam.setNodeId(cachePoint.cacheRoot.getId().asInt());
        queryCacheParam.setDigest(digest);
        queryCacheParam.setForceRefreshQueryCache(sessionVariable.isQueryCacheForceRefresh());
        queryCacheParam.setEntryMaxBytes(sessionVariable.getQueryCacheEntryMaxBytes());
        queryCacheParam.setEntryMaxRows(sessionVariable.getQueryCacheEntryMaxRows());
        queryCacheParam.setAllowIncremental(computeAllowIncremental(cachePoint, sessionVariable));
        queryCacheParam.setIsMergeOnWrite(computeIsMergeOnWrite(cachePoint));

        queryCacheParam.setOutputSlotMapping(
                cachePoint.cacheRoot.getOutputTupleIds()
                    .stream()
                    .flatMap(tupleId -> descriptorTable.getTupleDesc(tupleId).getSlots().stream())
                    .map(slot -> {
                        int slotId = slot.getId().asInt();
                        return Pair.of(slotId, normalizeSlotId(slotId));
                    })
                    .collect(Collectors.toMap(Pair::key, Pair::value))
        );

        return Optional.of(queryCacheParam);
    }

    private Optional<CachePoint> computeCachePoint() {
        if (!fragment.getTargetRuntimeFilterIds().isEmpty()) {
            return Optional.empty();
        }
        PlanNode planRoot = fragment.getPlanRoot();
        return doComputeCachePoint(planRoot);
    }

    private Optional<CachePoint> doComputeCachePoint(PlanNode planRoot) {
        if (planRoot instanceof AggregationNode) {
            PlanNode child = planRoot.getChild(0);
            if (child instanceof OlapScanNode) {
                if (((AggregationNode) planRoot).isQueryCacheCandidate()) {
                    return Optional.of(new CachePoint(planRoot, planRoot));
                }
            } else if (child instanceof AggregationNode) {
                Optional<CachePoint> childCachePoint = doComputeCachePoint(child);
                if (childCachePoint.isPresent()) {
                    if (((AggregationNode) planRoot).isQueryCacheCandidate()) {
                        return Optional.of(new CachePoint(planRoot, planRoot));
                    }
                    return childCachePoint;
                }
            }
        } else if (planRoot instanceof ExchangeNode) {
            return Optional.empty();
        } else if (planRoot.getChildren().size() == 1) {
            return doComputeCachePoint(planRoot.getChildren().get(0));
        }
        return Optional.empty();
    }

    /**
     * Decide whether BE may serve a stale cache entry by scanning only the delta
     * rowsets since the cached version and emitting them together with the cached
     * partial aggregation blocks (the upstream aggregation merges both).
     *
     * <p>This is only correct when all of the following hold:
     * <ul>
     * <li>The cache point aggregation does not finalize: its output is a partial
     * state that an upstream aggregation always merges, so emitting the cached
     * blocks and the delta blocks side by side yields the correct result. A
     * finalized output has no downstream merge, and the two emissions would
     * produce duplicated group keys.</li>
     * <li>The cache point aggregates the raw detail rows directly (its child is
     * the olap scan node): with another aggregation in between, that inner
     * aggregation would see only the delta rows during an incremental run, and
     * its finalized output over the delta is not a mergeable complement of its
     * output over the cached snapshot.</li>
     * <li>The scanned index is append-only, guaranteeing "cached snapshot +
     * delta rowsets == new snapshot": either DUP_KEYS, or merge-on-write
     * UNIQUE_KEYS, for which BE additionally verifies per tablet (through the
     * delete bitmap of the delta window) that no pre-existing key was rewritten
     * and falls back otherwise. Merge-on-read UNIQUE resolves duplicates by
     * merging across rowsets at read time, so a delta-only scan cannot stand
     * alone there; AGG tables merge rows inside the storage layer likewise.
     * DELETE predicates are handled on BE: a delta containing delete predicates
     * falls back to a full recompute.</li>
     * </ul>
     */
    private boolean computeAllowIncremental(CachePoint cachePoint, SessionVariable sessionVariable) {
        if (!sessionVariable.getEnableQueryCacheIncremental()) {
            return false;
        }
        // The cache point is always an aggregation node (see doComputeCachePoint).
        if (((AggregationNode) cachePoint.cacheRoot).isNeedsFinalize()) {
            return false;
        }
        // The cached partial state and the delta partial state merge correctly
        // only when the cache point aggregates the raw detail rows directly.
        // With a nested cache point (partial agg over a finalized/deduplicating
        // agg over scan), the inner agg sees only the delta rows during an
        // incremental run, so its finalized output over the delta is NOT a
        // mergeable complement of its output over the cached snapshot (e.g.
        // "group by cnt" buckets computed from partial counts are simply wrong).
        if (!(cachePoint.cacheRoot.getChild(0) instanceof OlapScanNode)) {
            return false;
        }
        OlapScanNode scanNode = (OlapScanNode) cachePoint.cacheRoot.getChild(0);
        OlapTable olapTable = scanNode.getOlapTable();
        long selectIndexId = scanNode.getSelectedIndexId() == -1
                ? olapTable.getBaseIndexId()
                : scanNode.getSelectedIndexId();
        // The scanned index must be append-only, guaranteeing "cached snapshot +
        // delta rowsets == new snapshot". Note: judged on the selected index, not
        // the base table -- a DUP table may serve the query from an aggregated
        // materialized view, whose data is no longer append-only.
        //
        // DUP_KEYS is always append-only. A merge-on-write UNIQUE index is
        // append-only as long as a load does not touch pre-existing keys, which
        // covers the common "hourly append plus occasional backfill" pattern: BE
        // verifies per tablet through the delete bitmap of the delta window and
        // falls back to a full recompute for the rare load that rewrites history.
        // A merge-on-read UNIQUE index resolves duplicates by merging across
        // rowsets at read time, so a delta-only scan cannot stand alone there;
        // AGG tables merge rows inside the storage layer likewise.
        KeysType keysType = olapTable.getKeysTypeByIndexId(selectIndexId);
        boolean mergeOnWrite =
                keysType == KeysType.UNIQUE_KEYS && olapTable.getEnableUniqueKeyMergeOnWrite();
        if (keysType != KeysType.DUP_KEYS && !mergeOnWrite) {
            return false;
        }
        // Freshness tolerance and prefer-cached-rowset (both cloud-only in
        // effect) trade exactness for speed and locality: the scan may read a
        // warmed-up layout that stops below the queried version (freshness halts
        // at the warmed boundary) or reaches beyond it (neither walk clips an
        // edge spanning it). An incremental merge would defeat such a knob,
        // because the delta capture always targets the exact queried version (it
        // must, to keep the merged entry correct), forcing the un-warmed reads
        // the query explicitly chose to skip. Correctness against version-inexact
        // reads does NOT rest on this per-query gate (an entry filled by such a
        // read could still be reused by a different, knob-free query sharing the
        // cache key): on cloud, the only mode where these reads occur, the BE
        // suppresses their cache write-back, so no entry whose content mismatches
        // its version stamp exists in the first place. The gate stays mode-
        // agnostic (no is_cloud_mode): the knobs are inert on local storage, so
        // excluding incremental there only forgoes it for a query that opted into
        // a cloud trade-off, and a mode-conditioned gate cannot be exercised by
        // the local FE unit-test harness (planning under a flipped cloud flag
        // casts SystemInfoService to CloudSystemInfoService and fails).
        if (sessionVariable.getQueryFreshnessToleranceMs() > 0) {
            return false;
        }
        // Prefer-cached-rowset is honored by the storage layer only for non-MOW
        // tables: CloudTablet::capture_consistent_versions_unlocked guards the
        // prefer branch on !enable_unique_key_merge_on_write(), so a MOW query
        // reads the exact queried version regardless of the knob. Its delta
        // capture is therefore version-exact and safe to merge incrementally, so
        // only non-MOW tables are excluded here. This carve-out keys on table
        // type, not mode, so the FE unit test can exercise it directly; it is
        // sound in both modes because the read is version-exact for MOW whether
        // the knob is inert (local) or explicitly ignored (cloud). Freshness
        // above has no such MOW guard on the storage side, so it excludes
        // incremental for every table type.
        if (sessionVariable.getEnablePreferCachedRowset() && !mergeOnWrite) {
            return false;
        }
        return true;
    }

    // Whether the selected index of the cache point's scan is a merge-on-write
    // UNIQUE table. BE consumes this in the cloud cache write-back gate: cloud
    // ignores enable_prefer_cached_rowset for a MOW table, so a prefer-only MOW
    // read is version-exact and its fill must not be suppressed. Reported
    // independently of allow_incremental because the write-back gate also applies
    // to a MISS (a MOW MISS under prefer must still be cached). Determined on the
    // selected index, mirroring computeAllowIncremental. False when the cache
    // point has no direct OlapScanNode child (a nested-agg shape, never
    // incrementally cacheable), which leaves BE at its safe suppress default: such
    // a shape over a MOW scan also forgoes the prefer-cached-rowset write-back,
    // a deliberate over-suppression (the safe direction, it never caches a
    // version-inexact entry) rather than threading scan resolution through the
    // aggregation chain for a cache point whose incremental merge is already off.
    private boolean computeIsMergeOnWrite(CachePoint cachePoint) {
        if (!(cachePoint.cacheRoot.getChild(0) instanceof OlapScanNode)) {
            return false;
        }
        OlapScanNode scanNode = (OlapScanNode) cachePoint.cacheRoot.getChild(0);
        OlapTable olapTable = scanNode.getOlapTable();
        long selectIndexId = scanNode.getSelectedIndexId() == -1
                ? olapTable.getBaseIndexId()
                : scanNode.getSelectedIndexId();
        KeysType keysType = olapTable.getKeysTypeByIndexId(selectIndexId);
        return keysType == KeysType.UNIQUE_KEYS && olapTable.getEnableUniqueKeyMergeOnWrite();
    }

    private List<TNormalizedPlanNode> normalizePlanTree(ConnectContext context, CachePoint cachePoint) {
        List<TNormalizedPlanNode> normalizedPlans = new ArrayList<>();
        doNormalizePlanTree(context, cachePoint.digestRoot, normalizedPlans);
        return normalizedPlans;
    }

    private void doNormalizePlanTree(
            ConnectContext context, PlanNode plan, List<TNormalizedPlanNode> normalizedPlans) {
        for (PlanNode child : plan.getChildren()) {
            doNormalizePlanTree(context, child, normalizedPlans);
        }
        normalizedPlans.add(plan.normalize(this));
    }

    public static byte[] computeDigest(
            ConnectContext context, List<TNormalizedPlanNode> normalizedDigestPlans) throws Exception {
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        for (TNormalizedPlanNode node : normalizedDigestPlans) {
            digest.update(serializer.serialize(node));
        }

        StringBuffer variables = new StringBuffer();
        context.getSessionVariable().readAffectQueryResultVariables((k, v) -> {
            variables.append(k).append("=").append(v).append("|");
        });
        digest.update(serializer.serialize(new TStringLiteral(variables.toString())));
        return digest.digest();
    }

    @Override
    public int normalizeSlotId(int slotId) {
        return normalizedSlotIds.normalize(slotId);
    }

    @Override
    public void setSlotIdToNormalizeId(int slotId, int normalizedId) {
        normalizedSlotIds.set(slotId, normalizedId);
    }

    @Override
    public int normalizeTupleId(int tupleId) {
        return normalizedTupleIds.normalize(tupleId);
    }

    @Override
    public int normalizePlanId(int planId) {
        return normalizedPlanIds.normalize(planId);
    }

    @Override
    public DescriptorTable getDescriptorTable() {
        return descriptorTable;
    }

    @Override
    public void setNormalizedPartitionPredicates(OlapScanNode olapScanNode, NormalizedPartitionPredicates predicates) {
        OlapTable olapTable = olapScanNode.getOlapTable();
        long selectIndexId = olapScanNode.getSelectedIndexId() == -1
                ? olapTable.getBaseIndexId()
                : olapScanNode.getSelectedIndexId();

        Map<Long, String> tabletToRange = Maps.newLinkedHashMap();
        for (Long partitionId : olapScanNode.getSelectedPartitionIds()) {
            Set<Long> tabletIds = olapTable.getPartition(partitionId)
                    .getIndex(selectIndexId)
                    .getTablets()
                    .stream()
                    .map(Tablet::getId)
                    .collect(Collectors.toSet());

            String filterRange = predicates.intersectPartitionRanges.get(partitionId);
            for (Long tabletId : tabletIds) {
                tabletToRange.put(tabletId, filterRange);
            }
        }
        queryCacheParam.setTabletToRange(tabletToRange);
    }

    private static class CachePoint {
        PlanNode digestRoot;
        PlanNode cacheRoot;

        public CachePoint(PlanNode digestRoot, PlanNode cacheRoot) {
            this.digestRoot = digestRoot;
            this.cacheRoot = cacheRoot;
        }
    }

    private static class NormalizedIdGenerator {
        private final AtomicInteger idGenerator = new AtomicInteger(0);
        private final Map<Integer, Integer> originIdToNormalizedId = Maps.newLinkedHashMap();

        public Integer normalize(Integer originId) {
            return originIdToNormalizedId.computeIfAbsent(originId, id -> idGenerator.getAndIncrement());
        }

        public void set(int originId, Integer normalizedId) {
            originIdToNormalizedId.put(originId, normalizedId);
        }
    }
}
