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

package org.apache.doris.datasource.lance.source;

import org.apache.doris.analysis.Expr;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentGroup;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceFragmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics;
import org.apache.doris.datasource.lance.source.LanceScanPlan.SearchKind;
import org.apache.doris.datasource.lance.source.LanceScanPlan.VectorIndexStatus;
import org.apache.doris.spi.Split;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/** Plans fragment ownership and index selection without accessing Doris execution state. */
final class LanceScanPlanner {
    private static final long COUNT_WITH_PARALLEL_SPLITS = 10000;
    private final LanceTableMetadata plannedMetadata;
    private final SearchKind searchKind;
    private final TExternalSearchRequest externalSearchRequest;
    private final int searchFieldId;
    private final List<Expr> lancePushedConjuncts;
    private final int fragmentsPerSplit;
    private final int countParallelism;
    private int plannedFragments;
    private int plannedFragmentsPerSplit;
    private int plannedUnindexedFragments;
    private int plannedIndexSegments;
    private int plannedIndexFragments;
    private LanceScalarIndexPlanner.Plan scalarIndexPlan;
    private VectorIndexStatus vectorIndexStatus = VectorIndexStatus.NOT_PLANNED;

    private LanceScanPlanner(LanceTableMetadata metadata, SearchKind kind, TExternalSearchRequest request,
            int fieldId, List<Expr> pushedConjuncts, int fragmentsPerSplit, int countParallelism) {
        this.plannedMetadata = metadata;
        this.searchKind = kind;
        this.externalSearchRequest = request;
        this.searchFieldId = fieldId;
        this.lancePushedConjuncts = pushedConjuncts;
        this.fragmentsPerSplit = fragmentsPerSplit;
        this.countParallelism = countParallelism;
    }

    static LanceScanPlan plan(LanceTableMetadata metadata, SearchKind kind, TExternalSearchRequest request,
            int fieldId, List<Expr> pushedConjuncts, int fragmentsPerSplit, int countParallelism) throws UserException {
        try (LanceMetadataMetrics metrics = LanceMetadataMetrics.startSplitPlanning()) {
            LanceScanPlanner planner = new LanceScanPlanner(metadata, kind, request,
                    fieldId, pushedConjuncts, fragmentsPerSplit, countParallelism);
            List<Split> splits = planner.buildSplits();
            LanceScanPlan plan = new LanceScanPlan(splits, metadata.getVersion(), planner.plannedFragments,
                    planner.plannedFragmentsPerSplit, planner.plannedIndexSegments, planner.plannedIndexFragments,
                    planner.plannedUnindexedFragments,
                    planner.scalarIndexPlan == null ? null : planner.scalarIndexPlan.indexName,
                    planner.vectorIndexStatus);
            metrics.succeeded();
            return plan;
        }
    }

    private List<Split> buildSplits() throws UserException {
        LanceTableMetadata metadata = plannedMetadata;
        plannedFragments = metadata.getFragments().size();
        plannedFragmentsPerSplit = searchKind == SearchKind.NORMAL ? fragmentsPerSplit : 1;
        if (plannedFragmentsPerSplit < 0) {
            throw new UserException("lance_fragments_per_split must be non-negative");
        }
        plannedUnindexedFragments = searchKind == SearchKind.NORMAL ? 0 : plannedFragments;
        if (searchKind.isExternalSearch() && metadata.getVersion() <= 0) {
            throw new UserException(
                    "Lance external search requires a fixed positive dataset version");
        }

        if (countParallelism > 0) {
            return buildCountSplits(metadata);
        }

        Map<Long, LanceFragmentInfo> visibleFragments = getVisibleFragments(metadata);
        switch (searchKind) {
            case FULL_TEXT:
                return createFullTextIndexSegmentSplits(metadata, visibleFragments);
            case VECTOR:
                vectorIndexStatus = VectorIndexStatus.DISABLED;
                if (isVectorIndexEnabled()) {
                    Optional<List<Split>> indexSplits = createVectorIndexSegmentSplits(
                            metadata, visibleFragments);
                    if (indexSplits.isPresent()) {
                        return indexSplits.get();
                    }
                }
                LanceSplitBuilder plan = new LanceSplitBuilder(
                        metadata.getDatasetUri(), metadata.getVersion(), 0);
                plan.addUncoveredFragments(visibleFragments.values(), 1);
                return plan.buildSplits();
            case NORMAL:
                return createNormalFragmentSplits(metadata, visibleFragments);
            default:
                throw new IllegalStateException("Unsupported Lance search kind " + searchKind);
        }
    }

    // COUNT(*)/COUNT(1) with no filter is answered from Lance metadata. Each carrier contains a
    // disjoint fragment group and its logical row count, so a BE that cannot use the metadata count
    // falls back to an equivalent fixed-snapshot scan. Large counts use several carriers to retain
    // parallelism; small counts use one.
    private List<Split> buildCountSplits(LanceTableMetadata metadata) {
        long rowCount = metadata.getRowCount();
        int carrierCount = 1;
        if (rowCount >= COUNT_WITH_PARALLEL_SPLITS && !metadata.getFragments().isEmpty()) {
            int parallelism = countParallelism;
            carrierCount = Math.min(metadata.getFragments().size(), Math.max(1, parallelism));
        }
        List<List<LanceFragmentInfo>> fragmentGroups = new ArrayList<>(carrierCount);
        for (int i = 0; i < carrierCount; i++) {
            fragmentGroups.add(new ArrayList<>());
        }
        List<LanceFragmentInfo> fragments = metadata.getFragments();
        for (int i = 0; i < fragments.size(); i++) {
            fragmentGroups.get(i % carrierCount).add(fragments.get(i));
        }

        List<Split> splits = new ArrayList<>(carrierCount);
        for (List<LanceFragmentInfo> group : fragmentGroups) {
            List<Long> fragmentIds = new ArrayList<>(group.size());
            long logicalRows = 0;
            long physicalRows = 0;
            for (LanceFragmentInfo fragment : group) {
                fragmentIds.add(fragment.getId());
                logicalRows += fragment.getRowCount();
                physicalRows += fragment.getPhysicalRows();
            }
            splits.add(LanceSplit.metadataCount(metadata.getDatasetUri(), metadata.getVersion(),
                    fragmentIds, logicalRows, physicalRows));
        }
        return splits;
    }

    private Map<Long, LanceFragmentInfo> getVisibleFragments(LanceTableMetadata metadata)
            throws UserException {
        Map<Long, LanceFragmentInfo> visible = new LinkedHashMap<>();
        for (LanceFragmentInfo fragment : metadata.getFragments()) {
            if (visible.put(fragment.getId(), fragment) != null) {
                throw new UserException("Duplicate Lance fragment id " + fragment.getId()
                        + " at dataset version " + metadata.getVersion());
            }
        }
        return visible;
    }

    private List<Split> createNormalFragmentSplits(LanceTableMetadata metadata,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        if (plannedFragmentsPerSplit > 0) {
            // Keep the debug grouping exact, even when it produces fewer splits than BEs.
            return LanceSplitBuilder.buildFragmentSplits(metadata.getDatasetUri(), metadata.getVersion(),
                    visibleFragments.values(), plannedFragmentsPerSplit);
        }
        scalarIndexPlan = LanceScalarIndexPlanner.plan(metadata, lancePushedConjuncts, visibleFragments);
        LanceSplitBuilder plan;
        if (scalarIndexPlan != null) {
            plan = scalarIndexPlan.splits;
            plannedIndexSegments = plan.splitCount();
            plannedIndexFragments = plan.indexSegmentFragmentCount();
            plannedUnindexedFragments = plannedFragments - plannedIndexFragments;
        } else {
            plan = new LanceSplitBuilder(metadata.getDatasetUri(), metadata.getVersion(), 0);
        }
        plan.addUncoveredFragments(visibleFragments.values(), 1, scalarIndexPlan != null);
        return plan.buildSplits();
    }

    private Optional<List<Split>> createVectorIndexSegmentSplits(LanceTableMetadata metadata,
            Map<Long, LanceFragmentInfo> visibleFragments) throws UserException {
        vectorIndexStatus = VectorIndexStatus.NO_MATCH;
        if (metadata.getIndexes().isEmpty()) {
            return Optional.empty();
        }
        TVectorSearchParams vectorSearchParam = externalSearchRequest.getSearchQuery().getVectorSearch();
        if (searchFieldId < 0) {
            throw new UserException("Lance vector column '" + vectorSearchParam.getColumn()
                    + "' has no field ID in the Lance schema");
        }

        for (List<LanceIndexSegmentInfo> matchingSegments : selectVectorIndexSegmentGroups(
                metadata.getIndexes(), searchFieldId)) {
            if (!metricMatches(vectorSearchParam, matchingSegments)) {
                vectorIndexStatus = VectorIndexStatus.METRIC_MISMATCH;
                continue;
            }
            Optional<LanceSplitBuilder> indexPlan = planIndexSegments(
                    metadata, matchingSegments, visibleFragments, IndexCoveragePolicy.VECTOR_FALLBACK);
            if (!indexPlan.isPresent()) {
                vectorIndexStatus = matchingSegments.stream().anyMatch(segment -> !segment.getFragmentIds().isPresent())
                        ? VectorIndexStatus.UNKNOWN_COVERAGE : VectorIndexStatus.NO_VISIBLE_COVERAGE;
                continue;
            }
            vectorIndexStatus = VectorIndexStatus.USED;
            LanceSplitBuilder plan = indexPlan.get();
            plannedIndexSegments = plan.splitCount();
            plannedIndexFragments = plan.indexSegmentFragmentCount();
            plannedUnindexedFragments = plannedFragments - plannedIndexFragments;
            plan.addUncoveredFragments(visibleFragments.values(), 1);
            return Optional.of(plan.buildSplits());
        }
        return Optional.empty();
    }

    private List<Split> createFullTextIndexSegmentSplits(LanceTableMetadata metadata,
            Map<Long, LanceFragmentInfo> visibleFragments) throws UserException {
        TFullTextSearchParams fullText =
                externalSearchRequest.getSearchQuery().getFullTextSearch();
        if (searchFieldId < 0) {
            throw new UserException("Lance full-text column '" + fullText.getColumn()
                    + "' has no field ID in the Lance schema");
        }
        List<LanceIndexSegmentInfo> matchingSegments = selectFullTextIndexSegments(
                metadata.getIndexes(), searchFieldId, fullText.getColumn());
        if (matchingSegments.isEmpty()) {
            throw new UserException("No committed Lance FTS index exists for column '"
                    + fullText.getColumn() + "' at dataset version " + metadata.getVersion());
        }
        // A matching index with no visible coverage is a valid empty INDEX_ONLY result.
        // Unknown coverage still throws in planIndexSegments; STRICT checks uncovered fragments below.
        LanceSplitBuilder plan = planIndexSegments(
                metadata, matchingSegments, visibleFragments, IndexCoveragePolicy.FTS_REQUIRED)
                .orElseGet(() -> new LanceSplitBuilder(
                        metadata.getDatasetUri(), metadata.getVersion(), 0));
        plannedIndexSegments = plan.splitCount();
        plannedIndexFragments = plan.indexSegmentFragmentCount();
        plannedUnindexedFragments = plannedFragments - plannedIndexFragments;
        if (fullText.getCoverageMode() == TFtsCoverageMode.STRICT
                && plannedUnindexedFragments != 0) {
            throw new UserException("Lance FTS coverage_mode=STRICT requires every fragment at "
                    + "dataset version " + metadata.getVersion() + " to be indexed; column '"
                    + fullText.getColumn() + "' has " + plannedUnindexedFragments
                    + " unindexed fragments. Rebuild the index or use coverage_mode=index_only");
        }
        return plan.buildSplits();
    }

    private static List<List<LanceIndexSegmentInfo>> selectVectorIndexSegmentGroups(
            List<LanceIndexSegmentGroup> indexes, int fieldId) {
        // Keep index selection stable when metadata lists logical indexes in a different order.
        Map<String, List<LanceIndexSegmentInfo>> groupsByName = new TreeMap<>();
        for (LanceIndexSegmentGroup index : indexes) {
            List<LanceIndexSegmentInfo> matches = index.getVectorSegments(fieldId);
            if (!matches.isEmpty()) {
                groupsByName.put(index.getName(), matches);
            }
        }
        return new ArrayList<>(groupsByName.values());
    }

    private static List<LanceIndexSegmentInfo> selectFullTextIndexSegments(
            List<LanceIndexSegmentGroup> indexes, int fieldId, String column) throws UserException {
        List<LanceIndexSegmentInfo> selected = Collections.emptyList();
        for (LanceIndexSegmentGroup index : indexes) {
            List<LanceIndexSegmentInfo> matches = index.getFullTextSegments(fieldId);
            if (matches.isEmpty()) {
                continue;
            }
            if (!selected.isEmpty()) {
                throw new UserException("Multiple Lance FTS indexes exist for column '" + column
                        + "'; distributed FTS requires one unambiguous logical index");
            }
            selected = matches;
        }
        return selected;
    }

    /** Vector search may fall back to fragments; FTS requires known, non-overlapping coverage. */
    private enum IndexCoveragePolicy {
        VECTOR_FALLBACK,
        FTS_REQUIRED;

        boolean requiresKnownDisjointCoverage() {
            return this == FTS_REQUIRED;
        }
    }

    /** Intersects manifest coverage with the fixed snapshot, retaining only visible fragments. */
    private static Optional<LanceSplitBuilder> planIndexSegments(
            LanceTableMetadata metadata,
            List<LanceIndexSegmentInfo> indexSegments,
            Map<Long, LanceFragmentInfo> visibleFragments,
            IndexCoveragePolicy coveragePolicy) throws UserException {
        LanceSplitBuilder plan = new LanceSplitBuilder(
                metadata.getDatasetUri(), metadata.getVersion(), indexSegments.size());
        for (LanceIndexSegmentInfo segment : indexSegments) {
            Optional<List<Long>> segmentFragments = segment.getFragmentIds();
            if (!segmentFragments.isPresent()) {
                if (coveragePolicy.requiresKnownDisjointCoverage()) {
                    throw new UserException("Lance FTS segment " + segment.getUuid()
                            + " has no fragment coverage metadata");
                }
                return Optional.empty();
            }
            List<Long> visibleIndexSegmentFragmentIds = effectiveFragmentIds(
                    segmentFragments.get(), visibleFragments);
            if (coveragePolicy.requiresKnownDisjointCoverage()) {
                for (Long fragmentId : visibleIndexSegmentFragmentIds) {
                    if (plan.isCoveredByIndexSegment(fragmentId)) {
                        throw new UserException("Lance FTS fragment " + fragmentId
                                + " is covered by multiple physical index segments");
                    }
                }
            }
            if (!visibleIndexSegmentFragmentIds.isEmpty()) {
                plan.addIndexSegmentSplit(
                        segment.getUuid(), visibleIndexSegmentFragmentIds,
                        sumPhysicalRows(visibleIndexSegmentFragmentIds, visibleFragments));
            }
        }
        if (plan.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(plan);
    }

    private static List<Long> effectiveFragmentIds(List<Long> fragmentIds,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        List<Long> visibleIndexSegmentFragmentIds = new ArrayList<>(fragmentIds.size());
        for (Long fragmentId : fragmentIds) {
            if (visibleFragments.containsKey(fragmentId)) {
                visibleIndexSegmentFragmentIds.add(fragmentId);
            }
        }
        return visibleIndexSegmentFragmentIds;
    }

    private static long sumPhysicalRows(List<Long> fragmentIds,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        long physicalRows = 0;
        for (Long fragmentId : fragmentIds) {
            LanceFragmentInfo fragment = visibleFragments.get(fragmentId);
            physicalRows += fragment.getSchedulingWeight();
        }
        return physicalRows;
    }

    private boolean isVectorIndexEnabled() {
        // default use_index is true
        if (!externalSearchRequest.isSetVectorSearchOptions()) {
            return true;
        }
        TVectorSearchOptions options = externalSearchRequest.getVectorSearchOptions();
        return !options.isSetUseIndex() || options.isUseIndex();
    }

    private static boolean metricMatches(TVectorSearchParams vector,
            List<LanceIndexSegmentInfo> segments) {
        // Leaving the metric unset in lance-c keeps Lance's L2 default. A segment built with a
        // different metric must not be forced into that query.
        String requestedMetric = !vector.isSetMetric() || vector.getMetric() == TVectorMetric.DEFAULT
                ? "L2" : VectorSearchTableValuedFunction.metricName(vector.getMetric()).toUpperCase(Locale.ROOT);
        for (LanceIndexSegmentInfo segment : segments) {
            if (!segment.getMetric().isPresent()
                    || !requestedMetric.equals(segment.getMetric().get())) {
                return false;
            }
        }
        return true;
    }
}
