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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.lance.LanceSearchPlan.VectorIndexStatus;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TVectorSearchParams;

import org.lance.index.IndexType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Plans which fragments each range of a Lance scan reads, from metadata alone: of an ordinary scan, and of a
 * vector or full-text search. Planning is deterministic, so the scan properties (EXPLAIN) and the range list of one
 * statement agree.
 */
final class LanceScanPlanner {
    /** Below this many rows one COUNT(*) carrier is enough; above it each fragment gets one. */
    static final long COUNT_WITH_PARALLEL_SPLITS = 10000;

    private LanceScanPlanner() {
    }

    /**
     * Groups the fragments of {@code metadata}'s version: {@code fragmentsPerSplit} at a time when it is
     * positive (a debugging override), else one range per scalar-index segment that can drive the pushed
     * filter plus one range per fragment no such segment covers.
     *
     * @param indexMetadata read only when an index segment could be planned: a positive
     *         {@code fragmentsPerSplit} or a scan with nothing pushed never reads it
     */
    static LanceScanPlan plan(LanceTableMetadata metadata, LancePredicateConverter converter,
            List<ConnectorExpression> pushedConjuncts, int fragmentsPerSplit,
            Supplier<LanceIndexMetadata> indexMetadata) {
        if (fragmentsPerSplit < 0) {
            throw new DorisConnectorException("lance_fragments_per_split must be non-negative");
        }
        Map<Long, LanceFragmentInfo> visibleFragments = visibleFragments(metadata);
        int fragmentCount = visibleFragments.size();
        if (fragmentsPerSplit > 0) {
            // Keep the debug grouping exact, even when it produces fewer splits than BEs.
            LanceSplitBuilder splits = new LanceSplitBuilder(metadata.getDatasetUri(), metadata.getVersion(), 0);
            splits.addUncoveredFragments(visibleFragments.values(), fragmentsPerSplit, false);
            return new LanceScanPlan(splits, fragmentCount, fragmentsPerSplit, null, 0, 0, 0);
        }
        LanceScalarIndexPlanner.Plan scalarIndexPlan = pushedConjuncts.isEmpty() ? null
                : LanceScalarIndexPlanner.plan(metadata, indexMetadata.get(), converter, pushedConjuncts,
                        visibleFragments);
        LanceSplitBuilder splits;
        int indexSegments = 0;
        int indexedFragments = 0;
        if (scalarIndexPlan != null) {
            splits = scalarIndexPlan.splits;
            indexSegments = splits.splitCount();
            indexedFragments = splits.indexSegmentFragmentCount();
        } else {
            splits = new LanceSplitBuilder(metadata.getDatasetUri(), metadata.getVersion(), 0);
        }
        // Uncovered fragments must not repeat the selected segment's search. When FE
        // could not select a segment, retain native selection unless the filter itself
        // is known to cause expensive repeated searches (a complement or oversized tree).
        splits.addUncoveredFragments(visibleFragments.values(), 1, scalarIndexPlan != null
                || LanceScalarIndexPlanner.shouldDisableFragmentIndex(pushedConjuncts));
        return new LanceScanPlan(splits, fragmentCount, 0,
                scalarIndexPlan == null ? null : scalarIndexPlan.indexName, indexSegments, indexedFragments,
                scalarIndexPlan == null ? 0 : fragmentCount - indexedFragments);
    }

    /**
     * COUNT(*) / COUNT(1) with no filter, answered from the manifest's row counts. Each carrier holds a disjoint
     * group of fragments and their logical rows, so a BE that cannot serve the count scans an equivalent pinned
     * snapshot instead. A large count gets one carrier per fragment to keep the scan parallel; a small one, or a
     * version without fragments, gets one carrier.
     */
    static List<ConnectorScanRange> countRanges(LanceTableMetadata metadata) {
        long rowCount = metadata.getRowCount();
        List<LanceFragmentInfo> fragments = new ArrayList<>(visibleFragments(metadata).values());
        List<List<LanceFragmentInfo>> groups = new ArrayList<>();
        if (rowCount >= COUNT_WITH_PARALLEL_SPLITS) {
            for (LanceFragmentInfo fragment : fragments) {
                groups.add(Collections.singletonList(fragment));
            }
        } else {
            groups.add(fragments);
        }
        long targetSplitSize = 1;
        for (List<LanceFragmentInfo> group : groups) {
            targetSplitSize = Math.max(targetSplitSize, physicalRows(group));
        }
        List<ConnectorScanRange> ranges = new ArrayList<>(groups.size());
        for (List<LanceFragmentInfo> group : groups) {
            List<Long> fragmentIds = new ArrayList<>(group.size());
            long logicalRows = 0;
            for (LanceFragmentInfo fragment : group) {
                fragmentIds.add(fragment.getId());
                logicalRows += fragment.getRowCount();
            }
            ranges.add(LanceScanRange.metadataCount(metadata.getDatasetUri(), metadata.getVersion(), fragmentIds,
                    logicalRows, rowCount, physicalRows(group), targetSplitSize));
        }
        return ranges;
    }

    /**
     * Plans a search over {@code metadata}'s version: one range per physical segment of the index the search can
     * use, each searching its own fragments, and for a vector search one flat-search range per fragment no such
     * segment covers. A vector search uses the first index over its column, by name, that was built with its metric
     * and whose segments cover a fragment of the version; without one, every range is a flat search. A full-text
     * search needs exactly one inverted index over its column; under {@code coverage_mode=strict} it must cover
     * every fragment, under {@code index_only} the fragments it does not cover are not searched.
     *
     * @param indexMetadata read unless a vector search sets {@code use_index=false}
     */
    static LanceSearchPlan planSearch(LanceTableMetadata metadata, LanceSearchTableHandle search,
            Supplier<LanceIndexMetadata> indexMetadata) {
        Map<Long, LanceFragmentInfo> visibleFragments = visibleFragments(metadata);
        TExternalSearchRequest request = search.getRequest();
        if (!search.isVectorSearch()) {
            return planFullTextSearch(metadata, request.getSearchQuery().getFullTextSearch(),
                    search.getSearchFieldId(), indexMetadata.get(), visibleFragments);
        }
        VectorIndexStatus status = VectorIndexStatus.DISABLED;
        if (usesVectorIndex(request)) {
            if (search.getSearchFieldId() < 0) {
                throw new IllegalStateException("A vector search that may use an index needs the field id of "
                        + request.getSearchQuery().getVectorSearch().getColumn());
            }
            status = VectorIndexStatus.NO_MATCH;
            TVectorSearchParams vector = request.getSearchQuery().getVectorSearch();
            for (List<LanceIndexSegmentInfo> segments : vectorIndexSegmentGroups(indexMetadata.get().getIndexes(),
                    search.getSearchFieldId())) {
                if (!metricMatches(vector, segments)) {
                    status = VectorIndexStatus.METRIC_MISMATCH;
                    continue;
                }
                boolean hasLower = vector.isSetDistanceLowerBound();
                boolean hasUpper = vector.isSetDistanceUpperBound();
                // Quantized indexes apply bounds before exact refinement and can discard valid rows, and the
                // parallel IVF_FLAT search replaces an absent bound with a finite sentinel. Only a two-sided range
                // over IVF_FLAT is exact; any other range scans the fragments exactly.
                if ((hasLower || hasUpper) && (!hasLower || !hasUpper || segments.stream()
                        .anyMatch(segment -> segment.getIndexType() != IndexType.IVF_FLAT))) {
                    status = VectorIndexStatus.DISTANCE_RANGE_FALLBACK;
                    continue;
                }
                Optional<LanceSplitBuilder> indexPlan = planIndexSegments(metadata, segments, visibleFragments,
                        false);
                if (!indexPlan.isPresent()) {
                    status = segments.stream().anyMatch(segment -> !segment.getFragmentIds().isPresent())
                            ? VectorIndexStatus.UNKNOWN_COVERAGE : VectorIndexStatus.NO_VISIBLE_COVERAGE;
                    continue;
                }
                LanceSplitBuilder splits = indexPlan.get();
                int indexSegments = splits.splitCount();
                int indexedFragments = splits.indexSegmentFragmentCount();
                splits.addUncoveredFragments(visibleFragments.values(), 1, false);
                return new LanceSearchPlan(splits, VectorIndexStatus.USED, visibleFragments.size(), indexSegments,
                        indexedFragments);
            }
        }
        LanceSplitBuilder splits = new LanceSplitBuilder(metadata.getDatasetUri(), metadata.getVersion(), 0);
        splits.addUncoveredFragments(visibleFragments.values(), 1, false);
        return new LanceSearchPlan(splits, status, visibleFragments.size(), 0, 0);
    }

    private static LanceSearchPlan planFullTextSearch(LanceTableMetadata metadata, TFullTextSearchParams fullText,
            int fieldId, LanceIndexMetadata indexMetadata, Map<Long, LanceFragmentInfo> visibleFragments) {
        if (fieldId < 0) {
            throw new IllegalStateException("A full-text search needs the field id of " + fullText.getColumn());
        }
        List<LanceIndexSegmentInfo> segments = fullTextIndexSegments(indexMetadata.getIndexes(), fieldId,
                fullText.getColumn());
        if (segments.isEmpty()) {
            throw new DorisConnectorException("No committed Lance FTS index exists for column '"
                    + fullText.getColumn() + "' at dataset version " + metadata.getVersion());
        }
        // An index that covers no fragment of the version is a valid empty INDEX_ONLY result; unknown coverage
        // fails in planIndexSegments, and STRICT checks the uncovered fragments below.
        LanceSplitBuilder splits = planIndexSegments(metadata, segments, visibleFragments, true)
                .orElseGet(() -> new LanceSplitBuilder(metadata.getDatasetUri(), metadata.getVersion(), 0));
        int indexedFragments = splits.indexSegmentFragmentCount();
        int unindexedFragments = visibleFragments.size() - indexedFragments;
        if (fullText.getCoverageMode() == TFtsCoverageMode.STRICT && unindexedFragments != 0) {
            throw new DorisConnectorException("Lance FTS coverage_mode=STRICT requires every fragment at dataset "
                    + "version " + metadata.getVersion() + " to be indexed; column '" + fullText.getColumn()
                    + "' has " + unindexedFragments + " unindexed fragments. Rebuild the index or use "
                    + "coverage_mode=index_only");
        }
        return new LanceSearchPlan(splits, VectorIndexStatus.NOT_PLANNED, visibleFragments.size(),
                splits.splitCount(), indexedFragments);
    }

    /** Whether a vector search may search an index: unless it sets {@code use_index=false}. */
    private static boolean usesVectorIndex(TExternalSearchRequest request) {
        return !request.isSetVectorSearchOptions() || !request.getVectorSearchOptions().isSetUseIndex()
                || request.getVectorSearchOptions().isUseIndex();
    }

    /** The segments of each vector index over the field, by index name: stable whatever order Lance lists them in. */
    private static List<List<LanceIndexSegmentInfo>> vectorIndexSegmentGroups(List<LanceIndexSegmentGroup> indexes,
            int fieldId) {
        Map<String, List<LanceIndexSegmentInfo>> groupsByName = new TreeMap<>();
        for (LanceIndexSegmentGroup index : indexes) {
            List<LanceIndexSegmentInfo> matches = index.getVectorSegments(fieldId);
            if (!matches.isEmpty()) {
                groupsByName.put(index.getName(), matches);
            }
        }
        return new ArrayList<>(groupsByName.values());
    }

    private static List<LanceIndexSegmentInfo> fullTextIndexSegments(List<LanceIndexSegmentGroup> indexes,
            int fieldId, String column) {
        List<LanceIndexSegmentInfo> selected = Collections.emptyList();
        for (LanceIndexSegmentGroup index : indexes) {
            List<LanceIndexSegmentInfo> matches = index.getFullTextSegments(fieldId);
            if (matches.isEmpty()) {
                continue;
            }
            if (!selected.isEmpty()) {
                throw new DorisConnectorException("Multiple Lance FTS indexes exist for column '" + column
                        + "'; distributed FTS requires one unambiguous logical index");
            }
            selected = matches;
        }
        return selected;
    }

    /**
     * One range per segment, over the fragments of the version it covers; empty when no segment covers one. A
     * vector search falls back to fragments on a segment of unknown coverage; a full-text search
     * ({@code requireKnownDisjointCoverage}) needs every segment's coverage, and no fragment in two segments.
     */
    private static Optional<LanceSplitBuilder> planIndexSegments(LanceTableMetadata metadata,
            List<LanceIndexSegmentInfo> segments, Map<Long, LanceFragmentInfo> visibleFragments,
            boolean requireKnownDisjointCoverage) {
        LanceSplitBuilder splits = new LanceSplitBuilder(metadata.getDatasetUri(), metadata.getVersion(),
                segments.size());
        for (LanceIndexSegmentInfo segment : segments) {
            Optional<List<Long>> segmentFragments = segment.getFragmentIds();
            if (!segmentFragments.isPresent()) {
                if (requireKnownDisjointCoverage) {
                    throw new DorisConnectorException("Lance FTS segment " + segment.getUuid()
                            + " has no fragment coverage metadata");
                }
                return Optional.empty();
            }
            List<Long> visibleSegmentFragments = new ArrayList<>(segmentFragments.get().size());
            long physicalRows = 0;
            for (Long fragmentId : segmentFragments.get()) {
                LanceFragmentInfo fragment = visibleFragments.get(fragmentId);
                if (fragment == null) {
                    continue;
                }
                if (requireKnownDisjointCoverage && splits.isCoveredByIndexSegment(fragmentId)) {
                    throw new DorisConnectorException("Lance FTS fragment " + fragmentId
                            + " is covered by multiple physical index segments");
                }
                visibleSegmentFragments.add(fragmentId);
                physicalRows += fragment.getSchedulingWeight();
            }
            if (!visibleSegmentFragments.isEmpty()) {
                splits.addIndexSegmentSplit(segment.getUuid(), visibleSegmentFragments, physicalRows);
            }
        }
        return splits.isEmpty() ? Optional.empty() : Optional.of(splits);
    }

    /**
     * Whether every segment was built with the search's metric, which the request always names (L2 when the user
     * named none); an index whose details name no metric matches none.
     */
    private static boolean metricMatches(TVectorSearchParams vector, List<LanceIndexSegmentInfo> segments) {
        String requested = LanceVectorQuery.metricName(vector.getMetric()).toUpperCase(Locale.ROOT);
        for (LanceIndexSegmentInfo segment : segments) {
            if (!segment.getMetric().isPresent() || !requested.equals(segment.getMetric().get())) {
                return false;
            }
        }
        return true;
    }

    private static long physicalRows(List<LanceFragmentInfo> fragments) {
        long physicalRows = 0;
        for (LanceFragmentInfo fragment : fragments) {
            physicalRows += fragment.getSchedulingWeight();
        }
        return Math.max(physicalRows, 1);
    }

    private static Map<Long, LanceFragmentInfo> visibleFragments(LanceTableMetadata metadata) {
        Map<Long, LanceFragmentInfo> visible = new LinkedHashMap<>();
        for (LanceFragmentInfo fragment : metadata.getFragments()) {
            if (visible.put(fragment.getId(), fragment) != null) {
                throw new DorisConnectorException("Duplicate Lance fragment id " + fragment.getId()
                        + " at dataset version " + metadata.getVersion());
            }
        }
        return visible;
    }
}
