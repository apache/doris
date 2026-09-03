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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LanceFragmentInfo;
import org.apache.doris.datasource.lance.LanceIndexSegmentInfo;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFtsQueryType;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TLanceScanParams;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Scan node for both ordinary Lance table scans and Lance external-search scans.
 *
 * <p>These modes share dataset metadata, storage properties, and BE scan-range serialization.
 * Keeping them in one node prevents those common parts from drifting apart. The search request is
 * also an explicit mode marker. Ordinary scans are split by fragment. Indexed vector searches are
 * split by physical index segment, with uncovered fragments retained as flat-search fallbacks.
 * Full-text searches are split only by committed inverted-index segments, with coverage governed
 * by the request's STRICT or INDEX_ONLY mode. Each search split produces local candidates; a Doris
 * TopN above this scan merges them into the requested snapshot-wide result.
 */
public class LanceScanNode extends FileQueryScanNode {
    private enum SearchKind {
        NORMAL,
        VECTOR,
        FULL_TEXT
    }

    private LanceExternalTable lanceTable;
    private LanceTableMetadata plannedMetadata;
    private final int searchFieldId;
    private final TExternalSearchRequest externalSearchRequest;
    private final SearchKind searchKind;
    private byte[] lanceSubstraitFilter = new byte[0];
    private String lancePushdownPredicate = "";
    private long plannedVersion = -1;
    private int plannedFragments;
    private int plannedUnindexedFragments;
    private int plannedIndexSegments;
    private int plannedIndexFragments;

    public LanceScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
            SessionVariable sessionVariable, ScanContext scanContext) {
        super(id, desc, "LANCE_SCAN_NODE", StatisticalType.LANCE_SCAN_NODE,
                scanContext, needCheckColumnPriv, sessionVariable);
        this.searchFieldId = -1;
        this.externalSearchRequest = null;
        this.searchKind = SearchKind.NORMAL;
    }

    /**
     * Creates the search mode of this node.
     *
     * <p>The tuple descriptor belongs to a FunctionGenTable and contains generated columns such as
     * {@code _distance} or {@code _score}. Therefore the real Lance table and the metadata
     * snapshot selected while analyzing the TVF must be passed separately.
     */
    public static LanceScanNode forExternalSearch(PlanNodeId id, TupleDescriptor desc,
            LanceExternalTable lanceTable, LanceTableMetadata plannedMetadata, int searchFieldId,
            TExternalSearchRequest externalSearchRequest, SessionVariable sessionVariable) {
        return new LanceScanNode(id, desc, lanceTable, plannedMetadata, searchFieldId,
                externalSearchRequest, sessionVariable);
    }

    private LanceScanNode(PlanNodeId id, TupleDescriptor desc, LanceExternalTable lanceTable,
            LanceTableMetadata plannedMetadata, int searchFieldId,
            TExternalSearchRequest externalSearchRequest, SessionVariable sessionVariable) {
        super(id, desc, "LANCE_SCAN_NODE", StatisticalType.LANCE_SCAN_NODE,
                ScanContext.builder().clusterName(sessionVariable.resolveCloudClusterName()).build(),
                false, sessionVariable);
        this.lanceTable = lanceTable;
        this.plannedMetadata = plannedMetadata;
        this.searchFieldId = searchFieldId;
        if (externalSearchRequest == null) {
            throw new IllegalArgumentException("Lance external search request must not be null");
        }
        this.externalSearchRequest = externalSearchRequest.deepCopy();
        this.searchKind = resolveSearchKind(this.externalSearchRequest);
    }

    @Override
    protected void doInitialize() throws UserException {
        List<Column> sourceColumns;
        if (searchKind != SearchKind.NORMAL) {
            sourceColumns = desc.getTable().getColumns();
        } else {
            lanceTable = (LanceExternalTable) desc.getTable();
            Optional<MvccSnapshot> relationSnapshot = getRelationSnapshot();
            plannedMetadata = lanceTable.getMetadata(relationSnapshot);
            sourceColumns = lanceTable.getFullSchema(relationSnapshot);
        }
        super.doInitialize();
        ExternalUtil.initSchemaInfo(params, -1L, sourceColumns);

        if (searchKind != SearchKind.NORMAL) {
            // Search output comes from the FunctionGenTable because it adds generated columns such
            // as _distance or _score. The real Lance table is retained for storage and metadata.
            getOrCreateLanceScanParams()
                    .setExternalSearchRequest(createSplitSearchRequest());
        }
    }

    private TLanceScanParams getOrCreateLanceScanParams() {
        if (!params.isSetLanceScanParams()) {
            params.setLanceScanParams(new TLanceScanParams());
        }
        return params.getLanceScanParams();
    }

    // A fragment-level LIMIT can be pushed into an ordinary Lance scan only when every predicate
    // is already pushed into Lance (conjuncts is empty). Otherwise Doris re-filters the returned
    // rows and truncating a fragment early could drop valid results.
    //
    // OFFSET needs no special handling: the Nereids SplitLimit rule rewrites Limit(limit, offset)
    // into a global Limit(limit, offset) over a local Limit(limit + offset, 0), and the local
    // bound is what lands on this scan node. So getLimit() already accounts for the offset and
    // getOffset() is always 0 here; each fragment fetches up to limit + offset rows and the upper
    // global LIMIT still applies the offset and the final bound.
    private boolean canPushDownLimit() {
        return hasLimit() && conjuncts.isEmpty();
    }

    @Override
    protected void convertPredicate() {
        if (searchKind != SearchKind.NORMAL) {
            // The TVF "filter" property is already serialized in externalSearchRequest and is
            // evaluated by Lance before candidate search. Outer WHERE conjuncts have different
            // semantics: keep them as Doris scan residuals. Each fragment first returns its Lance
            // ANN candidates, then Doris evaluates these conjuncts before the local/global TopN.
        } else {
            LancePredicateConverter.ConversionResult result =
                    new LancePredicateConverter(plannedMetadata.getSchema()).convert(conjuncts);
            lanceSubstraitFilter = result.getSubstraitFilter();
            lancePushdownPredicate = result.getDebugPredicate();
            conjuncts.removeAll(result.getPushedConjuncts());
        }
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
        if (lanceSubstraitFilter.length > 0) {
            getOrCreateLanceScanParams()
                    .setLanceSubstraitFilter(ByteBuffer.wrap(lanceSubstraitFilter));
        }
        // Set at ScanNode level so credentials are not serialized once per fragment split.
        Map<String, String> lanceStorageOptions = plannedMetadata.getLanceStorageOptions();
        if (!lanceStorageOptions.isEmpty()) {
            getOrCreateLanceScanParams().setLanceStorageOptions(lanceStorageOptions);
        }
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        LanceTableMetadata metadata = plannedMetadata;
        plannedVersion = metadata.getVersion();
        plannedFragments = metadata.getFragments().size();
        plannedUnindexedFragments = searchKind == SearchKind.NORMAL ? 0 : plannedFragments;
        plannedIndexSegments = 0;
        plannedIndexFragments = 0;
        if (searchKind != SearchKind.NORMAL && plannedVersion <= 0) {
            throw new UserException(
                    "Lance external search requires a fixed positive dataset version");
        }

        Map<Long, LanceFragmentInfo> visibleFragments = getVisibleFragments(metadata);
        switch (searchKind) {
            case FULL_TEXT:
                return createFullTextIndexSegmentSplits(metadata, visibleFragments);
            case VECTOR:
                if (isVectorIndexEnabled()) {
                    Optional<List<Split>> indexSplits = createVectorIndexSegmentSplits(
                            metadata, visibleFragments);
                    if (indexSplits.isPresent()) {
                        return indexSplits.get();
                    }
                }
                break;
            case NORMAL:
                break;
            default:
                throw new IllegalStateException("Unsupported Lance search kind " + searchKind);
        }
        return createFragmentSplits(metadata, visibleFragments);
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

    private List<Split> createFragmentSplits(LanceTableMetadata metadata,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        long targetRows = 1;
        for (LanceFragmentInfo fragment : visibleFragments.values()) {
            targetRows = Math.max(targetRows, Math.max(fragment.getPhysicalRows(), 1));
        }

        // Keep one fragment per split. Use the largest fragment's physical row count as the
        // normalization baseline for split weights, so backend scheduling reflects the relative
        // amount of physical data each fragment scans, including rows covered by deletion metadata.
        List<Split> splits = new ArrayList<>(visibleFragments.size());
        for (LanceFragmentInfo fragment : visibleFragments.values()) {
            LanceSplit split = LanceSplit.forFragment(metadata.getDatasetUri(), metadata.getVersion(),
                    fragment.getId(), fragment.getPhysicalRows());
            split.setTargetSplitSize(targetRows);
            splits.add(split);
        }
        return splits;
    }

    private Optional<List<Split>> createVectorIndexSegmentSplits(LanceTableMetadata metadata,
            Map<Long, LanceFragmentInfo> visibleFragments) throws UserException {
        if (metadata.getIndexSegments().isEmpty()) {
            return Optional.empty();
        }
        TVectorSearchParams vectorSearchParam = externalSearchRequest.getSearchQuery().getVectorSearch();
        if (searchFieldId < 0) {
            throw new UserException("Lance vector column '" + vectorSearchParam.getColumn()
                    + "' has no field ID in the Lance schema");
        }

        List<LanceIndexSegmentInfo> matchingSegments = selectVectorIndexSegments(
                metadata.getIndexSegments(), searchFieldId);
        if (matchingSegments.isEmpty() || !metricMatches(vectorSearchParam, matchingSegments)) {
            return Optional.empty();
        }

        Optional<IndexSegmentSplitPlan> indexPlan = planIndexSegments(
                metadata, matchingSegments, visibleFragments, false);
        if (!indexPlan.isPresent()) {
            return Optional.empty();
        }
        IndexSegmentSplitPlan plan = indexPlan.get();
        plannedIndexSegments = plan.splitCount();
        plannedIndexFragments = plan.indexSegmentFragmentCount();
        plannedUnindexedFragments = plannedFragments - plannedIndexFragments;
        appendUnindexedFragmentSplits(plan, visibleFragments);
        return Optional.of(plan.buildSplits());
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
                metadata.getIndexSegments(), searchFieldId, fullText.getColumn());
        if (matchingSegments.isEmpty()) {
            throw new UserException("No committed Lance FTS index exists for column '"
                    + fullText.getColumn() + "' at dataset version " + metadata.getVersion());
        }
        IndexSegmentSplitPlan plan = planIndexSegments(
                metadata, matchingSegments, visibleFragments, true)
                .orElseThrow(() -> new UserException("Lance FTS index for column '"
                        + fullText.getColumn() + "' has no visible indexed fragments at dataset version "
                        + metadata.getVersion()));
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

    private static List<LanceIndexSegmentInfo> selectVectorIndexSegments(
            List<LanceIndexSegmentInfo> indexSegments, int fieldId) {
        List<LanceIndexSegmentInfo> selectedSegments = new ArrayList<>();
        String selectedIndexName = null;
        for (LanceIndexSegmentInfo segment : indexSegments) {
            if (!segment.isVectorIndex() || !segment.getFieldIds().contains(fieldId)) {
                continue;
            }
            if (selectedIndexName == null) {
                selectedIndexName = segment.getIndexName();
            }
            if (selectedIndexName.equals(segment.getIndexName())) {
                selectedSegments.add(segment);
            }
        }
        return selectedSegments;
    }

    private static List<LanceIndexSegmentInfo> selectFullTextIndexSegments(
            List<LanceIndexSegmentInfo> indexSegments, int fieldId, String column)
            throws UserException {
        List<LanceIndexSegmentInfo> selectedSegments = new ArrayList<>();
        String selectedIndexName = null;
        for (LanceIndexSegmentInfo segment : indexSegments) {
            if (!segment.isFullTextIndex() || !segment.getFieldIds().contains(fieldId)) {
                continue;
            }
            if (selectedIndexName == null) {
                selectedIndexName = segment.getIndexName();
            } else if (!selectedIndexName.equals(segment.getIndexName())) {
                throw new UserException("Multiple Lance FTS indexes exist for column '" + column
                        + "'; distributed FTS requires one unambiguous logical index");
            }
            selectedSegments.add(segment);
        }
        return selectedSegments;
    }

    private static Optional<IndexSegmentSplitPlan> planIndexSegments(
            LanceTableMetadata metadata,
            List<LanceIndexSegmentInfo> indexSegments,
            Map<Long, LanceFragmentInfo> visibleFragments,
            boolean requireKnownCoverage) throws UserException {
        IndexSegmentSplitPlan plan = new IndexSegmentSplitPlan(
                metadata.getDatasetUri(), metadata.getVersion(), indexSegments.size());
        for (LanceIndexSegmentInfo segment : indexSegments) {
            Optional<List<Long>> segmentFragments = segment.getFragmentIds();
            if (!segmentFragments.isPresent()) {
                if (requireKnownCoverage) {
                    throw new UserException("Lance FTS segment " + segment.getUuid()
                            + " has no fragment coverage metadata");
                }
                return Optional.empty();
            }
            List<Long> visibleIndexSegmentFragmentIds = effectiveFragmentIds(
                    segmentFragments.get(), visibleFragments);
            if (requireKnownCoverage) {
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
            physicalRows += Math.max(fragment.getPhysicalRows(), 1);
        }
        return physicalRows;
    }

    private static void appendUnindexedFragmentSplits(IndexSegmentSplitPlan plan,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        for (LanceFragmentInfo fragment : visibleFragments.values()) {
            if (!plan.isCoveredByIndexSegment(fragment.getId())) {
                plan.addUnindexedFragmentSplit(fragment);
            }
        }
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
                ? "L2" : metricName(vector.getMetric()).toUpperCase();
        for (LanceIndexSegmentInfo segment : segments) {
            if (!segment.getMetric().isPresent()
                    || !requestedMetric.equals(segment.getMetric().get())) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (!(split instanceof LanceSplit)) {
            throw new IllegalArgumentException("Expected LanceSplit but got " + split.getClass().getName());
        }
        LanceSplit lanceSplit = (LanceSplit) split;
        TLanceFileDesc lanceParams = new TLanceFileDesc();
        lanceParams.setDatasetUri(lanceSplit.getDatasetUri());
        lanceParams.setVersion(lanceSplit.getVersion());
        if (lanceSplit.getFragmentIds().isEmpty()) {
            throw new IllegalArgumentException("Lance scan split must contain fragments");
        }
        if (searchKind == SearchKind.NORMAL && (lanceSplit.getFragmentIds().size() != 1
                || lanceSplit.hasIndexSegmentUuids())) {
            throw new IllegalArgumentException(
                    "Ordinary Lance scan split must contain one fragment and no index segment");
        }
        if (searchKind == SearchKind.FULL_TEXT && !lanceSplit.hasIndexSegmentUuids()) {
            throw new IllegalArgumentException(
                    "Lance full-text search split must contain an FTS index segment");
        }
        lanceParams.setFragmentIds(lanceSplit.getFragmentIds());
        if (lanceSplit.hasIndexSegmentUuids()) {
            List<ByteBuffer> uuids = new ArrayList<>(lanceSplit.getIndexSegmentUuids().size());
            for (UUID uuid : lanceSplit.getIndexSegmentUuids()) {
                ByteBuffer uuidBytes = ByteBuffer.allocate(16);
                uuidBytes.putLong(uuid.getMostSignificantBits());
                uuidBytes.putLong(uuid.getLeastSignificantBits());
                uuidBytes.flip();
                uuids.add(uuidBytes);
            }
            lanceParams.setIndexSegmentUuids(uuids);
        }
        // Push LIMIT into each ordinary fragment scanner only when it is safe to truncate that
        // fragment early. External searches use their own per-split candidate bound.
        if (searchKind == SearchKind.NORMAL && canPushDownLimit()) {
            lanceParams.setLimit(getLimit());
        }

        TTableFormatFileDesc tableFormatParams = new TTableFormatFileDesc();
        tableFormatParams.setTableFormatType(TableFormatType.LANCE.value());
        tableFormatParams.setLanceParams(lanceParams);
        rangeDesc.setTableFormatParams(tableFormatParams);
    }

    @Override
    protected TFileFormatType getFileFormatType() {
        return TFileFormatType.FORMAT_LANCE;
    }

    @Override
    protected List<String> getPathPartitionKeys() {
        return Collections.emptyList();
    }

    @Override
    protected TableIf getTargetTable() {
        if (searchKind != SearchKind.NORMAL) {
            // In search mode desc.getTable() is a FunctionGenTable, but default-value expressions
            // and storage access still belong to the underlying Lance table.
            return lanceTable;
        } else {
            return desc.getTable();
        }
    }

    @Override
    protected Map<String, String> getLocationProperties() {
        // lance-c reads the dataset itself and takes its configuration from lance_storage_options,
        // so these serve only the shared file system layer and the file cache key.
        return lanceTable.getCatalog().getCatalogProperty().getBackendStorageProperties();
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder result = new StringBuilder(super.getNodeExplainString(prefix, detailLevel));
        if (searchKind != SearchKind.NORMAL) {
            if (searchKind == SearchKind.VECTOR) {
                TVectorSearchParams vector =
                        externalSearchRequest.getSearchQuery().getVectorSearch();
                result.append(prefix).append("externalSearchType=VECTOR\n");
                result.append(prefix).append("lanceVectorColumn=")
                        .append(vector.getColumn()).append("\n");
                result.append(prefix).append("lanceMetric=")
                        .append(vector.isSetMetric() ? metricName(vector.getMetric()) : "default")
                        .append("\n");
            } else {
                if (searchKind != SearchKind.FULL_TEXT) {
                    throw new IllegalStateException("Unsupported Lance search kind " + searchKind);
                }
                TFullTextSearchParams fullText =
                        externalSearchRequest.getSearchQuery().getFullTextSearch();
                result.append(prefix).append("externalSearchType=FULL_TEXT\n");
                result.append(prefix).append("lanceFullTextColumn=")
                        .append(fullText.getColumn()).append("\n");
                result.append(prefix).append("lanceFtsCoverageMode=")
                        .append(fullText.getCoverageMode()).append("\n");
                result.append(prefix).append("lanceFtsQueryType=")
                        .append(fullText.getQueryType()).append("\n");
                if (fullText.getQueryType() == TFtsQueryType.MATCH) {
                    result.append(prefix).append("lanceFtsMatchOperator=")
                            .append(fullText.getMatchOperator()).append("\n");
                    result.append(prefix).append("lanceFtsMaxFuzzyDistance=")
                            .append(fullText.getMaxFuzzyDistance()).append("\n");
                } else {
                    result.append(prefix).append("lanceFtsPhraseSlop=")
                            .append(fullText.getPhraseSlop()).append("\n");
                }
            }
            result.append(prefix).append("lanceVersion=")
                    .append(plannedMetadata.getVersion()).append("\n");
            result.append(prefix).append("lanceSearchFragments=")
                    .append(plannedFragments).append("\n");
            result.append(prefix).append("lanceSearchUnindexedFragments=")
                    .append(plannedUnindexedFragments).append("\n");
            result.append(prefix).append("lanceSearchIndexSegments=")
                    .append(plannedIndexSegments).append("\n");
            result.append(prefix).append("lanceSearchIndexFragments=")
                    .append(plannedIndexFragments).append("\n");
        } else {
            result.append(prefix).append("lanceCatalogType=")
                    .append(((LanceExternalCatalog) lanceTable.getCatalog()).getLanceCatalogType()).append("\n");
            result.append(prefix).append("lanceVersion=").append(plannedVersion).append("\n");
            result.append(prefix).append("lanceFragments=").append(plannedFragments).append("\n");
            if (canPushDownLimit()) {
                result.append(prefix).append("lanceLimit=").append(getLimit()).append("\n");
            }
            if (!lancePushdownPredicate.isEmpty()) {
                result.append(prefix).append("lancePushdownPredicate=")
                        .append(lancePushdownPredicate).append("\n");
            }
        }
        return result.toString();
    }

    TExternalSearchRequest createSplitSearchRequest() {
        TExternalSearchRequest splitRequest = externalSearchRequest.deepCopy();
        // Every split must retain enough rows for the later global OFFSET/LIMIT. Applying the
        // logical offset independently inside each split could discard rows that belong to the
        // snapshot-wide result.
        switch (searchKind) {
            case VECTOR:
                TVectorSearchParams vector = splitRequest.getSearchQuery().getVectorSearch();
                vector.setTopK(vector.getTopK() + vector.getOffset());
                vector.setOffset(0);
                break;
            case FULL_TEXT:
                TFullTextSearchParams fullText = splitRequest.getSearchQuery().getFullTextSearch();
                fullText.setTopK(fullText.getTopK() + fullText.getOffset());
                fullText.setOffset(0);
                break;
            case NORMAL:
            default:
                throw new IllegalStateException("Cannot create a search split for " + searchKind);
        }
        return splitRequest;
    }

    private static SearchKind resolveSearchKind(TExternalSearchRequest searchRequest) {
        if (!searchRequest.isSetSearchQuery()) {
            throw new IllegalArgumentException("Lance external search request requires search_query");
        }
        boolean hasVector = searchRequest.getSearchQuery().isSetVectorSearch();
        boolean hasFullText = searchRequest.getSearchQuery().isSetFullTextSearch();
        if (hasVector == hasFullText) {
            throw new IllegalArgumentException(
                    "Lance external search query must set exactly one search kind");
        }
        return hasVector ? SearchKind.VECTOR : SearchKind.FULL_TEXT;
    }

    private static String metricName(TVectorMetric metric) {
        switch (metric) {
            case L2:
                return "l2";
            case COSINE:
                return "cosine";
            case DOT_PRODUCT:
                return "dot";
            case HAMMING:
                return "hamming";
            case DEFAULT:
            default:
                return "default";
        }
    }
}
