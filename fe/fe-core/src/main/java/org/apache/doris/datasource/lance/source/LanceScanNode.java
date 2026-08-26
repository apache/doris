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
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TLanceScanParams;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TVectorMetric;
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
 * Each search split produces local candidates; a Doris TopN above this scan merges them into the
 * requested snapshot-wide result.
 */
public class LanceScanNode extends FileQueryScanNode {
    private LanceExternalTable lanceTable;
    private LanceTableMetadata plannedMetadata;
    private int vectorFieldId = -1;
    private TExternalSearchRequest externalSearchRequest;
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
    }

    /**
     * Creates the search mode of this node.
     *
     * <p>The tuple descriptor belongs to a FunctionGenTable and contains generated columns such as
     * {@code _distance}. Therefore the real Lance table and the metadata snapshot selected while
     * analyzing the TVF must be passed separately.
     */
    public static LanceScanNode forVectorSearch(PlanNodeId id, TupleDescriptor desc,
            LanceExternalTable lanceTable, LanceTableMetadata plannedMetadata, int vectorFieldId,
            TExternalSearchRequest externalSearchRequest, SessionVariable sessionVariable) {
        return new LanceScanNode(id, desc, lanceTable, plannedMetadata, vectorFieldId,
                externalSearchRequest, sessionVariable);
    }

    private LanceScanNode(PlanNodeId id, TupleDescriptor desc, LanceExternalTable lanceTable,
            LanceTableMetadata plannedMetadata, int vectorFieldId,
            TExternalSearchRequest externalSearchRequest, SessionVariable sessionVariable) {
        super(id, desc, "LANCE_SCAN_NODE", StatisticalType.LANCE_SCAN_NODE,
                ScanContext.builder().clusterName(sessionVariable.resolveCloudClusterName()).build(),
                false, sessionVariable);
        this.lanceTable = lanceTable;
        this.plannedMetadata = plannedMetadata;
        this.vectorFieldId = vectorFieldId;
        this.externalSearchRequest = externalSearchRequest.deepCopy();
    }

    @Override
    protected void doInitialize() throws UserException {
        List<Column> sourceColumns;
        if (isExternalSearch()) {
            sourceColumns = desc.getTable().getColumns();
        } else {
            lanceTable = (LanceExternalTable) desc.getTable();
            Optional<MvccSnapshot> relationSnapshot = getRelationSnapshot();
            plannedMetadata = lanceTable.getMetadata(relationSnapshot);
            sourceColumns = lanceTable.getFullSchema(relationSnapshot);
        }
        super.doInitialize();
        ExternalUtil.initSchemaInfo(params, -1L, sourceColumns);

        if (isExternalSearch()) {
            // Search output comes from the FunctionGenTable because it adds generated columns such
            // as _distance. The real Lance table is still retained for storage and metadata access.
            getOrCreateLanceScanParams()
                    .setExternalSearchRequest(createFragmentSearchRequest(externalSearchRequest));
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
        if (isExternalSearch()) {
            // The TVF "filter" property is already serialized in externalSearchRequest and is
            // evaluated by Lance before vector search. Outer WHERE conjuncts have different
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
        plannedUnindexedFragments = isExternalSearch() ? plannedFragments : 0;
        plannedIndexSegments = 0;
        plannedIndexFragments = 0;
        if (isExternalSearch() && plannedVersion <= 0) {
            throw new UserException(
                    "Lance vector search requires a fixed positive dataset version");
        }

        Map<Long, LanceFragmentInfo> visibleFragments = getVisibleFragments(metadata);
        if (isExternalSearch() && shouldUseIndex()) {
            Optional<List<Split>> indexSplits = createIndexSegmentSplits(metadata, visibleFragments);
            if (indexSplits.isPresent()) {
                return indexSplits.get();
            }
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

    private Optional<List<Split>> createIndexSegmentSplits(LanceTableMetadata metadata,
            Map<Long, LanceFragmentInfo> visibleFragments) throws UserException {
        if (metadata.getIndexSegments().isEmpty()) {
            return Optional.empty();
        }
        TVectorSearchParams vectorSearchParam = externalSearchRequest.getSearchQuery().getVectorSearch();
        if (vectorFieldId < 0) {
            throw new UserException("Lance vector column '" + vectorSearchParam.getColumn()
                    + "' has no field ID in the Lance schema");
        }

        List<LanceIndexSegmentInfo> matchingSegments = selectIndexSegments(
                metadata.getIndexSegments(), vectorFieldId);
        if (matchingSegments.isEmpty() || !metricMatches(vectorSearchParam, matchingSegments)) {
            return Optional.empty();
        }

        Optional<IndexSegmentSplitPlan> indexPlan = planIndexSegments(
                metadata, matchingSegments, visibleFragments);
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

    private static List<LanceIndexSegmentInfo> selectIndexSegments(
            List<LanceIndexSegmentInfo> indexSegments, int vectorFieldId) {
        List<LanceIndexSegmentInfo> selectedSegments = new ArrayList<>();
        String selectedIndexName = null;
        for (LanceIndexSegmentInfo segment : indexSegments) {
            if (!segment.getFieldIds().contains(vectorFieldId)) {
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

    private static Optional<IndexSegmentSplitPlan> planIndexSegments(
            LanceTableMetadata metadata,
            List<LanceIndexSegmentInfo> indexSegments,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        IndexSegmentSplitPlan plan = new IndexSegmentSplitPlan(
                metadata.getDatasetUri(), metadata.getVersion(), indexSegments.size());
        for (LanceIndexSegmentInfo segment : indexSegments) {
            Optional<List<Long>> segmentFragments = segment.getFragmentIds();
            if (!segmentFragments.isPresent()) {
                return Optional.empty();
            }
            List<Long> visibleIndexSegmentFragmentIds = effectiveFragmentIds(
                    segmentFragments.get(), visibleFragments);
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

    private boolean shouldUseIndex() {
        return !externalSearchRequest.isSetVectorSearchOptions()
                || !externalSearchRequest.getVectorSearchOptions().isSetUseIndex()
                || externalSearchRequest.getVectorSearchOptions().isUseIndex();
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
        if (!isExternalSearch() && (lanceSplit.getFragmentIds().size() != 1
                || lanceSplit.hasIndexSegmentUuids())) {
            throw new IllegalArgumentException(
                    "Ordinary Lance scan split must contain one fragment and no index segment");
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
        // fragment early. Vector search uses its own per-split candidate bound.
        if (!isExternalSearch() && canPushDownLimit()) {
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
        if (isExternalSearch()) {
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
        if (isExternalSearch()) {
            TVectorSearchParams vector = externalSearchRequest.getSearchQuery().getVectorSearch();
            result.append(prefix).append("externalSearchType=VECTOR\n");
            result.append(prefix).append("lanceMetric=")
                    .append(vector.isSetMetric() ? metricName(vector.getMetric()) : "default")
                    .append("\n");
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

    private boolean isExternalSearch() {
        return externalSearchRequest != null;
    }

    static TExternalSearchRequest createFragmentSearchRequest(TExternalSearchRequest searchRequest) {
        TExternalSearchRequest fragmentRequest = searchRequest.deepCopy();
        TVectorSearchParams vector = fragmentRequest.getSearchQuery().getVectorSearch();
        // Every fragment must retain enough rows for the later global OFFSET/LIMIT. Applying the
        // logical offset independently inside each fragment could discard rows that belong to the
        // snapshot-wide result.
        vector.setTopK(vector.getTopK() + vector.getOffset());
        vector.setOffset(0);
        return fragmentRequest;
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
