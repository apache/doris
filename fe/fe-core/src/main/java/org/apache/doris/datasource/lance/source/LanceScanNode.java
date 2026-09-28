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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.metadata.LanceSchemaHelper;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.lance.metadata.LanceTypeConverter;
import org.apache.doris.datasource.lance.source.LanceScanPlan.SearchKind;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFtsQueryType;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TLanceScanParams;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchParams;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Scan node for both ordinary Lance table scans and Lance external-search scans.
 *
 * <p>These modes share dataset metadata, storage properties, and BE scan-range serialization.
 * Keeping them in one node prevents those common parts from drifting apart. The search request is
 * also an explicit mode marker. Ordinary scans assign one BTree/Bitmap/LabelList segment per split when
 * a pushed filter and known, disjoint coverage allow it; uncovered fragments use non-indexed scans.
 * Other ordinary scans use fragment splits. Indexed vector searches are split by physical index
 * segment, with uncovered fragments retained as flat-search fallbacks.
 * Full-text searches are split only by committed inverted-index segments, with coverage governed
 * by the request's STRICT or INDEX_ONLY mode. Each search split produces local candidates; a Doris
 * TopN above this scan merges them into the requested snapshot-wide result.
 */
public class LanceScanNode extends FileQueryScanNode {
    private LanceExternalTable lanceTable;
    private LanceTableMetadata plannedMetadata;
    private LanceScanPlan scanPlan = LanceScanPlan.empty();
    private final int searchFieldId;
    private final TExternalSearchRequest externalSearchRequest;
    private final SearchKind searchKind;
    private byte[] lanceSubstraitFilter = new byte[0];
    private String lancePushdownPredicate = "";
    private final Set<String> lazyMaterializedColumns = new HashSet<>();
    private final List<Expr> lancePushedConjuncts = new ArrayList<>();

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
        this.searchKind = SearchKind.fromSearchRequest(this.externalSearchRequest);
    }

    @Override
    protected void doInitialize() throws UserException {
        List<Column> sourceColumns;
        if (searchKind.isExternalSearch()) {
            sourceColumns = desc.getTable().getColumns();
        } else {
            lanceTable = (LanceExternalTable) desc.getTable();
            Optional<MvccSnapshot> relationSnapshot = getRelationSnapshot();
            plannedMetadata = lanceTable.getMetadata(relationSnapshot);
            sourceColumns = LanceSchemaHelper.toDorisColumns(plannedMetadata.getSchema());
        }
        super.doInitialize();
        checkAdditionalTypeBackendCompatibility(
                projectsCurrentReaderType(), backendPolicy.getBackends());
        ExternalUtil.initSchemaInfo(params, -1L, sourceColumns);

        if (searchKind.isExternalSearch()) {
            // Search output comes from the FunctionGenTable because it adds generated columns such
            // as _distance or _score. The real Lance table is retained for storage and metadata.
            getOrCreateLanceScanParams()
                    .setExternalSearchRequest(createSplitSearchRequest());
        }
    }

    public void addLazyMaterializedColumn(String columnName) {
        lazyMaterializedColumns.add(columnName.toLowerCase(Locale.ROOT));
    }

    /** Checks columns read in either phase of a Lance scan. */
    private boolean projectsCurrentReaderType() {
        // Global row IDs route the second-phase take back to the first-phase BE, so lazy pruning
        // must not hide a column's reader requirement when checking mixed-version backends.
        Set<String> projectedColumns = new HashSet<>(lazyMaterializedColumns);
        for (SlotDescriptor slot : desc.getSlots()) {
            if (slot.getColumn() != null) {
                projectedColumns.add(slot.getColumn().getName().toLowerCase(Locale.ROOT));
            }
        }
        for (Field field : plannedMetadata.getSchema().getFields()) {
            if (projectedColumns.contains(field.getName().toLowerCase(Locale.ROOT))
                    && LanceTypeConverter.requiresCurrentBeReader(field)) {
                return true;
            }
        }
        return false;
    }

    /** Rejects old smooth-upgrade source BEs for additional Lance type projections. */
    @VisibleForTesting
    public static void checkAdditionalTypeBackendCompatibility(
            boolean requiresCurrentReader, Iterable<Backend> backends) throws UserException {
        if (!requiresCurrentReader) {
            return;
        }
        for (Backend backend : backends) {
            if (backend.isSmoothUpgradeSrc()) {
                throw new UserException(
                        "Additional Lance types are unavailable while backend "
                                + backend.getId() + " is a smooth upgrade source");
            }
        }
    }

    private TLanceScanParams getOrCreateLanceScanParams() {
        if (!params.isSetLanceScanParams()) {
            params.setLanceScanParams(new TLanceScanParams());
        }
        return params.getLanceScanParams();
    }

    // A split-level LIMIT can be pushed into an ordinary Lance scan only when every predicate
    // is already pushed into Lance (conjuncts is empty). Otherwise Doris re-filters the returned
    // rows and truncating a split early could drop valid results.
    //
    // OFFSET needs no special handling: the Nereids SplitLimit rule rewrites Limit(limit, offset)
    // into a global Limit(limit, offset) over a local Limit(limit + offset, 0), and the local
    // bound is what lands on this scan node. So getLimit() already accounts for the offset and
    // getOffset() is always 0 here; each split fetches up to limit + offset rows and the upper
    // global LIMIT still applies the offset and the final bound.
    private boolean canPushDownLimit() {
        return hasLimit() && conjuncts.isEmpty();
    }

    // COUNT(*)/COUNT(1) can be answered from Lance metadata only when nothing narrows the row set:
    // no residual Doris conjunct and no predicate pushed into Lance. Any filter would make the
    // dataset-wide logical row count larger than the real result, so this is stricter than
    // canPushDownLimit(), which still allows predicates already pushed into Lance.
    private boolean canPushDownCountStar() {
        return searchKind == SearchKind.NORMAL && isTableLevelCountStarPushdown()
                && conjuncts.isEmpty() && lanceSubstraitFilter.length == 0;
    }

    @Override
    protected void convertPredicate() {
        if (searchKind.isExternalSearch()) {
            // The TVF "filter" property is already serialized in externalSearchRequest and is
            // evaluated by Lance before candidate search. Outer WHERE conjuncts have different
            // semantics: keep them as Doris scan residuals. Each fragment first returns its Lance
            // ANN candidates, then Doris evaluates these conjuncts before the local/global TopN.
        } else {
            LancePredicateConverter.ConversionResult result =
                    new LancePredicateConverter(plannedMetadata.getSchema()).convert(conjuncts);
            lanceSubstraitFilter = result.getSubstraitFilter();
            lancePushdownPredicate = result.getDebugPredicate();
            lancePushedConjuncts.clear();
            lancePushedConjuncts.addAll(result.getPushedConjuncts());
            conjuncts.clear();
            conjuncts.addAll(result.getResidualConjuncts());
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
        int countParallelism = 0;
        if (canPushDownCountStar()) {
            setPushDownCount(plannedMetadata.getRowCount());
            countParallelism = Math.max(1, sessionVariable.getParallelExecInstanceNum(scanContext.getClusterName())
                    * Math.max(numBackends, 1));
        }
        scanPlan = LanceScanPlanner.plan(plannedMetadata, searchKind, externalSearchRequest, searchFieldId,
                lancePushedConjuncts, sessionVariable.lanceFragmentsPerSplit, countParallelism);
        return scanPlan.createSchedulingSplits();
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
        if (lanceSplit.hasFragmentIds()) {
            if (searchKind == SearchKind.FULL_TEXT && lanceSplit.getKind() != LanceSplit.Kind.INDEX_SEGMENT) {
                throw new IllegalArgumentException(
                        "Lance full-text search split must contain an FTS index segment");
            }
            lanceParams.setFragmentIds(lanceSplit.getFragmentIds());
            lanceSplit.getIndexSegmentUuid().ifPresent(uuid -> {
                ByteBuffer uuidBytes = ByteBuffer.allocate(16);
                uuidBytes.putLong(uuid.getMostSignificantBits());
                uuidBytes.putLong(uuid.getLeastSignificantBits());
                uuidBytes.flip();
                lanceParams.setIndexSegmentUuids(Collections.singletonList(uuidBytes));
            });
        } else if (!lanceSplit.isMetadataCount()) {
            // Only the metadata COUNT(*) split may omit fragment ids; it opens no BE scanner and
            // BE serves the row count from table_level_row_count below, leaving fragment_ids unset.
            throw new IllegalArgumentException("Lance scan split must contain fragments");
        }
        if (lanceSplit.isScalarIndexDisabled()) {
            // Uncovered fragments belong to separate tasks. Do not repeat global index
            // evaluation on these tasks; the complete filter still applies to their rows.
            lanceParams.setUseScalarIndex(false);
        }
        // Push LIMIT into each ordinary split scanner only when it is safe to truncate that
        // split early. External searches use their own per-split candidate bound.
        if (searchKind == SearchKind.NORMAL && canPushDownLimit()) {
            lanceParams.setLimit(getLimit());
        }

        TTableFormatFileDesc tableFormatParams = new TTableFormatFileDesc();
        tableFormatParams.setTableFormatType(TableFormatType.LANCE.value());
        // Match the Iceberg convention: always set explicitly, -1 for ordinary and search scans
        // so BE never mistakes a stale value for a metadata count.
        tableFormatParams.setTableLevelRowCount(lanceSplit.getTableLevelRowCount());
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
        if (searchKind.isExternalSearch()) {
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
        if (searchKind.isExternalSearch()) {
            if (searchKind == SearchKind.VECTOR) {
                TVectorSearchParams vector =
                        externalSearchRequest.getSearchQuery().getVectorSearch();
                result.append(prefix).append("externalSearchType=VECTOR\n");
                result.append(prefix).append("lanceVectorIndexStatus=")
                        .append(scanPlan.vectorIndexStatus).append("\n");
                result.append(prefix).append("lanceVectorColumn=")
                        .append(vector.getColumn()).append("\n");
                result.append(prefix).append("lanceMetric=")
                        .append(vector.isSetMetric()
                                ? VectorSearchTableValuedFunction.metricName(vector.getMetric()) : "default")
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
                    .append(scanPlan.fragmentCount).append("\n");
            result.append(prefix).append("lanceSearchUnindexedFragments=")
                    .append(scanPlan.unindexedFragmentCount).append("\n");
            result.append(prefix).append("lanceSearchIndexSegments=")
                    .append(scanPlan.indexSegmentCount).append("\n");
            result.append(prefix).append("lanceSearchIndexFragments=")
                    .append(scanPlan.indexedFragmentCount).append("\n");
        } else {
            result.append(prefix).append("lanceCatalogType=")
                    .append(((LanceExternalCatalog) lanceTable.getCatalog()).getLanceCatalogType()).append("\n");
            result.append(prefix).append("lanceVersion=").append(scanPlan.version).append("\n");
            result.append(prefix).append("lanceFragments=").append(scanPlan.fragmentCount).append("\n");
            if (scanPlan.fragmentsPerSplit > 0) {
                result.append(prefix).append("lanceFragmentGrouping=DEBUG\n");
                result.append(prefix).append("lanceFragmentsPerSplit=")
                        .append(scanPlan.fragmentsPerSplit).append("\n");
            } else if (scanPlan.scalarIndexName == null) {
                result.append(prefix).append("lanceFragmentGrouping=FRAGMENT\n");
            } else {
                result.append(prefix).append("lanceFragmentGrouping=INDEX_SEGMENT\n");
                result.append(prefix).append("lanceScalarIndexScan=SEGMENT\n");
                result.append(prefix).append("lanceGroupingIndex=").append(scanPlan.scalarIndexName).append("\n");
                result.append(prefix).append("lanceGroupingIndexSegments=")
                        .append(scanPlan.indexSegmentCount).append("\n");
                result.append(prefix).append("lanceGroupingIndexedFragments=")
                        .append(scanPlan.indexedFragmentCount).append("\n");
                result.append(prefix).append("lanceGroupingUnindexedFragments=")
                        .append(scanPlan.unindexedFragmentCount).append("\n");
            }
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
                // A scanner must not infer a different metric from its local index coverage.
                if (!vector.isSetMetric() || vector.getMetric() == TVectorMetric.DEFAULT) {
                    vector.setMetric(TVectorMetric.L2);
                }
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
}
