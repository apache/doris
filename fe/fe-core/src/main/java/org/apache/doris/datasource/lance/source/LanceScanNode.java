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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LanceTableMetadata;
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
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Scan node for both ordinary Lance table scans and Lance external-search scans.
 *
 * <p>These modes share dataset metadata, storage properties, and BE scan-range serialization.
 * Keeping them in one node prevents those common parts from drifting apart. The search request is
 * also an explicit mode marker: ordinary scans are split by fragment, while the first version of
 * vector search deliberately sends one whole-snapshot split to one scanner so Lance can compute a
 * global TopK result.
 */
public class LanceScanNode extends FileQueryScanNode {
    private LanceExternalTable lanceTable;
    private LanceTableMetadata plannedMetadata;
    private TExternalSearchRequest externalSearchRequest;
    private byte[] lanceSubstraitFilter = new byte[0];
    private String lancePushdownPredicate = "";
    private long plannedVersion = -1;
    private int plannedFragments;

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
    public LanceScanNode(PlanNodeId id, TupleDescriptor desc, LanceExternalTable lanceTable,
            LanceTableMetadata plannedMetadata, TExternalSearchRequest externalSearchRequest,
            SessionVariable sessionVariable) {
        super(id, desc, "LANCE_SCAN_NODE", StatisticalType.LANCE_SCAN_NODE,
                ScanContext.builder().clusterName(sessionVariable.resolveCloudClusterName()).build(),
                false, sessionVariable);
        this.lanceTable = lanceTable;
        this.plannedMetadata = plannedMetadata;
        this.externalSearchRequest = externalSearchRequest.deepCopy();
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();

        if (isExternalSearch()) {
            // Search output comes from the FunctionGenTable because it adds generated columns such
            // as _distance. The real Lance table is still retained for storage and metadata access.
            ExternalUtil.initSchemaInfo(params, -1L, desc.getTable().getColumns());
            params.setExternalSearchRequest(externalSearchRequest.deepCopy());
        } else {
            lanceTable = (LanceExternalTable) desc.getTable();
            ExternalUtil.initSchemaInfo(params, -1L, lanceTable.getColumns());
        }
    }

    @Override
    protected void convertPredicate() {
        if (isExternalSearch()) {
            // The TVF "filter" property is already serialized in externalSearchRequest and is
            // evaluated by Lance before vector search. Outer WHERE conjuncts have different
            // semantics: Doris must keep them here and evaluate them after Lance returns TopK.
        } else {
            plannedMetadata = lanceTable.loadMetadata();
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
            params.setLanceSubstraitFilter(ByteBuffer.wrap(lanceSubstraitFilter));
        }
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        if (isExternalSearch()) {
            plannedVersion = plannedMetadata.getVersion();
            plannedFragments = plannedMetadata.getFragments().size();

            // Do not attach fragment IDs. A vector index is a dataset-wide structure and one
            // scanner must see every fragment visible in this pinned snapshot to produce global
            // TopK. Fragment-level parallel search and result merging are intentionally deferred.
            return Collections.singletonList(LanceSplit.wholeDatasetAtVersion(
                    plannedMetadata.getDatasetUri(), plannedMetadata.getVersion(),
                    plannedMetadata.getRowCount()));
        } else {
            LanceTableMetadata metadata;
            try {
                metadata = plannedMetadata == null ? lanceTable.loadMetadata() : plannedMetadata;
            } catch (RuntimeException e) {
                throw new UserException("Failed to plan Lance fragments: " + e.getMessage(), e);
            }
            plannedVersion = metadata.getVersion();
            plannedFragments = metadata.getFragments().size();
            Set<Long> fragmentIds = new HashSet<>();
            List<Split> splits = new ArrayList<>(plannedFragments);
            for (LanceTableMetadata.LanceFragmentInfo fragment : metadata.getFragments()) {
                if (!fragmentIds.add(fragment.getId())) {
                    throw new UserException("Duplicate Lance fragment id " + fragment.getId()
                            + " at dataset version " + metadata.getVersion());
                }
                splits.add(new LanceSplit(metadata.getDatasetUri(), metadata.getVersion(),
                        fragment.getId(), fragment.getRowCount()));
            }
            return splits;
        }
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
        if (isExternalSearch()) {
            if (lanceSplit.hasFragmentId()) {
                throw new IllegalArgumentException(
                        "Lance external search split must cover the whole dataset");
            }
            // Leaving fragment_ids unset instructs lance-c to scan/search all fragments in the
            // selected dataset version.
        } else {
            if (!lanceSplit.hasFragmentId()) {
                throw new IllegalArgumentException(
                        "Ordinary Lance scan split must contain one fragment");
            }
            lanceParams.setFragmentIds(Collections.singletonList(lanceSplit.getFragmentId()));
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
        LanceExternalCatalog catalog = (LanceExternalCatalog) lanceTable.getCatalog();
        return plannedMetadata == null
                ? catalog.getBackendStorageOptions() : plannedMetadata.getBackendStorageOptions();
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder result = new StringBuilder(super.getNodeExplainString(prefix, detailLevel));
        if (isExternalSearch()) {
            TVectorSearchParams vector = externalSearchRequest.getQuery().getVector();
            result.append(prefix).append("externalSearchType=VECTOR\n");
            result.append(prefix).append("lanceVectorColumn=").append(vector.getColumn()).append("\n");
            result.append(prefix).append("lanceTopK=").append(vector.getTopK()).append("\n");
            result.append(prefix).append("lanceOffset=").append(vector.getOffset()).append("\n");
            result.append(prefix).append("lanceMetric=")
                    .append(vector.isSetMetric() ? metricName(vector.getMetric()) : "default")
                    .append("\n");
            result.append(prefix).append("lanceVersion=")
                    .append(plannedMetadata.getVersion()).append("\n");
            result.append(prefix).append("lanceSearchScanners=1\n");
        } else {
            result.append(prefix).append("lanceCatalogType=")
                    .append(((LanceExternalCatalog) lanceTable.getCatalog()).getLanceCatalogType()).append("\n");
            result.append(prefix).append("lanceVersion=").append(plannedVersion).append("\n");
            result.append(prefix).append("lanceFragments=").append(plannedFragments).append("\n");
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
