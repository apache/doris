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

package org.apache.doris.datasource.es.source;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EsResource;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.es.EsExternalTable;
import org.apache.doris.datasource.es.EsShardPartitions;
import org.apache.doris.datasource.es.EsShardRouting;
import org.apache.doris.datasource.es.EsTablePartitions;
import org.apache.doris.datasource.es.QueryBuilders;
import org.apache.doris.datasource.es.QueryBuilders.BoolQueryBuilder;
import org.apache.doris.datasource.es.QueryBuilders.BuilderOptions;
import org.apache.doris.datasource.es.QueryBuilders.QueryBuilder;
import org.apache.doris.planner.PartitionPruner;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RangePartitionPrunerV2;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.query.StatsDelta;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TEsScanNode;
import org.apache.doris.thrift.TEsScanRange;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ScanNode for Elasticsearch.
 **/
public class EsScanNode extends ExternalScanNode {

    private static final Logger LOG = LogManager.getLogger(EsScanNode.class);

    private final EsTablePartitions esTablePartitions;
    private final EsTable table;
    private QueryBuilder queryBuilder;
    private boolean isFinalized = false;

    public EsScanNode(PlanNodeId id, TupleDescriptor desc) {
        this(id, desc, false);
    }

    /**
     * For multicatalog es.
     **/
    public EsScanNode(PlanNodeId id, TupleDescriptor desc, boolean esExternalTable) {
        super(id, desc, "EsScanNode", StatisticalType.ES_SCAN_NODE, false);
        if (esExternalTable) {
            EsExternalTable externalTable = (EsExternalTable) (desc.getTable());
            table = externalTable.getEsTable();
        } else {
            table = (EsTable) (desc.getTable());
        }
        esTablePartitions = table.getEsTablePartitions();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        buildQuery();
    }

    @Override
    public void init() throws UserException {
        super.init();
        buildQuery();
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        doFinalize();
    }

    @Override
    public void finalizeForNereids() throws UserException {
        doFinalize();
    }

    private void doFinalize() throws UserException {
        if (isFinalized) {
            return;
        }
        createScanRangeLocations();
        isFinalized = true;
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        scanRangeLocations = getShardLocations();
    }

    /**
     * return whether can use the doc_values scan
     * 0 and 1 are returned to facilitate Doris BE processing
     *
     * @param desc the fields needs to read from ES
     * @param docValueContext the mapping for docvalues fields from origin field to doc_value fields
     */
    private int useDocValueScan(TupleDescriptor desc, Map<String, String> docValueContext) {
        ArrayList<SlotDescriptor> slotDescriptors = desc.getSlots();
        List<String> selectedFields = new ArrayList<>(slotDescriptors.size());
        for (SlotDescriptor slotDescriptor : slotDescriptors) {
            selectedFields.add(slotDescriptor.getColumn().getName());
        }
        if (selectedFields.size() > table.getMaxDocValueFields()) {
            return 0;
        }
        Set<String> docValueFields = docValueContext.keySet();
        boolean useDocValue = true;
        for (String selectedField : selectedFields) {
            if (!docValueFields.contains(selectedField)) {
                useDocValue = false;
                break;
            }
        }
        return useDocValue ? 1 : 0;
    }

    @SneakyThrows
    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ES_HTTP_SCAN_NODE;
        Map<String, String> properties = Maps.newHashMap();
        if (table.getUserName() != null) {
            properties.put(EsResource.USER, table.getUserName());
        }
        if (table.getPasswd() != null) {
            properties.put(EsResource.PASSWORD, table.getPasswd());
        }
        properties.put(EsResource.HTTP_SSL_ENABLED, String.valueOf(table.isHttpSslEnabled()));
        TEsScanNode esScanNode = new TEsScanNode(desc.getId().asInt());
        if (table.isEnableDocValueScan()) {
            esScanNode.setDocvalueContext(table.docValueContext());
            properties.put(EsResource.DOC_VALUES_MODE, String.valueOf(useDocValueScan(desc, table.docValueContext())));
        }
        properties.put(EsResource.QUERY_DSL, queryBuilder.toJson());
        if (table.isEnableKeywordSniff() && table.fieldsContext().size() > 0) {
            esScanNode.setFieldsContext(table.fieldsContext());
        }
        esScanNode.setProperties(properties);
        msg.es_scan_node = esScanNode;
        super.toThrift(msg);
    }

    // only do partition(es index level) prune
    private List<TScanRangeLocations> getShardLocations() throws UserException {
        // has to get partition info from es state not from table because the partition
        // info is generated from es cluster state dynamically
        if (esTablePartitions == null) {
            if (table.getLastMetaDataSyncException() != null) {
                throw new UserException("fetch es table [" + table.getName() + "] metadata failure: "
                        + table.getLastMetaDataSyncException().getLocalizedMessage());
            }
            throw new UserException("EsTable metadata has not been synced, Try it later");
        }
        Collection<Long> partitionIds = partitionPrune(esTablePartitions.getPartitionInfo());
        List<EsShardPartitions> selectedIndex = Lists.newArrayList();
        ArrayList<String> unPartitionedIndices = Lists.newArrayList();
        ArrayList<String> partitionedIndices = Lists.newArrayList();
        for (EsShardPartitions esShardPartitions : esTablePartitions.getUnPartitionedIndexStates().values()) {
            selectedIndex.add(esShardPartitions);
            unPartitionedIndices.add(esShardPartitions.getIndexName());
        }
        if (partitionIds != null) {
            for (Long partitionId : partitionIds) {
                EsShardPartitions indexState = esTablePartitions.getEsShardPartitions(partitionId);
                selectedIndex.add(indexState);
                partitionedIndices.add(indexState.getIndexName());
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("partition prune finished, unpartitioned index [{}], " + "partitioned index [{}]",
                    String.join(",", unPartitionedIndices), String.join(",", partitionedIndices));
        }
        List<TScanRangeLocations> result = Lists.newArrayList();
        boolean enableESParallelScroll = isEnableESParallelScroll();
        for (EsShardPartitions indexState : selectedIndex) {
            // When disabling parallel scroll, only use the first shard routing.
            // Because we only need plan a single scan range.
            List<List<EsShardRouting>> shardRoutings = enableESParallelScroll
                    ? new ArrayList<>(indexState.getShardRoutings().values()) :
                    Collections.singletonList(indexState.getShardRoutings().get(0));

            for (List<EsShardRouting> shardRouting : shardRoutings) {
                // get backends
                List<TNetworkAddress> shardAllocations = new ArrayList<>();
                List<String> preLocations = new ArrayList<>();
                for (EsShardRouting item : shardRouting) {
                    shardAllocations.add(item.getHttpAddress());
                    preLocations.add(item.getHttpAddress().getHostname());
                }

                FederationBackendPolicy backendPolicy = new FederationBackendPolicy();
                backendPolicy.init(preLocations);
                TScanRangeLocations locations = new TScanRangeLocations();
                // When disabling parallel scroll, only use the first backend.
                // Because we only need plan a single query to one backend.
                int numBackends = enableESParallelScroll ? backendPolicy.numBackends() : 1;
                for (int i = 0; i < numBackends; ++i) {
                    TScanRangeLocation location = new TScanRangeLocation();
                    Backend be = backendPolicy.getNextBe();
                    location.setBackendId(be.getId());
                    location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
                    locations.addToLocations(location);
                }

                // Generate on es scan range
                TEsScanRange esScanRange = new TEsScanRange();
                esScanRange.setEsHosts(shardAllocations);
                // When disabling parallel scroll, use the index state's index name to prevent the index aliases from
                // being expanded.
                // eg: index alias `log-20240501` may point to multiple indices,
                // such as `log-20240501-1`/`log-20240501-2`.
                // When we plan a single query, we should use the index alias instead of the real indices names.
                esScanRange.setIndex(
                        enableESParallelScroll ? shardRouting.get(0).getIndexName() : indexState.getIndexName());
                if (table.getType() != null) {
                    esScanRange.setType(table.getMappingType());
                }
                // When disabling parallel scroll, set shard id to -1 to disable shard preference in query option.
                esScanRange.setShardId(enableESParallelScroll ? shardRouting.get(0).getShardId() : -1);
                // Scan range
                TScanRange scanRange = new TScanRange();
                scanRange.setEsScanRange(esScanRange);
                locations.setScanRange(scanRange);
                // result
                result.add(locations);
            }

        }
        if (LOG.isDebugEnabled()) {
            StringBuilder scratchBuilder = new StringBuilder();
            for (TScanRangeLocations scanRangeLocations : result) {
                scratchBuilder.append(scanRangeLocations.toString());
                scratchBuilder.append(" ");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("ES table {}  scan ranges {}", table.getName(), scratchBuilder.toString());
            }
        }
        return result;
    }

    private boolean isEnableESParallelScroll() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext != null && connectContext.getSessionVariable().isEnableESParallelScroll();
    }

    /**
     * if the index name is an alias or index pattern, then the es table is related
     * with one or more indices some indices could be pruned by using partition info
     * in index settings currently only support range partition setting
     *
     * @param partitionInfo partitionInfo
     */
    private Collection<Long> partitionPrune(PartitionInfo partitionInfo) throws AnalysisException {
        if (partitionInfo == null) {
            return null;
        }
        PartitionPruner partitionPruner;
        switch (partitionInfo.getType()) {
            case RANGE: {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                Map<Long, PartitionItem> keyRangeById = rangePartitionInfo.getIdToItem(false);
                partitionPruner = new RangePartitionPrunerV2(keyRangeById, rangePartitionInfo.getPartitionColumns(),
                        columnNameToRange);
                return partitionPruner.prune();
            }
            case UNPARTITIONED: {
                return null;
            }
            default: {
                return null;
            }
        }
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(table.getName()).append("\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("LOCAL_PREDICATES: ").append(getExplainString(conjuncts)).append("\n");
        }
        output.append(prefix).append("REMOTE_PREDICATES: ").append(queryBuilder.toJson()).append("\n");
        String indexName = table.getIndexName();
        String typeName = table.getMappingType();
        output.append(prefix).append(String.format("ES index/type: %s/%s", indexName, typeName)).append("\n");
        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }
        return output.toString();
    }

    private void buildQuery() throws UserException {
        if (conjuncts.isEmpty()) {
            queryBuilder = QueryBuilders.matchAllQuery();
        } else {
            // col -> col.keyword
            Map<String, String> fieldsContext = new HashMap<>();
            if (table.isEnableKeywordSniff() && !table.fieldsContext().isEmpty()) {
                fieldsContext = table.fieldsContext();
            }
            boolean hasFilter = false;
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            List<Expr> notPushDownList = new ArrayList<>();
            for (Expr expr : conjuncts) {
                QueryBuilder queryBuilder = QueryBuilders.toEsDsl(expr, notPushDownList, fieldsContext,
                        BuilderOptions.builder().likePushDown(table.isLikePushDown())
                                .needCompatDateFields(table.needCompatDateFields()).build());
                if (queryBuilder != null) {
                    hasFilter = true;
                    boolQueryBuilder.must(queryBuilder);
                }
            }
            if (!hasFilter) {
                queryBuilder = QueryBuilders.matchAllQuery();
            } else {
                queryBuilder = boolQueryBuilder;
            }
            conjuncts.removeIf(expr -> !notPushDownList.contains(expr));
        }
    }

    @Override
    public StatsDelta genStatsDelta() throws AnalysisException {
        return new StatsDelta(Env.getCurrentEnv().getCurrentCatalog().getId(),
                Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(table.getQualifiedDbName()).getId(),
                table.getId(), -1L);
    }
}
