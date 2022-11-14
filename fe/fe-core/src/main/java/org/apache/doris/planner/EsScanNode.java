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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.external.EsExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.external.elasticsearch.EsShardPartitions;
import org.apache.doris.external.elasticsearch.EsShardRouting;
import org.apache.doris.external.elasticsearch.EsTablePartitions;
import org.apache.doris.external.elasticsearch.EsUtil;
import org.apache.doris.external.elasticsearch.QueryBuilders;
import org.apache.doris.external.elasticsearch.QueryBuilders.BoolQueryBuilder;
import org.apache.doris.external.elasticsearch.QueryBuilders.QueryBuilder;
import org.apache.doris.statistics.StatisticalType;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * ScanNode for Elasticsearch.
 **/
public class EsScanNode extends ScanNode {

    private static final Logger LOG = LogManager.getLogger(EsScanNode.class);

    private final Random random = new Random(System.currentTimeMillis());
    private Multimap<String, Backend> backendMap;
    private List<Backend> backendList;
    private EsTablePartitions esTablePartitions;
    private List<TScanRangeLocations> shardScanRanges = Lists.newArrayList();
    private EsTable table;
    private QueryBuilder queryBuilder;
    private boolean isFinalized = false;

    public EsScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        this(id, desc, planNodeName, false);
    }

    /**
     * For multicatalog es.
     **/
    public EsScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, boolean esExternalTable) {
        super(id, desc, planNodeName, StatisticalType.ES_SCAN_NODE);
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
        computeColumnFilter();
        assignBackends();
        computeStats(analyzer);
        buildQuery();
    }

    @Override
    public int getNumInstances() {
        return shardScanRanges.size();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return shardScanRanges;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }

        try {
            shardScanRanges = getShardLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }

        isFinalized = true;
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
            properties.put(EsTable.USER, table.getUserName());
        }
        if (table.getPasswd() != null) {
            properties.put(EsTable.PASSWORD, table.getPasswd());
        }
        properties.put(EsTable.HTTP_SSL_ENABLED, String.valueOf(table.isHttpSslEnabled()));
        TEsScanNode esScanNode = new TEsScanNode(desc.getId().asInt());
        if (table.isEnableDocValueScan()) {
            esScanNode.setDocvalueContext(table.docValueContext());
            properties.put(EsTable.DOC_VALUES_MODE, String.valueOf(useDocValueScan(desc, table.docValueContext())));
        }
        if (Config.enable_new_es_dsl) {
            properties.put(EsTable.QUERY_DSL, queryBuilder.toJson());
        }
        if (table.isEnableKeywordSniff() && table.fieldsContext().size() > 0) {
            esScanNode.setFieldsContext(table.fieldsContext());
        }
        esScanNode.setProperties(properties);
        msg.es_scan_node = esScanNode;
    }

    private void assignBackends() throws UserException {
        backendMap = HashMultimap.create();
        backendList = Lists.newArrayList();
        for (Backend be : Env.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                backendMap.put(be.getHost(), be);
                backendList.add(be);
            }
        }
        if (backendMap.isEmpty()) {
            throw new UserException("No Alive backends");
        }
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
        int size = backendList.size();
        int beIndex = random.nextInt(size);
        List<TScanRangeLocations> result = Lists.newArrayList();
        for (EsShardPartitions indexState : selectedIndex) {
            for (List<EsShardRouting> shardRouting : indexState.getShardRoutings().values()) {
                // get backends
                Set<Backend> colocatedBes = Sets.newHashSet();
                int numBe = Math.min(3, size);
                List<TNetworkAddress> shardAllocations = new ArrayList<>();
                for (EsShardRouting item : shardRouting) {
                    shardAllocations.add(item.getHttpAddress());
                }

                Collections.shuffle(shardAllocations, random);
                for (TNetworkAddress address : shardAllocations) {
                    colocatedBes.addAll(backendMap.get(address.getHostname()));
                }
                boolean usingRandomBackend = colocatedBes.size() == 0;
                List<Backend> candidateBeList = Lists.newArrayList();
                if (usingRandomBackend) {
                    for (int i = 0; i < numBe; ++i) {
                        candidateBeList.add(backendList.get(beIndex++ % size));
                    }
                } else {
                    candidateBeList.addAll(colocatedBes);
                    Collections.shuffle(candidateBeList);
                }

                // Locations
                TScanRangeLocations locations = new TScanRangeLocations();
                for (int i = 0; i < numBe && i < candidateBeList.size(); ++i) {
                    TScanRangeLocation location = new TScanRangeLocation();
                    Backend be = candidateBeList.get(i);
                    location.setBackendId(be.getId());
                    location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
                    locations.addToLocations(location);
                }

                // Generate on es scan range
                TEsScanRange esScanRange = new TEsScanRange();
                esScanRange.setEsHosts(shardAllocations);
                esScanRange.setIndex(shardRouting.get(0).getIndexName());
                if (table.getType() != null) {
                    esScanRange.setType(table.getMappingType());
                }
                esScanRange.setShardId(shardRouting.get(0).getShardId());
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
            LOG.debug("ES table {}  scan ranges {}", table.getName(), scratchBuilder.toString());
        }
        return result;
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
                partitionPruner = new RangePartitionPruner(keyRangeById, rangePartitionInfo.getPartitionColumns(),
                        columnFilters);
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
        return output.toString();
    }

    private void buildQuery() {
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
                QueryBuilder queryBuilder = EsUtil.toEsDsl(expr, notPushDownList, fieldsContext);
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
            if (Config.enable_new_es_dsl) {
                conjuncts.removeIf(expr -> !notPushDownList.contains(expr));
            }
        }
    }
}
