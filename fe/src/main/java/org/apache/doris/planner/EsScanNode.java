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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.external.EsIndexState;
import org.apache.doris.external.EsShardRouting;
import org.apache.doris.external.EsTableState;
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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class EsScanNode extends ScanNode {
    
    private static final Logger LOG = LogManager.getLogger(EsScanNode.class);

    private final Random random = new Random(System.currentTimeMillis());
    private Multimap<String, Backend> backendMap;
    private List<Backend> backendList;
    private EsTableState esTableState;
    private List<TScanRangeLocations> shardScanRanges = Lists.newArrayList();
    private EsTable table;

    boolean isFinalized = false;

    public EsScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        table = (EsTable) (desc.getTable());
        esTableState = table.getEsTableState();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        
        assignBackends();
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

    @Override
    protected void toThrift(TPlanNode msg) {
        if (EsTable.TRANSPORT_HTTP.equals(table.getTransport())) {
            msg.node_type = TPlanNodeType.ES_HTTP_SCAN_NODE;
        } else {
            msg.node_type = TPlanNodeType.ES_SCAN_NODE;
        }
        Map<String, String> properties = Maps.newHashMap();
        properties.put(EsTable.USER, table.getUserName());
        properties.put(EsTable.PASSWORD, table.getPasswd());
        TEsScanNode esScanNode = new TEsScanNode(desc.getId().asInt());
        esScanNode.setProperties(properties);
        if (table.isDocValueScanEnable()) {
            esScanNode.setDocvalue_context(table.docValueContext());
        }
        if (table.isKeywordSniffEnable() && table.fieldsContext().size() > 0) {
            esScanNode.setFields_context(table.fieldsContext());
        }
        msg.es_scan_node = esScanNode;
    }

    // TODO(ygl) assign backend that belong to the same cluster
    private void assignBackends() throws UserException {
        backendMap = HashMultimap.create();
        backendList = Lists.newArrayList();
        for (Backend be : Catalog.getCurrentSystemInfo().getIdToBackend().values()) {
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
    // TODO (ygl) should not get all shards, prune unrelated shard 
    private List<TScanRangeLocations> getShardLocations() throws UserException {
        // has to get partition info from es state not from table because the partition info is generated from es cluster state dynamically
        if (esTableState == null) {
            throw new UserException("EsTable shard info has not been synced, wait some time or check log");
        }
        Collection<Long> partitionIds = partitionPrune(esTableState.getPartitionInfo()); 
        List<EsIndexState> selectedIndex = Lists.newArrayList();
        ArrayList<String> unPartitionedIndices = Lists.newArrayList();
        ArrayList<String> partitionedIndices = Lists.newArrayList();
        for (EsIndexState esIndexState : esTableState.getUnPartitionedIndexStates().values()) {
            selectedIndex.add(esIndexState);
            unPartitionedIndices.add(esIndexState.getIndexName());
        }
        if (partitionIds != null) {
            for (Long partitionId : partitionIds) {
                EsIndexState indexState = esTableState.getIndexState(partitionId);
                selectedIndex.add(indexState);
                partitionedIndices.add(indexState.getIndexName());
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("partition prune finished, unpartitioned index [{}], " 
                    + "partitioned index [{}]", 
                    String.join(",", unPartitionedIndices), 
                    String.join(",", partitionedIndices));
        }
        int beIndex = random.nextInt(backendList.size());
        List<TScanRangeLocations> result = Lists.newArrayList();
        for (EsIndexState indexState : selectedIndex) {
            for (List<EsShardRouting> shardRouting : indexState.getShardRoutings().values()) {
                // get backends
                Set<Backend> colocatedBes = Sets.newHashSet();
                int numBe = Math.min(3, backendMap.size());
                List<TNetworkAddress> shardAllocations = new ArrayList<>();
                for (EsShardRouting item : shardRouting) {
                    shardAllocations.add(EsTable.TRANSPORT_HTTP.equals(table.getTransport()) ? item.getHttpAddress() : item.getAddress());
                }

                Collections.shuffle(shardAllocations, random);
                for (TNetworkAddress address : shardAllocations) {
                    colocatedBes.addAll(backendMap.get(address.getHostname()));
                }
                boolean usingRandomBackend = colocatedBes.size() == 0;
                List<Backend> candidateBeList = Lists.newArrayList();
                if (usingRandomBackend) {
                    for (int i = 0; i < numBe; ++i) {
                        candidateBeList.add(backendList.get(beIndex++ % numBe));
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
                    location.setBackend_id(be.getId());
                    location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
                    locations.addToLocations(location);
                }

                // Generate on es scan range
                TEsScanRange esScanRange = new TEsScanRange();
                esScanRange.setEs_hosts(shardAllocations);
                esScanRange.setIndex(indexState.getIndexName());
                esScanRange.setType(table.getMappingType());
                esScanRange.setShard_id(shardRouting.get(0).getShardId());
                // Scan range
                TScanRange scanRange = new TScanRange();
                scanRange.setEs_scan_range(esScanRange);
                locations.setScan_range(scanRange);
                // result
                result.add(locations);
            }

        }
        LOG.debug("generate [{}] scan ranges to scan node [{}]", result.size(), result.get(0));
        return result;
    }

    /**
     * if the index name is an alias or index pattern, then the es table is related
     * with one or more indices some indices could be pruned by using partition info
     * in index settings currently only support range partition setting
     * 
     * @param partitionInfo
     * @return
     * @throws AnalysisException
     */
    private Collection<Long> partitionPrune(PartitionInfo partitionInfo) throws AnalysisException {
        if (partitionInfo == null) {
            return null;
        }
        PartitionPruner partitionPruner = null;
        switch (partitionInfo.getType()) {
            case RANGE: {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    Map<Long, Range<PartitionKey>> keyRangeById = rangePartitionInfo.getIdToRange(false);
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
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(table.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
            // reserved for later using: LOCAL_PREDICATES is processed by Doris EsScanNode
            output.append(prefix).append("LOCAL_PREDICATES: ").append(" ").append("\n");
            // reserved for later using: REMOTE_PREDICATES is processed by remote ES Cluster
            output.append(prefix).append("REMOTE_PREDICATES: ").append(" ").append("\n");
            // reserved for later using: translate predicates to ES queryDSL
            output.append(prefix).append("ES_QUERY_DSL: ").append(" ").append("\n");
        } else {
            output.append(prefix).append("ES_QUERY_DSL: ").append("{\"match_all\": {}}").append("\n");
        }
        String indexName = table.getIndexName();
        String typeName = table.getMappingType();
        output.append(prefix)
                .append(String.format("ES index/type: %s/%s", indexName, typeName))
                .append("\n");
        return output.toString();
    }
}
