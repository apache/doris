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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.MaxLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.external.elasticsearch.EsMajorVersion;
import org.apache.doris.external.elasticsearch.EsShardPartitions;
import org.apache.doris.external.elasticsearch.EsShardRouting;
import org.apache.doris.external.elasticsearch.EsTablePartitions;
import org.apache.doris.qe.ConnectContext;
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
import org.apache.doris.thrift.TExprOpcode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class EsScanNode extends ScanNode {

    private static final Logger LOG = LogManager.getLogger(EsScanNode.class);

    private final Random random = new Random(System.currentTimeMillis());
    private Multimap<String, Backend> backendMap;
    private List<Backend> backendList;
    private EsTablePartitions esTablePartitions;
    private List<TScanRangeLocations> shardScanRanges = Lists.newArrayList();
    private EsTable table;

    private AggregateInfo aggInfo;
    private List<String> group_by_column_names = Lists.newArrayList();
    private List<String> aggregate_column_names = Lists.newArrayList();
    private List<String> aggregate_function_names = Lists.newArrayList();

    private List<Expr> es_scan_conjuncts_when_aggregate;
    boolean isFinalized = false;
    boolean isAggregated = false;

    public EsScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        table = (EsTable) (desc.getTable());
        esTablePartitions = table.getEsTablePartitions();
    }

    public void pushDownAggregationNode(AggregateInfo aggInfo) {
        this.aggInfo = aggInfo;
        this.planNodeName = "EsAggregationNode";
        this.tupleIds = aggInfo.getIntermediateTupleId().asList();
        this.isAggregated = true;
        es_scan_conjuncts_when_aggregate = conjuncts;
        conjuncts = Lists.newArrayList();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        computeColumnFilter();
        assignBackends();
        computeStats(analyzer);
    }

    @Override
    public int getNumInstances() {
        return shardScanRanges.size();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return shardScanRanges;
    }

    public AggregateInfo getAggInfo() {
        return aggInfo;
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
     * @param desc            the fields needs to read from ES
     * @param docValueContext the mapping for docvalues fields from origin field to doc_value fields
     * @return
     */
    private int useDocValueScan(TupleDescriptor desc, Map<String, String> docValueContext) {
        ArrayList<SlotDescriptor> slotDescriptors = desc.getSlots();
        List<String> selectedFields = new ArrayList<>(slotDescriptors.size());
        for (SlotDescriptor slotDescriptor : slotDescriptors) {
            selectedFields.add(slotDescriptor.getColumn().getName());
        }
        if (selectedFields.size() > table.maxDocValueFields()) {
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
        properties.put(EsTable.HTTP_SSL_ENABLED, String.valueOf(table.isHttpSslEnabled()));
        TEsScanNode esScanNode = new TEsScanNode(desc.getId().asInt());
        esScanNode.setProperties(properties);
        if (table.isDocValueScanEnable()) {
            esScanNode.setDocvalueContext(table.docValueContext());
            properties.put(EsTable.DOC_VALUES_MODE, String.valueOf(useDocValueScan(desc, table.docValueContext())));
        }
        if (table.isKeywordSniffEnable() && table.fieldsContext().size() > 0) {
            esScanNode.setFieldsContext(table.fieldsContext());
        }
        if (isAggregated) {
            esScanNode.setIsAggregated(true);
            esScanNode.setIntermediateTupleId(aggInfo.getIntermediateTupleId().asInt());
            esScanNode.setAggregateFunctions(aggregate_function_names);
            esScanNode.setAggregateColumnNames(aggregate_column_names);

            List<Expr> groupingExprs = aggInfo.getGroupingExprs();
            if (groupingExprs != null) {
                // esScanNode.setGroupingExprs(Expr.treesToThrift(groupingExprs));
                esScanNode.setGroupByColumnNames(group_by_column_names);
            }

            if (es_scan_conjuncts_when_aggregate != null) {
                esScanNode.setEsScanConjunctsWhenAggregate(Expr.treesToThrift(es_scan_conjuncts_when_aggregate));
            }
        }
        msg.es_scan_node = esScanNode;
    }

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
    private List<TScanRangeLocations> getShardLocations() throws UserException {
        // has to get partition info from es state not from table because the partition info is generated from es cluster state dynamically
        if (esTablePartitions == null) {
            if (table.getLastMetaDataSyncException() != null) {
                throw new UserException("fetch es table [" + table.getName() + "] metadata failure: " + table.getLastMetaDataSyncException().getLocalizedMessage());
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
            LOG.debug("partition prune finished, unpartitioned index [{}], "
                            + "partitioned index [{}]",
                    String.join(",", unPartitionedIndices),
                    String.join(",", partitionedIndices));
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
                esScanRange.setType(table.getMappingType());
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

        if (isAggregated) {
            if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
                output.append(prefix + "output: ").append(
                        getExplainString(aggInfo.getAggregateExprs()) + "\n");
            }
            // TODO: group by can be very long. Break it into multiple lines
            output.append(prefix + "group by: ").append(
                    getExplainString(aggInfo.getGroupingExprs()) + "\n");
        }

        output.append(prefix).append("TABLE: ").append(table.getName()).append("\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }

        List<Expr> es_scan_conjuncts = isAggregated ? es_scan_conjuncts_when_aggregate : conjuncts;
        if (!es_scan_conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(es_scan_conjuncts)).append("\n");
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

    public boolean supportAggregationPushDown(AggregationNode aggregationNode) {
        if (!ConnectContext.get().getSessionVariable().isEnablePushDownAggToES()) {
            return false;
        }

        if (table.syncEsVersion().before(EsMajorVersion.V_7_X)) {
            // before 6.5, composite group by query hasn't been supported.
            return false;
        }

        if (this.hasLimit()) {
            // select avg from (select * from t1 limit x) t group by xx
            return false;
        }
        
        ImmutableSet<String> functions = ImmutableSet.of("sum","avg","count","min","max");
        if (!(aggregationNode.getChildren().size() == 1
                && aggregationNode.getChild(0) instanceof EsScanNode)) {
            return false;
        }

        AggregateInfo aggInfo = aggregationNode.getAggInfo();

        // aggregation Expr check
        for (FunctionCallExpr function : aggInfo.getMaterializedAggregateExprs()) {
            if (functions.contains(function.getFnName().getFunction())) {
                aggregate_function_names.add(function.getFnName().getFunction());
            } else {
                return false;
            }
        }

        // group by Expr check
        for (Expr groupByExpr : aggInfo.getGroupingExprs()) {
            if (!(groupByExpr instanceof SlotRef)) {
                return false;
            }
        }

        // ensure intermediate/output tuple desc is string/int/double/date
        // eg : avg(decimal value col), intermediate slot is varchar, but output slot is decimal
        for (SlotDescriptor slotDescriptor : aggInfo.getIntermediateTupleDesc().getSlots()) {
            if (!supportAggregationResultType(slotDescriptor)) {
                return false;
            }
        }
        for (SlotDescriptor slotDescriptor : aggInfo.getOutputTupleDesc().getSlots()) {
            if (!supportAggregationResultType(slotDescriptor)) {
                return false;
            }
        }

        for (SlotDescriptor slotDescriptor : aggInfo.getIntermediateTupleDesc().getSlots()) {
            List<Expr> sourceExprs = slotDescriptor.getSourceExprs();
            if (sourceExprs.size() == 1) {
                if (sourceExprs.get(0) instanceof SlotRef) {
                    SlotRef colRef = (SlotRef) sourceExprs.get(0);
                    group_by_column_names.add(colRef.getColumnName());
                    // it's a group by column
                } else if (sourceExprs.get(0) instanceof FunctionCallExpr) {
                    // it's a aggregation Function column
                    FunctionCallExpr functionCallExpr = (FunctionCallExpr) sourceExprs.get(0);
                    FunctionParams functionParams = functionCallExpr.getParams();
                    List<Expr> functionParamExprs = null;
                    if (functionParams != null) {
                        functionParamExprs = functionParams.exprs();
                    }
                    if (functionParamExprs != null && functionParamExprs.size() == 1) {
                        // only support agg(col), not support agg(1/1.0)
                        // count is special, count(1/*) is ok, count(col) is not allowed now
                        if (functionParamExprs.get(0) instanceof CastExpr) {
                            // it's ok, maybe sum(int) -> bigint[sum(int)]
                            CastExpr castExpr = (CastExpr) functionParamExprs.get(0);
                            SlotRef colRef = castExpr.unwrapSlotRef();
                            if (colRef == null || castExpr.getChildren().size() != 1) {
                                return false;
                            }
                            aggregate_column_names.add(colRef.getColumnName());
                        } else if (functionParamExprs.get(0) instanceof SlotRef) {
                            SlotRef colRef = (SlotRef) functionParamExprs.get(0);
                            aggregate_column_names.add(colRef.getColumnName());
                        } else {
                            if (!functionCallExpr.getFnName().getFunction().equals("count")) {
                                return false;
                            }
                            String label_ = slotDescriptor.getLabel();
                            if (label_.equals("count(1)")) {
                                aggregate_column_names.add("");
                            } else {
                                return false;
                            }
                        }
                    } else if (functionParamExprs == null && functionParams.isStar()){
                        aggregate_column_names.add("");
                        Preconditions.checkState(slotDescriptor.getLabel().equals("count(*)"));
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }

        // predicate check
        if (!supportConjunctPushDown()) return false;

        // check field can do aggregate operator or not
        for (String group_field : group_by_column_names) {
            if (!table.docValueContext().containsKey(group_field)) {
                return false;
            }
        }
        for (String aggregate_field : aggregate_column_names) {
            if (aggregate_field.isEmpty()) {
                continue;
            }
            if (!table.docValueContext().containsKey(aggregate_field)) {
                return false;
            }
        }

        // Special case 1 : Es Aggregation do not support string
        {
            Map<String, Type> col_name_to_functions= new HashMap<>();
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                String colName = slotDescriptor.getColumn().getName();
                col_name_to_functions.put(colName, slotDescriptor.getType());
            }
            Preconditions.checkState(aggregate_column_names.size() == aggregate_function_names.size());
            for (int i = 0; i < aggregate_function_names.size(); i++) {
                if (aggregate_function_names.get(i).equals("max") || aggregate_function_names.get(i).equals("min")) {
                    if (!col_name_to_functions.containsKey(aggregate_column_names.get(i))) {
                        return false;
                    }
                    Type type = col_name_to_functions.get(aggregate_column_names.get(i));
                    if (type.isStringType()) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private boolean supportConjunctPushDown() {
        Set<SlotId> slotIds = new HashSet<>();
        for (SlotDescriptor slotDescriptor : desc.getSlots()) {
            slotIds.add(slotDescriptor.getId());
        }

        for (Expr conjunctExpr : conjuncts) {
            if (!supportExprPushDown(conjunctExpr, slotIds)) {
                return false;
            }
        }
        return true;
    }

    private boolean supportExprPushDown(Expr conjunctExpr, Set<SlotId> slotIds) {
        if (conjunctExpr instanceof BinaryPredicate) {
            Expr expr;
            SlotRef slot_ref;
            if (conjunctExpr.getChildren().size() != 2) return false;
            if (conjunctExpr.getChild(0) instanceof SlotRef ||
                    conjunctExpr.getChild(0) instanceof CastExpr) {
                expr = conjunctExpr.getChild(1);
                slot_ref = conjunctExpr.getChild(0).unwrapSlotRef();
                if (slot_ref == null) return false;
            } else if (conjunctExpr.getChild(1) instanceof SlotRef ||
                    conjunctExpr.getChild(1) instanceof CastExpr) {
                expr = conjunctExpr.getChild(0);
                slot_ref = conjunctExpr.getChild(1).unwrapSlotRef();
                if (slot_ref == null) return false;
            } else {
                return false;
            }

            if (!slotIds.contains(slot_ref.getSlotId())) return false;
            if (expr instanceof LiteralExpr) {
                if (expr instanceof MaxLiteral || expr instanceof NullLiteral) {
                    return false;
                }
            } else {
                return false;
            }
            return true;
        }

        if (conjunctExpr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) conjunctExpr;
            String fn_name = functionCallExpr.getFnName().getFunction();
            if (fn_name.equals("esquery")) {
                if (functionCallExpr.getChildren().size() != 2) return false;
            } else {
                return false;
            }
            return true;
        } else if (conjunctExpr instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) conjunctExpr;
            if (isNullPredicate.getChildren().size() != 1) return false;
            SlotRef slot_ref = isNullPredicate.getChild(0).unwrapSlotRef();
            if (slot_ref == null || !slotIds.contains(slot_ref.getSlotId())) return false;
            return true;
        } else if (conjunctExpr instanceof LikePredicate) {
            SlotRef slot_ref;
            Expr expr;
            LikePredicate likePredicate = (LikePredicate) conjunctExpr;
            if (likePredicate.getChildren().size() != 2) return false;
            if (likePredicate.getChild(0) instanceof SlotRef) {
                slot_ref = likePredicate.getChild(0).unwrapSlotRef();
                expr = likePredicate.getChild(1);
                if (slot_ref == null) return false;
            } else if (likePredicate.getChild(1) instanceof SlotRef) {
                slot_ref = likePredicate.getChild(1).unwrapSlotRef();
                expr = likePredicate.getChild(0);
                if (slot_ref == null) return false;
            } else {
                return false;
            }
            if (!slotIds.contains(slot_ref.getSlotId())) return false;
            if (expr.getType().getPrimitiveType() != PrimitiveType.VARCHAR &&
                    expr.getType().getPrimitiveType() != PrimitiveType.CHAR) {
                return false;
            }
            return true;
        }

        if (conjunctExpr instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) conjunctExpr;
            if (inPredicate.getOpcode() != TExprOpcode.FILTER_NOT_IN &&
                    inPredicate.getOpcode() != TExprOpcode.FILTER_IN) {
                return false;
            }

            SlotRef slot_ref = inPredicate.getChild(0).unwrapSlotRef();
            if (slot_ref == null || !slotIds.contains(slot_ref.getSlotId())) return false;
            PrimitiveType type1 = inPredicate.getChild(0).getType().getPrimitiveType();
            PrimitiveType type2 = slot_ref.getType().getPrimitiveType();
            if (type1 != type2) {
                if (type1.isDateType() && type2.isDateType()) {

                } else if (type1.isStringType() && type2.isStringType()) {

                } else {
                    return false;
                }
            }

            for (int j = 1; j < inPredicate.getChildren().size(); j++) {
                Expr expr = inPredicate.getChild(j);
                if (expr instanceof NullLiteral) {
                    // In list can't has a null value
                    return false;
                } else if (expr instanceof LiteralExpr) {
                    // can get value
                } else {
                    return false;
                }
            }
            return true;
        }

        if (conjunctExpr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) conjunctExpr;
            switch (compoundPredicate.getOp()) {
                case AND:
                    for (Expr expr : compoundPredicate.getChildren()) {
                        if (!supportExprPushDown(expr, slotIds)) return false;
                    }
                    break;
                case NOT:
                    // currently do not support COMPOUND_NOT
                    return false;
                case OR:
                    if (compoundPredicate.getChildren().size() != 2) return false;
                    if (!supportExprPushDown(compoundPredicate.getChild(0), slotIds) ||
                            !supportExprPushDown(compoundPredicate.getChild(1), slotIds)) {
                        return false;
                    }
                    break;
                default:
                    return false;
            }
            return true;
        }

        return false;
    }

    private boolean supportAggregationResultType(SlotDescriptor slotDescriptor) {
        if (slotDescriptor.getType() instanceof ScalarType) {
            ScalarType scalarType = (ScalarType) slotDescriptor.getType();
            switch (scalarType.getPrimitiveType()) {
                case CHAR:
                case VARCHAR:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case LARGEINT:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                case DATE:
                case DATETIME:
                    return true;
                default:
                    return false;
            }
        } else {
            // not support ArrayType/MapType/StructType
            return false;
        }
    }
}
