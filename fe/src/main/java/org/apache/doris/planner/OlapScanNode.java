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
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TOlapScanNode;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Full scan of an Olap table.
 */
public class OlapScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(OlapScanNode.class);

    private List<TScanRangeLocations> result = new ArrayList<TScanRangeLocations>();
    private boolean isPreAggregation = false;
    private String reasonOfPreAggregation = null;
    private boolean canTurnOnPreAggr = true;
    private ArrayList<String> tupleColumns = new ArrayList<String>();
    private HashSet<String> predicateColumns = new HashSet<String>();
    private HashSet<String> inPredicateColumns = new HashSet<String>();
    private HashSet<String> eqJoinColumns = new HashSet<String>();
    private OlapTable olapTable = null;
    private long selectedTabletsNum = 0;
    private long totalTabletsNum = 0;
    private long selectedIndexId = -1;
    private int selectedPartitionNum = 0;
    private long totalBytes = 0;

    boolean isFinalized = false;

    private HashSet<Long> scanBackendIds = new HashSet<>();

    /**
     * Constructs node to scan given data files of table 'tbl'.
     */
    public OlapScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        olapTable = (OlapTable) desc.getTable();
    }

    public void setIsPreAggregation(boolean isPreAggregation, String reason) {
        this.isPreAggregation = isPreAggregation;
        this.reasonOfPreAggregation = reason;
    }


    public boolean isPreAggregation() {
        return isPreAggregation;
    }

    public boolean getCanTurnOnPreAggr() {
        return canTurnOnPreAggr;
    }

    public void setCanTurnOnPreAggr(boolean canChangePreAggr) {
        this.canTurnOnPreAggr = canChangePreAggr;
    }

    @Override
    protected String debugString() {
        ToStringHelper helper = Objects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("olapTable=" + olapTable.getName());
        return helper.toString();
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }

        LOG.debug("OlapScanNode finalize. Tuple: {}", desc);
        try {
            getScanRangeLocations(analyzer);
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }

        computeStats(analyzer);
        isFinalized = true;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        if (cardinality > 0) {
            avgRowSize = totalBytes / (float) cardinality;
            if (hasLimit()) {
                cardinality = Math.min(cardinality, limit);
            }
            numNodes = scanBackendIds.size();
        }
    }


    // private void analyzeVectorizedConjuncts(Analyzer analyzer) throws InternalException {
    //     for (SlotDescriptor slot : desc.getSlots()) {
    //         for (Expr conjunct : conjuncts) {
    //             if (expr.isConstant()) {
    //                 continue;
    //             }
    //             if (analyzer.isWhereClauseConjunct(conjunct)
    //                     && expr.isBound(slot.getId())
    //                     && conjunct.isVectorized()
    //                     && conjunct instanceof Predicate) {
    //                 conjunct.computeOutputColumn(analyzer);
    //             } else {
    //                 Preconditions.checkState(false);
    //             }
    //         }
    //     }
    // }

    private List<MaterializedIndex> selectRollupIndex(Partition partition) throws UserException {
        if (olapTable.getKeysType() == KeysType.DUP_KEYS) {
            isPreAggregation = true;
        }

        List<MaterializedIndex> allIndices = Lists.newArrayList();
        allIndices.add(partition.getBaseIndex());
        allIndices.addAll(partition.getRollupIndices());
        LOG.debug("num of rollup(base included): {}, pre aggr: {}", allIndices.size(), isPreAggregation);

        // 1. find all rollup indexes which contains all tuple columns
        List<MaterializedIndex> containTupleIndexes = Lists.newArrayList();
        List<Column> baseIndexKeyColumns = olapTable.getKeyColumnsByIndexId(partition.getBaseIndex().getId());
        for (MaterializedIndex index : allIndices) {
            Set<String> indexColNames = Sets.newHashSet();
            for (Column col : olapTable.getSchemaByIndexId(index.getId())) {
                indexColNames.add(col.getName());
            }

            if (indexColNames.containsAll(tupleColumns)) {
                // If preAggregation is off, so that we only can use base table
                // or those rollup tables which key columns is the same with base table
                // (often in different order)
                if (isPreAggregation) {
                    LOG.debug("preAggregation is on. add index {} which contains all tuple columns", index.getId());
                    containTupleIndexes.add(index);
                } else if (olapTable.getKeyColumnsByIndexId(index.getId()).size() == baseIndexKeyColumns.size()) {
                    LOG.debug("preAggregation is off, but index {} have same key columns with base index.",
                              index.getId());
                    containTupleIndexes.add(index);
                }
            } else {
                LOG.debug("exclude index {} because it does not contain all tuple columns", index.getId());
            }
        }

        if (containTupleIndexes.isEmpty()) {
            throw new UserException("Failed to select index, no match index");
        }

        // 2. find all indexes which match the prefix most based on predicate/sort/in predicate columns
        // from containTupleIndices.
        List<MaterializedIndex> prefixMatchedIndexes = Lists.newArrayList();
        int maxPrefixMatchCount = 0;
        int prefixMatchCount = 0;
        for (MaterializedIndex index : containTupleIndexes) {
            prefixMatchCount = 0;
            for (Column col : olapTable.getSchemaByIndexId(index.getId())) {
                if (sortColumn != null) {
                    if (inPredicateColumns.contains(col.getName())) {
                        prefixMatchCount++;
                    } else if (sortColumn.equals(col.getName())) {
                        prefixMatchCount++;
                        break;
                    } else {
                        break;
                    }
                } else {
                    if (predicateColumns.contains(col.getName())) {
                        break;
                    }
                }
            }
            if (prefixMatchCount == maxPrefixMatchCount) {
                LOG.debug("s2: find a equal prefix match index {}. match count: {}", index.getId(), prefixMatchCount);
                prefixMatchedIndexes.add(index);
            } else if (prefixMatchCount > maxPrefixMatchCount) {
                LOG.debug("s2: find a better prefix match index {}. match count: {}", index.getId(), prefixMatchCount);
                maxPrefixMatchCount = prefixMatchCount;
                prefixMatchedIndexes.clear();
                prefixMatchedIndexes.add(index);
            }
        }

        // 3. find all indexes which match the prefix most based on equal join columns
        // from containTupleIndices.
        List<MaterializedIndex> eqJoinPrefixMatchedIndexes = Lists.newArrayList();
        maxPrefixMatchCount = 0;
        for (MaterializedIndex index : containTupleIndexes) {
            prefixMatchCount = 0;
            for (Column col : olapTable.getSchemaByIndexId(index.getId())) {
                if (eqJoinColumns.contains(col.getName()) || predicateColumns.contains(col.getName())) {
                    prefixMatchCount++;
                } else {
                    break;
                }
            }
            if (prefixMatchCount == maxPrefixMatchCount) {
                LOG.debug("s3: find a equal prefix match index {}. match count: {}", index.getId(), prefixMatchCount);
                eqJoinPrefixMatchedIndexes.add(index);
            } else if (prefixMatchCount > maxPrefixMatchCount) {
                LOG.debug("s3: find a better prefix match index {}. match count: {}", index.getId(), prefixMatchCount);
                maxPrefixMatchCount = prefixMatchCount;
                eqJoinPrefixMatchedIndexes.clear();
                eqJoinPrefixMatchedIndexes.add(index);
            }
        }

        // 4. find the intersection of prefixMatchIndices and eqJoinPrefixMatchIndices as candidate indexes
        List<MaterializedIndex> finalCandidateIndexes = Lists.newArrayList();
        for (MaterializedIndex index : prefixMatchedIndexes) {
            for (MaterializedIndex oneIndex : eqJoinPrefixMatchedIndexes) {
                if (oneIndex.getId() == index.getId()) {
                    finalCandidateIndexes.add(index);
                    LOG.debug("find a matched index {} in intersection of "
                            + "prefixMatchIndices and eqJoinPrefixMatchIndices",
                              index.getId());
                }
            }
        }
        // maybe there is no intersection between prefixMatchIndices and eqJoinPrefixMatchIndices.
        // in this case, use prefixMatchIndices;
        if (finalCandidateIndexes.isEmpty()) {
            finalCandidateIndexes = prefixMatchedIndexes;
        }

        // 5. sorted the final candidate indexes by index id
        // this is to make sure that candidate indexes find in all partitions will be returned in same order
        Collections.sort(finalCandidateIndexes, new Comparator<MaterializedIndex>() {
            @Override
            public int compare(MaterializedIndex index1, MaterializedIndex index2)
            {
                return (int) (index1.getId() - index2.getId());
            }
        });
        return finalCandidateIndexes;
    }

    private void normalizePredicate(Analyzer analyzer) throws UserException {
        // 1. Get Columns which has eqJoin on it
        List<Expr> eqJoinPredicate = analyzer.getEqJoinConjuncts(desc.getId());
        for (Expr expr : eqJoinPredicate) {
            for (SlotDescriptor slot : desc.getSlots()) {
                for (int i = 0; i < 2; i++) {
                    if (expr.getChild(i).isBound(slot.getId())) {
                        eqJoinColumns.add(slot.getColumn().getName());
                        LOG.debug("Add eqJoinColumn: ColName=" + slot.getColumn().getName());
                        break;
                    }
                }
            }
        }

        // 2. Get Columns which has predicate on it
        for (SlotDescriptor slot : desc.getSlots()) {
            for (Expr expr : conjuncts) {
                if (expr.isConstant()) {
                    continue;
                }
                if (expr.isBound(slot.getId())) {
                    predicateColumns.add(slot.getColumn().getName());
                    LOG.debug("Add predicateColumn: ColName=" + slot.getColumn().getName());
                    if (expr instanceof InPredicate) {
                        inPredicateColumns.add(slot.getColumn().getName());
                        LOG.debug("Add inPredicateColumn: ColName=" + slot.getColumn().getName());
                    }
                }
            }
        }

        // 3. Get Columns of this tuple
        for (SlotDescriptor slot : desc.getSlots()) {
            Column col = slot.getColumn();
            tupleColumns.add(col.getName());
            LOG.debug("Add tupleColumn: ColName=" + col.getName());
        }
    }

    private Collection<Long> partitionPrune(PartitionInfo partitionInfo) throws AnalysisException {
        PartitionPruner partitionPruner = null;
        switch(partitionInfo.getType()) {
            case RANGE: {
                BaseTableRef ref = (BaseTableRef) desc.getRef();
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                Map<Long, Range<PartitionKey>> keyRangeById = null;
                if (ref.getPartitions() != null) {
                    keyRangeById = Maps.newHashMap();
                    for (String partName : ref.getPartitions()) {
                        Partition part = olapTable.getPartition(partName);
                        if (part == null) {
                            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_SUCH_PARTITION, partName);
                        }
                        keyRangeById.put(part.getId(), rangePartitionInfo.getRange(part.getId()));
                    }
                } else {
                    keyRangeById = rangePartitionInfo.getIdToRange();
                }
                partitionPruner = new RangePartitionPruner(keyRangeById,
                                                           rangePartitionInfo.getPartitionColumns(),
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

    private Collection<Long> distributionPrune(
            MaterializedIndex table,
            DistributionInfo distributionInfo) throws AnalysisException {
        DistributionPruner distributionPruner = null;
        switch(distributionInfo.getType()) {
            case HASH: {
                HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                distributionPruner = new HashDistributionPruner(table.getTabletIdsInOrder(),
                                                                info.getDistributionColumns(),
                                                                columnFilters,
                                                                info.getBucketNum());
                return distributionPruner.prune();
            }
            case RANDOM: {
                return null;
            }
            default: {
                return null;
            }
        }
    }

    private void addScanRangeLocations(Partition partition,
                                       MaterializedIndex index,
                                       List<Tablet> tablets,
                                       long localBeId)
            throws UserException, AnalysisException {

        int logNum = 0;
        String schemaHashStr = String.valueOf(olapTable.getSchemaHashByIndexId(index.getId()));
        long visibleVersion = partition.getVisibleVersion();
        long visibleVersionHash = partition.getVisibleVersionHash();
        String visibleVersionStr = String.valueOf(visibleVersion);
        String visibleVersionHashStr = String.valueOf(partition.getVisibleVersionHash());

        for (Tablet tablet : tablets) {
            long tabletId = tablet.getId();
            LOG.debug("{} tabletId={}", (logNum++), tabletId);
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

            TPaloScanRange paloRange = new TPaloScanRange();
            paloRange.setDb_name("");
            paloRange.setSchema_hash(schemaHashStr);
            paloRange.setVersion(visibleVersionStr);
            paloRange.setVersion_hash(visibleVersionHashStr);
            paloRange.setTablet_id(tabletId);

            // random shuffle List && only collect one copy
            List<Replica> allQueryableReplicas = Lists.newArrayList();
            List<Replica> localReplicas = Lists.newArrayList();
            tablet.getQueryableReplicas(allQueryableReplicas, localReplicas,
                                        visibleVersion, visibleVersionHash,
                                        localBeId);
            if (allQueryableReplicas.isEmpty()) {
                LOG.error("no queryable replica found in tablet[{}]. committed version[{}], committed version hash[{}]",
                         tabletId, visibleVersion, visibleVersionHash);
                throw new UserException("Failed to get scan range, no replica!");
            }

            List<Replica> replicas = null;
            if (!localReplicas.isEmpty()) {
                replicas = localReplicas;
            } else {
                replicas = allQueryableReplicas;

            }

            Collections.shuffle(replicas);
            boolean tabletIsNull = true;
            boolean collectedStat = false;
            for (Replica replica : replicas) {
                Backend backend = Catalog.getCurrentSystemInfo().getBackend(replica.getBackendId());
                if (backend == null) {
                    LOG.debug("replica {} not exists", replica.getBackendId());
                    continue;
                }
                String ip = backend.getHost();
                int port = backend.getBePort();
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(ip, port));
                scanRangeLocation.setBackend_id(replica.getBackendId());
                scanRangeLocations.addToLocations(scanRangeLocation);
                paloRange.addToHosts(new TNetworkAddress(ip, port));
                tabletIsNull = false;

                //for CBO
                if (!collectedStat && replica.getRowCount() != -1) {
                    cardinality += replica.getRowCount();
                    totalBytes += replica.getDataSize();
                    collectedStat = true;
                }
                scanBackendIds.add(backend.getId());
            }
            if (tabletIsNull) {
                throw new UserException(tabletId + "have no alive replicas");
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setPalo_scan_range(paloRange);
            scanRangeLocations.setScan_range(scanRange);
            result.add(scanRangeLocations);
        }
    }

    private void getScanRangeLocations(Analyzer analyzer) throws UserException, AnalysisException {
        normalizePredicate(analyzer);

        long start = System.currentTimeMillis();
        Collection<Long> partitionIds = partitionPrune(olapTable.getPartitionInfo());

        if (partitionIds == null) {
            partitionIds = new ArrayList<Long>();
            for (Partition partition : olapTable.getPartitions()) {
                partitionIds.add(partition.getId());
            }
        }
        selectedPartitionNum = partitionIds.size();
        LOG.debug("partition prune cost: {} ms, partitions: {}", (System.currentTimeMillis() - start), partitionIds);

        start = System.currentTimeMillis();

        // find all candidate rollups
        int candidateTableSize = 0;
        List<List<MaterializedIndex>> tables = Lists.newArrayList();
        for (Long partitionId : partitionIds) {
            Partition partition = olapTable.getPartition(partitionId);
            List<MaterializedIndex> candidateTables = selectRollupIndex(partition);
            if (candidateTableSize == 0) {
                candidateTableSize = candidateTables.size();
            } else {
                if (candidateTableSize != candidateTables.size()) {
                    String errMsg = "two partition's candidate_table_size not equal, one is " + candidateTableSize
                            + ", the other is" + candidateTables.size();
                    throw new AnalysisException(errMsg);
                }
            }
            tables.add(candidateTables);
        }

        // chose one rollup from candidate rollups
        long minRowCount = Long.MAX_VALUE;
        int partitionPos = -1;
        for (int i = 0; i < candidateTableSize; i++) {
            MaterializedIndex candidateIndex = null;
            long rowCount = 0;
            for (List<MaterializedIndex> candidateTables : tables) {
                if (candidateIndex == null) {
                    candidateIndex = candidateTables.get(i);
                } else {
                    if (candidateIndex.getId() != candidateTables.get(i).getId()) {
                        String errMsg = "two partition's candidate_table not equal, one is "
                                + candidateIndex.getId() + ", the other is " + candidateTables.get(i).getId();
                        throw new AnalysisException(errMsg);
                    }
                }
                rowCount += candidateTables.get(i).getRowCount();
            }
            LOG.debug("rowCount={} for table={}", rowCount, candidateIndex.getId());
            if (rowCount < minRowCount) {
                minRowCount = rowCount;
                selectedIndexId = tables.get(0).get(i).getId();
                partitionPos = i;
            } else if (rowCount == minRowCount) {
                // check column number, select one minimum column number
                int selectedColumnSize = olapTable.getIndexIdToSchema().get(selectedIndexId).size();
                int currColumnSize = olapTable.getIndexIdToSchema().get(tables.get(0).get(i).getId()).size();
                if (currColumnSize < selectedColumnSize) {
                    selectedIndexId = tables.get(0).get(i).getId();
                    partitionPos = i;
                }
            }
        }

        long localBeId = -1;
        if (Config.enable_local_replica_selection) {
            localBeId = Catalog.getCurrentSystemInfo().getBackendIdByHost(FrontendOptions.getLocalHostAddress());
        }
        MaterializedIndex selectedTable = null;
        int j = 0;
        for (Long partitionId : partitionIds) {
            Partition partition = olapTable.getPartition(partitionId);
            LOG.debug("selected partition: " + partition.getName());
            selectedTable = tables.get(j++).get(partitionPos);
            List<Tablet> tablets = new ArrayList<Tablet>();
            Collection<Long> tabletIds = distributionPrune(selectedTable, partition.getDistributionInfo());
            LOG.debug("distribution prune tablets: {}", tabletIds);
            if (tabletIds != null) {
                for (Long id : tabletIds) {
                    tablets.add(selectedTable.getTablet(id));
                }
            } else {
                tablets.addAll(selectedTable.getTablets());
            }
            totalTabletsNum += selectedTable.getTablets().size();
            selectedTabletsNum += tablets.size();
            addScanRangeLocations(partition, selectedTable, tablets, localBeId);
        }
        LOG.debug("distribution prune cost: {} ms", (System.currentTimeMillis() - start));
    }


    /**
     * We query Palo Meta to get request's data location
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }


    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(olapTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (isPreAggregation) {
            output.append(prefix).append("PREAGGREGATION: ON").append("\n");
        } else {
            output.append(prefix).append("PREAGGREGATION: OFF. Reason: ").append(reasonOfPreAggregation).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
        }

        output.append(prefix).append(String.format(
                    "partitions=%s/%s",
                    selectedPartitionNum,
                    olapTable.getPartitions().size()));

        String indexName = olapTable.getIndexNameById(selectedIndexId);
        output.append("\n").append(prefix).append(String.format("rollup: %s", indexName));


        output.append("\n");

        output.append(prefix).append(String.format(
                    "buckets=%s/%s", selectedTabletsNum, totalTabletsNum));
        output.append("\n");

        output.append(prefix).append(String.format(
                "cardinality=%s", cardinality));
        output.append("\n");

        output.append(prefix).append(String.format(
                "avgRowSize=%s", avgRowSize));
        output.append("\n");

        output.append(prefix).append(String.format(
                "numNodes=%s", numNodes));
        output.append("\n");

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return result.size();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        List<String> keyColumnNames = new ArrayList<String>();
        List<TPrimitiveType> keyColumnTypes = new ArrayList<TPrimitiveType>();
        if (selectedIndexId != -1) {
            for (Column col : olapTable.getSchemaByIndexId(selectedIndexId)) {
                if (!col.isKey()) {
                    break;
                }
                keyColumnNames.add(col.getName());
                keyColumnTypes.add(col.getDataType().toThrift());
            }
        }
        msg.node_type = TPlanNodeType.OLAP_SCAN_NODE;
        msg.olap_scan_node =
              new TOlapScanNode(desc.getId().asInt(), keyColumnNames, keyColumnTypes, isPreAggregation);
        if (null != sortColumn) {
            msg.olap_scan_node.setSort_column(sortColumn);
        }
    }

    // export some tablets
    public static OlapScanNode createOlapScanNodeByLocation(
            PlanNodeId id, TupleDescriptor desc, String planNodeName, List<TScanRangeLocations> locationsList) {
        OlapScanNode olapScanNode = new OlapScanNode(id, desc, planNodeName);
        olapScanNode.numInstances = 1;

        Collection<Long> partitionIds = new ArrayList<Long>();
        ArrayList<Partition> partitions = Lists.newArrayList(olapScanNode.olapTable.getPartitions());
        Preconditions.checkState(!partitions.isEmpty());
        if (!partitions.isEmpty()) {
            olapScanNode.selectedIndexId = partitions.get(0).getBaseIndex().getId();
        }

        olapScanNode.selectedPartitionNum = 1;
        olapScanNode.selectedTabletsNum = 1;
        olapScanNode.totalTabletsNum = 1;
        olapScanNode.isPreAggregation = false;

        olapScanNode.isFinalized = true;

        olapScanNode.result.addAll(locationsList);
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        return olapScanNode;
    }
}
