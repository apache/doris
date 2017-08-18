// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.planner;

import com.baidu.palo.analysis.Analyzer;
import com.baidu.palo.analysis.BaseTableRef;
import com.baidu.palo.analysis.Expr;
import com.baidu.palo.analysis.InPredicate;
import com.baidu.palo.analysis.SlotDescriptor;
import com.baidu.palo.analysis.TupleDescriptor;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.DistributionInfo;
import com.baidu.palo.catalog.HashDistributionInfo;
import com.baidu.palo.catalog.KeysType;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.PartitionInfo;
import com.baidu.palo.catalog.PartitionKey;
import com.baidu.palo.catalog.RangePartitionInfo;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.system.Backend;
import com.baidu.palo.thrift.TExplainLevel;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TOlapScanNode;
import com.baidu.palo.thrift.TPaloScanRange;
import com.baidu.palo.thrift.TPlanNode;
import com.baidu.palo.thrift.TPlanNodeType;
import com.baidu.palo.thrift.TPrimitiveType;
import com.baidu.palo.thrift.TScanRange;
import com.baidu.palo.thrift.TScanRangeLocation;
import com.baidu.palo.thrift.TScanRangeLocations;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Full scan of an Olap table.
 */
public class OlapScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(OlapScanNode.class);

    private List<TScanRangeLocations> result = new ArrayList<TScanRangeLocations>();
    private boolean isPreAggregation = false;
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

    boolean isFinalized = false;

    /**
     * Constructs node to scan given data files of table 'tbl'.
     */
    public OlapScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        olapTable = (OlapTable) desc.getTable();
    }

    public void setIsPreAggregation(boolean isPreAggregation) {
        this.isPreAggregation = isPreAggregation;
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
    public void finalize(Analyzer analyzer) throws InternalException {
        if (isFinalized) {
            return;
        }

        LOG.debug("OlapScanNode finalize. Tuple: {}", desc);
        try {
            getScanRangeLocations(analyzer);
        } catch (AnalysisException e) {
            throw new InternalException(e.getMessage());
        }

        isFinalized = true;
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

    private List<MaterializedIndex> selectRollupIndex(Partition partition) throws InternalException {
        ArrayList<MaterializedIndex> containTupleIndices = Lists.newArrayList();

        if (olapTable.getKeysType() == KeysType.DUP_KEYS) {
            isPreAggregation = true;
        }

        // 4.1 find table has tuple column
        List<MaterializedIndex> allIndices = Lists.newArrayList();
        allIndices.add(partition.getBaseIndex());
        allIndices.addAll(partition.getRollupIndices());
        LOG.debug("rollup size={} isPreAggregation={}", allIndices.size(), isPreAggregation);

        List<Column> baseIndexKeyColumns = olapTable.getKeyColumnsByIndexId(partition.getBaseIndex().getId());
        for (MaterializedIndex index : allIndices) {
            LOG.debug("index id = " + index.getId());
            HashSet<String> indexColumns = new HashSet<String>();
            for (Column col : olapTable.getSchemaByIndexId(index.getId())) {
                indexColumns.add(col.getName());
            }

            if (indexColumns.containsAll(tupleColumns)) {
                // If preAggregation is off, so that we only can use base table
                // or those rollup tables whose key columns is the same with base table
                // (often in different order)
                if (isPreAggregation) {
                    containTupleIndices.add(index);
                } else if (olapTable.getKeyColumnsByIndexId(index.getId()).size() == baseIndexKeyColumns.size()) {
                    LOG.debug("preAggregation is off, but index id (" + index.getId()
                            + ") have same key columns with base index.");
                    containTupleIndices.add(index);
                }
            }
        }

        if (containTupleIndices.isEmpty()) {
            throw new InternalException("Failed to select index, no match index");
        }

        // 4.2 find table match index
        ArrayList<MaterializedIndex> predicateIndexMatchIndices = new ArrayList<MaterializedIndex>();
        int maxIndexMatchCount = 0;
        int indexMatchCount = 0;
        for (MaterializedIndex index : containTupleIndices) {
            LOG.debug("containTupleIndex: " + index.getId());
            indexMatchCount = 0;
            for (Column col : olapTable.getSchemaByIndexId(index.getId())) {
                if (sortColumn != null) {
                    if (inPredicateColumns.contains(col.getName())) {
                        indexMatchCount++;
                    } else if (sortColumn.equals(col.getName())) {
                        indexMatchCount++;
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
            if (indexMatchCount == maxIndexMatchCount) {
                predicateIndexMatchIndices.add(index);
            } else if (indexMatchCount > maxIndexMatchCount) {
                maxIndexMatchCount = indexMatchCount;
                predicateIndexMatchIndices.clear();
                predicateIndexMatchIndices.add(index);
            }
        }

        ArrayList<MaterializedIndex> eqJoinIndexMatchIndices = new ArrayList<MaterializedIndex>();
        maxIndexMatchCount = 0;
        indexMatchCount = 0;
        for (MaterializedIndex index : containTupleIndices) {
            indexMatchCount = 0;
            for (Column col : olapTable.getSchemaByIndexId(index.getId())) {
                if (eqJoinColumns.contains(col.getName()) || predicateColumns.contains(col.getName())) {
                    indexMatchCount++;
                } else {
                    break;
                }
            }
            if (indexMatchCount == maxIndexMatchCount) {
                eqJoinIndexMatchIndices.add(index);
            } else if (indexMatchCount > maxIndexMatchCount) {
                maxIndexMatchCount = indexMatchCount;
                eqJoinIndexMatchIndices.clear();
                eqJoinIndexMatchIndices.add(index);
            }
        }

        ArrayList<MaterializedIndex> indexMatchIndices = new ArrayList<MaterializedIndex>();
        for (MaterializedIndex index : predicateIndexMatchIndices) {
            LOG.debug("predicateIndexMatchIndex: " + index.getId());
            for (MaterializedIndex oneIndex : eqJoinIndexMatchIndices) {
                if (oneIndex.getId() == index.getId()) {
                    indexMatchIndices.add(index);
                    LOG.debug("Add indexMatchId: " + index.getId());
                }
            }
        }

        if (indexMatchIndices.isEmpty()) {
            indexMatchIndices = predicateIndexMatchIndices;
        }

        // 4.3 return all the candidate index
        List<MaterializedIndex> selectedIndex = new ArrayList<MaterializedIndex>();
        for (MaterializedIndex table : indexMatchIndices) {
            selectedIndex.add(table);
        }

        Collections.sort(selectedIndex, new Comparator<MaterializedIndex>() {
            @Override
            public int compare(MaterializedIndex index1, MaterializedIndex index2)
            {
                return (int) (index1.getId() - index2.getId());
            }
        });
        return selectedIndex;
    }

    private void normalizePredicate(Analyzer analyzer) throws InternalException {
        // 1. Get Columns which has eqJoin on it
        List<Expr> eqJoinPredicate = analyzer.getEqJoinConjuncts(desc.getId(), null);
        if (null != eqJoinPredicate) {
            for (SlotDescriptor slot : desc.getSlots()) {
                for (Expr expr : eqJoinPredicate) {
                    for (int i = 0; i < 2; ++i) {
                        if (expr.getChild(i).isBound(slot.getId())) {
                            eqJoinColumns.add(slot.getColumn().getName());
                            LOG.debug("Add eqJoinColumn: ColName=" + slot.getColumn().getName());
                            break;
                        }
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
                                       List<Tablet> tablets)
            throws InternalException, AnalysisException {
        int logNum = 0;
        String schemaHashStr = String.valueOf(olapTable.getSchemaHashByIndexId(index.getId()));
        long committedVersion = partition.getCommittedVersion();
        long committedVersionHash = partition.getCommittedVersionHash();
        String committedVersionStr = String.valueOf(committedVersion);
        String committedVersionHashStr = String.valueOf(partition.getCommittedVersionHash());
        for (Tablet tablet : tablets) {
            long tabletId = tablet.getId();
            LOG.debug("{} tabletId={}", (logNum++), tabletId);
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

            TPaloScanRange paloRange = new TPaloScanRange();
            paloRange.setDb_name("");
            paloRange.setSchema_hash(schemaHashStr);
            paloRange.setVersion(committedVersionStr);
            paloRange.setVersion_hash(committedVersionHashStr);
            paloRange.setTablet_id(tabletId);

            // random shuffle List && only collect one copy
            List<Replica> replicas =
                    Lists.newArrayList(tablet.getQueryableReplicas(committedVersion, committedVersionHash));
            if (replicas.isEmpty()) {
                LOG.error("no queryable replica found in tablet[{}]. committed version[{}], committed version hash[{}]",
                         tabletId, committedVersion, committedVersionHash);
                throw new InternalException("Failed to get scan range, no replica!");
            }

            Collections.shuffle(replicas);
            boolean tabletIsNull = true;
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
            }
            if (tabletIsNull) {
                throw new InternalException(tabletId + "have no alive replicas");
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setPalo_scan_range(paloRange);
            scanRangeLocations.setScan_range(scanRange);
            result.add(scanRangeLocations);
        }
    }

    private void getScanRangeLocations(Analyzer analyzer) throws InternalException, AnalysisException {
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
                // check column number, select one mimumum column number
                int selectedColumnSize = olapTable.getIndexIdToSchema().get(selectedIndexId).size();
                int currColumnSize = olapTable.getIndexIdToSchema().get(tables.get(0).get(i).getId()).size();
                if (currColumnSize < selectedColumnSize) {
                    selectedIndexId = tables.get(0).get(i).getId();
                    partitionPos = i;
                }
            }
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
            addScanRangeLocations(partition, selectedTable, tablets);
        }
        LOG.debug("distribution prune cost: {} ms", (System.currentTimeMillis() - start));
    }


    /**
     * We query Palo Meta to get request's data localtion
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
            output.append(prefix).append("PREAGGREGATION: OFF").append("\n");
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
