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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.PrepareStmt;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.statistics.query.StatsDelta;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TOlapScanNode;
import org.apache.doris.thrift.TOlapTableIndex;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// Full scan of an Olap table.
public class OlapScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(OlapScanNode.class);

    // average compression ratio in doris storage engine
    private static final int COMPRESSION_RATIO = 5;

    /*
     * When the field value is ON, the storage engine can return the data directly
     * without pre-aggregation.
     * When the field value is OFF, the storage engine needs to aggregate the data
     * before returning to scan node. And if the table is an aggregation table,
     * all key columns need to be read an participate in aggregation.
     * For example:
     * Aggregate table: k1, k2, v1 sum
     * Field value is ON
     * Query1: select k1, sum(v1) from table group by k1
     * This aggregation function in query is same as the schema.
     * So the field value is ON while the query can scan data directly.
     *
     * Field value is OFF
     * Query1: select k1 , k2 from table
     * This aggregation info is null.
     * Query2: select k1, min(v1) from table group by k1
     * This aggregation function in query is min which different from the schema.
     * So the data stored in storage engine need to be merged firstly before
     * returning to scan node. Although we only queried key column k1, key column
     * k2 still needs to be detected and participate in aggregation to ensure the
     * results are correct.
     *
     * There are currently two places to modify this variable:
     * 1. The turnOffPreAgg() method of SingleNodePlanner.
     * This method will only be called on the left deepest OlapScanNode the plan
     * tree,
     * while other nodes are false by default (because the Aggregation operation is
     * executed after Join,
     * we cannot judge whether other OlapScanNodes can close the pre-aggregation).
     * So even the Duplicate key table, if it is not the left deepest node, it will
     * remain false too.
     *
     * 2. After MaterializedViewSelector selects the materialized view, the
     * updateScanRangeInfoByNewMVSelector()\
     * method of OlapScanNode may be called to update this variable.
     * This call will be executed on all ScanNodes in the plan tree. In this step,
     * for the DuplicateKey table, the variable will be set to true.
     * See comment of "isPreAggregation" variable in MaterializedViewSelector for
     * details.
     */
    private boolean isPreAggregation = false;
    private String reasonOfPreAggregation = null;
    private boolean canTurnOnPreAggr = true;
    private boolean forceOpenPreAgg = false;
    private OlapTable olapTable = null;
    private long totalTabletsNum = 0;
    private long selectedIndexId = -1;
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();
    private long totalBytes = 0;
    // tablet id to single replica bytes
    private Map<Long, Long> tabletBytes = Maps.newLinkedHashMap();

    private SortInfo sortInfo = null;
    private Set<Integer> outputColumnUniqueIds = new HashSet<>();

    // When scan match sort_info, we can push limit into OlapScanNode.
    // It's limit for scanner instead of scanNode so we add a new limit.
    private long sortLimit = -1;

    // List of tablets will be scanned by current olap_scan_node
    private ArrayList<Long> scanTabletIds = Lists.newArrayList();

    private ArrayList<Long> scanReplicaIds = Lists.newArrayList();

    private Set<Long> sampleTabletIds = Sets.newHashSet();
    private TableSample tableSample;

    private HashSet<Long> scanBackendIds = new HashSet<>();

    private Map<Long, Integer> tabletId2BucketSeq = Maps.newHashMap();
    // a bucket seq may map to many tablets, and each tablet has a
    // TScanRangeLocations.
    public ArrayListMultimap<Integer, TScanRangeLocations> bucketSeq2locations = ArrayListMultimap.create();
    public Map<Integer, Long> bucketSeq2Bytes = Maps.newLinkedHashMap();

    boolean isFromPrepareStmt = false;
    // For point query
    private Map<SlotRef, Expr> pointQueryEqualPredicats;
    private DescriptorTable descTable;

    private Set<Integer> distributionColumnIds;

    private boolean shouldColoScan = false;

    protected List<Expr> rewrittenProjectList;

    // cached for prepared statement to quickly prune partition
    // only used in short circuit plan at present
    private final PartitionPruneV2ForShortCircuitPlan cachedPartitionPruner =
                        new PartitionPruneV2ForShortCircuitPlan();
    PrepareStmt preparedStatment = null;

    // Constructs node to scan given data files of table 'tbl'.
    public OlapScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName, StatisticalType.OLAP_SCAN_NODE);
        olapTable = (OlapTable) desc.getTable();
        distributionColumnIds = Sets.newTreeSet();

        Set<String> distColumnName = getDistributionColumnNames();
        // use for Nereids to generate uniqueId set for inverted index to avoid scan unnecessary big size column

        int columnId = 0;
        for (SlotDescriptor slotDescriptor : desc.getSlots()) {
            if (slotDescriptor.getColumn() != null) {
                outputColumnUniqueIds.add(slotDescriptor.getColumn().getUniqueId());
                if (distColumnName.contains(slotDescriptor.getColumn().getName().toLowerCase())) {
                    distributionColumnIds.add(columnId);
                }
                columnId++;
            }
        }
    }

    public void setIsPreAggregation(boolean isPreAggregation, String reason) {
        this.isPreAggregation = isPreAggregation;
        this.reasonOfPreAggregation = this.reasonOfPreAggregation == null ? reason :
                this.reasonOfPreAggregation + " " + reason;
    }


    public boolean isPreAggregation() {
        return isPreAggregation;
    }

    public boolean getCanTurnOnPreAggr() {
        return canTurnOnPreAggr;
    }

    public Set<Long> getSampleTabletIds() {
        return sampleTabletIds;
    }

    public HashSet<Long> getScanBackendIds() {
        return scanBackendIds;
    }

    public void setSampleTabletIds(List<Long> sampleTablets) {
        if (sampleTablets != null) {
            this.sampleTabletIds.addAll(sampleTablets);
        }
    }

    public void setRewrittenProjectList(List<Expr> rewrittenProjectList) {
        this.rewrittenProjectList = rewrittenProjectList;
    }

    public void setTableSample(TableSample tSample) {
        this.tableSample = tSample;
    }

    public void setCanTurnOnPreAggr(boolean canChangePreAggr) {
        this.canTurnOnPreAggr = canChangePreAggr;
    }

    public void closePreAggregation(String reason) {
        setIsPreAggregation(false, reason);
        setCanTurnOnPreAggr(false);
    }

    public long getTotalTabletsNum() {
        return totalTabletsNum;
    }

    public boolean getForceOpenPreAgg() {
        return forceOpenPreAgg;
    }

    public ArrayList<Long> getScanTabletIds() {
        return scanTabletIds;
    }

    public void setForceOpenPreAgg(boolean forceOpenPreAgg) {
        this.forceOpenPreAgg = forceOpenPreAgg;
    }

    public SortInfo getSortInfo() {
        return sortInfo;
    }

    public void setSortInfo(SortInfo sortInfo) {
        this.sortInfo = sortInfo;
    }

    public void setSortLimit(long sortLimit) {
        this.sortLimit = sortLimit;
    }

    public Collection<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    public void setTupleIds(ArrayList<TupleId> tupleIds) {
        this.tupleIds = tupleIds;
    }

    // only used for UT and Nereids
    public void setSelectedPartitionIds(Collection<Long> selectedPartitionIds) {
        this.selectedPartitionIds = selectedPartitionIds;
    }

    /**
     * Only used for Nereids to set rollup or materialized view selection result.
     */
    public void setSelectedIndexInfo(
            long selectedIndexId,
            boolean isPreAggregation,
            String reasonOfPreAggregation) {
        this.selectedIndexId = selectedIndexId;
        this.isPreAggregation = isPreAggregation;
        this.reasonOfPreAggregation = reasonOfPreAggregation;
    }

    /**
     * The function is used to directly select the index id of the base table as the
     * selectedIndexId.
     * It makes sure that the olap scan node must scan the base data rather than
     * scan the materialized view data.
     * <p>
     * This function is mainly used to update stmt.
     * Update stmt also needs to scan data like normal queries.
     * But its syntax is different from ordinary queries,
     * so planner cannot use the logic of query to automatically match the best
     * index id.
     * So, here it need to manually specify the index id to scan the base table
     * directly.
     */
    public void useBaseIndexId() {
        this.selectedIndexId = olapTable.getBaseIndexId();
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public void ignoreConjuncts(Expr whereExpr) {
        if (whereExpr == null) {
            return;
        }
        Expr vconjunct = convertConjunctsToAndCompoundPredicate(conjuncts).replaceSubPredicate(whereExpr);
        conjuncts = splitAndCompoundPredicateToConjuncts(vconjunct).stream().collect(Collectors.toList());
    }

    /**
     * This method is mainly used to update scan range info in OlapScanNode by the
     * new materialized selector.
     * Situation1:
     * If the new scan range is same as the old scan range which determined by the
     * old materialized selector,
     * the scan range will not be changed.
     * <p>
     * Situation2: Scan range is difference. The type of table is duplicated.
     * The new scan range is used directly.
     * The reason is that the old selector does not support SPJ<->SPJG, so the
     * result of old one must be incorrect.
     * <p>
     * Situation3: Scan range is difference. The type of table is aggregated.
     * The new scan range is different from the old one.
     * If the test_materialized_view is set to true, an error will be reported.
     * The query will be cancelled.
     * <p>
     * Situation4: Scan range is difference. The type of table is aggregated.
     * `test_materialized_view` is set to false.
     * The result of the old version selector will be selected. Print the warning
     * log
     *
     * @param selectedIndexId
     * @param isPreAggregation
     * @param reasonOfDisable
     * @throws UserException
     */
    public void updateScanRangeInfoByNewMVSelector(long selectedIndexId,
            boolean isPreAggregation, String reasonOfDisable)
            throws UserException {
        if (selectedIndexId == this.selectedIndexId && isPreAggregation == this.isPreAggregation) {
            return;
        }
        StringBuilder stringBuilder = new StringBuilder("The new selected index id ")
                .append(selectedIndexId)
                .append(", pre aggregation tag ").append(isPreAggregation)
                .append(", reason ").append(reasonOfDisable == null ? "null" : reasonOfDisable)
                .append(". The old selected index id ").append(this.selectedIndexId)
                .append(" pre aggregation tag ").append(this.isPreAggregation)
                .append(" reason ").append(this.reasonOfPreAggregation == null ? "null" : this.reasonOfPreAggregation);
        String scanRangeInfo = stringBuilder.toString();
        String situation;
        boolean update;
        CHECK:
        { // CHECKSTYLE IGNORE THIS LINE
            if (olapTable.getKeysType() == KeysType.DUP_KEYS || (olapTable.getKeysType() == KeysType.UNIQUE_KEYS
                    && olapTable.getEnableUniqueKeyMergeOnWrite())) {
                situation = "The key type of table is duplicate, or unique key with merge-on-write.";
                update = true;
                break CHECK;
            }
            if (ConnectContext.get() == null) {
                situation = "Connection context is null";
                update = true;
                break CHECK;
            }
            situation = "The key type of table is aggregated.";
            update = false;
        } // CHECKSTYLE IGNORE THIS LINE

        if (update) {
            this.selectedIndexId = selectedIndexId;
            updateSlotUniqueId();
            setIsPreAggregation(isPreAggregation, reasonOfDisable);
            updateColumnType();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using the new scan range info instead of the old one. {}, {}",
                        situation, scanRangeInfo);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using the old scan range info instead of the new one. {}, {}",
                        situation, scanRangeInfo);
            }
        }
    }

    /**
     * In some situation, the column type between base and mv is different.
     * If mv selector selects the mv index, the type of column should be changed to
     * the type of mv column.
     * For example:
     * base table: k1 int, k2 int
     * mv table: k1 int, k2 bigint sum
     * The type of `k2` column between base and mv is different.
     * When mv selector selects the mv table to scan, the type of column should be
     * changed to bigint in here.
     * Currently, only `SUM` aggregate type could match this changed.
     */
    private void updateColumnType() throws UserException {
        if (selectedIndexId == olapTable.getBaseIndexId()) {
            return;
        }
        MaterializedIndexMeta meta = olapTable.getIndexMetaByIndexId(selectedIndexId);
        for (SlotDescriptor slotDescriptor : desc.getSlots()) {
            if (!slotDescriptor.isMaterialized()) {
                continue;
            }
            Column baseColumn = slotDescriptor.getColumn();
            Preconditions.checkNotNull(baseColumn);
            Column mvColumn = meta.getColumnByName(baseColumn.getName());
            if (mvColumn == null) {
                mvColumn = meta.getColumnByName(CreateMaterializedViewStmt.mvColumnBuilder(baseColumn.getName()));
            }
            if (mvColumn == null) {
                throw new UserException("updateColumnType: Do not found mvColumn=" + baseColumn.getName()
                        + " from index=" + olapTable.getIndexNameById(selectedIndexId));
            }

            if (mvColumn.getType() != baseColumn.getType()) {
                slotDescriptor.setColumn(mvColumn);
            }
        }
    }

    /**
     * In some situation, we need use mv col unique id , because mv col unique and
     * base col unique id is different.
     * For example: select count(*) from table (table has a mv named mv1)
     * if Optimizer deceide use mv1, we need updateSlotUniqueId.
     */
    private void updateSlotUniqueId() throws UserException {
        if (!olapTable.getEnableLightSchemaChange() || selectedIndexId == olapTable.getBaseIndexId()) {
            return;
        }
        MaterializedIndexMeta meta = olapTable.getIndexMetaByIndexId(selectedIndexId);
        for (SlotDescriptor slotDescriptor : desc.getSlots()) {
            if (!slotDescriptor.isMaterialized()) {
                continue;
            }
            Column baseColumn = slotDescriptor.getColumn();
            Column mvColumn = meta.getColumnByName(baseColumn.getName());
            if (mvColumn == null) {
                boolean isBound = false;
                for (Expr conjunct : conjuncts) {
                    List<TupleId> tids = Lists.newArrayList();
                    conjunct.getIds(tids, null);
                    if (!tids.isEmpty() && conjunct.isBound(slotDescriptor.getId())) {
                        isBound = true;
                        break;
                    }
                }
                if (isBound) {
                    slotDescriptor.setIsMaterialized(false);
                } else {
                    throw new UserException("updateSlotUniqueId: Do not found mvColumn=" + baseColumn.getName()
                            + " from index=" + olapTable.getIndexNameById(selectedIndexId));
                }
            } else {
                slotDescriptor.setColumn(mvColumn);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("updateSlotUniqueId() slots: {}", desc.getSlots());
        }
    }

    public OlapTable getOlapTable() {
        return olapTable;
    }

    public boolean isDupKeysOrMergeOnWrite() {
        return olapTable.isDupKeysOrMergeOnWrite();
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("olapTable=" + olapTable.getName());
        return helper.toString();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        filterDeletedRows(analyzer);
        // point query could do lazy evaluation, since stmt is a prepared statment
        preparedStatment = analyzer.getPrepareStmt();
        if (preparedStatment == null || !preparedStatment.isPointQueryShortCircuit()) {
            if (olapTable.getPartitionInfo().enableAutomaticPartition()) {
                partitionsInfo = olapTable.getPartitionInfo();
                analyzerPartitionExpr(analyzer, partitionsInfo);
            }
            computeColumnsFilter();
            computePartitionInfo();
        }
        computeTupleState(analyzer);

        /**
         * Compute InAccurate cardinality before mv selector and tablet pruning.
         * - Accurate statistical information relies on the selector of materialized
         * views and bucket reduction.
         * - However, Those both processes occur after the reorder algorithm is
         * completed.
         * - When Join reorder is turned on, the cardinality must be calculated before
         * the reorder algorithm.
         * - So only an inaccurate cardinality can be calculated here.
         */
        mockRowCountInStatistic();
        if (analyzer.safeIsEnableJoinReorderBasedCost()) {
            computeInaccurateCardinality();
        }
    }

    /**
     * Init OlapScanNode, ONLY used for Nereids. Should NOT use this function in anywhere else.
     */
    public void init() throws UserException {
        selectedPartitionNum = selectedPartitionIds.size();
        try {
            createScanRangeLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
    }

    /**
     * Remove the method after statistics collection is working properly
     */
    public void mockRowCountInStatistic() {
        cardinality = 0;
        for (long selectedPartitionId : selectedPartitionIds) {
            final Partition partition = olapTable.getPartition(selectedPartitionId);
            final MaterializedIndex baseIndex = partition.getBaseIndex();
            cardinality += baseIndex.getRowCount();
        }
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("OlapScanNode get scan range locations. Tuple: {}", desc);
        }
        /**
         * If JoinReorder is turned on, it will be calculated init(), and this value is
         * not accurate.
         * In the following logic, cardinality will be accurately calculated again.
         * So here we need to reset the value of cardinality.
         */
        if (analyzer.safeIsEnableJoinReorderBasedCost()) {
            cardinality = 0;
        }

        // prepare stmt evaluate lazily in Coordinator execute
        if (preparedStatment == null || !preparedStatment.isPointQueryShortCircuit()) {
            try {
                createScanRangeLocations();
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
        }

        // Relatively accurate cardinality according to ScanRange in
        // getScanRangeLocations
        computeStats(analyzer);
        computeNumNodes();
    }

    public void computeTupleState(Analyzer analyzer) {
        for (TupleId id : tupleIds) {
            analyzer.getDescTbl().getTupleDesc(id).computeStat();
        }
    }

    @Override
    public void computeStats(Analyzer analyzer) throws UserException {
        super.computeStats(analyzer);
        if (cardinality > 0) {
            avgRowSize = totalBytes / (float) cardinality * COMPRESSION_RATIO;
            capCardinalityAtLimit();
        }
        // when node scan has no data, cardinality should be 0 instead of a invalid
        // value after computeStats()
        cardinality = cardinality == -1 ? 0 : cardinality;

        // update statsDeriveResult for real statistics
        // After statistics collection is complete, remove the logic
        if (analyzer.safeIsEnableJoinReorderBasedCost()) {
            statsDeriveResult = new StatsDeriveResult(cardinality, statsDeriveResult.getSlotIdToColumnStats());
        }
    }

    @Override
    protected void computeNumNodes() {
        if (cardinality > 0) {
            numNodes = scanBackendIds.size();
        }
        // even current node scan has no data,at least on backend will be assigned when
        // the fragment actually execute
        numNodes = numNodes <= 0 ? 1 : numNodes;
    }

    private void computeInaccurateCardinality() throws UserException {
        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();
    }

    private Collection<Long> partitionPrune(PartitionInfo partitionInfo,
            PartitionNames partitionNames) throws AnalysisException {
        PartitionPruner partitionPruner = null;
        Map<Long, PartitionItem> keyItemMap;
        if (partitionNames != null) {
            keyItemMap = Maps.newHashMap();
            for (String partName : partitionNames.getPartitionNames()) {
                Partition partition = olapTable.getPartition(partName, partitionNames.isTemp());
                if (partition == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_SUCH_PARTITION, partName);
                }
                keyItemMap.put(partition.getId(), partitionInfo.getItem(partition.getId()));
            }
        } else {
            keyItemMap = partitionInfo.getIdToItem(false);
        }
        if (partitionInfo.getType() == PartitionType.RANGE) {
            if (isPointQuery() && partitionInfo.getPartitionColumns().size() == 1) {
                // short circuit, a quick path to find partition
                ColumnRange filterRange = columnNameToRange.get(partitionInfo.getPartitionColumns().get(0).getName());
                LiteralExpr lowerBound = filterRange.getRangeSet().get().asRanges().stream()
                        .findFirst().get().lowerEndpoint().getValue();
                LiteralExpr upperBound = filterRange.getRangeSet().get().asRanges().stream()
                        .findFirst().get().upperEndpoint().getValue();
                cachedPartitionPruner.update(keyItemMap);
                return cachedPartitionPruner.prune(lowerBound, upperBound);
            }
            partitionPruner = new RangePartitionPrunerV2(keyItemMap,
                    partitionInfo.getPartitionColumns(), columnNameToRange);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            partitionPruner = new ListPartitionPrunerV2(keyItemMap, partitionInfo.getPartitionColumns(),
                    columnNameToRange);
        }
        return partitionPruner.prune();
    }

    private Collection<Long> distributionPrune(
            MaterializedIndex table,
            DistributionInfo distributionInfo) throws AnalysisException {
        DistributionPruner distributionPruner = null;
        switch (distributionInfo.getType()) {
            case HASH: {
                HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                distributionPruner = new HashDistributionPruner(table.getTabletIdsInOrder(),
                        info.getDistributionColumns(),
                        columnFilters,
                        info.getBucketNum(),
                        getSelectedIndexId() == olapTable.getBaseIndexId());
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

    // Update the visible version of the scan range locations.
    public void updateScanRangeVersions(Map<Long, Long> visibleVersionMap) {
        if (LOG.isDebugEnabled() && ConnectContext.get() != null) {
            LOG.debug("query id: {}, selectedPartitionIds: {}, visibleVersionMap: {}",
                    DebugUtil.printId(ConnectContext.get().queryId()), selectedPartitionIds, visibleVersionMap);
        }

        Map<Long, TScanRangeLocations> locationsMap = scanRangeLocations.stream()
                .collect(Collectors.toMap(loc -> loc.getScanRange().getPaloScanRange().getTabletId(), loc -> loc));
        for (Long partitionId : selectedPartitionIds) {
            final Partition partition = olapTable.getPartition(partitionId);
            final MaterializedIndex selectedTable = partition.getIndex(selectedIndexId);
            final List<Tablet> tablets = selectedTable.getTablets();
            Long visibleVersion = visibleVersionMap.get(partitionId);
            assert visibleVersion != null : "the acquried version is not exists in the visible version map";
            String visibleVersionStr = String.valueOf(visibleVersion);
            for (Tablet tablet : tablets) {
                TScanRangeLocations locations = locationsMap.get(tablet.getId());
                if (locations == null) {
                    continue;
                }
                TPaloScanRange scanRange = locations.getScanRange().getPaloScanRange();
                scanRange.setVersion(visibleVersionStr);
            }
        }
    }

    public Long getTabletSingleReplicaSize(Long tabletId) {
        return tabletBytes.get(tabletId);
    }

    private void addScanRangeLocations(Partition partition,
            List<Tablet> tablets) throws UserException {
        long visibleVersion = Partition.PARTITION_INIT_VERSION;

        // For cloud mode, set scan range visible version in Coordinator.exec so that we could
        // assign a snapshot version of all partitions.
        if (!(Config.isCloudMode() && Config.enable_cloud_snapshot_version)) {
            visibleVersion = partition.getVisibleVersion();
        }
        String visibleVersionStr = String.valueOf(visibleVersion);

        Set<Tag> allowedTags = Sets.newHashSet();
        int useFixReplica = -1;
        boolean needCheckTags = false;
        boolean skipMissingVersion = false;
        if (ConnectContext.get() != null) {
            allowedTags = ConnectContext.get().getResourceTags();
            needCheckTags = ConnectContext.get().isResourceTagsSet();
            useFixReplica = ConnectContext.get().getSessionVariable().useFixReplica;
            // if use_fix_replica is set to true, set skip_missing_version to false
            skipMissingVersion = useFixReplica == -1 && ConnectContext.get().getSessionVariable().skipMissingVersion;
            if (LOG.isDebugEnabled()) {
                LOG.debug("query id: {}, partition id:{} visibleVersion: {}",
                        DebugUtil.printId(ConnectContext.get().queryId()), partition.getId(), visibleVersion);
            }
        }
        for (Tablet tablet : tablets) {
            long tabletId = tablet.getId();
            if (skipMissingVersion) {
                long tabletVersion = -1L;
                for (Replica replica : tablet.getReplicas()) {
                    if (replica.getVersion() > tabletVersion) {
                        tabletVersion = replica.getVersion();
                    }
                }
                if (tabletVersion != visibleVersion) {
                    LOG.warn("tablet {} version {} is not equal to partition {} version {}",
                            tabletId, tabletVersion, partition.getId(), visibleVersion);
                    visibleVersion = tabletVersion;
                    visibleVersionStr = String.valueOf(visibleVersion);
                }
            }
            TScanRangeLocations locations = new TScanRangeLocations();
            TPaloScanRange paloRange = new TPaloScanRange();
            paloRange.setDbName("");
            paloRange.setSchemaHash("0");
            paloRange.setVersion(visibleVersionStr);
            paloRange.setVersionHash("");
            paloRange.setTabletId(tabletId);

            // random shuffle List && only collect one copy
            //
            // ATTN: visibleVersion is not used in cloud mode, see CloudReplica.checkVersionCatchup
            // for details.
            List<Replica> replicas = tablet.getQueryableReplicas(visibleVersion, skipMissingVersion);
            if (replicas.isEmpty()) {
                if (ConnectContext.get().getSessionVariable().skipBadTablet) {
                    continue;
                }
                LOG.warn("no queryable replica found in tablet {}. visible version {}", tabletId, visibleVersion);
                StringBuilder sb = new StringBuilder(
                        "Failed to get scan range, no queryable replica found in tablet: " + tabletId);
                if (Config.show_details_for_unaccessible_tablet) {
                    sb.append(". Reason: ").append(tablet.getDetailsStatusForQuery(visibleVersion));
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(sb.toString());
                }
                throw new UserException(sb.toString());
            }

            if (useFixReplica <= -1) {
                if (skipMissingVersion) {
                    // sort by replica's last success version, higher success version in the front.
                    replicas.sort(Replica.LAST_SUCCESS_VERSION_COMPARATOR);
                } else {
                    Collections.shuffle(replicas);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("use fix replica, value: {}, replica count: {}", useFixReplica, replicas.size());
                }
                // sort by replica id
                replicas.sort(Replica.ID_COMPARATOR);
                Replica replica = replicas.get(useFixReplica >= replicas.size() ? replicas.size() - 1 : useFixReplica);
                if (ConnectContext.get().getSessionVariable().fallbackOtherReplicaWhenFixedCorrupt) {
                    Backend backend = Env.getCurrentSystemInfo().getBackend(replica.getBackendId());
                    // If the fixed replica is bad, then not clear the replicas using random replica
                    if (backend == null || !backend.isAlive()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("backend {} not exists or is not alive for replica {}", replica.getBackendId(),
                                    replica.getId());
                        }
                        Collections.shuffle(replicas);
                    } else {
                        replicas.clear();
                        replicas.add(replica);
                    }
                } else {
                    replicas.clear();
                    replicas.add(replica);
                }
            }

            if (Config.enable_cooldown_replica_affinity) {
                final long coolDownReplicaId = tablet.getCooldownReplicaId();
                // we prefer to query using cooldown replica to make sure the cache is fully utilized
                // for example: consider there are 3BEs(A,B,C) and each has one replica for tablet X. and X
                // is now under cooldown
                // first time we choose BE A, and A will download data into cache while the other two's cache is empty
                // second time we choose BE B, this time B will be cached, C is still empty
                // third time we choose BE C, after this time all replica is cached
                // but it means we will do 3 S3 IO to get the data which will bring 3 slow query
                if (-1L != coolDownReplicaId) {
                    final Optional<Replica> replicaOptional = replicas.stream()
                            .filter(r -> r.getId() == coolDownReplicaId).findAny();
                    replicaOptional.ifPresent(
                            r -> {
                                Backend backend = Env.getCurrentSystemInfo()
                                        .getBackend(r.getBackendId());
                                if (backend != null && backend.isAlive()) {
                                    replicas.clear();
                                    replicas.add(r);
                                }
                            }
                    );
                }
            }

            boolean tabletIsNull = true;
            boolean collectedStat = false;
            List<String> errs = Lists.newArrayList();

            int replicaInTablet = 0;
            long oneReplicaBytes = 0;
            for (Replica replica : replicas) {
                Backend backend = Env.getCurrentSystemInfo().getBackend(replica.getBackendId());
                if (backend == null || !backend.isAlive()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("backend {} not exists or is not alive for replica {}", replica.getBackendId(),
                                replica.getId());
                    }
                    String err = "replica " + replica.getId() + "'s backend " + replica.getBackendId()
                            + " does not exist or not alive";
                    if (Config.isCloudMode()) {
                        errs.add(ConnectContext.cloudNoBackendsReason());
                    }
                    errs.add(err);
                    continue;
                }
                if (!backend.isMixNode()) {
                    continue;
                }
                if (needCheckTags && !allowedTags.isEmpty() && !allowedTags.contains(backend.getLocationTag())) {
                    String err = String.format(
                            "Replica on backend %d with tag %s," + " which is not in user's resource tags: %s",
                            backend.getId(), backend.getLocationTag(), allowedTags);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(err);
                    }
                    errs.add(err);
                    continue;
                }
                scanReplicaIds.add(replica.getId());
                String ip = backend.getHost();
                int port = backend.getBePort();
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(ip, port));
                scanRangeLocation.setBackendId(replica.getBackendId());
                locations.addToLocations(scanRangeLocation);
                paloRange.addToHosts(new TNetworkAddress(ip, port));
                tabletIsNull = false;

                // for CBO
                if (!collectedStat && replica.getRowCount() != -1) {
                    long dataSize = replica.getDataSize();
                    if (replicaInTablet == 0) {
                        oneReplicaBytes = dataSize;
                        tabletBytes.put(tabletId, dataSize);
                    }
                    replicaInTablet++;
                    totalBytes += dataSize;
                    collectedStat = true;
                }
                scanBackendIds.add(backend.getId());
                // For skipping missing version of tablet, we only select the backend with the highest last
                // success version replica to save as much data as possible.
                if (skipMissingVersion) {
                    break;
                }
            }
            if (tabletIsNull) {
                throw new UserException("tablet " + tabletId + " has no queryable replicas. err: "
                        + Joiner.on(", ").join(errs));
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setPaloScanRange(paloRange);
            locations.setScanRange(scanRange);

            Integer bucketSeq = tabletId2BucketSeq.get(tabletId);
            bucketSeq2locations.put(bucketSeq, locations);
            bucketSeq2Bytes.merge(bucketSeq, oneReplicaBytes, Long::sum);
            scanRangeLocations.add(locations);
        }

        if (tablets.size() == 0) {
            desc.setCardinality(0);
        } else {
            desc.setCardinality(cardinality);
        }
    }

    private void computePartitionInfo() throws AnalysisException {
        long start = System.currentTimeMillis();
        // Step1: compute partition ids
        PartitionNames partitionNames = ((BaseTableRef) desc.getRef()).getPartitionNames();
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() == PartitionType.RANGE || partitionInfo.getType() == PartitionType.LIST) {
            selectedPartitionIds = partitionPrune(partitionInfo, partitionNames);
        } else {
            selectedPartitionIds = olapTable.getPartitionIds();
        }
        selectedPartitionIds = olapTable.selectNonEmptyPartitionIds(selectedPartitionIds);
        selectedPartitionNum = selectedPartitionIds.size();

        for (long id : selectedPartitionIds) {
            Partition partition = olapTable.getPartition(id);
            if (partition.getState() == PartitionState.RESTORE) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_PARTITION_STATE,
                        partition.getName(), "RESTORING");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("partition prune cost: {} ms, partitions: {}",
                    (System.currentTimeMillis() - start), selectedPartitionIds);
        }
    }

    public void selectBestRollupByRollupSelector(Analyzer analyzer) throws UserException {
        // Step2: select best rollup
        long start = System.currentTimeMillis();
        if (olapTable.getKeysType() == KeysType.DUP_KEYS || (olapTable.getKeysType() == KeysType.UNIQUE_KEYS
                && olapTable.getEnableUniqueKeyMergeOnWrite())) {
            // This function is compatible with the INDEX selection logic of ROLLUP,
            // so the Duplicate table here returns base index directly
            // and the selection logic of materialized view is selected in
            // "MaterializedViewSelector"
            selectedIndexId = olapTable.getBaseIndexId();
            if (LOG.isDebugEnabled()) {
                LOG.debug("The best index will be selected later in mv selector");
            }
            return;
        }
        final RollupSelector rollupSelector = new RollupSelector(analyzer, desc, olapTable);
        selectedIndexId = rollupSelector.selectBestRollup(selectedPartitionIds, conjuncts, isPreAggregation);
        updateSlotUniqueId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("select best roll up cost: {} ms, best index id: {}", (System.currentTimeMillis() - start),
                    selectedIndexId);
        }
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        scanRangeLocations = Lists.newArrayList();
        if (selectedPartitionIds.size() == 0) {
            desc.setCardinality(0);
            return;
        }
        Preconditions.checkState(selectedIndexId != -1);
        // compute tablet info by selected index id and selected partition ids
        long start = System.currentTimeMillis();
        computeSampleTabletIds();
        computeTabletInfo();
        if (LOG.isDebugEnabled()) {
            LOG.debug("distribution prune cost: {} ms", (System.currentTimeMillis() - start));
        }
    }

    public void setOutputColumnUniqueIds(Set<Integer> outputColumnUniqueIds) {
        this.outputColumnUniqueIds = outputColumnUniqueIds;
    }

    /**
     * Sample some tablets in the selected partition.
     * If Seek is specified, the tablets sampled each time are the same.
     */
    public void computeSampleTabletIds() {
        if (tableSample == null) {
            return;
        }
        OlapTable olapTable = (OlapTable) desc.getTable();

        // 1. Calculate the total number of rows in the selected partition, and sort partition list.
        long selectedRows = 0;
        long totalSampleRows = 0;
        List<Long> selectedPartitionList = new ArrayList<>();
        Preconditions.checkState(selectedIndexId != -1);
        for (Long partitionId : selectedPartitionIds) {
            final Partition partition = olapTable.getPartition(partitionId);
            final MaterializedIndex selectedIndex = partition.getIndex(selectedIndexId);
            // selectedIndex is not expected to be null, because MaterializedIndex ids in one rollup's partitions
            // are all same. skip this partition here.
            if (selectedIndex != null) {
                selectedRows += selectedIndex.getRowCount();
                selectedPartitionList.add(partitionId);
            }
        }
        selectedPartitionList.sort(Comparator.naturalOrder());

        // 2.Sampling is not required in some cases, will not take effect after clear sampleTabletIds.
        if (tableSample.isPercent()) {
            if (tableSample.getSampleValue() >= 100) {
                return;
            }
            totalSampleRows = (long) Math.max(selectedRows * (tableSample.getSampleValue() / 100.0), 1);
        } else {
            if (tableSample.getSampleValue() > selectedRows) {
                return;
            }
            totalSampleRows = tableSample.getSampleValue();
        }

        // 3. Sampling partition. If Seek is specified, the partition will be the same for each sampling.
        long hitRows = 0; // The number of rows hit by the tablet
        Set<Long> hitTabletIds = Sets.newHashSet();
        long partitionSeek = tableSample.getSeek() != -1
                ? tableSample.getSeek() : (long) (new SecureRandom().nextDouble() * selectedPartitionList.size());
        for (int i = 0; i < selectedPartitionList.size(); i++) {
            int seekPid = (int) ((i + partitionSeek) % selectedPartitionList.size());
            final Partition partition = olapTable.getPartition(selectedPartitionList.get(seekPid));
            final MaterializedIndex selectedTable = partition.getIndex(selectedIndexId);
            List<Tablet> tablets = selectedTable.getTablets();
            if (tablets.isEmpty()) {
                continue;
            }

            // 4. Calculate the number of rows that need to be sampled in the current partition.
            long sampleRows = 0; // The number of sample rows in partition
            if (tableSample.isPercent()) {
                sampleRows = (long) Math.max(selectedTable.getRowCount() * (tableSample.getSampleValue() / 100.0), 1);
            } else {
                sampleRows = (long) Math.max(
                        tableSample.getSampleValue() * (selectedTable.getRowCount() / selectedRows), 1);
            }

            // 5. Sampling tablets. If Seek is specified, the same tablet will be sampled each time.
            long tabletSeek = tableSample.getSeek() != -1
                    ? tableSample.getSeek() : (long) (new SecureRandom().nextDouble() * tablets.size());
            for (int j = 0; j < tablets.size(); j++) {
                int seekTid = (int) ((j + tabletSeek) % tablets.size());
                Tablet tablet = tablets.get(seekTid);
                if (sampleTabletIds.size() != 0 && !sampleTabletIds.contains(tablet.getId())) {
                    // After PruneOlapScanTablet, sampleTabletIds.size() != 0,
                    // continue sampling only in sampleTabletIds.
                    // If it is percentage sample, the number of sampled rows is a percentage of the
                    // total number of rows, and It is not related to sampleTabletI after PruneOlapScanTablet.
                    continue;
                }
                long tabletRowCount;
                if (!FeConstants.runningUnitTest) {
                    tabletRowCount = tablet.getRowCount(true);
                } else {
                    tabletRowCount = selectedTable.getRowCount() / tablets.size();
                }
                if (tabletRowCount == 0) {
                    continue;
                }
                hitTabletIds.add(tablet.getId());
                sampleRows -= tabletRowCount;
                hitRows += tabletRowCount;
                if (sampleRows <= 0) {
                    break;
                }
            }
            if (hitRows > totalSampleRows) {
                break;
            }
        }
        if (sampleTabletIds.size() != 0) {
            sampleTabletIds.retainAll(hitTabletIds);
            if (LOG.isDebugEnabled()) {
                LOG.debug("after computeSampleTabletIds, hitRows {}, totalRows {}, selectedTablets {}, sampleRows {}",
                        hitRows, selectedRows, sampleTabletIds.size(), totalSampleRows);
            }
        } else {
            sampleTabletIds = hitTabletIds;
            if (LOG.isDebugEnabled()) {
                LOG.debug("after computeSampleTabletIds, hitRows {}, selectedRows {}, sampleRows {}",
                        hitRows, selectedRows, totalSampleRows);
            }
        }
    }

    public boolean isFromPrepareStmt() {
        return this.isFromPrepareStmt;
    }

    public void setPointQueryEqualPredicates(Map<SlotRef, Expr> predicates) {
        this.pointQueryEqualPredicats = predicates;
    }

    public Map<SlotRef, Expr> getPointQueryEqualPredicates() {
        return this.pointQueryEqualPredicats;
    }

    public boolean isPointQuery() {
        return this.pointQueryEqualPredicats != null
                    || (preparedStatment != null && preparedStatment.isPointQueryShortCircuit())
                    || ConnectContext.get().getStatementContext().isShortCircuitQuery();
    }

    private void computeTabletInfo() throws UserException {
        /**
         * The tablet info could be computed only once.
         * So the scanBackendIds should be empty in the beginning.
         */
        Preconditions.checkState(scanBackendIds.size() == 0);
        Preconditions.checkState(scanTabletIds.size() == 0);
        for (Long partitionId : selectedPartitionIds) {
            final Partition partition = olapTable.getPartition(partitionId);
            final MaterializedIndex selectedTable = partition.getIndex(selectedIndexId);
            final List<Tablet> tablets = Lists.newArrayList();
            Collection<Long> tabletIds = distributionPrune(selectedTable, partition.getDistributionInfo());
            if (LOG.isDebugEnabled()) {
                LOG.debug("distribution prune tablets: {}", tabletIds);
            }
            if (sampleTabletIds.size() != 0) {
                if (tabletIds != null) {
                    tabletIds.retainAll(sampleTabletIds);
                } else {
                    tabletIds = sampleTabletIds;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("after sample tablets: {}", tabletIds);
                }
            }

            List<Long> allTabletIds = selectedTable.getTabletIdsInOrder();
            if (tabletIds != null) {
                for (Long id : tabletIds) {
                    if (selectedTable.getTablet(id) != null) {
                        tablets.add(selectedTable.getTablet(id));
                        scanTabletIds.add(id);
                    } else {
                        // The tabletID specified in query does not exist in this partition, skip scan partition.
                        Preconditions.checkState(sampleTabletIds.size() != 0);
                    }
                }
            } else {
                tablets.addAll(selectedTable.getTablets());
                scanTabletIds.addAll(allTabletIds);
            }

            for (int i = 0; i < allTabletIds.size(); i++) {
                tabletId2BucketSeq.put(allTabletIds.get(i), i);
            }

            totalTabletsNum += selectedTable.getTablets().size();
            selectedSplitNum += tablets.size();
            addScanRangeLocations(partition, tablets);
        }
    }

    /**
     * Check Parent sort node can push down to child olap scan.
     */
    public boolean checkPushSort(SortNode sortNode) {
        // Ensure limit is less then threshold
        if (sortNode.getLimit() <= 0
                || sortNode.getLimit() > ConnectContext.get().getSessionVariable().topnOptLimitThreshold) {
            return false;
        }

        // Ensure all isAscOrder is same, ande length != 0.
        // Can't be zorder.
        if (sortNode.getSortInfo().getIsAscOrder().stream().distinct().count() != 1
                || olapTable.isZOrderSort()) {
            return false;
        }

        // Tablet's order by key only can be the front part of schema.
        // Like: schema: a.b.c.d.e.f.g order by key: a.b.c (no a,b,d)
        // Do **prefix match** to check if order by key can be pushed down.
        // olap order by key: a.b.c.d
        // sort key: (a) (a,b) (a,b,c) (a,b,c,d) is ok
        //           (a,c) (a,c,d), (a,c,b) (a,c,f) (a,b,c,d,e)is NOT ok
        List<Expr> sortExprs = sortNode.getSortInfo().getOrigOrderingExprs();
        List<Boolean> nullsFirsts = sortNode.getSortInfo().getNullsFirst();
        List<Boolean> isAscOrders = sortNode.getSortInfo().getIsAscOrder();
        if (sortExprs.size() > olapTable.getDataSortInfo().getColNum()) {
            return false;
        }
        for (int i = 0; i < sortExprs.size(); i++) {
            // table key.
            Column tableKey = olapTable.getFullSchema().get(i);
            // sort slot.
            Expr sortExpr = sortExprs.get(i);
            if (sortExpr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) sortExpr;
                if (tableKey.equals(slotRef.getColumn())) {
                    // ORDER BY DESC NULLS FIRST can not be optimized to only read file tail,
                    // since NULLS is at file head but data is at tail
                    if (tableKey.isAllowNull() && nullsFirsts.get(i) && !isAscOrders.get(i)) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        return true;
    }

    /**
     * We query Palo Meta to get request's data location
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    // Only called when Coordinator exec in high performance point query
    public List<TScanRangeLocations> lazyEvaluateRangeLocations() throws UserException {
        // Lazy evaluation
        selectedIndexId = olapTable.getBaseIndexId();
        // Only key columns
        computeColumnsFilter(olapTable.getBaseSchemaKeyColumns(), olapTable.getPartitionInfo());
        computePartitionInfo();
        scanBackendIds.clear();
        scanTabletIds.clear();
        bucketSeq2locations.clear();
        scanReplicaIds.clear();
        sampleTabletIds.clear();
        try {
            createScanRangeLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
        return scanRangeLocations;
    }

    public void setDescTable(DescriptorTable descTable) {
        this.descTable = descTable;
    }

    public DescriptorTable getDescTable() {
        return this.descTable;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        long selectedIndexIdForExplain = selectedIndexId;
        if (selectedIndexIdForExplain == -1) {
            // If there is no data in table, the selectedIndexId will be -1, set it to base index id,
            // so that to avoid "null" in explain result.
            selectedIndexIdForExplain = olapTable.getBaseIndexId();
        }
        String indexName = olapTable.getIndexNameById(selectedIndexIdForExplain);
        output.append(prefix).append("TABLE: ").append(olapTable.getQualifiedName())
                .append("(").append(indexName).append(")");
        if (detailLevel == TExplainLevel.BRIEF) {
            output.append("\n").append(prefix).append(String.format("cardinality=%,d", cardinality));
            if (cardinalityAfterFilter != -1) {
                output.append("\n").append(prefix).append(String.format("afterFilter=%,d", cardinalityAfterFilter));
            }
            if (!runtimeFilters.isEmpty()) {
                output.append("\n").append(prefix).append("Apply RFs: ");
                output.append(getRuntimeFilterExplainString(false, true));
            }
            if (!conjuncts.isEmpty()) {
                output.append("\n").append(prefix).append("PREDICATES: ").append(conjuncts.size()).append("\n");
            }
            return output.toString();
        }
        if (isPreAggregation) {
            output.append(", PREAGGREGATION: ON");
        } else {
            output.append(", PREAGGREGATION: OFF. Reason: ").append(reasonOfPreAggregation);
        }
        output.append("\n");

        if (sortColumn != null) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (sortInfo != null) {
            output.append(prefix).append("SORT INFO:\n");
            sortInfo.getMaterializedOrderingExprs().forEach(expr -> {
                output.append(prefix).append(prefix).append(expr.toSql()).append("\n");
            });
        }
        if (sortLimit != -1) {
            output.append(prefix).append("SORT LIMIT: ").append(sortLimit).append("\n");
        }
        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ").append(expr.toSql()).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            output.append(prefix).append("runtime filters: ");
            output.append(getRuntimeFilterExplainString(false));
        }

        String selectedPartitions = getSelectedPartitionIds().stream().sorted()
                .map(id -> olapTable.getPartition(id).getName())
                .collect(Collectors.joining(","));
        output.append(prefix).append(String.format("partitions=%s/%s (%s)", selectedPartitionNum,
                olapTable.getPartitions().size(), selectedPartitions)).append("\n");
        output.append(prefix).append(String.format("tablets=%s/%s", selectedSplitNum, totalTabletsNum));
        // We print up to 3 tablet, and we print "..." if the number is more than 3
        if (scanTabletIds.size() > 3) {
            List<Long> firstTenTabletIds = scanTabletIds.subList(0, 3);
            output.append(String.format(", tabletList=%s ...", Joiner.on(",").join(firstTenTabletIds)));
        } else {
            output.append(String.format(", tabletList=%s", Joiner.on(",").join(scanTabletIds)));
        }
        output.append("\n");

        output.append(prefix).append(String.format("cardinality=%s", cardinality))
                .append(String.format(", avgRowSize=%s", avgRowSize)).append(String.format(", numNodes=%s", numNodes));
        output.append("\n");
        if (pushDownAggNoGroupingOp != null) {
            output.append(prefix).append("pushAggOp=").append(pushDownAggNoGroupingOp).append("\n");
        }
        if (isPointQuery()) {
            output.append(prefix).append("SHORT-CIRCUIT\n");
        }

        if (!CollectionUtils.isEmpty(rewrittenProjectList)) {
            output.append(prefix).append("rewrittenProjectList: ").append(
                    getExplainString(rewrittenProjectList)).append("\n");
        }

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        // In pipeline exec engine, the instance num equals be_num * parallel instance.
        // so here we need count distinct be_num to do the work. make sure get right instance
        if (ConnectContext.get().getSessionVariable().isIgnoreStorageDataDistribution()) {
            return ConnectContext.get().getSessionVariable().getParallelExecInstanceNum();
        }
        return scanRangeLocations.size();
    }

    @Override
    public void setShouldColoScan() {
        shouldColoScan = true;
    }

    @Override
    public boolean getShouldColoScan() {
        return shouldColoScan;
    }

    public int getBucketNum() {
        // In bucket shuffle join, we have 2 situation.
        // 1. Only one partition: in this case, we use scanNode.getTotalTabletsNum() to get the right bucket num
        //    because when table turn on dynamic partition, the bucket number in default distribution info
        //    is not correct.
        // 2. Table is colocated: in this case, table could have more than one partition, but all partition's
        //    bucket number must be same, so we use default bucket num is ok.
        if (olapTable.isColocateTable()) {
            return olapTable.getDefaultDistributionInfo().getBucketNum();
        } else {
            return (int) totalTabletsNum;
        }
    }

    @Override
    // If scan is key search, should not enable the shared scan opt to prevent the performance problem
    // 1. where contain the eq or in expr of key column slot
    // 2. key column slot is distribution column and first column
    // FIXME: this is not a good check, we can not guarantee that the predicate we check can truly
    // help to prune the data, so we should check the predicate's effect on the data.
    protected boolean isKeySearch() {
        List<SlotRef> whereSlot = Lists.newArrayList();
        for (Expr conjunct : conjuncts) {
            if (conjunct instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) conjunct;
                if (binaryPredicate.getOp().isEquivalence()) {
                    if (binaryPredicate.getChild(0) instanceof SlotRef) {
                        whereSlot.add((SlotRef) binaryPredicate.getChild(0));
                    }
                    if (binaryPredicate.getChild(1) instanceof SlotRef) {
                        whereSlot.add((SlotRef) binaryPredicate.getChild(1));
                    }
                }
            }

            if (conjunct instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) conjunct;
                if (!inPredicate.isNotIn()) {
                    if (inPredicate.getChild(0) instanceof SlotRef) {
                        whereSlot.add((SlotRef) inPredicate.getChild(0));
                    }
                    if (inPredicate.getChild(1) instanceof SlotRef) {
                        whereSlot.add((SlotRef) inPredicate.getChild(1));
                    }
                }
            }
        }

        for (SlotRef slotRef : whereSlot) {
            String columnName = slotRef.getDesc().getColumn().getName().toLowerCase();
            if (olapTable != null) {
                if (olapTable.getDistributionColumnNames().contains(columnName)
                        && olapTable.getBaseSchema().get(0).getName().toLowerCase().equals(columnName)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        List<String> keyColumnNames = new ArrayList<String>();
        List<TPrimitiveType> keyColumnTypes = new ArrayList<TPrimitiveType>();
        List<TColumn> columnsDesc = new ArrayList<TColumn>();
        olapTable.getColumnDesc(selectedIndexId, columnsDesc, keyColumnNames, keyColumnTypes);
        List<TOlapTableIndex> indexDesc = Lists.newArrayList();

        // Add extra row id column
        ArrayList<SlotDescriptor> slots = desc.getSlots();
        Column lastColumn = slots.get(slots.size() - 1).getColumn();
        if (lastColumn != null && lastColumn.getName().equalsIgnoreCase(Column.ROWID_COL)) {
            TColumn tColumn = new TColumn();
            tColumn.setColumnName(Column.ROWID_COL);
            tColumn.setColumnType(ScalarType.createStringType().toColumnTypeThrift());
            tColumn.setAggregationType(AggregateType.REPLACE.toThrift());
            tColumn.setIsKey(false);
            tColumn.setIsAllowNull(false);
            // keep compatibility
            tColumn.setVisible(false);
            tColumn.setColUniqueId(Integer.MAX_VALUE);
            columnsDesc.add(tColumn);
        }

        for (Index index : olapTable.getIndexes()) {
            TOlapTableIndex tIndex = index.toThrift();
            indexDesc.add(tIndex);
        }

        msg.node_type = TPlanNodeType.OLAP_SCAN_NODE;
        if (olapTable.getBaseSchema().stream().anyMatch(Column::isClusterKey)) {
            keyColumnNames.clear();
            keyColumnTypes.clear();
        }
        msg.olap_scan_node = new TOlapScanNode(desc.getId().asInt(), keyColumnNames, keyColumnTypes, isPreAggregation);
        msg.olap_scan_node.setColumnsDesc(columnsDesc);
        msg.olap_scan_node.setIndexesDesc(indexDesc);
        if (selectedIndexId != -1) {
            msg.olap_scan_node.setSchemaVersion(olapTable.getIndexSchemaVersion(selectedIndexId));
        }
        if (null != sortColumn) {
            msg.olap_scan_node.setSortColumn(sortColumn);
        }
        if (sortInfo != null) {
            TSortInfo tSortInfo = new TSortInfo(
                    Expr.treesToThrift(sortInfo.getOrderingExprs()),
                    sortInfo.getIsAscOrder(),
                    sortInfo.getNullsFirst());
            if (sortInfo.getSortTupleSlotExprs() != null) {
                tSortInfo.setSortTupleSlotExprs(Expr.treesToThrift(sortInfo.getSortTupleSlotExprs()));
            }
            msg.olap_scan_node.setSortInfo(tSortInfo);
        }
        if (sortLimit != -1) {
            msg.olap_scan_node.setSortLimit(sortLimit);
        }
        msg.olap_scan_node.setKeyType(olapTable.getKeysType().toThrift());
        String tableName = olapTable.getName();
        if (selectedIndexId != -1) {
            tableName = tableName + "(" + getSelectedIndexName() + ")";
        }
        msg.olap_scan_node.setTableName(tableName);
        msg.olap_scan_node.setEnableUniqueKeyMergeOnWrite(olapTable.getEnableUniqueKeyMergeOnWrite());

        msg.setPushDownAggTypeOpt(pushDownAggNoGroupingOp);

        msg.olap_scan_node.setPushDownAggTypeOpt(pushDownAggNoGroupingOp);
        // In TOlapScanNode , pushDownAggNoGroupingOp field is deprecated.

        if (outputColumnUniqueIds != null) {
            msg.olap_scan_node.setOutputColumnUniqueIds(outputColumnUniqueIds);
        }

        msg.olap_scan_node.setDistributeColumnIds(new ArrayList<>(distributionColumnIds));

        super.toThrift(msg);
    }

    public void collectColumns(Analyzer analyzer, Set<String> equivalenceColumns, Set<String> unequivalenceColumns) {
        // 1. Get columns which has predicate on it.
        for (Expr expr : conjuncts) {
            if (!isPredicateUsedForPrefixIndex(expr, false)) {
                continue;
            }
            for (SlotDescriptor slot : desc.getMaterializedSlots()) {
                if (expr.isBound(slot.getId())) {
                    if (!isEquivalenceExpr(expr)) {
                        unequivalenceColumns.add(slot.getColumn().getName());
                    } else {
                        equivalenceColumns.add(slot.getColumn().getName());
                    }
                    break;
                }
            }
        }

        // 2. Equal join predicates when pushing inner child.
        List<Expr> eqJoinPredicate = analyzer.getEqJoinConjuncts(desc.getId());
        for (Expr expr : eqJoinPredicate) {
            if (!isPredicateUsedForPrefixIndex(expr, true)) {
                continue;
            }
            for (SlotDescriptor slot : desc.getMaterializedSlots()) {
                Preconditions.checkState(expr.getChildren().size() == 2);
                for (Expr child : expr.getChildren()) {
                    if (child.isBound(slot.getId())) {
                        equivalenceColumns.add(slot.getColumn().getName());
                        break;
                    }
                }
            }
        }
    }

    private void analyzerPartitionExpr(Analyzer analyzer, PartitionInfo partitionInfo) throws AnalysisException {
        ArrayList<Expr> exprs = partitionInfo.getPartitionExprs();
        for (Expr e : exprs) {
            e.analyze(analyzer);
        }
    }

    public TupleId getTupleId() {
        Preconditions.checkNotNull(desc);
        return desc.getId();
    }

    private boolean isEquivalenceExpr(Expr expr) {
        if (expr instanceof InPredicate) {
            return true;
        }
        if (expr instanceof BinaryPredicate) {
            final BinaryPredicate predicate = (BinaryPredicate) expr;
            if (predicate.getOp().isEquivalence()) {
                return true;
            }
        }
        return false;
    }

    private boolean isPredicateUsedForPrefixIndex(Expr expr, boolean isJoinConjunct) {
        if (!(expr instanceof InPredicate)
                && !(expr instanceof BinaryPredicate)) {
            return false;
        }
        if (expr instanceof InPredicate) {
            return isInPredicateUsedForPrefixIndex((InPredicate) expr);
        } else if (expr instanceof BinaryPredicate) {
            if (isJoinConjunct) {
                return isEqualJoinConjunctUsedForPrefixIndex((BinaryPredicate) expr);
            } else {
                return isBinaryPredicateUsedForPrefixIndex((BinaryPredicate) expr);
            }
        }
        return true;
    }

    private boolean isEqualJoinConjunctUsedForPrefixIndex(BinaryPredicate expr) {
        Preconditions.checkArgument(expr.getOp().isEquivalence());
        if (expr.isAuxExpr()) {
            return false;
        }
        for (Expr child : expr.getChildren()) {
            for (SlotDescriptor slot : desc.getMaterializedSlots()) {
                if (child.isBound(slot.getId()) && isSlotRefNested(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isBinaryPredicateUsedForPrefixIndex(BinaryPredicate expr) {
        if (expr.isAuxExpr() || expr.getOp().isUnequivalence()) {
            return false;
        }
        return (isSlotRefNested(expr.getChild(0)) && expr.getChild(1).isConstant())
                || (isSlotRefNested(expr.getChild(1)) && expr.getChild(0).isConstant());
    }

    private boolean isInPredicateUsedForPrefixIndex(InPredicate expr) {
        if (expr.isNotIn()) {
            return false;
        }
        return isSlotRefNested(expr.getChild(0)) && expr.isLiteralChildren();
    }

    private boolean isSlotRefNested(Expr expr) {
        while (expr instanceof CastExpr) {
            expr = expr.getChild(0);
        }
        return expr instanceof SlotRef;
    }

    private void filterDeletedRows(Analyzer analyzer) throws AnalysisException {
        if (!Util.showHiddenColumns() && olapTable.hasDeleteSign() && !ConnectContext.get().getSessionVariable()
                .skipDeleteSign()) {
            SlotRef deleteSignSlot = new SlotRef(desc.getAliasAsName(), Column.DELETE_SIGN);
            deleteSignSlot.analyze(analyzer);
            deleteSignSlot.getDesc().setIsMaterialized(true);
            Expr conjunct = new BinaryPredicate(BinaryPredicate.Operator.EQ, deleteSignSlot, new IntLiteral(0));
            conjunct.analyze(analyzer);
            conjuncts.add(conjunct);
            if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
                closePreAggregation(Column.DELETE_SIGN + " is used as conjuncts.");
            }
        }
    }

    /*
     * Although sometimes the scan range only involves one instance,
     * the data distribution cannot be set to UNPARTITIONED here.
     * The reason is that @coordinator will not set the scan range for the fragment,
     * when data partition of fragment is UNPARTITIONED.
     */
    public DataPartition constructInputPartitionByDistributionInfo() throws UserException {
        ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
        if ((colocateTableIndex.isColocateTable(olapTable.getId())
                && !colocateTableIndex.isGroupUnstable(colocateTableIndex.getGroup(olapTable.getId())))
                || olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED
                || olapTable.getPartitions().size() == 1) {
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            if (!(distributionInfo instanceof HashDistributionInfo)) {
                return DataPartition.RANDOM;
            }
            List<Column> distributeColumns = ((HashDistributionInfo) distributionInfo).getDistributionColumns();
            List<Expr> dataDistributeExprs = Lists.newArrayList();
            for (Column column : distributeColumns) {
                SlotRef slotRef = new SlotRef(desc.getRef().getName(), column.getName());
                dataDistributeExprs.add(slotRef);
            }
            return DataPartition.hashPartitioned(dataDistributeExprs);
        } else {
            return DataPartition.RANDOM;
        }
    }

    @VisibleForTesting
    public String getReasonOfPreAggregation() {
        return reasonOfPreAggregation;
    }

    @VisibleForTesting
    public String getSelectedIndexName() {
        return olapTable.getIndexNameById(selectedIndexId);
    }

    @Override
    public void finalizeForNereids() {
        computeNumNodes();
        computeStatsForNereids();
        // NOTICE: must call here to get selected tablet row count to let block rules work well.
        mockRowCountInStatistic();
    }

    private void computeStatsForNereids() {
        if (cardinality > 0 && avgRowSize <= 0) {
            avgRowSize = totalBytes / (float) cardinality * COMPRESSION_RATIO;
            capCardinalityAtLimit();
        }
        // when node scan has no data, cardinality should be 0 instead of a invalid
        // value after computeStats()
        cardinality = cardinality == -1 ? 0 : cardinality;
    }

    Set<String> getDistributionColumnNames() {
        return olapTable != null
                ? olapTable.getDistributionColumnNames()
                : Sets.newTreeSet();
    }

    @Override
    public void updateRequiredSlots(PlanTranslatorContext context,
            Set<SlotId> requiredByProjectSlotIdSet) {
        outputColumnUniqueIds.clear();
        for (SlotDescriptor slot : context.getTupleDesc(this.getTupleId()).getSlots()) {
            if (requiredByProjectSlotIdSet.contains(slot.getId()) && slot.getColumn() != null) {
                outputColumnUniqueIds.add(slot.getColumn().getUniqueId());
            }
        }
    }

    @Override
    public StatsDelta genStatsDelta() throws AnalysisException {
        return new StatsDelta(Env.getCurrentEnv().getCurrentCatalog().getId(),
                Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(
                        olapTable.getQualifiedDbName()).getId(),
                olapTable.getId(), selectedIndexId == -1 ? olapTable.getBaseIndexId() : selectedIndexId,
                scanReplicaIds);
    }

    @Override
    public boolean pushDownAggNoGrouping(FunctionCallExpr aggExpr) {
        KeysType type = getOlapTable().getKeysType();
        if (type == KeysType.UNIQUE_KEYS || type == KeysType.PRIMARY_KEYS) {
            return false;
        }

        String aggFunctionName = aggExpr.getFnName().getFunction();
        if (aggFunctionName.equalsIgnoreCase("COUNT") && type != KeysType.DUP_KEYS) {
            return false;
        }

        return true;
    }

    @Override
    public boolean pushDownAggNoGroupingCheckCol(FunctionCallExpr aggExpr, Column col) {
        KeysType type = getOlapTable().getKeysType();

        // The value column of the agg does not support zone_map index.
        if (type == KeysType.AGG_KEYS && !col.isKey()) {
            return false;
        }

        if (aggExpr.getChild(0) instanceof SlotRef) {
            SlotRef slot = (SlotRef) aggExpr.getChild(0);
            if (CreateMaterializedViewStmt.isMVColumn(slot.getColumnName()) && slot.getColumn().isAggregated()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int getScanRangeNum() {
        return getScanTabletIds().size();
    }

    @Override
    public int numScanBackends() {
        return scanBackendIds.size();
    }
}
