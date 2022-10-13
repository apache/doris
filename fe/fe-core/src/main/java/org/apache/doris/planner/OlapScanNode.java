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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
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
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.Tag;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TOlapScanNode;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TPushAggOp;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// Full scan of an Olap table.
public class OlapScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(OlapScanNode.class);

    // average compression ratio in doris storage engine
    private static final int COMPRESSION_RATIO = 5;

    private List<TScanRangeLocations> result = new ArrayList<>();
    /*
     * When the field value is ON, the storage engine can return the data directly
     * without pre-aggregation.
     * When the field value is OFF, the storage engine needs to aggregate the data
     * before returning to scan node.
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
     * returning to scan node.
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
    private long selectedTabletsNum = 0;
    private long totalTabletsNum = 0;
    private long selectedIndexId = -1;
    private int selectedPartitionNum = 0;
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();
    private long totalBytes = 0;

    private SortInfo sortInfo = null;

    // When scan match sort_info, we can push limit into OlapScanNode.
    // It's limit for scanner instead of scanNode so we add a new limit.
    private long sortLimit = -1;

    private TPushAggOp pushDownAggNoGroupingOp = null;

    // List of tablets will be scanned by current olap_scan_node
    private ArrayList<Long> scanTabletIds = Lists.newArrayList();

    private Set<Long> sampleTabletIds = Sets.newHashSet();
    private TableSample tableSample;

    private HashSet<Long> scanBackendIds = new HashSet<>();

    private Map<Long, Integer> tabletId2BucketSeq = Maps.newHashMap();
    // a bucket seq may map to many tablets, and each tablet has a
    // TScanRangeLocations.
    public ArrayListMultimap<Integer, TScanRangeLocations> bucketSeq2locations = ArrayListMultimap.create();

    // Constructs node to scan given data files of table 'tbl'.
    public OlapScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName, StatisticalType.OLAP_SCAN_NODE);
        olapTable = (OlapTable) desc.getTable();
    }

    public void setIsPreAggregation(boolean isPreAggregation, String reason) {
        this.isPreAggregation = isPreAggregation;
        this.reasonOfPreAggregation = this.reasonOfPreAggregation == null ? reason :
                                      this.reasonOfPreAggregation + " " + reason;
    }

    public void setPushDownAggNoGrouping(TPushAggOp pushDownAggNoGroupingOp) {
        this.pushDownAggNoGroupingOp = pushDownAggNoGroupingOp;
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

    public void setSampleTabletIds(List<Long> sampleTablets) {
        if (sampleTablets != null) {
            this.sampleTabletIds.addAll(sampleTablets);
        }
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

    public void setForceOpenPreAgg(boolean forceOpenPreAgg) {
        this.forceOpenPreAgg = forceOpenPreAgg;
    }

    public Integer getSelectedPartitionNum() {
        return selectedPartitionNum;
    }

    public Long getSelectedTabletsNum() {
        return selectedTabletsNum;
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
     * Only used for Neredis to set rollup or materialized view selection result.
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
        CHECK: { // CHECKSTYLE IGNORE THIS LINE
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
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
            if (sessionVariable.getTestMaterializedView()) {
                throw new AnalysisException("The old scan range info is different from the new one when "
                        + "test_materialized_view is true. "
                        + scanRangeInfo);
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
    private void updateColumnType() {
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
            Preconditions.checkNotNull(mvColumn);
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
    private void updateSlotUniqueId() {
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
            slotDescriptor.setColumn(mvColumn);
        }
        LOG.debug("updateSlotUniqueId() slots: {}", desc.getSlots());
    }

    public OlapTable getOlapTable() {
        return olapTable;
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
        computeColumnFilter();
        computePartitionInfo();
        computeTupleState(analyzer);
        computeSampleTabletIds();

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
            getScanRangeLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
    }

    /**
     * Remove the method after statistics collection is working properly
     */
    public void mockRowCountInStatistic() {
        long tableId = desc.getTable().getId();
        cardinality = 0;
        for (long selectedPartitionId : selectedPartitionIds) {
            final Partition partition = olapTable.getPartition(selectedPartitionId);
            final MaterializedIndex baseIndex = partition.getBaseIndex();
            cardinality += baseIndex.getRowCount();
        }
        Env.getCurrentEnv().getStatisticsManager()
                .getStatistics().mockTableStatsWithRowCount(tableId, cardinality);
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        LOG.debug("OlapScanNode get scan range locations. Tuple: {}", desc);
        /**
         * If JoinReorder is turned on, it will be calculated init(), and this value is
         * not accurate.
         * In the following logic, cardinality will be accurately calculated again.
         * So here we need to reset the value of cardinality.
         */
        if (analyzer.safeIsEnableJoinReorderBasedCost()) {
            cardinality = 0;
        }
        try {
            getScanRangeLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
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
            statsDeriveResult.setRowCount(cardinality);
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
            if (analyzer.partitionPruneV2Enabled()) {
                partitionPruner = new RangePartitionPrunerV2(keyItemMap,
                        partitionInfo.getPartitionColumns(), columnNameToRange);
            } else {
                partitionPruner = new RangePartitionPruner(keyItemMap,
                        partitionInfo.getPartitionColumns(), columnFilters);
            }
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            if (analyzer.partitionPruneV2Enabled()) {
                partitionPruner = new ListPartitionPrunerV2(keyItemMap, partitionInfo.getPartitionColumns(),
                        columnNameToRange);
            } else {
                partitionPruner = new ListPartitionPruner(keyItemMap,
                        partitionInfo.getPartitionColumns(), columnFilters);
            }
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
            List<Tablet> tablets) throws UserException {
        long visibleVersion = partition.getVisibleVersion();
        String visibleVersionStr = String.valueOf(visibleVersion);

        Set<Tag> allowedTags = Sets.newHashSet();
        boolean needCheckTags = false;
        if (ConnectContext.get() != null) {
            allowedTags = ConnectContext.get().getResourceTags();
            needCheckTags = ConnectContext.get().isResourceTagsSet();
        }
        for (Tablet tablet : tablets) {
            long tabletId = tablet.getId();
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
            TPaloScanRange paloRange = new TPaloScanRange();
            paloRange.setDbName("");
            paloRange.setSchemaHash("0");
            paloRange.setVersion(visibleVersionStr);
            paloRange.setVersionHash("");
            paloRange.setTabletId(tabletId);

            // random shuffle List && only collect one copy
            List<Replica> replicas = tablet.getQueryableReplicas(visibleVersion);
            if (replicas.isEmpty()) {
                LOG.error("no queryable replica found in tablet {}. visible version {}",
                        tabletId, visibleVersion);
                if (LOG.isDebugEnabled()) {
                    for (Replica replica : tablet.getReplicas()) {
                        LOG.debug("tablet {}, replica: {}", tabletId, replica.toString());
                    }
                }
                throw new UserException("Failed to get scan range, no queryable replica found in tablet: " + tabletId);
            }

            Collections.shuffle(replicas);
            boolean tabletIsNull = true;
            boolean collectedStat = false;
            List<String> errs = Lists.newArrayList();
            for (Replica replica : replicas) {
                Backend backend = Env.getCurrentSystemInfo().getBackend(replica.getBackendId());
                if (backend == null || !backend.isAlive()) {
                    LOG.debug("backend {} not exists or is not alive for replica {}", replica.getBackendId(),
                            replica.getId());
                    errs.add(replica.getId() + "'s backend " + replica.getBackendId() + " does not exist or not alive");
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
                String ip = backend.getHost();
                int port = backend.getBePort();
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(ip, port));
                scanRangeLocation.setBackendId(replica.getBackendId());
                scanRangeLocations.addToLocations(scanRangeLocation);
                paloRange.addToHosts(new TNetworkAddress(ip, port));
                tabletIsNull = false;

                // for CBO
                if (!collectedStat && replica.getRowCount() != -1) {
                    cardinality += replica.getRowCount();
                    totalBytes += replica.getDataSize();
                    collectedStat = true;
                }
                scanBackendIds.add(backend.getId());
            }
            if (tabletIsNull) {
                throw new UserException(tabletId + " have no queryable replicas. err: " + Joiner.on(", ").join(errs));
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setPaloScanRange(paloRange);
            scanRangeLocations.setScanRange(scanRange);

            bucketSeq2locations.put(tabletId2BucketSeq.get(tabletId), scanRangeLocations);

            result.add(scanRangeLocations);
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
            selectedPartitionIds = null;
        }

        if (selectedPartitionIds == null) {
            selectedPartitionIds = Lists.newArrayList();
            for (Partition partition : olapTable.getPartitions()) {
                if (!partition.hasData()) {
                    continue;
                }
                selectedPartitionIds.add(partition.getId());
            }
        } else {
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(id -> olapTable.getPartition(id).hasData())
                    .collect(Collectors.toList());
        }
        selectedPartitionNum = selectedPartitionIds.size();

        for (long id : selectedPartitionIds) {
            Partition partition = olapTable.getPartition(id);
            if (partition.getState() == PartitionState.RESTORE) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_PARTITION_STATE,
                        partition.getName(), "RESTORING");
            }
        }
        LOG.debug("partition prune cost: {} ms, partitions: {}",
                (System.currentTimeMillis() - start), selectedPartitionIds);
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
            LOG.debug("The best index will be selected later in mv selector");
            return;
        }
        final RollupSelector rollupSelector = new RollupSelector(analyzer, desc, olapTable);
        selectedIndexId = rollupSelector.selectBestRollup(selectedPartitionIds, conjuncts, isPreAggregation);
        updateSlotUniqueId();
        LOG.debug("select best roll up cost: {} ms, best index id: {}",
                (System.currentTimeMillis() - start), selectedIndexId);
    }

    private void getScanRangeLocations() throws UserException {
        if (selectedPartitionIds.size() == 0) {
            desc.setCardinality(0);
            return;
        }
        Preconditions.checkState(selectedIndexId != -1);
        // compute tablet info by selected index id and selected partition ids
        long start = System.currentTimeMillis();
        computeTabletInfo();
        LOG.debug("distribution prune cost: {} ms", (System.currentTimeMillis() - start));
    }

    /**
     * First, determine how many rows to sample from each partition according to the number of partitions.
     * Then determine the number of Tablets to be selected for each partition according to the average number
     * of rows of Tablet,
     * If seek is not specified, the specified number of Tablets are pseudo-randomly selected from each partition.
     * If seek is specified, it will be selected sequentially from the seek tablet of the partition.
     * And add the manually specified Tablet id to the selected Tablet.
     * simpleTabletNums = simpleRows / partitionNums / (partitionRows / partitionTabletNums)
     */
    public void computeSampleTabletIds() {
        if (tableSample == null) {
            return;
        }
        OlapTable olapTable = (OlapTable) desc.getTable();
        long sampleRows; // The total number of sample rows
        long hitRows = 1; // The total number of rows hit by the tablet
        long totalRows = 0; // The total number of partition rows hit
        long totalTablet = 0; // The total number of tablets in the hit partition
        if (tableSample.isPercent()) {
            sampleRows = (long) Math.max(olapTable.getRowCount() * (tableSample.getSampleValue() / 100.0), 1);
        } else {
            sampleRows = Math.max(tableSample.getSampleValue(), 1);
        }

        // calculate the number of tablets by each partition
        long avgRowsPerPartition = sampleRows / Math.max(olapTable.getPartitions().size(), 1);

        for (Partition p : olapTable.getPartitions()) {
            List<Long> ids = p.getBaseIndex().getTabletIdsInOrder();

            if (ids.isEmpty()) {
                continue;
            }

            // Skip partitions with row count < row count / 2 expected to be sampled per partition.
            // It can be expected to sample a smaller number of partitions to avoid uneven distribution
            // of sampling results.
            if (p.getBaseIndex().getRowCount() < (avgRowsPerPartition / 2)) {
                continue;
            }

            long avgRowsPerTablet = Math.max(p.getBaseIndex().getRowCount() / ids.size(), 1);
            long tabletCounts = Math.max(
                    avgRowsPerPartition / avgRowsPerTablet + (avgRowsPerPartition % avgRowsPerTablet != 0 ? 1 : 0), 1);
            tabletCounts = Math.min(tabletCounts, ids.size());

            long seek = tableSample.getSeek() != -1
                    ? tableSample.getSeek() : (long) (Math.random() * ids.size());
            for (int i = 0; i < tabletCounts; i++) {
                int seekTid = (int) ((i + seek) % ids.size());
                sampleTabletIds.add(ids.get(seekTid));
            }

            hitRows += avgRowsPerTablet * tabletCounts;
            totalRows += p.getBaseIndex().getRowCount();
            totalTablet += ids.size();
        }

        // all hit, direct full
        if (totalRows < sampleRows) {
            // can't fill full sample rows
            sampleTabletIds.clear();
        } else if (sampleTabletIds.size() == totalTablet) {
            // TODO add limit
            sampleTabletIds.clear();
        } else if (!sampleTabletIds.isEmpty()) {
            // TODO add limit
        }
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
            final Collection<Long> tabletIds = distributionPrune(selectedTable, partition.getDistributionInfo());
            LOG.debug("distribution prune tablets: {}", tabletIds);
            if (sampleTabletIds.size() != 0) {
                tabletIds.retainAll(sampleTabletIds);
                LOG.debug("after sample tablets: {}", tabletIds);
            }

            List<Long> allTabletIds = selectedTable.getTabletIdsInOrder();
            if (tabletIds != null) {
                for (Long id : tabletIds) {
                    tablets.add(selectedTable.getTablet(id));
                }
                scanTabletIds.addAll(tabletIds);
            } else {
                tablets.addAll(selectedTable.getTablets());
                scanTabletIds.addAll(allTabletIds);
            }

            for (int i = 0; i < allTabletIds.size(); i++) {
                tabletId2BucketSeq.put(allTabletIds.get(i), i);
            }

            totalTabletsNum += selectedTable.getTablets().size();
            selectedTabletsNum += tablets.size();
            addScanRangeLocations(partition, tablets);
        }
    }

    /**
     * Check Parent sort node can push down to child olap scan.
     */
    public boolean checkPushSort(SortNode sortNode) {
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
        List<Expr> sortExprs = sortNode.getSortInfo().getMaterializedOrderingExprs();
        if (sortExprs.size() > olapTable.getDataSortInfo().getColNum()) {
            return false;
        }
        for (int i = 0; i < sortExprs.size(); i++) {
            // table key.
            Column tableKey = olapTable.getFullSchema().get(i);
            // sort slot.
            Expr sortExpr = sortExprs.get(i);
            if (!(sortExpr instanceof SlotRef) || !tableKey.equals(((SlotRef) sortExpr).getColumn())) {
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
        return result;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        String indexName = olapTable.getIndexNameById(selectedIndexId);
        output.append(prefix).append("TABLE: ").append(olapTable.getQualifiedName())
                .append("(").append(indexName).append(")");
        if (detailLevel == TExplainLevel.BRIEF) {
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
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            output.append(prefix).append("runtime filters: ");
            output.append(getRuntimeFilterExplainString(false));
        }

        output.append(prefix).append(String.format("partitions=%s/%s, tablets=%s/%s", selectedPartitionNum,
                olapTable.getPartitions().size(), selectedTabletsNum, totalTabletsNum));
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
        List<TColumn> columnsDesc = new ArrayList<TColumn>();

        if (selectedIndexId != -1) {
            for (Column col : olapTable.getSchemaByIndexId(selectedIndexId, true)) {
                TColumn tColumn = col.toThrift();
                col.setIndexFlag(tColumn, olapTable);
                columnsDesc.add(tColumn);
                if ((Util.showHiddenColumns() || (!Util.showHiddenColumns() && col.isVisible())) && col.isKey()) {
                    keyColumnNames.add(col.getName());
                    keyColumnTypes.add(col.getDataType().toThrift());
                }
            }
        }

        msg.node_type = TPlanNodeType.OLAP_SCAN_NODE;
        msg.olap_scan_node = new TOlapScanNode(desc.getId().asInt(), keyColumnNames, keyColumnTypes, isPreAggregation);
        msg.olap_scan_node.setColumnsDesc(columnsDesc);
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
        msg.olap_scan_node.setTableName(olapTable.getName());
        msg.olap_scan_node.setEnableUniqueKeyMergeOnWrite(olapTable.getEnableUniqueKeyMergeOnWrite());

        if (pushDownAggNoGroupingOp != null) {
            msg.olap_scan_node.setPushDownAggTypeOpt(pushDownAggNoGroupingOp);
        }
    }

    // export some tablets
    public static OlapScanNode createOlapScanNodeByLocation(
            PlanNodeId id, TupleDescriptor desc, String planNodeName, List<TScanRangeLocations> locationsList) {
        OlapScanNode olapScanNode = new OlapScanNode(id, desc, planNodeName);
        olapScanNode.numInstances = 1;

        olapScanNode.selectedIndexId = olapScanNode.olapTable.getBaseIndexId();
        olapScanNode.selectedPartitionNum = 1;
        olapScanNode.selectedTabletsNum = 1;
        olapScanNode.totalTabletsNum = 1;
        olapScanNode.setIsPreAggregation(false, "Export job");
        olapScanNode.result.addAll(locationsList);

        return olapScanNode;
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
        if (!Util.showHiddenColumns() && olapTable.hasDeleteSign()) {
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
}
